use futures_lite::future;
use smol::{
    channel::{self, Receiver, Sender, TrySendError},
    Executor,
    Timer,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};

const UPDATE_PREFIXES: [&[u8]; 11] = [
    b"@UPW ",
    b"@UPL ",
    b"@UVL ",
    b"@UDT ",
    b"@UAT ",
    b"@UST ",
    b"@UIS ",
    b"@U3D ",
    b"@UAR ",
    b"@UTC ",
    b"@UVO ",
];

const REQUEST_CHANNEL_CAP: usize = 32;
const CLIENT_OUT_CAP: usize = 256;
const BACKEND_EVENT_CAP: usize = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

struct Backoff {
    current: Duration,
    next_attempt_at: Instant,
}

impl Backoff {
    const STEP: Duration = Duration::from_millis(500);
    const MAX: Duration = Duration::from_secs(15);

    fn new() -> Self {
        Self {
            current: Duration::ZERO,
            next_attempt_at: Instant::now(),
        }
    }

    fn is_ready(&self) -> bool {
        Instant::now() >= self.next_attempt_at
    }

    fn delay_until_ready(&self) -> Duration {
        self.next_attempt_at.saturating_duration_since(Instant::now())
    }

    fn on_success(&mut self) {
        self.current = Duration::ZERO;
        self.next_attempt_at = Instant::now();
    }

    fn on_failure(&mut self) {
        self.current = (self.current + Self::STEP).min(Self::MAX);
        self.next_attempt_at = Instant::now() + self.current;
    }
}

/// Per-client state held in the broadcast map. The `TcpStream` clone is kept so
/// that a stuck/dead client can be force-disconnected from the broadcast path,
/// which makes both the writer and the reader half of `handle_client` error out
/// and clean up — instead of leaving the client task running on a broken socket.
type ClientEntry = (Sender<Arc<[u8]>>, TcpStream);
type Clients = Arc<Mutex<HashMap<u64, ClientEntry>>>;

fn lock_clients(clients: &Clients) -> MutexGuard<'_, HashMap<u64, ClientEntry>> {
    clients.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct BackendRequest {
    peer: Arc<str>,
    msg: Vec<u8>,
    response_tx: Sender<Result<Vec<u8>, String>>,
}

enum BackendEvent {
    Line(Vec<u8>),
    Error(String),
}

struct BackendConn {
    writer: TcpStream,
    events: Receiver<BackendEvent>,
    _reader_task: smol::Task<()>,
}

enum BrokerEvent {
    Request(BackendRequest),
    Backend(BackendEvent),
    Timeout,
    ReconnectTick,
    RequestsClosed,
}

#[cfg(target_os = "linux")]
fn init_logging() {
    use tracing_subscriber::{layer::SubscriberExt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let journald = tracing_journald::layer().expect("failed to connect to journald socket");
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(journald);
    tracing::subscriber::set_global_default(subscriber)
        .expect("failed to set global tracing subscriber");
}

#[cfg(not(target_os = "linux"))]
fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn main() {
    init_logging();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <listen_port> <backend_host:backend_port> <timeout_seconds>", args[0]);
        std::process::exit(1);
    }
    let listen_port = args[1].clone();
    let backend_addr: Arc<str> = args[2].as_str().into();
    let timeout = Duration::from_secs(args[3].parse::<u64>().unwrap());

    let ex = Arc::new(Executor::new());
    let spawner = Arc::clone(&ex);

    smol::block_on(ex.run(async move {
        let clients: Clients = Arc::new(Mutex::new(HashMap::new()));
        let (request_tx, request_rx) = channel::bounded::<BackendRequest>(REQUEST_CHANNEL_CAP);

        let broker_clients = Arc::clone(&clients);
        let broker_backend_addr = Arc::clone(&backend_addr);
        let broker_spawner = Arc::clone(&spawner);
        spawner
            .spawn(backend_broker(
                request_rx,
                broker_clients,
                broker_backend_addr,
                timeout,
                broker_spawner,
            ))
            .detach();

        let listener = TcpListener::bind(format!("0.0.0.0:{listen_port}"))
            .await
            .unwrap_or_else(|e| panic!("Failed to bind to 0.0.0.0:{listen_port}: {e}"));
        info!("listening on 0.0.0.0:{listen_port}, backend {backend_addr}");

        let mut next_client_id = 1_u64;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("client {addr} connected");
                    let client_id = next_client_id;
                    next_client_id = next_client_id.wrapping_add(1);
                    let requests = request_tx.clone();
                    let clients = Arc::clone(&clients);
                    let task_spawner = Arc::clone(&spawner);
                    let writer_spawner = Arc::clone(&spawner);
                    task_spawner
                        .spawn(handle_client(stream, client_id, requests, clients, writer_spawner))
                        .detach();
                }
                Err(e) => error!("accept error: {e}"),
            }
        }
    }));
}

// ---------- helpers ----------

async fn try_connect(
    addr: &str,
    spawner: &Arc<Executor<'static>>,
    backoff: &mut Backoff,
) -> Option<BackendConn> {
    let result = future::or(
        async { TcpStream::connect(addr).await.map_err(|e| e.to_string()) },
        async {
            Timer::after(CONNECT_TIMEOUT).await;
            Err(format!("connect timed out after {}s", CONNECT_TIMEOUT.as_secs()))
        },
    )
    .await;

    match result {
        Ok(stream) => {
            // Disable Nagle's algorithm — we send small commands and need low latency.
            let _ = stream.set_nodelay(true);
            info!("connected to backend at {addr}");
            let writer = stream.clone();
            let (events_tx, events_rx) = channel::bounded::<BackendEvent>(BACKEND_EVENT_CAP);
            let reader_task = spawner.spawn(backend_reader(stream, events_tx));
            backoff.on_success();
            Some(BackendConn {
                writer,
                events: events_rx,
                _reader_task: reader_task,
            })
        }
        Err(e) => {
            warn!("could not connect to backend at {addr}: {e}");
            backoff.on_failure();
            None
        }
    }
}

async fn backend_reader(stream: TcpStream, tx: Sender<BackendEvent>) {
    let mut reader = BufReader::with_capacity(256, stream);
    let mut buf = Vec::with_capacity(256);
    loop {
        buf.clear();
        match reader.read_until(b'\r', &mut buf).await {
            Ok(0) => {
                let _ = tx
                    .send(BackendEvent::Error("backend closed connection".to_string()))
                    .await;
                return;
            }
            Ok(_) if buf.last() != Some(&b'\r') => {
                // EOF was hit after partial bytes were read but before the terminator.
                // Forwarding a truncated line would corrupt either a broadcast or an
                // in-flight response, so treat this as a fatal read error and let the
                // broker surface it / drop the connection.
                let _ = tx
                    .send(BackendEvent::Error("backend closed mid-line".to_string()))
                    .await;
                return;
            }
            Ok(_) => {
                // Hand the filled buffer off and pre-allocate a fresh one in its
                // place. capacity().max(256) preserves the high-water mark so a
                // previously-grown buffer doesn't shrink back to the default.
                let next_capacity = buf.capacity().max(256);
                let line = std::mem::replace(&mut buf, Vec::with_capacity(next_capacity));

                // Updates are fire-and-forget telemetry (a fresh @UTC arrives every
                // second). If the broker is briefly stalled — e.g. inside a 3s
                // try_connect — drop the update on Full instead of blocking here.
                // Blocking would stop draining the TCP socket and could backpressure
                // the player into stalling its own send queue.
                //
                // Responses and protocol errors are not fire-and-forget: we keep the
                // awaiting send so they cannot be silently lost. At most one response
                // can be in flight at a time, so this path rarely fills the channel.
                if is_backend_update(&line) {
                    match tx.try_send(BackendEvent::Line(line)) {
                        Ok(()) | Err(TrySendError::Full(_)) => {}
                        Err(TrySendError::Closed(_)) => return,
                    }
                } else if tx.send(BackendEvent::Line(line)).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                let _ = tx
                    .send(BackendEvent::Error(format!("backend read error: {e}")))
                    .await;
                return;
            }
        }
    }
}

async fn recv_backend_event(events: &Receiver<BackendEvent>) -> BrokerEvent {
    match events.recv().await {
        Ok(ev) => BrokerEvent::Backend(ev),
        Err(_) => BrokerEvent::Backend(BackendEvent::Error(
            "backend reader ended".to_string(),
        )),
    }
}

async fn recv_request(requests: &Receiver<BackendRequest>) -> BrokerEvent {
    match requests.recv().await {
        Ok(req) => BrokerEvent::Request(req),
        Err(_) => BrokerEvent::RequestsClosed,
    }
}

async fn backend_broker(
    request_rx: Receiver<BackendRequest>,
    clients: Clients,
    backend_addr: Arc<str>,
    timeout: Duration,
    spawner: Arc<Executor<'static>>,
) {
    let mut backoff = Backoff::new();
    let mut backend: Option<BackendConn> = try_connect(&backend_addr, &spawner, &mut backoff).await;
    let mut in_flight: Option<(BackendRequest, Instant)> = None;

    loop {
        let event = match (backend.as_ref(), in_flight.as_ref()) {
            (Some(conn), Some((_, deadline))) => {
                if Instant::now() >= *deadline {
                    // Drain pending events before declaring a timeout:
                    //   - broadcast any update lines so they aren't lost,
                    //   - if a response is queued, use it — handles
                    //     the race where the response landed just before the deadline,
                    //   - otherwise emit Timeout. Bounded by BACKEND_EVENT_CAP, so this
                    //     cannot spin past the deadline indefinitely.
                    let mut result = BrokerEvent::Timeout;
                    loop {
                        match conn.events.try_recv() {
                            Ok(BackendEvent::Line(line)) => {
                                if is_backend_update(&line) {
                                    broadcast_update(&clients, line);
                                    continue;
                                }
                                result = BrokerEvent::Backend(BackendEvent::Line(line));
                                break;
                            }
                            Ok(BackendEvent::Error(reason)) => {
                                result = BrokerEvent::Backend(BackendEvent::Error(reason));
                                break;
                            }
                            Err(channel::TryRecvError::Empty) => break,
                            Err(channel::TryRecvError::Closed) => {
                                result = BrokerEvent::Backend(BackendEvent::Error(
                                    "backend reader ended".to_string(),
                                ));
                                break;
                            }
                        }
                    }
                    result
                } else {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let timeout_fut = async move {
                        Timer::after(remaining).await;
                        BrokerEvent::Timeout
                    };
                    future::or(recv_backend_event(&conn.events), timeout_fut).await
                }
            }
            (Some(conn), None) => {
                // Bias requests over backend events: requests are latency-sensitive
                // (someone pressed a button), updates are passive state changes.
                future::or(recv_request(&request_rx), recv_backend_event(&conn.events)).await
            }
            (None, _) => {
                let delay = backoff.delay_until_ready();
                let reconnect_fut = async move {
                    Timer::after(delay).await;
                    BrokerEvent::ReconnectTick
                };
                future::or(recv_request(&request_rx), reconnect_fut).await
            }
        };

        match event {
            BrokerEvent::Request(req) => {
                handle_new_request(
                    req,
                    &mut backend,
                    &backend_addr,
                    &spawner,
                    &mut backoff,
                    timeout,
                    &mut in_flight,
                )
                .await;
            }
            BrokerEvent::Backend(BackendEvent::Line(line)) => {
                if is_backend_update(&line) {
                    broadcast_update(&clients, line);
                } else if let Some((req, _)) = in_flight.take() {
                    debug!(
                        "[backend → {}] {}",
                        req.peer,
                        String::from_utf8_lossy(&line).trim_end_matches('\r')
                    );
                    let _ = req.response_tx.send(Ok(line)).await;
                } else {
                    warn!(
                        "unsolicited non-update backend line: {}",
                        String::from_utf8_lossy(&line).trim_end_matches('\r')
                    );
                }
            }
            BrokerEvent::Backend(BackendEvent::Error(reason)) => {
                warn!("{reason}");
                backend = None;
                backoff.on_failure();
                if let Some((req, _)) = in_flight.take() {
                    let _ = req.response_tx.send(Err(reason)).await;
                }
            }
            BrokerEvent::Timeout => {
                if let Some((req, _)) = in_flight.take() {
                    let reason = format!("backend response timed out ({} s)", timeout.as_secs());
                    let _ = req.response_tx.send(Err(reason)).await;
                    // Drop backend: protocol state may be desynced after a missed response.
                    backend = None;
                    backoff.on_failure();
                }
            }
            BrokerEvent::ReconnectTick => {
                if backend.is_none() {
                    backend = try_connect(&backend_addr, &spawner, &mut backoff).await;
                }
            }
            BrokerEvent::RequestsClosed => break,
        }
    }
}

/// Handles one request against the backend.
///
/// Failure / retry policy (intentional; do not change without re-reading this):
///
/// 1. If we have a live `backend` connection, attempt the write. On success the
///    request becomes the in-flight request and we wait for the response on
///    the broker's main loop.
///
/// 2. If that write fails, we have just lost a previously-working connection.
///    We bump backoff (so future requests get throttled) AND set
///    `retry_after_write_fail = true` so this single request bypasses the
///    `is_ready()` gate below for one reconnect-and-retry attempt. This is
///    the "transient network blip" recovery path: when the player power-
///    cycles or a Wi-Fi packet is lost, the user's first command after the
///    blip should still succeed instead of surfacing an error.
///
///    The bypass is **bounded**: it can only fire when `backend.is_some()`,
///    which is true at most once per healthy→broken transition. After
///    `*backend = None`, every subsequent request takes the no-outer-write
///    path and is gated by `is_ready()`. So a misbehaving backend (accepts
///    connections then immediately rejects writes) produces at most one
///    connect+write attempt per backoff window — i.e. throttled, not a storm.
///
/// 3. If there is no backend connection at all (first request after broker
///    start, or after a previous failure), we respect `is_ready()`. During
///    the backoff cooldown we fast-fail with `"backend unavailable"` instead
///    of hammering the player with connect attempts.
///
/// 4. After `try_connect`, the inner write may also fail. Bump backoff again
///    so the next request is gated; surface the error to the client.
async fn handle_new_request(
    req: BackendRequest,
    backend: &mut Option<BackendConn>,
    backend_addr: &str,
    spawner: &Arc<Executor<'static>>,
    backoff: &mut Backoff,
    timeout: Duration,
    in_flight: &mut Option<(BackendRequest, Instant)>,
) {
    // See the function doc for why this exists. TL;DR: write-error retry is
    // bounded to one attempt per healthy→broken transition; later requests
    // are gated by `is_ready()` because `backend` is None and the outer
    // branch is skipped.
    let mut retry_after_write_fail = false;

    if let Some(conn) = backend.as_mut() {
        match conn.writer.write_all(&req.msg).await {
            Ok(()) => {
                debug!(
                    "[{} → backend] {}",
                    req.peer,
                    String::from_utf8_lossy(&req.msg).trim_end_matches('\r')
                );
                *in_flight = Some((req, Instant::now() + timeout));
                return;
            }
            Err(e) => {
                warn!("backend write error ({e}) while handling {}, reconnecting", req.peer);
                *backend = None;
                backoff.on_failure();
                retry_after_write_fail = true;
            }
        }
    }

    // Path A (`!retry_after_write_fail`): no backend at entry; obey the cooldown.
    // Path B (`retry_after_write_fail`): we just bumped backoff after losing a
    // live connection — bypass the gate this one time for the retry.
    if !retry_after_write_fail && !backoff.is_ready() {
        let _ = req.response_tx.send(Err("backend unavailable".to_string())).await;
        return;
    }

    *backend = try_connect(backend_addr, spawner, backoff).await;
    let Some(conn) = backend.as_mut() else {
        let _ = req.response_tx.send(Err("backend unavailable".to_string())).await;
        return;
    };

    match conn.writer.write_all(&req.msg).await {
        Ok(()) => {
            debug!(
                "[{} → backend] {}",
                req.peer,
                String::from_utf8_lossy(&req.msg).trim_end_matches('\r')
            );
            *in_flight = Some((req, Instant::now() + timeout));
        }
        Err(e) => {
            *backend = None;
            backoff.on_failure();
            let _ = req
                .response_tx
                .send(Err(format!("backend write error: {e}")))
                .await;
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    client_id: u64,
    request_tx: Sender<BackendRequest>,
    clients: Clients,
    spawner: Arc<Executor<'static>>,
) {
    let peer: Arc<str> = stream
        .peer_addr()
        .ok()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "?".to_string())
        .into();
    let _ = stream.set_nodelay(true);

    // Three TcpStream clones share the same underlying socket: one for the
    // writer task, one stored in the clients map so broadcast can force a
    // shutdown if the client falls fatally behind, one consumed by BufReader.
    let writer_stream = stream.clone();
    let map_stream = stream.clone();
    let mut client = BufReader::with_capacity(256, stream);

    let (out_tx, out_rx) = channel::bounded::<Arc<[u8]>>(CLIENT_OUT_CAP);
    {
        let mut guard = lock_clients(&clients);
        guard.insert(client_id, (out_tx.clone(), map_stream));
    }

    let writer_peer = Arc::clone(&peer);
    spawner
        .spawn(async move {
            let mut writer = writer_stream;
            while let Ok(payload) = out_rx.recv().await {
                if writer.write_all(&payload).await.is_err() {
                    break;
                }
            }
            debug!("writer for client {writer_peer} closed");
        })
        .detach();

    let mut msg = Vec::with_capacity(256);

    loop {
        msg.clear();

        match client.read_until(b'\r', &mut msg).await {
            Ok(0) | Err(_) => break,
            // EOF mid-line: forwarding a truncated command to the player could
            // make it execute something partial. Treat as a disconnect.
            Ok(_) if msg.last() != Some(&b'\r') => break,
            Ok(_) => {}
        }

        let (response_tx, response_rx) = channel::bounded(1);
        // Same pattern as backend_reader: hand the buffer off and pre-allocate a
        // fresh one so the next read_until has no growth churn.
        let next_capacity = msg.capacity().max(256);
        let req = BackendRequest {
            peer: Arc::clone(&peer),
            msg: std::mem::replace(&mut msg, Vec::with_capacity(next_capacity)),
            response_tx,
        };

        if request_tx.send(req).await.is_err() {
            let _ = out_tx
                .send(Arc::from(b"ERROR: backend worker unavailable\r".as_slice()))
                .await;
            break;
        }

        let result = response_rx
            .recv()
            .await
            .unwrap_or_else(|_| Err("backend worker unavailable".to_string()));

        match result {
            Ok(response) => {
                if out_tx.send(Arc::from(response)).await.is_err() {
                    break;
                }
            }
            Err(reason) => {
                let err = format!("ERROR: {reason}\r");
                if out_tx.send(Arc::from(err.into_bytes())).await.is_err() {
                    break;
                }
            }
        }
    }

    {
        let mut guard = lock_clients(&clients);
        guard.remove(&client_id);
    }
    drop(out_tx);

    info!("client {peer} disconnected");
}

fn is_backend_update(line: &[u8]) -> bool {
    UPDATE_PREFIXES.iter().any(|prefix| line.starts_with(prefix))
}

fn broadcast_update<T: Into<Arc<[u8]>>>(clients: &Clients, line: T) {
    // Accept owned data so callers can hand off a Vec<u8> for a zero-copy
    // conversion into Arc<[u8]>. Borrowed slices still work via Arc::from(&[u8]).
    let shared: Arc<[u8]> = line.into();

    // Keep the locked section as short as possible — the executor thread is
    // blocked while we hold this std::sync::Mutex. Inside the lock we only do
    // non-blocking work (try_send + bookkeeping).
    let mut delivered: usize = 0;
    let mut stale: Vec<(u64, TcpStream)> = Vec::new();
    {
        let mut guard = lock_clients(clients);
        guard.retain(|id, (tx, stream)| match tx.try_send(Arc::clone(&shared)) {
            Ok(()) => {
                delivered += 1;
                true
            }
            Err(TrySendError::Closed(_)) => false,
            Err(TrySendError::Full(_)) => {
                stale.push((*id, stream.clone()));
                false
            }
        });
    }

    if delivered > 0 {
        debug!(
            "broadcast '{}' to {delivered} client(s)",
            String::from_utf8_lossy(&shared).trim_end_matches('\r'),
        );
    }

    // For each stale client: force-disconnect so its handle_client task tears
    // down promptly. Without this, removing the entry would only stop broadcasts
    // and the broken task would linger until kernel TCP keepalive
    // eventually reset the connection. At ~1 Hz sustained events, a
    // full 256-deep buffer means the client hasn't drained for ~4 minutes.
    for (id, stream) in stale {
        warn!("client {id} send buffer full, force-disconnecting");
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
}
