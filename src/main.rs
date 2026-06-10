use futures_lite::future;
use smol::{
    channel::{self, Receiver, Sender, TrySendError},
    Executor,
    Timer,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock, Mutex, MutexGuard,
    },
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
const BACKEND_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_MAX_CONSECUTIVE_TIMEOUTS: u32 = 3;
/// Hard cap on concurrent accepted clients. Prevents a connection flood from
/// spawning unbounded tasks (each holds channels + TcpStream clones). Existing
/// clients are not affected; new ones over the cap are refused at accept time.
const MAX_CLIENTS: usize = 11;
/// Hard cap on a single `\r`-terminated line from either a client or the
/// backend. Prevents a peer that never sends `\r` from growing the read
/// buffer without bound (OOM/DoS). Oppo protocol lines are <100 bytes in
/// practice.
const MAX_LINE_LEN: usize = 4096;

/// Pre-built shared payloads for synthesized `@UPW` broadcasts. Avoids a heap
/// allocation per power-state transition that would otherwise happen inside
/// `Arc::from(&'static [u8])`.
static SYNTHETIC_UPW_OFF: LazyLock<Arc<[u8]>> =
    LazyLock::new(|| Arc::from(b"@UPW 0\r".as_slice()));
static SYNTHETIC_UPW_ON: LazyLock<Arc<[u8]>> =
    LazyLock::new(|| Arc::from(b"@UPW 1\r".as_slice()));

/// Linear backoff for backend reconnect attempts. Tracks both the current
/// wait duration and the absolute time of the next allowed attempt. Both the
/// scheduled (ReconnectTick) path and the on-demand (handle_new_request) path
/// consult this single source of truth so they cannot race past each other.
///
/// Sequence on repeated failures: 0.5s, 1.0s, 1.5s, ... capped at 15s.
/// Resets to zero on any successful connect.
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

    /// True if a connect attempt is allowed right now.
    fn is_ready(&self) -> bool {
        Instant::now() >= self.next_attempt_at
    }

    /// Duration until the next attempt is allowed; zero if already allowed.
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

/// RAII counter slot for `MAX_CLIENTS`. Increment happens at accept time in
/// `main`; the guard is moved into `handle_client`, and dropping it (clean
/// exit, break-out, panic, or task cancellation) decrements the counter.
struct ClientSlotGuard(Arc<AtomicUsize>);

impl Drop for ClientSlotGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Per-client state held in the broadcast map. The `TcpStream` clone is kept so
/// that a stuck/dead client can be force-disconnected from the broadcast path,
/// which makes both the writer and the reader half of `handle_client` error out
/// and clean up — instead of leaving the client task running on a broken socket.
type ClientEntry = (Sender<Arc<[u8]>>, TcpStream);
type Clients = Arc<Mutex<HashMap<u64, ClientEntry>>>;

/// Locks the clients map, recovering from poison so a panicking client task
/// cannot bring down the whole server.
fn lock_clients(clients: &Clients) -> MutexGuard<'_, HashMap<u64, ClientEntry>> {
    clients.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A client command waiting for its backend response. `response_tx` is a
/// per-request one-shot (bounded(1)) populated by the broker.
struct BackendRequest {
    peer: Arc<str>,
    msg: Vec<u8>,
    response_tx: Sender<Result<Vec<u8>, String>>,
}

/// An event read from the backend by the dedicated reader task.
enum BackendEvent {
    /// A `\r`-terminated protocol line — either an unsolicited update or the
    /// response to the current in-flight request.
    Line(Vec<u8>),
    /// The backend connection is no longer usable (EOF, read error, or
    /// truncated mid-line). The broker drops the connection on receipt.
    Error(String),
}

/// A live backend connection plus the channel its reader task feeds into.
/// Dropping this value drops the held `Task`, which cancels the reader at its
/// next await point and closes the socket.
struct BackendConn {
    writer: TcpStream,
    events: Receiver<BackendEvent>,
    /// Owned to bind the reader task's lifetime to this connection.
    /// `smol::Task::drop` cancels the task.
    _reader_task: smol::Task<()>,
}

/// Things the broker's main `select` can produce on a single tick.
enum BrokerEvent {
    Request(BackendRequest),
    Backend(BackendEvent),
    /// The in-flight request's deadline has passed.
    Timeout,
    /// The backoff window has elapsed; try to reconnect.
    ReconnectTick,
}

#[cfg(target_os = "linux")]
/// Sets up tracing via journald, falling back to the default `fmt` subscriber
/// (stderr) if the journald socket is unavailable (e.g. running outside
/// systemd). Level is controlled by `RUST_LOG`, defaulting to `info`.
fn init_logging() {
    use tracing_subscriber::{layer::SubscriberExt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    match tracing_journald::layer() {
        Ok(journald) => {
            let subscriber = tracing_subscriber::registry()
                .with(filter)
                .with(journald);
            tracing::subscriber::set_global_default(subscriber)
                .expect("failed to set global tracing subscriber");
        }
        Err(e) => {
            eprintln!("journald unavailable ({e}), falling back to stderr");
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }
}

#[cfg(not(target_os = "linux"))]
/// Sets up tracing via stderr.
/// Level is controlled by `RUST_LOG`, defaulting to `info`.
fn init_logging() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn main() {
    init_logging();

    let args: Vec<String> = std::env::args().collect();
    if !matches!(args.len(), 4 | 5) {
        eprintln!(
            "Usage: {} <listen_port> <backend_host:backend_port> <timeout_seconds> [max_consecutive_timeouts]",
            args[0]
        );
        std::process::exit(1);
    }
    let listen_port = args[1].parse::<u16>().unwrap_or_else(|_| {
        eprintln!("Invalid listen_port: '{}' is not a valid TCP port (1-65535)", args[1]);
        std::process::exit(1);
    });
    if listen_port == 0 {
        // Port 0 would have the OS pick an ephemeral port.
        eprintln!("Invalid listen_port: 0 is not allowed (use 1-65535)");
        std::process::exit(1);
    }
    let backend_addr: Arc<str> = args[2].as_str().into();
    let timeout_secs = args[3].parse::<u64>().unwrap_or_else(|_| {
        eprintln!("Invalid timeout_seconds: '{}' is not a non-negative integer", args[3]);
        std::process::exit(1);
    });
    if timeout_secs == 0 {
        eprintln!("Invalid timeout_seconds: must be > 0");
        std::process::exit(1);
    }
    let timeout = Duration::from_secs(timeout_secs);
    let max_consecutive_timeouts = if args.len() == 5 {
        parse_max_consecutive_timeouts(&args[4]).unwrap_or_else(|| {
            eprintln!(
                "Invalid max_consecutive_timeouts: '{}' must be an integer in the range 1-100",
                args[4]
            );
            std::process::exit(1);
        })
    } else {
        DEFAULT_MAX_CONSECUTIVE_TIMEOUTS
    };

    let ex = Arc::new(Executor::new());
    let spawner = Arc::clone(&ex);

    smol::block_on(ex.run(async move {
        let clients: Clients = Arc::new(Mutex::new(HashMap::new()));
        let active_clients = Arc::new(AtomicUsize::new(0));
        let (request_tx, request_rx) = channel::bounded::<BackendRequest>(REQUEST_CHANNEL_CAP);

        spawner
            .spawn(backend_broker(
                request_rx,
                Arc::clone(&clients),
                Arc::clone(&backend_addr),
                timeout,
                max_consecutive_timeouts,
                Arc::clone(&spawner),
            ))
            .detach();

        let listener = TcpListener::bind(format!("0.0.0.0:{listen_port}"))
            .await
            .expect("failed to bind listen port");
        info!("listening on 0.0.0.0:{listen_port}, backend {backend_addr}");

        let mut next_client_id = 1_u64;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    // Single-task accept loop, so this load+add cannot race with
                    // another accept. Decrements happen on `ClientSlotGuard::drop`
                    // inside the spawned task.
                    if active_clients.load(Ordering::Acquire) >= MAX_CLIENTS {
                        warn!(
                            "rejecting client {addr}: {MAX_CLIENTS} concurrent clients already active"
                        );
                        // Close immediately rather than writing a courtesy "ERROR:"
                        // line. An async write here would block the single accept
                        // loop if the peer never reads (or advertises a zero
                        // window), turning the cap itself into the availability
                        // problem under a connection flood.
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        drop(stream);
                        continue;
                    }
                    active_clients.fetch_add(1, Ordering::AcqRel);
                    let slot = ClientSlotGuard(Arc::clone(&active_clients));
                    info!("client {addr} connected");
                    let client_id = next_client_id;
                    next_client_id = next_client_id.wrapping_add(1);
                    spawner
                        .spawn(handle_client(
                            stream,
                            client_id,
                            request_tx.clone(),
                            Arc::clone(&clients),
                            Arc::clone(&spawner),
                            slot,
                        ))
                        .detach();
                }
                Err(e) => error!("accept error: {e}"),
            }
        }
    }));
}

// ---------- helpers ----------

/// Bounded variant of `AsyncBufReadExt::read_until`: appends bytes into `buf`
/// until `delim` is found or EOF, but fails with `InvalidData` if the line
/// would exceed `max` bytes. Prevents a peer that never sends `delim` from
/// growing `buf` without bound.
async fn read_until_capped<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    delim: u8,
    buf: &mut Vec<u8>,
    max: usize,
) -> std::io::Result<usize> {
    let start = buf.len();
    loop {
        let (consumed, found) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return Ok(buf.len() - start);
            }
            let (n, found) = match available.iter().position(|&b| b == delim) {
                Some(i) => (i + 1, true),
                None => (available.len(), false),
            };
            if buf.len() - start + n > max {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "line exceeded max length",
                ));
            }
            buf.extend_from_slice(&available[..n]);
            (n, found)
        };
        std::pin::Pin::new(&mut *reader).consume(consumed);
        if found {
            return Ok(buf.len() - start);
        }
    }
}

/// Writes `data` to `stream`, failing with `TimedOut` if the kernel does not
/// accept it within `BACKEND_WRITE_TIMEOUT`. A stuck TCP send buffer would
/// otherwise block the broker indefinitely; on timeout the caller drops the
/// connection (since a partial write may have already landed).
async fn write_with_timeout(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    future::or(
        async { stream.write_all(data).await },
        async {
            Timer::after(BACKEND_WRITE_TIMEOUT).await;
            Err::<(), _>(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "backend write timed out",
            ))
        },
    )
    .await
}

/// Attempt one TCP connect to the backend, capped by `CONNECT_TIMEOUT` so a
/// silent-drop player cannot hang the broker for the OS-default minutes.
/// Records the outcome on `backoff`. On success, spawns the reader task and
/// returns a live `BackendConn`.
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
            if let Err(e) = stream.set_nodelay(true) {
                warn!("set_nodelay on backend connection failed: {e}");
            }
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

/// Reads `\r`-terminated lines from the backend socket forever and pushes them
/// to the broker through `tx`. Runs as its own task so the broker's `select`
/// loop can never interrupt a partially-read line.
///
/// Update lines are sent non-blocking (`try_send`): under sustained event flow
/// they are fire-and-forget, and blocking here would propagate backpressure
/// into the player's TCP send window. Responses and protocol errors use the
/// awaiting `send` since they must not be silently dropped — there is only
/// ever one in-flight response at a time, so this path rarely fills.
///
/// Exits on EOF, read error, truncated mid-line, or once `tx` is closed.
async fn backend_reader(stream: TcpStream, tx: Sender<BackendEvent>) {
    let mut reader = BufReader::with_capacity(256, stream);
    let mut buf = Vec::with_capacity(256);
    loop {
        buf.clear();
        match read_until_capped(&mut reader, b'\r', &mut buf, MAX_LINE_LEN).await {
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
                // Hand the filled buffer off and pre-allocate a fresh one with
                // the same capacity so a previously-grown buffer keeps its
                // high-water mark instead of shrinking back to the default.
                let cap = buf.capacity();
                let line = std::mem::replace(&mut buf, Vec::with_capacity(cap));

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

/// Waits for the next event from the backend reader task. If the reader has
/// already exited (channel closed), returns a manufactured `BackendEvent::Error`
/// so the broker's existing "backend died" handling runs — no extra match arm
/// needed for the channel-closed case.
async fn recv_backend_event(events: &Receiver<BackendEvent>) -> BrokerEvent {
    match events.recv().await {
        Ok(ev) => BrokerEvent::Backend(ev),
        Err(_) => BrokerEvent::Backend(BackendEvent::Error(
            "backend reader ended".to_string(),
        )),
    }
}

/// Waits for the next client request. `main` holds the original `request_tx`
/// for the whole program lifetime, so `recv` cannot return `Err` here.
async fn recv_request(requests: &Receiver<BackendRequest>) -> BrokerEvent {
    let req = requests
        .recv()
        .await
        .expect("main holds the original request_tx");
    BrokerEvent::Request(req)
}

/// Single-owner state machine for the backend connection. Only one request is
/// in-flight at a time, matching the player's single-TCP-connection constraint.
///
/// Each loop iteration selects from one of three modes based on
/// (backend, in_flight) state:
/// - `(Some, Some)`: waiting for a response or timeout; queued updates broadcast
///   inline. A pre-deadline drain prevents a just-arrived response from being
///   discarded when the timer fires.
/// - `(Some, None)`: idle with a live connection; biased toward new requests so
///   commands are not delayed by a flood of unsolicited update events.
/// - `(None, _)`: no connection; either pull a new request (which may try to
///   reconnect inline) or fire a scheduled `ReconnectTick` when the backoff
///   window elapses.
async fn backend_broker(
    request_rx: Receiver<BackendRequest>,
    clients: Clients,
    backend_addr: Arc<str>,
    timeout: Duration,
    max_consecutive_timeouts: u32,
    spawner: Arc<Executor<'static>>,
) {
    let mut backoff = Backoff::new();
    let mut backend: Option<BackendConn> = try_connect(&backend_addr, &spawner, &mut backoff).await;
    let mut in_flight: Option<(BackendRequest, Instant)> = None;
    let mut last_power_state: Option<u8> = None;
    let mut consecutive_timeouts: u32 = 0;

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
                                    if let Some(state) = parse_upw_state(&line) {
                                        last_power_state = Some(state);
                                    }
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
                    if let Some(state) = parse_upw_state(&line) {
                        last_power_state = Some(state);
                    }
                    broadcast_update(&clients, line);
                } else if let Some((req, _)) = in_flight.take() {
                    // Any matched request/response exchange confirms backend liveness.
                    consecutive_timeouts = 0;
                    // Some power responses are acknowledged before the backend emits
                    // its own @UPW event; proactively fan out equivalent state now.
                    if let Some(state) = synthetic_power_state_from_exchange(&req.msg, &line) {
                        if last_power_state != Some(state) {
                            let update: Arc<[u8]> = match state {
                                0 => Arc::clone(&SYNTHETIC_UPW_OFF),
                                1 => Arc::clone(&SYNTHETIC_UPW_ON),
                                _ => unreachable!(),
                            };
                            broadcast_update(&clients, update);
                            last_power_state = Some(state);
                        }
                    }
                    debug!(
                        "[backend → {}] {}",
                        req.peer,
                        String::from_utf8_lossy(&line).trim_end_matches('\r')
                    );
                    let _ = req.response_tx.send(Ok(line)).await;
                } else {
                    warn!(
                        "orphan non-update backend line (no in-flight request): {}",
                        String::from_utf8_lossy(&line).trim_end_matches('\r')
                    );
                }
            }
            BrokerEvent::Backend(BackendEvent::Error(reason)) => {
                warn!("{reason}");
                backend = None;
                backoff.on_failure();
                consecutive_timeouts = 0;
                last_power_state = None;
                if let Some((req, _)) = in_flight.take() {
                    let _ = req.response_tx.send(Err(reason)).await;
                }
            }
            BrokerEvent::Timeout => {
                if let Some((req, _)) = in_flight.take() {
                    // Deployment invariant for this environment: if a request has
                    // not received a response within `timeout`, that response will
                    // never arrive later. If that invariant is violated, a late
                    // response would land while a *different* request is in-flight
                    // and be misdelivered as that request's response. The
                    // `max_consecutive_timeouts` threshold below doubles as the
                    // safety net for this: after enough timeouts in a row we drop
                    // the backend connection, which evicts any late bytes still
                    // queued in the kernel/reader pipeline so they cannot match a
                    // future request.
                    consecutive_timeouts = consecutive_timeouts.saturating_add(1);
                    let reason = format!("backend response timed out ({} s)", timeout.as_secs());
                    warn!(
                        "{reason} while waiting for {} from {} (timeout occurrences {}/{})",
                        String::from_utf8_lossy(&req.msg).trim_end_matches('\r'),
                        req.peer,
                        consecutive_timeouts,
                        max_consecutive_timeouts,
                    );
                    let _ = req.response_tx.send(Err(reason)).await;
                    if should_drop_backend_after_timeout(
                        consecutive_timeouts,
                        max_consecutive_timeouts,
                    ) {
                        // Drop backend after repeated timeouts: backend appears unhealthy
                        // because it is not producing responses in a timely manner.
                        backend = None;
                        backoff.on_failure();
                        consecutive_timeouts = 0;
                        last_power_state = None;
                    }
                }
            }
            BrokerEvent::ReconnectTick => {
                if backend.is_none() {
                    backend = try_connect(&backend_addr, &spawner, &mut backoff).await;
                    if backend.is_some() {
                        consecutive_timeouts = 0;
                    }
                }
            }
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
        match write_with_timeout(&mut conn.writer, &req.msg).await {
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

    match write_with_timeout(&mut conn.writer, &req.msg).await {
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

/// One task per accepted client. Reads `\r`-terminated commands from the
/// socket, forwards each to the broker, and writes the response back.
///
/// A dedicated writer task (spawned here) drains the per-client out-channel
/// in parallel. Both the writer and the broadcast path share the same channel,
/// so the client receives a serialized mix of its own command responses and
/// unsolicited backend updates (events).
async fn handle_client(
    stream: TcpStream,
    client_id: u64,
    request_tx: Sender<BackendRequest>,
    clients: Clients,
    spawner: Arc<Executor<'static>>,
    _slot: ClientSlotGuard,
) {
    let peer: Arc<str> = stream
        .peer_addr()
        .ok()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "?".to_string())
        .into();
    if let Err(e) = stream.set_nodelay(true) {
        warn!("set_nodelay on client {peer} failed: {e}");
    }

    // Three TcpStream clones share the same underlying socket: one for the
    // writer task, one stored in the clients map so broadcast can force a
    // shutdown if the client falls fatally behind, one consumed by BufReader.
    let mut writer_stream = stream.clone();
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
            while let Ok(payload) = out_rx.recv().await {
                if writer_stream.write_all(&payload).await.is_err() {
                    break;
                }
            }
            debug!("writer for client {writer_peer} closed");
        })
        .detach();

    let mut msg = Vec::with_capacity(256);

    loop {
        msg.clear();

        // Disconnect on read error or any line not terminated by `\r` (clean
        // EOF and EOF mid-line both leave the buffer without the terminator).
        // Forwarding a truncated command could make the player execute a
        // partial command. `read_until_capped` also enforces MAX_LINE_LEN, so
        // a client that never sends `\r` cannot grow `msg` without bound.
        if read_until_capped(&mut client, b'\r', &mut msg, MAX_LINE_LEN)
            .await
            .is_err()
            || msg.last() != Some(&b'\r')
        {
            break;
        }

        let (response_tx, response_rx) = channel::bounded(1);
        // Same pattern as backend_reader: hand the buffer off and pre-allocate a
        // fresh one with the same capacity so the next read has no growth churn.
        let cap = msg.capacity();
        let req = BackendRequest {
            peer: Arc::clone(&peer),
            msg: std::mem::replace(&mut msg, Vec::with_capacity(cap)),
            response_tx,
        };

        request_tx
            .send(req)
            .await
            .expect("broker holds request_rx for the program's lifetime");

        let result = response_rx
            .recv()
            .await
            .expect("broker fires response_tx on every code path");

        let payload: Arc<[u8]> = match result {
            Ok(response) => Arc::from(response),
            Err(reason) => Arc::from(format!("ERROR: {reason}\r").into_bytes()),
        };
        if out_tx.send(payload).await.is_err() {
            break;
        }
    }

    {
        let mut guard = lock_clients(&clients);
        guard.remove(&client_id);
    }

    info!("client {peer} disconnected");
}

/// True if `line` is one of the player's unsolicited status updates (any of
/// the `@U??` prefixes), as opposed to a response to an issued command.
fn is_backend_update(line: &[u8]) -> bool {
    UPDATE_PREFIXES.iter().any(|prefix| line.starts_with(prefix))
}

/// Fans out an update line to every registered client via non-blocking
/// `try_send`. Clients whose channel is closed are removed; clients whose
/// channel is full are force-disconnected via TCP shutdown so their tasks
/// tear down promptly instead of lingering on a broken socket.
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

fn parse_upw_state(line: &[u8]) -> Option<u8> {
    let body = line.strip_suffix(b"\r").unwrap_or(line);
    match body {
        b"@UPW 0" => Some(0),
        b"@UPW 1" => Some(1),
        _ => None,
    }
}

fn synthetic_power_state_from_exchange(request: &[u8], response: &[u8]) -> Option<u8> {
    let req = request.strip_suffix(b"\r").unwrap_or(request);
    let resp = response.strip_suffix(b"\r").unwrap_or(response);

    match (req, resp) {
        (b"#POF", b"@POF OK OFF") | (b"#QPW", b"@QPW OK OFF") => Some(0),
        (b"#PON", b"@PON OK ON") | (b"#QPW", b"@QPW OK ON") => Some(1),
        _ => None,
    }
}

/// After this many consecutive request timeouts, force backend reconnect.
/// Smaller thresholds fail over faster; larger thresholds tolerate more transient misses.
fn should_drop_backend_after_timeout(consecutive_timeouts: u32, max_consecutive_timeouts: u32) -> bool {
    consecutive_timeouts >= max_consecutive_timeouts
}

fn parse_max_consecutive_timeouts(raw: &str) -> Option<u32> {
    let parsed = raw.trim().parse::<u32>().ok()?;
    if !matches!(parsed, 1..=100) {
        return None;
    }
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STEP: Duration = Duration::from_millis(500);
    const MAX: Duration = Duration::from_secs(15);

    #[test]
    fn backoff_starts_ready() {
        let bo = Backoff::new();
        assert!(bo.is_ready());
        assert_eq!(bo.delay_until_ready(), Duration::ZERO);
    }

    #[test]
    fn backoff_grows_by_step_per_failure() {
        let mut bo = Backoff::new();
        for i in 1..=5 {
            bo.on_failure();
            let expected = STEP * i;
            let delay = bo.delay_until_ready();
            assert!(
                delay <= expected,
                "iter {i}: delay {delay:?} should be <= {expected:?}"
            );
            let lower = expected.saturating_sub(Duration::from_millis(50));
            assert!(
                delay >= lower,
                "iter {i}: delay {delay:?} should be ~{expected:?} (>= {lower:?})"
            );
        }
    }

    #[test]
    fn backoff_capped_at_max() {
        let mut bo = Backoff::new();
        for _ in 0..100 {
            bo.on_failure();
        }
        let delay = bo.delay_until_ready();
        assert!(delay <= MAX, "delay {delay:?} exceeded MAX {MAX:?}");
        let lower = MAX.saturating_sub(Duration::from_millis(50));
        assert!(delay >= lower, "delay {delay:?} not near MAX {MAX:?}");
    }

    #[test]
    fn backoff_resets_on_success() {
        let mut bo = Backoff::new();
        bo.on_failure();
        bo.on_failure();
        bo.on_failure();
        bo.on_success();
        assert!(bo.is_ready());
        assert_eq!(bo.delay_until_ready(), Duration::ZERO);
    }

    #[test]
    fn backoff_not_ready_immediately_after_failure() {
        let mut bo = Backoff::new();
        bo.on_failure();
        assert!(!bo.is_ready());
    }

    #[test]
    fn is_backend_update_matches_all_prefixes() {
        for prefix in UPDATE_PREFIXES {
            let mut line = prefix.to_vec();
            line.extend_from_slice(b"data\r");
            assert!(
                is_backend_update(&line),
                "{:?} should be an update",
                String::from_utf8_lossy(prefix),
            );
        }
    }

    #[test]
    fn is_backend_update_rejects_non_updates() {
        let cases: &[&[u8]] = &[
            b"@OK\r",
            b"@ERR INVALID\r",
            b"\r",
            b"",
            b"@U",
            b"@UPW",
            b"prefix @UTC mid\r",
        ];
        for line in cases {
            assert!(
                !is_backend_update(line),
                "{:?} should NOT be an update",
                String::from_utf8_lossy(line),
            );
        }
    }

    #[test]
    fn synthetic_power_update_maps_expected_ack_responses() {
        assert_eq!(
            synthetic_power_state_from_exchange(b"#POF\r", b"@POF OK OFF\r"),
            Some(0)
        );
        assert_eq!(
            synthetic_power_state_from_exchange(b"#QPW\r", b"@QPW OK OFF\r"),
            Some(0)
        );
        assert_eq!(
            synthetic_power_state_from_exchange(b"#PON\r", b"@PON OK ON\r"),
            Some(1)
        );
        assert_eq!(
            synthetic_power_state_from_exchange(b"#QPW\r", b"@QPW OK ON\r"),
            Some(1)
        );
    }

    #[test]
    fn synthetic_power_update_ignores_other_responses() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"#QPW\r", b"@QPW OK STANDBY\r"),
            (b"#POF\r", b"@POF ERR BUSY\r"),
            (b"#QPW\r", b"@UPW 0\r"),
            (b"#PON\r", b"@PON OK OFF\r"),
            (b"#QPW\r", b"@QPW OK OFFLINE\r"),
            (b"#QVL\r", b"@QPW OK OFF\r"),
            (b"#QVL\r", b"@QPW OK ON\r"),
            (b"#QVL\r", b"@POF OK OFF\r"),
            (b"#QVL\r", b"@PON OK ON\r"),
            (b"", b""),
        ];

        for &(req, line) in cases {
            assert_eq!(
                synthetic_power_state_from_exchange(req, line),
                None,
                "req={:?}, line={:?} should not map to a synthetic @UPW update",
                String::from_utf8_lossy(req),
                String::from_utf8_lossy(line)
            );
        }
    }

    #[test]
    fn parse_upw_state_recognizes_power_updates() {
        assert_eq!(parse_upw_state(b"@UPW 0\r"), Some(0));
        assert_eq!(parse_upw_state(b"@UPW 1\r"), Some(1));
        assert_eq!(parse_upw_state(b"@UPW 0"), Some(0));
        assert_eq!(parse_upw_state(b"@UPW 1"), Some(1));
        assert_eq!(parse_upw_state(b"@UPW OFF\r"), None);
        assert_eq!(parse_upw_state(b"@QPW OK ON\r"), None);
    }

    #[test]
    fn timeout_policy_reconnects_after_streak_threshold() {
        assert!(!should_drop_backend_after_timeout(0, 3));
        assert!(!should_drop_backend_after_timeout(1, 3));
        assert!(!should_drop_backend_after_timeout(2, 3));
        assert!(should_drop_backend_after_timeout(3, 3));
    }

    #[test]
    fn parse_max_consecutive_timeouts_accepts_positive_values() {
        assert_eq!(parse_max_consecutive_timeouts("1"), Some(1));
        assert_eq!(parse_max_consecutive_timeouts("3"), Some(3));
        assert_eq!(parse_max_consecutive_timeouts(" 7 "), Some(7));
        assert_eq!(parse_max_consecutive_timeouts("100"), Some(100));
    }

    #[test]
    fn parse_max_consecutive_timeouts_rejects_invalid_values() {
        assert_eq!(parse_max_consecutive_timeouts("0"), None);
        assert_eq!(parse_max_consecutive_timeouts("101"), None);
        assert_eq!(parse_max_consecutive_timeouts("-1"), None);
        assert_eq!(parse_max_consecutive_timeouts("abc"), None);
        assert_eq!(parse_max_consecutive_timeouts(""), None);
    }

    #[test]
    fn read_until_capped_reads_complete_line() {
        let input: &[u8] = b"#QPW\rrest";
        let mut reader = BufReader::with_capacity(64, futures_lite::io::Cursor::new(input));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 4096)).unwrap();
        assert_eq!(n, 5);
        assert_eq!(buf.as_slice(), b"#QPW\r");
    }

    #[test]
    fn read_until_capped_returns_zero_on_immediate_eof() {
        let mut reader = BufReader::with_capacity(64, futures_lite::io::Cursor::new(&[][..]));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 4096)).unwrap();
        assert_eq!(n, 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn read_until_capped_truncated_line_returns_partial_bytes() {
        let input: &[u8] = b"@UPW 0";
        let mut reader = BufReader::with_capacity(64, futures_lite::io::Cursor::new(input));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 4096)).unwrap();
        assert_eq!(n, 6);
        assert_eq!(buf.last(), Some(&b'0'));
        assert_ne!(buf.last(), Some(&b'\r'));
    }

    #[test]
    fn read_until_capped_rejects_oversized_line() {
        // 100 bytes of 'a' then \r, with max=50 — must error before crossing the cap.
        let mut input = vec![b'a'; 100];
        input.push(b'\r');
        let mut reader = BufReader::with_capacity(16, futures_lite::io::Cursor::new(input));
        let mut buf = Vec::new();
        let err = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 50))
            .expect_err("expected InvalidData on oversize line");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_until_capped_spans_multiple_fill_buf_calls() {
        // BufReader capacity 4 forces multiple fill_buf rounds before \r is found.
        let input: &[u8] = b"abcdefghij\r";
        let mut reader = BufReader::with_capacity(4, futures_lite::io::Cursor::new(input));
        let mut buf = Vec::new();
        let n = future::block_on(read_until_capped(&mut reader, b'\r', &mut buf, 64)).unwrap();
        assert_eq!(n, 11);
        assert_eq!(buf.as_slice(), b"abcdefghij\r");
    }
}
