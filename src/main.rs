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
    sync::{Arc, Mutex},
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

struct Backoff {
    current: Duration,
}

impl Backoff {
    const STEP: Duration = Duration::from_millis(500);
    const MAX: Duration = Duration::from_secs(15);

    fn new() -> Self {
        Self { current: Duration::ZERO }
    }

    fn current(&self) -> Duration {
        self.current
    }

    fn on_success(&mut self) {
        self.current = Duration::ZERO;
    }

    fn on_failure(&mut self) {
        self.current = (self.current + Self::STEP).min(Self::MAX);
    }
}

type Clients = Arc<Mutex<HashMap<u64, Sender<Arc<[u8]>>>>>;

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
    match TcpStream::connect(addr).await {
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
    loop {
        let mut line = Vec::with_capacity(256);
        match reader.read_until(b'\r', &mut line).await {
            Ok(0) => {
                let _ = tx
                    .send(BackendEvent::Error("backend closed connection".to_string()))
                    .await;
                return;
            }
            Ok(_) => {
                if tx.send(BackendEvent::Line(line)).await.is_err() {
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
                let backend_fut = async {
                    match conn.events.recv().await {
                        Ok(ev) => BrokerEvent::Backend(ev),
                        Err(_) => BrokerEvent::Backend(BackendEvent::Error(
                            "backend reader ended".to_string(),
                        )),
                    }
                };
                let remaining = deadline.saturating_duration_since(Instant::now());
                let timeout_fut = async move {
                    Timer::after(remaining).await;
                    BrokerEvent::Timeout
                };
                future::or(backend_fut, timeout_fut).await
            }
            (Some(conn), None) => {
                let backend_fut = async {
                    match conn.events.recv().await {
                        Ok(ev) => BrokerEvent::Backend(ev),
                        Err(_) => BrokerEvent::Backend(BackendEvent::Error(
                            "backend reader ended".to_string(),
                        )),
                    }
                };
                let request_fut = async {
                    match request_rx.recv().await {
                        Ok(req) => BrokerEvent::Request(req),
                        Err(_) => BrokerEvent::RequestsClosed,
                    }
                };
                future::or(backend_fut, request_fut).await
            }
            (None, _) => {
                let request_fut = async {
                    match request_rx.recv().await {
                        Ok(req) => BrokerEvent::Request(req),
                        Err(_) => BrokerEvent::RequestsClosed,
                    }
                };
                let delay = backoff.current();
                let reconnect_fut = async move {
                    Timer::after(delay).await;
                    BrokerEvent::ReconnectTick
                };
                future::or(request_fut, reconnect_fut).await
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
                    broadcast_update(&clients, &line);
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

async fn handle_new_request(
    req: BackendRequest,
    backend: &mut Option<BackendConn>,
    backend_addr: &str,
    spawner: &Arc<Executor<'static>>,
    backoff: &mut Backoff,
    timeout: Duration,
    in_flight: &mut Option<(BackendRequest, Instant)>,
) {
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
            }
        }
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

    let writer_stream = stream.clone();
    let mut client = BufReader::with_capacity(256, stream);

    let (out_tx, out_rx) = channel::bounded::<Arc<[u8]>>(CLIENT_OUT_CAP);
    {
        let mut guard = clients.lock().expect("clients mutex poisoned");
        guard.insert(client_id, out_tx.clone());
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
            Ok(_) => {}
        }

        let (response_tx, response_rx) = channel::bounded(1);
        let req = BackendRequest {
            peer: Arc::clone(&peer),
            msg: std::mem::take(&mut msg),
            response_tx,
        };
        msg = Vec::with_capacity(256);

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
        let mut guard = clients.lock().expect("clients mutex poisoned");
        guard.remove(&client_id);
    }
    drop(out_tx);

    info!("client {peer} disconnected");
}

fn is_backend_update(line: &[u8]) -> bool {
    UPDATE_PREFIXES.iter().any(|prefix| line.starts_with(prefix))
}

fn broadcast_update(clients: &Clients, line: &[u8]) {
    let shared: Arc<[u8]> = Arc::from(line);
    let mut guard = clients.lock().expect("clients mutex poisoned");
    guard.retain(|id, tx| match tx.try_send(Arc::clone(&shared)) {
        Ok(()) => {
            debug!(
                "[backend → client {id}] {}",
                String::from_utf8_lossy(&shared).trim_end_matches('\r')
            );
            true
        }
        Err(TrySendError::Closed(_)) => false,
        Err(TrySendError::Full(_)) => {
            warn!("client {id} send buffer full, dropping update");
            true
        }
    });
}
