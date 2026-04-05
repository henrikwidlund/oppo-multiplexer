use futures_lite::future;
use smol::{
    channel::{self, Receiver, Sender, TrySendError},
    Executor,
    Timer,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    lock::Mutex,
    net::{TcpListener, TcpStream},
};
use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};
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

type Clients = Arc<Mutex<HashMap<u64, Sender<Vec<u8>>>>>;

struct BackendRequest {
    peer: String,
    msg: Vec<u8>,
    response_tx: Sender<Result<Vec<u8>, String>>,
}

enum BrokerEvent {
    Request(BackendRequest),
    BackendLine(Vec<u8>),
    BackendError(String),
    ReconnectTick,
    RequestsClosed,
}

enum ReadLineOutcome {
    Line,
    TimedOut,
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
        let (request_tx, request_rx) = channel::unbounded::<BackendRequest>();

        let broker_clients = Arc::clone(&clients);
        let broker_backend_addr = Arc::clone(&backend_addr);
        spawner
            .spawn(backend_broker(
                request_rx,
                broker_clients,
                broker_backend_addr,
                timeout,
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

async fn try_connect(addr: &str) -> Option<BufReader<TcpStream>> {
    match TcpStream::connect(addr).await {
        Ok(stream) => {
            // Disable Nagle's algorithm — we send small commands and need low latency.
            let _ = stream.set_nodelay(true);
            info!("connected to backend at {addr}");
            Some(BufReader::with_capacity(256, stream))
        }
        Err(e) => {
            warn!("could not connect to backend at {addr}: {e}");
            None
        }
    }
}

async fn backend_broker(
    request_rx: Receiver<BackendRequest>,
    clients: Clients,
    backend_addr: Arc<str>,
    timeout: Duration,
) {
    let mut backend = try_connect(&backend_addr).await;

    loop {
        let event = if backend.is_some() {
            let request_fut = async {
                match request_rx.recv().await {
                    Ok(req) => BrokerEvent::Request(req),
                    Err(_) => BrokerEvent::RequestsClosed,
                }
            };

            let backend_fut = async {
                let mut line = Vec::with_capacity(256);
                match backend
                    .as_mut()
                    .expect("backend exists")
                    .read_until(b'\r', &mut line)
                    .await
                {
                    Ok(0) => BrokerEvent::BackendError("backend closed connection".to_string()),
                    Ok(_) => BrokerEvent::BackendLine(line),
                    Err(e) => BrokerEvent::BackendError(format!("backend read error: {e}")),
                }
            };

            future::or(request_fut, backend_fut).await
        } else {
            let request_fut = async {
                match request_rx.recv().await {
                    Ok(req) => BrokerEvent::Request(req),
                    Err(_) => BrokerEvent::RequestsClosed,
                }
            };

            let reconnect_fut = async {
                Timer::after(Duration::from_secs(1)).await;
                BrokerEvent::ReconnectTick
            };

            future::or(request_fut, reconnect_fut).await
        };

        match event {
            BrokerEvent::Request(req) => {
                process_request(req, &mut backend, &backend_addr, timeout, &clients).await;
            }
            BrokerEvent::BackendLine(line) => {
                if is_backend_update(&line) {
                    broadcast_update(&clients, &line).await;
                } else {
                    warn!(
                        "received unexpected backend line while idle: {}",
                        String::from_utf8_lossy(&line).trim_end_matches('\r')
                    );
                }
            }
            BrokerEvent::BackendError(reason) => {
                warn!("{reason}");
                backend = None;
            }
            BrokerEvent::ReconnectTick => {
                if backend.is_none() {
                    backend = try_connect(&backend_addr).await;
                }
            }
            BrokerEvent::RequestsClosed => break,
        }
    }
}

async fn process_request(
    req: BackendRequest,
    backend: &mut Option<BufReader<TcpStream>>,
    backend_addr: &str,
    timeout: Duration,
    clients: &Clients,
) {
    if backend.is_none() {
        *backend = try_connect(backend_addr).await;
    }

    if backend.is_none() {
        let _ = req
            .response_tx
            .send(Err("backend unavailable".to_string()))
            .await;
        return;
    }

    let result = match exchange(&req.msg, backend.as_mut().expect("backend exists"), timeout, clients).await {
        Ok(response) => Ok(response),
        Err(reason) => {
            warn!("backend error ({reason}) while handling {}, reconnecting", req.peer);
            *backend = try_connect(backend_addr).await;
            if let Some(conn) = backend.as_mut() {
                exchange(&req.msg, conn, timeout, clients).await
            } else {
                Err("backend unavailable".to_string())
            }
        }
    };

    if result.is_err() {
        *backend = None;
    }

    let _ = req.response_tx.send(result).await;
}

async fn handle_client(
    stream: TcpStream,
    client_id: u64,
    request_tx: Sender<BackendRequest>,
    clients: Clients,
    spawner: Arc<Executor<'static>>,
) {
    let peer = stream.peer_addr().ok().map(|a| a.to_string());
    let peer_str = peer.as_deref().unwrap_or("?").to_string();
    let _ = stream.set_nodelay(true);

    let writer_stream = stream.clone();
    let mut client = BufReader::with_capacity(256, stream);

    let (out_tx, out_rx) = channel::bounded::<Vec<u8>>(256);
    {
        let mut guard = clients.lock().await;
        guard.insert(client_id, out_tx.clone());
    }

    let writer_peer = peer_str.clone();
    spawner
        .spawn(async move {
            let mut writer = writer_stream;
            while let Ok(line) = out_rx.recv().await {
                if writer.write_all(&line).await.is_err() {
                    break;
                }
            }
            debug!("writer for client {writer_peer} closed");
        })
        .detach();

    let mut msg = Vec::with_capacity(256);

    loop {
        msg.clear();

        // Read a \r-terminated message from the client.
        match client.read_until(b'\r', &mut msg).await {
            Ok(0) | Err(_) => break, // client disconnected or unrecoverable error
            Ok(_) => {
                debug!(
                    "[{peer_str} → backend] {}",
                    String::from_utf8_lossy(&msg).trim_end_matches('\r')
                );
            }
        }

        let (response_tx, response_rx) = channel::bounded(1);
        let req = BackendRequest {
            peer: peer_str.clone(),
            msg: msg.clone(),
            response_tx,
        };

        if request_tx.send(req).await.is_err() {
            let _ = out_tx.send(b"ERROR: backend worker unavailable\r".to_vec()).await;
            break;
        }

        let result = response_rx.recv().await.unwrap_or_else(|_| Err("backend worker unavailable".to_string()));

        match result {
            Ok(response) => {
                debug!(
                    "[backend → {peer_str}] {}",
                    String::from_utf8_lossy(&response).trim_end_matches('\r')
                );
                if out_tx.send(response).await.is_err() {
                    break;
                }
            }
            Err(reason) => {
                let err = format!("ERROR: {reason}\r");
                if out_tx.send(err.into_bytes()).await.is_err() {
                    break;
                }
            }
        }
    }

    {
        let mut guard = clients.lock().await;
        guard.remove(&client_id);
    }
    drop(out_tx);

    info!("client {peer_str} disconnected");
}

/// Writes `msg` verbatim to the backend, then reads until a non-update `\r`-terminated line is found.
/// Matching update lines are broadcasted to all clients while waiting for the response.
async fn exchange(
    msg: &[u8],
    conn: &mut BufReader<TcpStream>,
    timeout: Duration,
    clients: &Clients,
) -> Result<Vec<u8>, String> {
    conn.get_mut()
        .write_all(msg)
        .await
        .map_err(|e| format!("backend write error: {e}"))?;

    let deadline = Instant::now() + timeout;
    let mut response = Vec::with_capacity(256);

    loop {
        response.clear();

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!("backend response timed out ({} s)", timeout.as_secs()));
        }

        match read_backend_line(conn, &mut response, remaining).await? {
            ReadLineOutcome::TimedOut => {
                return Err(format!("backend response timed out ({} s)", timeout.as_secs()));
            }
            ReadLineOutcome::Line => {
                if is_backend_update(&response) {
                    broadcast_update(clients, &response).await;
                    continue;
                }
                return Ok(response.clone());
            }
        }
    }
}

async fn read_backend_line(
    conn: &mut BufReader<TcpStream>,
    response: &mut Vec<u8>,
    timeout: Duration,
) -> Result<ReadLineOutcome, String> {
    let read_fut = async {
        match conn.read_until(b'\r', response).await {
            Ok(0) => Err("backend closed connection".to_string()),
            Ok(_) => Ok(ReadLineOutcome::Line),
            Err(e) => Err(format!("backend read error: {e}")),
        }
    };

    let timeout_fut = async {
        Timer::after(timeout).await;
        Ok(ReadLineOutcome::TimedOut)
    };

    future::or(read_fut, timeout_fut).await
}

fn is_backend_update(line: &[u8]) -> bool {
    UPDATE_PREFIXES.iter().any(|prefix| line.starts_with(prefix))
}

async fn broadcast_update(clients: &Clients, line: &[u8]) {
    let recipients = {
        let guard = clients.lock().await;
        guard
            .iter()
            .map(|(id, tx)| (*id, tx.clone()))
            .collect::<Vec<_>>()
    };

    if recipients.is_empty() {
        return;
    }

    let mut stale_clients = Vec::new();
    for (id, tx) in recipients {
        debug!(
            "[backend → client {id}] {}",
            String::from_utf8_lossy(line).trim_end_matches('\r')
        );
        match tx.try_send(line.to_vec()) {
            Ok(()) => {}
            Err(TrySendError::Closed(_)) | Err(TrySendError::Full(_)) => stale_clients.push(id),
        }
    }

    if !stale_clients.is_empty() {
        let mut guard = clients.lock().await;
        for id in stale_clients {
            guard.remove(&id);
        }
    }
}
