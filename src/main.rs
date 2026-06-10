mod backoff;
mod broker;
mod client;
mod io_util;
mod protocol;

use smol::{
    Executor,
    channel,
    net::TcpListener,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tracing::{error, info, warn};

use crate::broker::{BackendRequest, Clients, backend_broker, parse_max_consecutive_timeouts};
use crate::client::{ClientSlotGuard, handle_client};

const REQUEST_CHANNEL_CAP: usize = 32;
const DEFAULT_MAX_CONSECUTIVE_TIMEOUTS: u32 = 3;
/// Hard cap on concurrent accepted clients. Prevents a connection flood from
/// spawning unbounded tasks (each holds channels + TcpStream clones). Existing
/// clients are not affected; new ones over the cap are refused at accept time.
const MAX_CLIENTS: usize = 11;

#[cfg(target_os = "linux")]
/// Sets up tracing via journald, falling back to the default `fmt` subscriber
/// (stderr) if the journald socket is unavailable (e.g. running outside
/// systemd). Level is controlled by `RUST_LOG`, defaulting to `info`.
fn init_logging() {
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt};
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
        // Port 0 would have the OS pick an ephemeral port, but the program logs
        // and CLI contract advertise a concrete port — accepting 0 would print a
        // misleading "listening on 0.0.0.0:0" line. Reject explicitly.
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
                        continue;
                    }
                    active_clients.fetch_add(1, Ordering::AcqRel);
                    let slot = ClientSlotGuard::new(Arc::clone(&active_clients));
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
