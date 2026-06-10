use crate::broker::{BackendRequest, Clients, lock_clients};
use crate::io_util::{MAX_LINE_LEN, read_until_capped};

use smol::{
    Executor,
    channel::{self, Sender},
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tracing::{debug, info, warn};

const CLIENT_OUT_CAP: usize = 256;

/// RAII counter slot for the `MAX_CLIENTS` cap. Incremented at accept time in
/// `main`; the guard is moved into `handle_client`, and dropping it (clean
/// exit, break-out, panic, or task cancellation) decrements the counter.
pub struct ClientSlotGuard(Arc<AtomicUsize>);

impl ClientSlotGuard {
    pub fn new(counter: Arc<AtomicUsize>) -> Self {
        Self(counter)
    }
}

impl Drop for ClientSlotGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One task per accepted client. Reads `\r`-terminated commands from the
/// socket, forwards each to the broker, and writes the response back.
///
/// A dedicated writer task (spawned here) drains the per-client out-channel
/// in parallel. Both the writer and the broadcast path share the same channel,
/// so the client receives a serialized mix of its own command responses and
/// unsolicited backend updates (events).
pub async fn handle_client(
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
