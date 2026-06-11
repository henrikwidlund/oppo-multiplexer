use crate::backoff::Backoff;
use crate::io_util::{MAX_LINE_LEN, read_until_capped, write_with_timeout};
use crate::protocol::{
    SYNTHETIC_UPW_OFF, SYNTHETIC_UPW_ON, is_backend_update, parse_upw_state,
    synthetic_power_state_from_exchange,
};

use futures_lite::future;
use smol::{
    Executor, Timer,
    channel::{self, Receiver, Sender, TrySendError},
    io::BufReader,
    net::TcpStream,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};

const BACKEND_EVENT_CAP: usize = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// Minimum interval between consecutive successful writes to the player
/// in steady state (~10 req/s, no burst). Enforced by gating the broker's
/// next request pull on `last_request_sent_at`; backend events keep flowing
/// during the gate. Mirrors the rate limit used by the .NET Oppo client;
/// the request channel (REQUEST_CHANNEL_CAP) is FIFO, matching the .NET
/// `QueueProcessingOrder.OldestFirst`.
///
/// Carve-out: the reconnect-and-retry path in `handle_new_request` can
/// issue a second write < MIN_REQUEST_INTERVAL after a failed primary
/// write (the failed write may have partially landed before the kernel
/// reported the error, per `write_with_timeout`). Bounded to at most one
/// extra write per healthy→broken transition; documented here so the
/// steady-state guarantee is honest about this edge case. Enforcing the
/// gate on the retry path would force a broker-blocking sleep while a
/// freshly-spawned `backend_reader` is running, risking dropped @U??
/// updates once BACKEND_EVENT_CAP fills — a worse trade.
const MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(100);

/// A client command waiting for its backend response. `response_tx` is a
/// per-request one-shot (bounded(1)) populated by the broker.
pub struct BackendRequest {
    pub peer: Arc<str>,
    pub msg: Vec<u8>,
    pub response_tx: Sender<Result<Vec<u8>, String>>,
}

/// Per-client state held in the broadcast map. The `TcpStream` clone is kept so
/// that a stuck/dead client can be force-disconnected from the broadcast path,
/// which makes both the writer and the reader half of `handle_client` error out
/// and clean up — instead of leaving the client task running on a broken socket.
pub type ClientEntry = (Sender<Arc<[u8]>>, TcpStream);
pub type Clients = Arc<Mutex<HashMap<u64, ClientEntry>>>;

/// Locks the clients map, recovering from poison so a panicking client task
/// cannot bring down the whole server.
pub fn lock_clients(clients: &Clients) -> MutexGuard<'_, HashMap<u64, ClientEntry>> {
    clients.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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

/// Mutable state tied to the current backend connection. Bundled so
/// `handle_new_request` can take a single `&mut` parameter rather than four
/// separate ones. `last_power_state` and `consecutive_timeouts` live outside
/// because they belong to broadcast / liveness bookkeeping, not connection
/// management.
struct ConnSlot {
    backend: Option<BackendConn>,
    backoff: Backoff,
    in_flight: Option<(BackendRequest, Instant)>,
    /// Monotonic timestamp (`Instant`) of the last successful write to the
    /// player; drives the rate-limit gate in `backend_broker`.
    last_request_sent_at: Option<Instant>,
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

/// Remaining wait until a new request may be sent to the player. Used by the
/// broker's main-loop request-pull gate; the gate runs concurrently with the
/// backend-event arm so rate-limiting commands → player never blocks player
/// → clients.
fn rate_limit_remaining(last_sent_at: Option<Instant>) -> Duration {
    match last_sent_at {
        Some(t) => MIN_REQUEST_INTERVAL.saturating_sub(t.elapsed()),
        None => Duration::ZERO,
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
pub async fn backend_broker(
    request_rx: Receiver<BackendRequest>,
    clients: Clients,
    backend_addr: Arc<str>,
    timeout: Duration,
    max_consecutive_timeouts: u32,
    spawner: Arc<Executor<'static>>,
) {
    let mut backoff = Backoff::new();
    let backend = try_connect(&backend_addr, &spawner, &mut backoff).await;
    let mut slot = ConnSlot {
        backend,
        backoff,
        in_flight: None,
        last_request_sent_at: None,
    };
    let mut last_power_state: Option<u8> = None;
    let mut consecutive_timeouts: u32 = 0;

    loop {
        let event = match (slot.backend.as_ref(), slot.in_flight.as_ref()) {
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
                // Gate request pulls on MIN_REQUEST_INTERVAL so we never send
                // commands to the player faster than the rate limit allows.
                // The backend-event arm runs concurrently with the gate, so
                // unsolicited updates (@U??) and any other inbound lines are
                // forwarded to clients during the cooldown — the rate limit
                // only throttles us → player, never player → us.
                //
                // Recreating the Timer each iteration looks like it could
                // starve requests under a flood of events (event arm keeps
                // winning, gated_req's Timer keeps getting cancelled). It does
                // not: `last_request_sent_at` is fixed at the last write, so
                // `rate_wait` shrinks monotonically with wall-clock. After
                // MIN_REQUEST_INTERVAL has elapsed since the last write,
                // `rate_wait` is `Duration::ZERO` permanently, the Timer
                // branch is skipped, and gated_req is just `recv_request()` —
                // which `future::or` polls before the event arm, so a queued
                // request wins. Max request delay is thus MIN_REQUEST_INTERVAL.
                let rate_wait = rate_limit_remaining(slot.last_request_sent_at);
                let gated_req = async {
                    if !rate_wait.is_zero() {
                        Timer::after(rate_wait).await;
                    }
                    recv_request(&request_rx).await
                };
                future::or(gated_req, recv_backend_event(&conn.events)).await
            }
            (None, _) => {
                let delay = slot.backoff.delay_until_ready();
                let reconnect_fut = async move {
                    Timer::after(delay).await;
                    BrokerEvent::ReconnectTick
                };
                future::or(recv_request(&request_rx), reconnect_fut).await
            }
        };

        match event {
            BrokerEvent::Request(req) => {
                handle_new_request(req, &mut slot, &backend_addr, &spawner, timeout).await;
            }
            BrokerEvent::Backend(BackendEvent::Line(line)) => {
                if is_backend_update(&line) {
                    if let Some(state) = parse_upw_state(&line) {
                        last_power_state = Some(state);
                    }
                    broadcast_update(&clients, line);
                } else if let Some((req, _)) = slot.in_flight.take() {
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
                slot.backend = None;
                slot.backoff.on_failure();
                consecutive_timeouts = 0;
                last_power_state = None;
                if let Some((req, _)) = slot.in_flight.take() {
                    let _ = req.response_tx.send(Err(reason)).await;
                }
            }
            BrokerEvent::Timeout => {
                if let Some((req, _)) = slot.in_flight.take() {
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
                        slot.backend = None;
                        slot.backoff.on_failure();
                        consecutive_timeouts = 0;
                        last_power_state = None;
                    }
                }
            }
            BrokerEvent::ReconnectTick => {
                if slot.backend.is_none() {
                    slot.backend = try_connect(&backend_addr, &spawner, &mut slot.backoff).await;
                    if slot.backend.is_some() {
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
    slot: &mut ConnSlot,
    backend_addr: &str,
    spawner: &Arc<Executor<'static>>,
    timeout: Duration,
) {
    // See the function doc for why this exists. TL;DR: write-error retry is
    // bounded to one attempt per healthy→broken transition; later requests
    // are gated by `is_ready()` because `slot.backend` is None and the outer
    // branch is skipped.
    let mut retry_after_write_fail = false;

    if let Some(conn) = slot.backend.as_mut() {
        // No inner rate-limit sleep on this path: broker's (Some, None) select
        // arm gated the request pull, so MIN_REQUEST_INTERVAL is already
        // satisfied. Just stamp on success so the next pull's gate is correct.
        match write_with_timeout(&mut conn.writer, &req.msg).await {
            Ok(()) => {
                // Single `now` for both the rate-limit stamp and the in-flight
                // deadline so they share a consistent baseline.
                let now = Instant::now();
                slot.last_request_sent_at = Some(now);
                debug!(
                    "[{} → backend] {}",
                    req.peer,
                    String::from_utf8_lossy(&req.msg).trim_end_matches('\r')
                );
                slot.in_flight = Some((req, now + timeout));
                return;
            }
            Err(e) => {
                warn!("backend write error ({e}) while handling {}, reconnecting", req.peer);
                slot.backend = None;
                slot.backoff.on_failure();
                retry_after_write_fail = true;
            }
        }
    }

    // Path A (`!retry_after_write_fail`): no backend at entry; obey the cooldown.
    // Path B (`retry_after_write_fail`): we just bumped backoff after losing a
    // live connection — bypass the gate this one time for the retry.
    if !retry_after_write_fail && !slot.backoff.is_ready() {
        let _ = req.response_tx.send(Err("backend unavailable".to_string())).await;
        return;
    }

    slot.backend = try_connect(backend_addr, spawner, &mut slot.backoff).await;
    let Some(conn) = slot.backend.as_mut() else {
        let _ = req.response_tx.send(Err("backend unavailable".to_string())).await;
        return;
    };

    // No rate-limit sleep here. The 100 ms minimum is already preserved:
    // either the broker's main-loop gate held the request until at least
    // MIN_REQUEST_INTERVAL had passed since the previous successful write,
    // or there has been no successful write yet (last_request_sent_at is
    // None). The retry path then adds time on top (failed-write detection +
    // a real TCP handshake inside try_connect — never zero), so the
    // spacing from the previous successful write is always ≥
    // MIN_REQUEST_INTERVAL. Sleeping here would block the broker while the
    // new backend_reader is already running, risking dropped @U?? updates
    // once BACKEND_EVENT_CAP fills.
    match write_with_timeout(&mut conn.writer, &req.msg).await {
        Ok(()) => {
            let now = Instant::now();
            slot.last_request_sent_at = Some(now);
            debug!(
                "[{} → backend] {}",
                req.peer,
                String::from_utf8_lossy(&req.msg).trim_end_matches('\r')
            );
            slot.in_flight = Some((req, now + timeout));
        }
        Err(e) => {
            slot.backend = None;
            slot.backoff.on_failure();
            let _ = req
                .response_tx
                .send(Err(format!("backend write error: {e}")))
                .await;
        }
    }
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

/// After this many consecutive request timeouts, force backend reconnect.
/// Smaller thresholds fail over faster; larger thresholds tolerate more transient misses.
fn should_drop_backend_after_timeout(consecutive_timeouts: u32, max_consecutive_timeouts: u32) -> bool {
    consecutive_timeouts >= max_consecutive_timeouts
}

pub fn parse_max_consecutive_timeouts(raw: &str) -> Option<u32> {
    let parsed = raw.trim().parse::<u32>().ok()?;
    if !matches!(parsed, 1..=100) {
        return None;
    }
    Some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_remaining_none_when_never_sent() {
        assert_eq!(rate_limit_remaining(None), Duration::ZERO);
    }

    #[test]
    fn rate_limit_remaining_zero_when_interval_already_passed() {
        let now = Instant::now();
        let long_ago = now.checked_sub(MIN_REQUEST_INTERVAL * 2).unwrap_or(now);
        assert_eq!(rate_limit_remaining(Some(long_ago)), Duration::ZERO);
    }

    #[test]
    fn rate_limit_remaining_bounded_by_interval_when_just_sent() {
        let just_now = Instant::now();
        let remaining = rate_limit_remaining(Some(just_now));
        // Only the upper bound is scheduler-independent. The thread could be
        // descheduled for ≥ MIN_REQUEST_INTERVAL between `Instant::now()` and
        // this call (especially on a loaded CI runner), legitimately driving
        // `remaining` to 0 — so a positive lower bound would be flaky.
        assert!(
            remaining <= MIN_REQUEST_INTERVAL,
            "remaining {remaining:?} must not exceed {MIN_REQUEST_INTERVAL:?}"
        );
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
}
