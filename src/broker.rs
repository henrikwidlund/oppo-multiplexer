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
/// Minimum interval between two consecutive requests sent to the player.
/// Equivalent to a token bucket with capacity=1 and refill=1 per 100 ms:
/// at most 10 requests/second, no burst. Mirrors the rate limit used by
/// the .NET Oppo client and keeps a flood of client commands from
/// overwhelming the player. The request channel (REQUEST_CHANNEL_CAP)
/// is FIFO, matching the .NET `QueueProcessingOrder.OldestFirst`.
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

/// Sleeps just long enough that the next request is at least
/// `MIN_REQUEST_INTERVAL` after the previous one, then stamps "now" as the
/// new last-sent time. Called immediately before every backend write so the
/// player sees at most one request per interval. The broker is briefly
/// blocked during the sleep (≤ MIN_REQUEST_INTERVAL); backend events queue
/// in their channel and are drained when the broker resumes.
async fn await_rate_limit_and_mark(last_sent_at: &mut Option<Instant>) {
    if let Some(t) = *last_sent_at {
        let elapsed = t.elapsed();
        if elapsed < MIN_REQUEST_INTERVAL {
            Timer::after(MIN_REQUEST_INTERVAL - elapsed).await;
        }
    }
    *last_sent_at = Some(Instant::now());
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
    let mut backend: Option<BackendConn> = try_connect(&backend_addr, &spawner, &mut backoff).await;
    let mut in_flight: Option<(BackendRequest, Instant)> = None;
    let mut last_power_state: Option<u8> = None;
    let mut consecutive_timeouts: u32 = 0;
    let mut last_request_sent_at: Option<Instant> = None;

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
                    &mut last_request_sent_at,
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
    last_request_sent_at: &mut Option<Instant>,
) {
    // See the function doc for why this exists. TL;DR: write-error retry is
    // bounded to one attempt per healthy→broken transition; later requests
    // are gated by `is_ready()` because `backend` is None and the outer
    // branch is skipped.
    let mut retry_after_write_fail = false;

    if let Some(conn) = backend.as_mut() {
        await_rate_limit_and_mark(last_request_sent_at).await;
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

    await_rate_limit_and_mark(last_request_sent_at).await;
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
    fn rate_limit_first_call_does_not_sleep_and_stamps_now() {
        let mut last: Option<Instant> = None;
        let start = Instant::now();
        futures_lite::future::block_on(await_rate_limit_and_mark(&mut last));
        assert!(
            start.elapsed() < Duration::from_millis(20),
            "first call should not sleep"
        );
        assert!(last.is_some(), "first call should stamp last_sent_at");
    }

    #[test]
    fn rate_limit_sleeps_until_interval_elapsed() {
        let mut last: Option<Instant> = Some(Instant::now());
        let before = Instant::now();
        futures_lite::future::block_on(await_rate_limit_and_mark(&mut last));
        let elapsed = before.elapsed();
        // Should have slept ~MIN_REQUEST_INTERVAL (small slack for scheduler).
        assert!(
            elapsed >= MIN_REQUEST_INTERVAL.saturating_sub(Duration::from_millis(10)),
            "should have slept at least ~{MIN_REQUEST_INTERVAL:?}, slept {elapsed:?}"
        );
        assert!(
            elapsed < MIN_REQUEST_INTERVAL + Duration::from_millis(50),
            "should not have slept much beyond {MIN_REQUEST_INTERVAL:?}, slept {elapsed:?}"
        );
    }

    #[test]
    fn rate_limit_does_not_sleep_if_interval_already_passed() {
        let mut last: Option<Instant> = Some(Instant::now() - MIN_REQUEST_INTERVAL * 2);
        let start = Instant::now();
        futures_lite::future::block_on(await_rate_limit_and_mark(&mut last));
        assert!(
            start.elapsed() < Duration::from_millis(20),
            "should not sleep when interval already elapsed"
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
