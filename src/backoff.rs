use std::time::{Duration, Instant};

/// Linear backoff for backend reconnect attempts. Tracks both the current
/// wait duration and the absolute time of the next allowed attempt. Both the
/// scheduled (ReconnectTick) path and the on-demand (handle_new_request) path
/// consult this single source of truth so they cannot race past each other.
///
/// Sequence on repeated failures: 0.5s, 1.0s, 1.5s, ... capped at 15s.
/// Resets to zero on any successful connect.
pub struct Backoff {
    current: Duration,
    next_attempt_at: Instant,
}

impl Backoff {
    const STEP: Duration = Duration::from_millis(500);
    const MAX: Duration = Duration::from_secs(15);

    pub fn new() -> Self {
        Self {
            current: Duration::ZERO,
            next_attempt_at: Instant::now(),
        }
    }

    /// True if a connect attempt is allowed right now.
    pub fn is_ready(&self) -> bool {
        Instant::now() >= self.next_attempt_at
    }

    /// Duration until the next attempt is allowed; zero if already allowed.
    pub fn delay_until_ready(&self) -> Duration {
        self.next_attempt_at.saturating_duration_since(Instant::now())
    }

    pub fn on_success(&mut self) {
        self.current = Duration::ZERO;
        self.next_attempt_at = Instant::now();
    }

    pub fn on_failure(&mut self) {
        self.current = (self.current + Self::STEP).min(Self::MAX);
        self.next_attempt_at = Instant::now() + self.current;
    }
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
}
