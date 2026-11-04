use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use broker::ConnectTimeoutCounter;

use super::MetricMonitor;

/// How quickly old timeouts stop counting: after `HALF_LIFE`, a timeout
/// contributes half as much to the rate.
const HALF_LIFE: Duration = Duration::from_secs(1);

/// Exponentially weighted rate of `new_events` that happened over `elapsed`,
/// treating them as spread evenly over that time. The result does not depend
/// on how often it is updated: a steady `r` events/s converges to `r`.
fn decayed_rate(previous_rate: f64, new_events: u64, elapsed: Duration) -> f64 {
    let elapsed = elapsed.as_secs_f64();
    if elapsed <= 0.0 {
        return previous_rate;
    }
    let decay = 0.5f64.powf(elapsed / HALF_LIFE.as_secs_f64());
    previous_rate * decay + new_events as f64 * (1.0 - decay) / elapsed
}

struct State {
    last_total: u64,
    last_poll: Instant,
    rate: f64,
}

/// Recent rate of Redis connection acquire timeouts, in timeouts per second.
pub struct ConnectTimeoutMonitor {
    total: ConnectTimeoutCounter,
    state: Mutex<State>,
    last_value: AtomicU64,
}

impl ConnectTimeoutMonitor {
    pub fn new(total: ConnectTimeoutCounter) -> Self {
        Self {
            state: Mutex::new(State {
                last_total: total.get(),
                last_poll: Instant::now(),
                rate: 0.0,
            }),
            total,
            last_value: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl MetricMonitor for ConnectTimeoutMonitor {
    async fn poll(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let now = Instant::now();
        let total = self.total.get();
        state.rate = decayed_rate(
            state.rate,
            total.saturating_sub(state.last_total),
            now - state.last_poll,
        );
        state.last_total = total;
        state.last_poll = now;
        self.last_value.store(state.rate as u64, Ordering::Relaxed);
    }

    fn last_value(&self) -> u64 {
        self.last_value.load(Ordering::Relaxed)
    }

    fn name(&self) -> &'static str {
        "connect_timeout"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decayed_rate_converges_regardless_of_period() {
        let rate_per_sec = 10.0;
        for period in [0.1, 0.5, 1.0, 5.0] {
            let mut rate = 0.0;
            for _ in 0..1000 {
                rate = decayed_rate(
                    rate,
                    (rate_per_sec * period) as u64,
                    Duration::from_secs_f64(period),
                );
            }
            assert!(
                (rate - rate_per_sec).abs() < 0.01,
                "period {period}s converged to {rate}"
            );
        }
    }

    #[test]
    fn test_decayed_rate_halves_after_half_life() {
        assert_eq!(decayed_rate(8.0, 0, HALF_LIFE), 4.0);
        assert_eq!(decayed_rate(8.0, 0, HALF_LIFE * 2), 2.0);
    }

    #[test]
    fn test_decayed_rate_zero_elapsed() {
        assert_eq!(decayed_rate(8.0, 5, Duration::ZERO), 8.0);
    }

    #[tokio::test]
    async fn test_connect_timeout_monitor_ignores_timeouts_before_creation() {
        let total = ConnectTimeoutCounter::default();
        (0..1000).for_each(|_| total.inc());
        let monitor = ConnectTimeoutMonitor::new(total.clone());
        monitor.poll().await;
        assert_eq!(monitor.last_value(), 0);

        (0..1000).for_each(|_| total.inc());
        monitor.poll().await;
        assert!(monitor.last_value() > 0);
    }
}
