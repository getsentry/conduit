use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::MetricMonitor;

/// Factor applied to the value on every poll. With a steady rate of `r`
/// timeouts per poll interval, the value converges to `r / (1 - DECAY)`.
const DECAY: f64 = 0.5;

/// Decaying count of recent Redis connection acquire timeouts.
///
/// On every poll, the timeouts that happened since the previous poll are added
/// to the value after it is decayed by [`DECAY`].
pub struct ConnectTimeoutMonitor {
    total: Arc<AtomicU64>,
    last_total: AtomicU64,
    /// `f64` bits of the decayed value.
    value: AtomicU64,
}

impl ConnectTimeoutMonitor {
    pub fn new(total: Arc<AtomicU64>) -> Self {
        Self {
            last_total: AtomicU64::new(total.load(Ordering::Relaxed)),
            total,
            value: AtomicU64::new(0f64.to_bits()),
        }
    }
}

impl MetricMonitor for ConnectTimeoutMonitor {
    fn poll(&self) {
        let total = self.total.load(Ordering::Relaxed);
        let new = total.saturating_sub(self.last_total.swap(total, Ordering::Relaxed));
        let value = f64::from_bits(self.value.load(Ordering::Relaxed));
        let value = value * DECAY + new as f64;
        self.value.store(value.to_bits(), Ordering::Relaxed);
    }

    fn last_value(&self) -> u64 {
        f64::from_bits(self.value.load(Ordering::Relaxed)) as u64
    }

    fn name(&self) -> &'static str {
        "connect_timeout"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_timeout_monitor_decays() {
        let total = Arc::new(AtomicU64::new(3));
        let monitor = ConnectTimeoutMonitor::new(total.clone());

        // Timeouts from before the monitor was created are not counted.
        monitor.poll();
        assert_eq!(monitor.last_value(), 0);

        total.fetch_add(10, Ordering::Relaxed);
        monitor.poll();
        assert_eq!(monitor.last_value(), 10);

        monitor.poll();
        assert_eq!(monitor.last_value(), 5);

        total.fetch_add(4, Ordering::Relaxed);
        monitor.poll();
        // 5 * 0.5 + 4
        assert_eq!(monitor.last_value(), 6);
    }
}
