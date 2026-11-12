//! Process-wide count of connected SSE clients.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use loadshedding::MetricMonitor;

static CONCURRENT_CLIENTS: AtomicU64 = AtomicU64::new(0);

pub fn inc() {
    CONCURRENT_CLIENTS.fetch_add(1, Ordering::Relaxed);
}

pub fn dec() {
    CONCURRENT_CLIENTS.fetch_sub(1, Ordering::Relaxed);
}

pub fn get() -> u64 {
    CONCURRENT_CLIENTS.load(Ordering::Relaxed)
}

/// Load shedding monitor for the number of connected clients.
///
/// Reads the counter live instead of polling it, so connection bursts are seen
/// immediately. The monitor's configured period has no effect.
pub struct ConcurrentClientsMonitor;

#[async_trait]
impl MetricMonitor for ConcurrentClientsMonitor {
    async fn poll(&self) {}

    fn last_value(&self) -> u64 {
        get()
    }

    fn name(&self) -> &'static str {
        "concurrent_clients"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The counter is shared with other tests running in parallel, so only
    // check that the monitor reflects it, not exact values.
    #[test]
    fn test_monitor_reads_counter() {
        inc();
        assert!(ConcurrentClientsMonitor.last_value() >= 1);
        dec();
    }
}
