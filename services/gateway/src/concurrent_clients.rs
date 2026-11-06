use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::loadshedding::MetricMonitor;

/// Number of connected SSE clients. Clones share the same count.
#[derive(Clone, Default)]
pub struct ConcurrentClients(Arc<AtomicU64>);

impl ConcurrentClients {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Load shedding monitor for the number of connected clients.
///
/// Reads the counter live instead of polling it, so connection bursts are seen
/// immediately. The monitor's configured period has no effect.
pub struct ConcurrentClientsMonitor(ConcurrentClients);

impl ConcurrentClientsMonitor {
    pub fn new(clients: ConcurrentClients) -> Self {
        Self(clients)
    }
}

#[async_trait]
impl MetricMonitor for ConcurrentClientsMonitor {
    async fn poll(&self) {}

    fn last_value(&self) -> u64 {
        self.0.get()
    }

    fn name(&self) -> &'static str {
        "concurrent_clients"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inc_dec() {
        let clients = ConcurrentClients::default();
        clients.inc();
        clients.inc();
        assert_eq!(clients.get(), 2);

        clients.dec();
        assert_eq!(clients.get(), 1);
    }

    #[test]
    fn test_monitor_reads_counter_live() {
        let clients = ConcurrentClients::default();
        let monitor = ConcurrentClientsMonitor::new(clients.clone());
        assert_eq!(monitor.last_value(), 0);

        clients.inc();
        assert_eq!(monitor.last_value(), 1, "visible without polling");
    }
}
