use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::loadshedding::MetricMonitor;

/// Number of connected SSE clients. Clones share the same count.
#[derive(Clone, Default)]
pub struct ConcurrentClients(Arc<AtomicU64>);

impl ConcurrentClients {
    /// Counts a client until the returned guard is dropped.
    pub fn track(&self) -> ClientGuard {
        self.0.fetch_add(1, Ordering::Relaxed);
        metrics::gauge!("connections.active").increment(1.0);
        ClientGuard(self.clone())
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Keeps a client counted in [`ConcurrentClients`] while alive.
pub struct ClientGuard(ConcurrentClients);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        (self.0).0.fetch_sub(1, Ordering::Relaxed);
        metrics::gauge!("connections.active").decrement(1.0);
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
    fn test_guard_counts_client_while_alive() {
        let clients = ConcurrentClients::default();
        let first = clients.track();
        let second = clients.track();
        assert_eq!(clients.get(), 2);

        drop(first);
        assert_eq!(clients.get(), 1);
        drop(second);
        assert_eq!(clients.get(), 0);
    }

    #[test]
    fn test_monitor_reads_counter_live() {
        let clients = ConcurrentClients::default();
        let monitor = ConcurrentClientsMonitor::new(clients.clone());
        assert_eq!(monitor.last_value(), 0);

        let _guard = clients.track();
        assert_eq!(monitor.last_value(), 1, "visible without polling");
    }
}
