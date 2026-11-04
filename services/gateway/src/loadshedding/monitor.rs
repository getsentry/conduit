use async_trait::async_trait;

/// A metric that is periodically polled on the load shedding monitor thread.
#[async_trait]
pub trait MetricMonitor: Send + Sync {
    /// Polls the metric value from the system and stores it.
    async fn poll(&self);

    /// Returns the last polled value.
    fn last_value(&self) -> u64;

    /// Name used in logs and metric tags.
    fn name(&self) -> &'static str;
}
