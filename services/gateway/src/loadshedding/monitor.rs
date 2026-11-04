/// A metric that is periodically polled in the background.
pub trait MetricMonitor: Send + Sync {
    /// Polls the metric value from the system and stores it.
    fn poll(&self);

    /// Returns the last polled value.
    fn last_value(&self) -> u64;

    /// Name used in logs and metric tags.
    fn name(&self) -> &'static str;
}
