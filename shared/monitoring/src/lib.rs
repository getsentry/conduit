pub mod logging;
pub mod metrics;

pub fn init(
    logging_config: logging::LoggingConfig,
    metrics_config: Option<metrics::MetricsConfig>,
    runtime: &tokio::runtime::Runtime,
) {
    logging::init(logging_config);

    if let Some(metrics_config) = metrics_config {
        let _runtime_guard = runtime.enter();
        metrics::init(metrics_config);
    }
}
