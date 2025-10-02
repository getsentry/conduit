pub mod logging;
pub mod metrics;

pub fn init(
    logging_config: logging::LoggingConfig,
    metrics_config: Option<metrics::MetricsConfig>,
) {
    logging::init(logging_config);

    if let Some(metrics_config) = metrics_config {
        metrics::init(metrics_config);
    }
}
