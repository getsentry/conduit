use sentry::ClientOptions;
use sentry::integrations::tracing::{ErrorLayer, LogLayer};
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;
use std::borrow::Cow;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// JSON structured logging for k8s
    Json,

    /// Text formatted logging for local dev
    Text,
}

#[derive(Debug)]
pub struct LoggingConfig {
    /// The sentry DSN to use for error reporting.
    pub sentry_dsn: Option<String>,

    /// The environment to report to sentry errors to.
    pub sentry_env: Option<Cow<'static, str>>,

    /// The tracing sample rate
    pub traces_sample_rate: f32,

    /// The log level to filter logging to.
    pub log_filter: String,

    /// The log format to use
    pub log_format: LogFormat,
}

impl LoggingConfig {
    pub fn new(
        sentry_dsn: Option<String>,
        environment: String,
        traces_sample_rate: f32,
        log_filter: String,
        log_format: LogFormat,
    ) -> Self {
        LoggingConfig {
            sentry_dsn,
            sentry_env: Some(Cow::Owned(environment)),
            traces_sample_rate,
            log_filter,
            log_format,
        }
    }
}

/// Setup sentry-sdk and logging/tracing
pub fn init(log_config: LoggingConfig) {
    if let Some(dsn) = &log_config.sentry_dsn {
        let options = ClientOptions::new()
            .dsn(dsn)
            .environment(log_config.sentry_env.unwrap_or("dev".into()))
            .traces_sample_rate(log_config.traces_sample_rate);
        let guard = sentry::init(options);

        std::mem::forget(guard);
    }

    let subscriber = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    let formatter = match log_config.log_format {
        LogFormat::Json => subscriber
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .with_file(true)
            .with_line_number(true)
            .boxed(),
        LogFormat::Text => subscriber.compact().boxed(),
    };

    // Capture ERROR level tracing events as error events. info, warn, error will
    // be captured as logs.
    let logs_subscriber = tracing_subscriber::registry()
        .with(formatter.with_filter(EnvFilter::new(log_config.log_filter)))
        .with(ErrorLayer::new().with_filter(LevelFilter::ERROR))
        .with(LogLayer::new().with_filter(LevelFilter::INFO));

    logs_subscriber.init();
}
