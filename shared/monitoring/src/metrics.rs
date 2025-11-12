use metrics_exporter_statsd::StatsdBuilder;
use std::{collections::BTreeMap, net::SocketAddr};

pub struct MetricsConfig {
    pub service_name: String,
    pub statsd_addr: SocketAddr,
    pub default_tags: BTreeMap<String, String>,
}

impl MetricsConfig {
    pub fn new(
        service_name: &str,
        statsd_addr: SocketAddr,
        default_tags: BTreeMap<String, String>,
    ) -> Self {
        MetricsConfig {
            service_name: service_name.to_string(),
            statsd_addr,
            default_tags,
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
    let address = metrics_config.statsd_addr;

    let builder = StatsdBuilder::from(address.ip().to_string(), address.port());

    let recorder = metrics_config
        .default_tags
        .into_iter()
        .fold(
            builder.with_queue_size(5000).with_buffer_size(256),
            |builder, (key, value)| builder.with_default_tag(key, value),
        )
        .build(Some(&format!("conduit.{}", metrics_config.service_name)))
        .expect("Could not create StatsdRecorder");

    metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder")
}
