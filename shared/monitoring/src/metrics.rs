use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
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

    let labels: Vec<Label> = metrics_config
        .default_tags
        .into_iter()
        .map(|(key, value)| Label::new(key, value))
        .collect();

    let recorder = DogStatsDBuilder::default()
        .with_remote_address(address.to_string())
        .expect("Failed to parse remote address")
        .set_global_prefix(&format!("conduit.{}", metrics_config.service_name))
        .with_global_labels(labels)
        .build()
        .expect("Could not create DogStatsDRecorder");

    metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder")
}
