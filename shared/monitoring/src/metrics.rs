use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::layers::{Layer, PrefixLayer};
use std::{collections::BTreeMap, net::SocketAddr};

const PROMETHEUS_ENABLED_ENV: &str = "PROMETHEUS_METRICS_ENABLED";
const PROMETHEUS_PORT_ENV: &str = "PROMETHEUS_METRICS_PORT";
const DEFAULT_PROMETHEUS_PORT: u16 = 9090;

#[derive(Debug, PartialEq)]
enum ExporterConfig {
    DogStatsD(SocketAddr),
    Prometheus(SocketAddr),
}

pub struct MetricsConfig {
    service_name: String,
    exporter: ExporterConfig,
    default_tags: BTreeMap<String, String>,
}

impl MetricsConfig {
    pub fn from_env(service_name: &str, default_tags: BTreeMap<String, String>) -> Option<Self> {
        let statsd_addr = std::env::var("STATSD_ADDR").ok().map(|address| {
            use std::net::ToSocketAddrs;

            let socket_addrs = address
                .to_socket_addrs()
                .expect("Could not resolve into a socket address");
            let [statsd_addr] = socket_addrs.as_slice() else {
                unreachable!("Expect statsd_addr to resolve into a single socket address");
            };
            *statsd_addr
        });
        let prometheus_enabled = std::env::var(PROMETHEUS_ENABLED_ENV)
            .map(|value| value == "true" || value == "1")
            .unwrap_or(false);
        let prometheus_port = std::env::var(PROMETHEUS_PORT_ENV)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_PROMETHEUS_PORT);

        Self::from_parts(
            service_name,
            default_tags,
            statsd_addr,
            prometheus_enabled,
            prometheus_port,
        )
    }

    fn from_parts(
        service_name: &str,
        default_tags: BTreeMap<String, String>,
        statsd_addr: Option<SocketAddr>,
        prometheus_enabled: bool,
        prometheus_port: u16,
    ) -> Option<Self> {
        let exporter = if prometheus_enabled {
            ExporterConfig::Prometheus(SocketAddr::from(([0, 0, 0, 0], prometheus_port)))
        } else {
            ExporterConfig::DogStatsD(statsd_addr?)
        };

        Some(Self {
            service_name: service_name.to_string(),
            exporter,
            default_tags,
        })
    }
}

pub fn init(metrics_config: MetricsConfig) {
    let service_name = metrics_config.service_name;

    match metrics_config.exporter {
        ExporterConfig::DogStatsD(address) => {
            let labels: Vec<Label> = metrics_config
                .default_tags
                .into_iter()
                .map(|(key, value)| Label::new(key, value))
                .collect();

            let recorder = DogStatsDBuilder::default()
                .with_remote_address(address.to_string())
                .expect("Failed to parse remote address")
                .set_global_prefix(format!("conduit.{}", service_name))
                .with_global_labels(labels)
                .build()
                .expect("Could not create DogStatsDRecorder");

            metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder")
        }
        ExporterConfig::Prometheus(address) => {
            let mut builder = PrometheusBuilder::new().with_http_listener(address);
            for (key, value) in metrics_config.default_tags {
                builder = builder.add_global_label(key, value);
            }

            let (recorder, exporter) = builder
                .build()
                .expect("Could not create Prometheus recorder/exporter");
            let recorder = PrefixLayer::new(format!("conduit.{}", service_name)).layer(recorder);

            metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder");
            tokio::spawn(exporter);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statsd_is_the_default_exporter() {
        let address = SocketAddr::from(([127, 0, 0, 1], 8125));
        let config = MetricsConfig::from_parts(
            "gateway",
            BTreeMap::new(),
            Some(address),
            false,
            DEFAULT_PROMETHEUS_PORT,
        )
        .expect("StatsD should enable metrics");

        assert_eq!(config.exporter, ExporterConfig::DogStatsD(address));
    }

    #[test]
    fn metrics_are_disabled_without_an_exporter() {
        let config = MetricsConfig::from_parts(
            "gateway",
            BTreeMap::new(),
            None,
            false,
            DEFAULT_PROMETHEUS_PORT,
        );

        assert!(config.is_none());
    }

    #[test]
    fn prometheus_does_not_require_statsd() {
        let config = MetricsConfig::from_parts(
            "gateway",
            BTreeMap::new(),
            None,
            true,
            DEFAULT_PROMETHEUS_PORT,
        )
        .expect("Prometheus should enable metrics without StatsD");

        assert_eq!(
            config.exporter,
            ExporterConfig::Prometheus(SocketAddr::from(([0, 0, 0, 0], DEFAULT_PROMETHEUS_PORT,)))
        );
    }

    #[test]
    fn prometheus_metrics_keep_the_conduit_prefix() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let recorder = PrefixLayer::new("conduit.gateway").layer(recorder);

        metrics::with_local_recorder(&recorder, || {
            metrics::gauge!("connections.active").set(42.0);
        });

        assert!(
            handle
                .render()
                .contains("conduit_gateway_connections_active 42")
        );
    }

    #[tokio::test]
    async fn prometheus_listener_exposes_metrics() {
        let address = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("Should reserve a local port")
            .local_addr()
            .expect("Should read the local address");
        let config =
            MetricsConfig::from_parts("gateway", BTreeMap::new(), None, true, address.port())
                .expect("Prometheus should enable metrics");
        init(config);

        metrics::gauge!("connections.active").set(42.0);

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("Should connect to the Prometheus listener");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("Should write the scrape request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("Should read the scrape response");

        assert!(response.contains("conduit_gateway_connections_active 42"));
    }
}
