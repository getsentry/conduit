use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::layers::{Layer, PrefixLayer};
use std::{collections::BTreeMap, net::SocketAddr};

const PROMETHEUS_PORT_ENV: &str = "PROMETHEUS_METRICS_PORT";
const DEFAULT_PROMETHEUS_PORT: u16 = 9090;

pub struct MetricsConfig {
    service_name: String,
    listen_addr: SocketAddr,
    default_tags: BTreeMap<String, String>,
}

impl MetricsConfig {
    pub fn from_env(service_name: &str, default_tags: BTreeMap<String, String>) -> Self {
        let prometheus_port = std::env::var(PROMETHEUS_PORT_ENV)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_PROMETHEUS_PORT);

        Self::from_parts(service_name, default_tags, prometheus_port)
    }

    fn from_parts(
        service_name: &str,
        default_tags: BTreeMap<String, String>,
        prometheus_port: u16,
    ) -> Self {
        Self {
            service_name: service_name.to_string(),
            listen_addr: SocketAddr::from(([0, 0, 0, 0], prometheus_port)),
            default_tags,
        }
    }
}

pub fn init(metrics_config: MetricsConfig) {
    let mut builder = PrometheusBuilder::new().with_http_listener(metrics_config.listen_addr);
    for (key, value) in metrics_config.default_tags {
        builder = builder.add_global_label(key, value);
    }

    let (recorder, exporter) = builder
        .build()
        .expect("Could not create Prometheus recorder/exporter");
    let recorder =
        PrefixLayer::new(format!("conduit.{}", metrics_config.service_name)).layer(recorder);

    metrics::set_global_recorder(recorder).expect("Could not set global metrics recorder");
    tokio::spawn(exporter);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_uses_the_configured_listener() {
        let config = MetricsConfig::from_parts("gateway", BTreeMap::new(), DEFAULT_PROMETHEUS_PORT);

        assert_eq!(
            config.listen_addr,
            SocketAddr::from(([0, 0, 0, 0], DEFAULT_PROMETHEUS_PORT))
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
        let config = MetricsConfig::from_parts("gateway", BTreeMap::new(), address.port());
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
