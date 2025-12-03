use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use axum::{
    Router,
    routing::{get, post},
};
use broker::RedisClient;
use dotenvy::dotenv;
use monitoring::logging;
use publish::{
    handlers::{
        health::{healthz_handler, readyz_handler},
        publish::publish_handler,
    },
    state::{AppState, JwtConfig},
};
use tokio::{net::TcpListener, runtime::Runtime, time::Instant};

const SERVICE_NAME: &str = "publish";
const POOL_METRICS_INTERVAL_SEC: u64 = 10;

fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let logging_config = logging::LoggingConfig::new(
        std::env::var("PUBLISH_SENTRY_DSN").ok(),
        std::env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
        1.0,
        std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
            logging::LogFormat::Json
        } else {
            logging::LogFormat::Text
        },
    );

    let metrics_config = if let Ok(statsd_addr_str) = std::env::var("STATSD_ADDR") {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("service".to_string(), SERVICE_NAME.to_string());
        tags.insert(
            "environment".to_string(),
            std::env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
        );

        let socket_addrs = statsd_addr_str
            .to_socket_addrs()
            .expect("Could not resolve into a socket address");
        let [statsd_addr] = socket_addrs.as_slice() else {
            unreachable!("Expect statsd_addr to resolve into a single socket address");
        };

        Some(monitoring::metrics::MetricsConfig::new(
            SERVICE_NAME,
            *statsd_addr,
            tags,
        ))
    } else {
        None
    };

    monitoring::init(logging_config, metrics_config);

    let runtime = Runtime::new()?;
    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<(), anyhow::Error> {
    let port = std::env::var("PUBLISH_PORT")?;
    let redis_url = std::env::var("REDIS_URL")?;

    let broker = RedisClient::new(redis_url.as_str()).await?;

    let jwt_config = JwtConfig::from_env()?;

    let state = AppState {
        redis: broker.clone(),
        start_time: Instant::now(),
        jwt_config,
    };

    let redis_for_metrics = state.redis.clone();

    let mut app = Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/publish/{org_id}/{channel_id}", post(publish_handler))
        .with_state(state);

    #[cfg(debug_assertions)]
    {
        use tower_http::cors::{Any, CorsLayer};

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        app = app.layer(cors);
    }

    tokio::spawn({
        async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(POOL_METRICS_INTERVAL_SEC));
            loop {
                interval.tick().await;
                let status = redis_for_metrics.pool_status();

                metrics::gauge!("redis.pool.size").set(status.size as f64);
                metrics::gauge!("redis.pool.available").set(status.available as f64);

                let active = status.size - status.available;
                metrics::gauge!("redis.pool.active").set(active as f64);
            }
        }
    });

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    println!("Running on http://{}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("Shutting down...");
        })
        .await?;

    Ok(())
}
