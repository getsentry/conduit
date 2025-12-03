use std::{
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use axum::{Router, routing::get};
use broker::RedisClient;
use dotenvy::dotenv;
use gateway::{
    handlers::{
        health::{healthz_handler, readyz_handler},
        sse::sse_handler,
    },
    state::{AppState, JwtConfig},
};
use http::Method;
use monitoring::logging;
use tokio::{net::TcpListener, runtime::Runtime, time::Instant};
use tower_http::cors::{Any, CorsLayer};

const SERVICE_NAME: &str = "gateway";
const POOL_METRICS_INTERVAL_SEC: u64 = 10;

fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let logging_config = logging::LoggingConfig::new(
        std::env::var("GATEWAY_SENTRY_DSN").ok(),
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
    let port = std::env::var("GATEWAY_PORT")?;
    let redis_url = std::env::var("REDIS_URL")?;

    let broker = RedisClient::new(redis_url.as_str()).await?;

    let jwt_config = JwtConfig::from_env()?;

    let state = AppState {
        redis: broker,
        start_time: Instant::now(),
        jwt_config,
    };

    let redis_for_metrics = state.redis.clone();

    // CORS: Allow all origins. JWT authentication (not cookies) is our security boundary.
    // If we switch to cookie-based auth, we will need to restrict origins to prevent CSRF.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET])
        .allow_headers(Any);

    let app = Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/events/{org_id}", get(sse_handler))
        .layer(cors)
        .with_state(state);

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
