use std::net::{SocketAddr, ToSocketAddrs};

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
use monitoring::logging;
use tokio::{net::TcpListener, runtime::Runtime, time::Instant};

const SERVICE_NAME: &str = "gateway";

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

    let mut app = Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/events/{org_id}", get(sse_handler))
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

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    println!("Running on http://{}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("Shutting down...");
        })
        .await?;

    Ok(())
}
