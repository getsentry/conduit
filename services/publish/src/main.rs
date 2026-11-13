use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    routing::{get, post},
};
use broker::RedisClient;
use dotenvy::dotenv;
use loadshedding::{LoadShedder, MemoryMonitor, MonitorConfig};
use monitoring::logging;
use publish::{
    handlers::{
        health::{healthz_handler, readyz_handler},
        publish::{DEFAULT_MAX_STREAM_LEN, publish_handler},
    },
    state::{AppState, JwtConfig},
};
use tokio::{net::TcpListener, runtime::Runtime, time::Instant};

const SERVICE_NAME: &str = "publish";
const POOL_METRICS_INTERVAL_SEC: u64 = 10;

fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let logging_config = logging::LoggingConfig::new(
        std::env::var("SENTRY_DSN").ok(),
        std::env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
        SERVICE_NAME,
        std::env::var("PUBLISH_TRACES_SAMPLE_RATE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1.0),
        std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
            logging::LogFormat::Json
        } else {
            logging::LogFormat::Text
        },
    );

    let metrics_config = {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("service".to_string(), SERVICE_NAME.to_string());
        tags.insert(
            "environment".to_string(),
            std::env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
        );

        monitoring::metrics::MetricsConfig::from_env(SERVICE_NAME, tags)
    };

    let runtime = Runtime::new()?;
    monitoring::init(logging_config, metrics_config, &runtime);
    runtime.block_on(async_main())
}

/// Registers the load shedding monitors enabled via `LOADSHEDDING_*` env vars.
async fn load_shedder() -> anyhow::Result<LoadShedder> {
    let mut shedder = LoadShedder::new();
    if let Some(config) = MonitorConfig::from_env("MEMORY") {
        shedder.add(MemoryMonitor::new().await?, config);
    }
    Ok(shedder)
}

async fn async_main() -> anyhow::Result<(), anyhow::Error> {
    let port = std::env::var("PUBLISH_PORT")?;
    let redis_url = std::env::var("REDIS_URL")?;

    let broker = RedisClient::new(redis_url.as_str()).await?;

    let jwt_config = JwtConfig::from_env()?;

    let load_shedder = load_shedder().await?;
    load_shedder.start().await?;
    let load_shedder = Arc::new(load_shedder);
    let max_stream_len = match std::env::var("PUBLISH_MAX_STREAM_LEN") {
        Ok(v) => v.parse()?,
        Err(_) => DEFAULT_MAX_STREAM_LEN,
    };

    let state = AppState {
        redis: broker.clone(),
        start_time: Instant::now(),
        jwt_config,
        load_shedder,
        max_stream_len,
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
