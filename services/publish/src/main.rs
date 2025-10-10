use std::{
    net::{SocketAddr, ToSocketAddrs},
    ops::Sub,
    time::Duration,
};

use axum::{
    Router,
    routing::{get, post},
};
use broker::{RedisClient, RedisOperations};
use chrono::Utc;
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
const WORKER_INTERVAL_SEC: u64 = 300; // 5 minutes
const STREAM_IDLE_SEC: i64 = 300; // 5 minutes

async fn cleanup_old_stream(
    redis: &impl RedisOperations,
    cutoff_timestamp: i64,
) -> anyhow::Result<(usize, usize, usize)> {
    let old_streams = redis.get_old_streams(cutoff_timestamp).await?;
    let mut deleted = 0;
    let mut untracked = 0;
    let total = old_streams.len();
    for old_stream in old_streams {
        match redis.delete_stream(&old_stream).await {
            Ok(res) => {
                if res {
                    deleted += 1;
                }
            }
            Err(e) => {
                tracing::error!(error = %e, stream = ?old_stream, "Failed to delete stream");
            }
        };
        match redis.untrack_stream(&old_stream).await {
            Ok(res) => {
                if res {
                    untracked += 1;
                }
            }
            Err(e) => {
                tracing::error!(error = %e, stream = ?old_stream, "Failed to untrack stream");
            }
        };
    }
    Ok((total, deleted, untracked))
}

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

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    println!("Running on http://{}", addr);

    // Background worker to clean up abandoned streams.
    // Runs every WORKER_INTERVAL_SEC seconds and deletes streams with no activity for STREAM_IDLE_SEC+ seconds.
    // Streams that reach Phase::End are untracked immediately since Redis TTL handles cleanup.
    tokio::spawn(async move {
        tracing::info!(
            interval_sec = WORKER_INTERVAL_SEC,
            idle_threshold_sec = STREAM_IDLE_SEC,
            "Stream cleanup worker started",
        );
        let mut interval = tokio::time::interval(Duration::from_secs(WORKER_INTERVAL_SEC));
        loop {
            interval.tick().await;
            let cutoff_timestamp = Utc::now().timestamp().sub(STREAM_IDLE_SEC);
            let (total, deleted, untracked) =
                match cleanup_old_stream(&broker, cutoff_timestamp).await {
                    Ok(counts) => counts,
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to get old streams for cleanup");
                        continue;
                    }
                };
            tracing::info!(total, deleted, untracked, "Cleanup cycle completed");
        }
    });

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("Shutting down...");
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::{MockRedisOperations, StreamKey};
    use mockall::predicate::eq;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_cleanup_success() {
        let mut mock = MockRedisOperations::new();
        let channel_id = Uuid::new_v4();

        mock.expect_get_old_streams()
            .with(eq(1000))
            .returning(move |_| Ok(vec![StreamKey::new(123, channel_id).as_redis_key()]));

        mock.expect_delete_stream().returning(|_| Ok(true));

        mock.expect_untrack_stream().returning(|_| Ok(true));

        let (total, deleted, untracked) = cleanup_old_stream(&mock, 1000).await.unwrap();

        assert_eq!(total, 1);
        assert_eq!(deleted, 1);
        assert_eq!(untracked, 1);
    }
}
