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
    state::{AppState, CleanupConfig, JwtConfig},
};
use tokio::{net::TcpListener, runtime::Runtime, select, time::Instant};
use tokio_util::sync::CancellationToken;

const SERVICE_NAME: &str = "publish";

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
            Ok(res) => deleted += res,
            Err(e) => {
                tracing::error!(error = %e, stream = ?old_stream, "Failed to delete stream");
                metrics::counter!("cleanup.errors.delete").increment(1);
                // Keep tracked so worker retries. Don't orphan streams that still exist
                continue;
            }
        };
        match redis.untrack_stream(&old_stream).await {
            Ok(res) => untracked += res,
            Err(e) => {
                tracing::error!(error = %e, stream = ?old_stream, "Failed to untrack stream");
                metrics::counter!("cleanup.errors.untrack").increment(1);
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
    let cleanup_config = CleanupConfig::from_env()?;

    let state = AppState {
        redis: broker.clone(),
        start_time: Instant::now(),
        jwt_config,
        cleanup_config: cleanup_config.clone(),
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

    let shutdown_token = CancellationToken::new();
    let worker_token = shutdown_token.clone();

    // Background worker to clean up abandoned streams.
    // Runs every worker_interval_sec and deletes streams with no activity for stream_idle_sec+ seconds.
    // Streams that reach Phase::End are untracked immediately since Redis TTL handles cleanup.
    tokio::spawn(async move {
        tracing::info!(
            interval_sec = cleanup_config.worker_interval_sec,
            idle_threshold_sec = cleanup_config.stream_idle_sec,
            "Stream cleanup worker started",
        );
        let mut interval =
            tokio::time::interval(Duration::from_secs(cleanup_config.worker_interval_sec));
        loop {
            select! {
                _ = interval.tick() => {
                    let start = Instant::now();
                    let cutoff_timestamp = Utc::now().timestamp().sub(cleanup_config.stream_idle_sec);

                    let (total, deleted, untracked) =
                        match cleanup_old_stream(&broker, cutoff_timestamp).await {
                            Ok(counts) => {
                                metrics::counter!("cleanup.cycles.completed").increment(1);
                                counts
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to get old streams for cleanup");
                                metrics::counter!("cleanup.cycles.failed").increment(1);
                                continue;
                            }
                        };
                    metrics::counter!("cleanup.streams.found").increment(total as u64);
                    metrics::counter!("cleanup.streams.deleted").increment(deleted as u64);
                    metrics::counter!("cleanup.streams.untracked").increment(untracked as u64);

                    let duration_ms = start.elapsed().as_millis() as f64;
                    metrics::histogram!("cleanup.duration_ms").record(duration_ms);

                    tracing::info!(total, deleted, untracked, duration_ms, "Cleanup cycle completed");
                }
                _ = worker_token.cancelled() => {
                    tracing::info!("Cleanup worker shutting down gracefully");
                    break;
                }
            }
        }
    });

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("Shutting down...");
            shutdown_token.cancel();
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::{Error as BrokerError, MockRedisOperations, StreamKey};
    use mockall::predicate::eq;
    use redis::RedisError;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_cleanup_success() {
        let mut mock_redis = MockRedisOperations::new();
        let stream_key = StreamKey::new(123, Uuid::new_v4()).as_redis_key();

        mock_redis
            .expect_get_old_streams()
            .with(eq(1000))
            .returning({
                let key = stream_key.clone();
                move |_| Ok(vec![key.clone()])
            });

        mock_redis
            .expect_delete_stream()
            .with(eq(stream_key.clone()))
            .times(1)
            .returning(|_| Ok(1));

        mock_redis
            .expect_untrack_stream()
            .with(eq(stream_key.clone()))
            .times(1)
            .returning(|_| Ok(1));

        let (total, deleted, untracked) = cleanup_old_stream(&mock_redis, 1000).await.unwrap();

        assert_eq!(total, 1);
        assert_eq!(deleted, 1);
        assert_eq!(untracked, 1);
    }

    #[tokio::test]
    async fn test_cleanup_get_streams_fails() {
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_get_old_streams()
            .with(eq(1000))
            .returning(|_| {
                Err(BrokerError::Redis(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        let result = cleanup_old_stream(&mock_redis, 1000).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cleanup_no_old_streams() {
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_get_old_streams()
            .with(eq(1000))
            .returning(move |_| Ok(vec![]));

        mock_redis.expect_delete_stream().never();

        mock_redis.expect_untrack_stream().never();

        let (total, deleted, untracked) = cleanup_old_stream(&mock_redis, 1000).await.unwrap();

        assert_eq!(total, 0);
        assert_eq!(deleted, 0);
        assert_eq!(untracked, 0);
    }

    #[tokio::test]
    async fn test_cleanup_delete_fails() {
        let mut mock_redis = MockRedisOperations::new();
        let stream_key = StreamKey::new(123, Uuid::new_v4()).as_redis_key();

        mock_redis
            .expect_get_old_streams()
            .with(eq(1000))
            .returning({
                let key = stream_key.clone();
                move |_| Ok(vec![key.clone()])
            });

        mock_redis
            .expect_delete_stream()
            .with(eq(stream_key.clone()))
            .times(1)
            .returning(|_| {
                Err(BrokerError::Redis(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        mock_redis.expect_untrack_stream().never();

        let (total, deleted, untracked) = cleanup_old_stream(&mock_redis, 1000).await.unwrap();

        assert_eq!(total, 1);
        assert_eq!(deleted, 0);
        assert_eq!(untracked, 0);
    }

    #[tokio::test]
    async fn test_cleanup_untrack_fails() {
        let mut mock_redis = MockRedisOperations::new();
        let stream_key = StreamKey::new(123, Uuid::new_v4()).as_redis_key();

        mock_redis
            .expect_get_old_streams()
            .with(eq(1000))
            .returning({
                let key = stream_key.clone();
                move |_| Ok(vec![key.clone()])
            });

        mock_redis
            .expect_delete_stream()
            .with(eq(stream_key.clone()))
            .times(1)
            .returning(|_| Ok(1));

        mock_redis
            .expect_untrack_stream()
            .with(eq(stream_key.clone()))
            .times(1)
            .returning(|_| {
                Err(BrokerError::Redis(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        let (total, deleted, untracked) = cleanup_old_stream(&mock_redis, 1000).await.unwrap();

        assert_eq!(total, 1);
        assert_eq!(deleted, 1);
        assert_eq!(untracked, 0);
    }
}
