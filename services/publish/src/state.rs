use std::env;

use broker::RedisClient;
use tokio::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub redis: RedisClient,
    pub start_time: Instant,
    pub jwt_config: JwtConfig,
    pub cleanup_config: CleanupConfig,
}

#[derive(Clone)]
pub struct JwtConfig {
    pub expected_issuer: String,
    pub expected_audience: String,
    pub secret: String,
}

impl JwtConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let issuer = env::var("PUBLISH_JWT_ISSUER")?;
        let audience = env::var("PUBLISH_JWT_AUDIENCE")?;
        let secret = env::var("PUBLISH_JWT_SECRET")?;

        Ok(Self {
            expected_issuer: issuer,
            expected_audience: audience,
            secret,
        })
    }
}

#[derive(Clone)]
pub struct CleanupConfig {
    pub worker_interval_sec: u64,
    pub stream_idle_sec: i64,
}

impl CleanupConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let worker_interval_sec = env::var("CLEANUP_WORKER_INTERVAL_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300); // Default: 5 minutes

        let stream_idle_sec = env::var("CLEANUP_STREAM_IDLE_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300); // Default: 5 minutes

        Ok(Self {
            worker_interval_sec,
            stream_idle_sec,
        })
    }
}

#[derive(Clone)]
pub struct HealthState {
    pub start_time: Instant,
}

impl axum::extract::FromRef<AppState> for HealthState {
    fn from_ref(app: &AppState) -> Self {
        Self {
            start_time: app.start_time,
        }
    }
}
