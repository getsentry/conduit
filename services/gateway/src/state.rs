use broker::RedisClient;
use tokio::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub redis: RedisClient,
    pub start_time: Instant,
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
