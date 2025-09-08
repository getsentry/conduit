use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;

use crate::state::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    status: String,
    uptime: u64,
}

pub async fn healthz_handler(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    let uptime = state.start_time.elapsed().as_secs();
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok".to_string(),
            uptime,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use tokio::time::Instant;

    fn mock_app_state() -> AppState {
        AppState {
            start_time: Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_healthz_returns_ok() {
        let state = mock_app_state();
        let (status, response) = healthz_handler(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.status, "ok");
    }
}
