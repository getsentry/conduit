use std::{collections::HashMap, time::Duration};

use axum::{Json, extract::State, http::StatusCode};
use broker::RedisOperations;
use serde::Serialize;
use tokio::time::timeout;

use crate::state::{AppState, HealthState};

const REDIS_PING_TIMEOUT: u64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Ok,
    Fail,
}

#[derive(Serialize)]
pub struct HealthResponse {
    status: Status,
    uptime: u64,
}

pub async fn healthz_handler(
    State(state): State<HealthState>,
) -> (StatusCode, Json<HealthResponse>) {
    let uptime = state.start_time.elapsed().as_secs();
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: Status::Ok,
            uptime,
        }),
    )
}

#[derive(Serialize)]
pub struct ReadyResponse {
    status: Status,
    dependencies: HashMap<&'static str, Status>,
}

async fn check_readiness<R: RedisOperations>(redis: &R) -> ReadyResponse {
    let mut deps = HashMap::new();
    let mut status = Status::Ok;

    match timeout(Duration::from_secs(REDIS_PING_TIMEOUT), redis.ping()).await {
        Ok(Ok(_)) => {
            deps.insert("redis", Status::Ok);
        }
        _ => {
            deps.insert("redis", Status::Fail);
            status = Status::Fail;
        }
    }

    ReadyResponse {
        status,
        dependencies: deps,
    }
}

pub async fn readyz_handler(State(state): State<AppState>) -> (StatusCode, Json<ReadyResponse>) {
    let response = check_readiness(&state.redis).await;
    let status_code = match response.status {
        Status::Ok => StatusCode::OK,
        Status::Fail => StatusCode::SERVICE_UNAVAILABLE,
    };

    (status_code, Json(response))
}

#[cfg(test)]
mod tests {
    use crate::state::HealthState;

    use super::*;
    use axum::http::StatusCode;
    use broker::{Error as BrokerError, MockRedisOperations};
    use tokio::time::Instant;

    #[tokio::test]
    async fn test_healthz_returns_ok() {
        let state = HealthState {
            start_time: Instant::now(),
        };
        let (status, response) = healthz_handler(State(state)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(response.status, Status::Ok);
    }

    #[tokio::test]
    async fn test_readyz_redis_healthy() {
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_ping()
            .times(1)
            .returning(|| Ok("PONG".to_string()));

        let response = check_readiness(&mock_redis).await;

        assert_eq!(response.status, Status::Ok);
        assert_eq!(response.dependencies.get("redis"), Some(&Status::Ok));
    }

    #[tokio::test]
    async fn test_readyz_redis_error() {
        let mut mock_redis = MockRedisOperations::new();

        mock_redis.expect_ping().times(1).returning(|| {
            Err(BrokerError::Redis(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Connection refused",
            ))))
        });

        let response = check_readiness(&mock_redis).await;

        assert_eq!(response.status, Status::Fail);
        assert_eq!(response.dependencies.get("redis"), Some(&Status::Fail));
    }
}
