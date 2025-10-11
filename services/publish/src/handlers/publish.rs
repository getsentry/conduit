use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use broker::{RedisOperations, StreamKey};
use chrono::Utc;
use prost::Message;

use sentry_protos::conduit::v1alpha::{Phase, PublishRequest};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    auth::{JwtValidationError, validate_token},
    state::AppState,
};

const STREAM_TTL_SEC: i64 = 300; // 5 minutes
const MAX_STREAM_LEN: usize = 500;

async fn do_publish<R: RedisOperations>(
    redis: &R,
    org_id: u64,
    channel_id: Uuid,
    body: Bytes,
) -> Result<String, (StatusCode, String)> {
    let stream_event = PublishRequest::decode(body.clone())
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid protobuf".to_string()))?;

    let stream_key = StreamKey::new(org_id, channel_id);

    redis
        .track_stream_update(&stream_key, Utc::now().timestamp())
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error occurred while publishing".to_string(),
            )
        })?;

    let id = redis
        .publish(&stream_key, body.to_vec(), MAX_STREAM_LEN)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error occurred while publishing".to_string(),
            )
        })?;

    if matches!(stream_event.phase(), Phase::End) {
        match redis.set_ttl(&stream_key, STREAM_TTL_SEC).await {
            Ok(_) => {
                // TTL is set, untrack so worker doesn't process it
                if let Err(e) = redis.untrack_stream(&stream_key.as_redis_key()).await {
                    tracing::error!(error = %e, stream = ?stream_key.as_redis_key(), "Failed to untrack stream")
                }
            }
            Err(e) => {
                // TTL wasn't set, worker will have to clean it up
                tracing::error!(error = %e, stream = ?stream_key.as_redis_key(), "Failed to set TTL");
            }
        }
    }

    Ok(id)
}

#[instrument(skip_all, fields(org_id = %org_id, channel_id = %channel_id))]
pub async fn publish_handler(
    State(state): State<AppState>,
    Path((org_id, channel_id)): Path<(u64, Uuid)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<String, (StatusCode, String)> {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or((
            StatusCode::UNAUTHORIZED,
            "Missing authorization header".to_string(),
        ))?;

    let token = auth_header.strip_prefix("Bearer ").ok_or((
        StatusCode::UNAUTHORIZED,
        "Invalid authorization format".to_string(),
    ))?;

    validate_token(
        token,
        state.jwt_config.secret.as_bytes(),
        &state.jwt_config.expected_issuer,
        &state.jwt_config.expected_audience,
    )
    .map_err(|e| match e {
        JwtValidationError::TokenExpired => (StatusCode::UNAUTHORIZED, "Token expired".to_string()),
        JwtValidationError::ValidationFailed(_) => (
            StatusCode::UNAUTHORIZED,
            "Token validation failed".to_string(),
        ),
    })?;

    do_publish(&state.redis, org_id, channel_id, body).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::{Error as BrokerError, MockRedisOperations};
    use mockall::predicate::*;
    use prost_types::{Struct, Timestamp};
    use redis::RedisError;
    use sentry_protos::conduit::v1alpha::Phase;

    #[tokio::test]
    async fn test_publish_success() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_track_stream_update()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| Ok(1));

        mock_redis
            .expect_publish()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
                eq(MAX_STREAM_LEN),
            )
            .times(1)
            .returning(|_, _, _| Ok("stream-id-123".to_string()));

        mock_redis.expect_set_ttl().never();

        let request = PublishRequest {
            channel_id: channel_id.to_string(),
            message_id: Uuid::new_v4().to_string(),
            client_timestamp: Some(Timestamp {
                seconds: 1736467200,
                nanos: 0,
            }),
            phase: Phase::Delta.into(),
            sequence: 2,
            payload: Some(Struct {
                fields: std::collections::BTreeMap::new(),
            }),
        };
        let body = Bytes::from(request.encode_to_vec());

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "stream-id-123");
    }

    #[tokio::test]
    async fn test_publish_ttl_on_end() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_track_stream_update()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| Ok(1));

        mock_redis
            .expect_publish()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
                eq(MAX_STREAM_LEN),
            )
            .times(1)
            .returning(|_, _, _| Ok("stream-id-123".to_string()));

        mock_redis
            .expect_set_ttl()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                eq(STREAM_TTL_SEC),
            )
            .times(1)
            .returning(|_, _| Ok(true));

        mock_redis
            .expect_untrack_stream()
            .with(function(move |key: &str| {
                key == format!("stream:123:{}", channel_id)
            }))
            .times(1)
            .returning(|_| Ok(1));

        let request = PublishRequest {
            channel_id: channel_id.to_string(),
            message_id: Uuid::new_v4().to_string(),
            client_timestamp: Some(Timestamp {
                seconds: 1736467200,
                nanos: 0,
            }),
            phase: Phase::End.into(),
            sequence: 2,
            payload: Some(Struct {
                fields: std::collections::BTreeMap::new(),
            }),
        };
        let body = Bytes::from(request.encode_to_vec());

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "stream-id-123");
    }

    #[tokio::test]
    async fn test_publish_track_failure() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_track_stream_update()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| {
                Err(BrokerError::Redis(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        mock_redis.expect_publish().never();

        mock_redis.expect_set_ttl().never();
        mock_redis.expect_untrack_stream().never();

        let request = PublishRequest {
            channel_id: channel_id.to_string(),
            message_id: Uuid::new_v4().to_string(),
            client_timestamp: Some(Timestamp {
                seconds: 1736467200,
                nanos: 0,
            }),
            phase: Phase::Delta.into(),
            sequence: 2,
            payload: Some(Struct {
                fields: std::collections::BTreeMap::new(),
            }),
        };
        let body = Bytes::from(request.encode_to_vec());

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        // Track failure should prevent publish
        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(msg, "Error occurred while publishing");
    }

    #[tokio::test]
    async fn test_publish_end_ttl_fails_stays_tracked() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_track_stream_update()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| Ok(1));

        mock_redis
            .expect_publish()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
                eq(MAX_STREAM_LEN),
            )
            .times(1)
            .returning(|_, _, _| Ok("stream-id-123".to_string()));

        mock_redis
            .expect_set_ttl()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                eq(STREAM_TTL_SEC),
            )
            .times(1)
            .returning(|_, _| {
                Err(BrokerError::Redis(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        mock_redis.expect_untrack_stream().never();

        let request = PublishRequest {
            channel_id: channel_id.to_string(),
            message_id: Uuid::new_v4().to_string(),
            client_timestamp: Some(Timestamp {
                seconds: 1736467200,
                nanos: 0,
            }),
            phase: Phase::End.into(),
            sequence: 2,
            payload: Some(Struct {
                fields: std::collections::BTreeMap::new(),
            }),
        };
        let body = Bytes::from(request.encode_to_vec());

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "stream-id-123");
    }

    #[tokio::test]
    async fn test_publish_end_untrack_fails_still_succeeds() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_track_stream_update()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| Ok(1));

        mock_redis
            .expect_publish()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
                eq(MAX_STREAM_LEN),
            )
            .times(1)
            .returning(|_, _, _| Ok("stream-id-123".to_string()));

        mock_redis
            .expect_set_ttl()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                eq(STREAM_TTL_SEC),
            )
            .times(1)
            .returning(|_, _| Ok(true));

        mock_redis
            .expect_untrack_stream()
            .with(function(move |key: &str| {
                key == format!("stream:123:{}", channel_id)
            }))
            .times(1)
            .returning(|_| {
                Err(BrokerError::Redis(RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        let request = PublishRequest {
            channel_id: channel_id.to_string(),
            message_id: Uuid::new_v4().to_string(),
            client_timestamp: Some(Timestamp {
                seconds: 1736467200,
                nanos: 0,
            }),
            phase: Phase::End.into(),
            sequence: 2,
            payload: Some(Struct {
                fields: std::collections::BTreeMap::new(),
            }),
        };
        let body = Bytes::from(request.encode_to_vec());

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "stream-id-123");
    }

    #[tokio::test]
    async fn test_publish_invalid_protobuf() {
        let mock_redis = MockRedisOperations::new();

        let body = Bytes::from(vec![255, 255, 255, 255]);

        let result = do_publish(&mock_redis, 123, Uuid::new_v4(), body).await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("Invalid protobuf"));
    }

    #[tokio::test]
    async fn test_publish_xadd_error() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_track_stream_update()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| Ok(1));

        mock_redis.expect_publish().returning(|_, _, _| {
            Err(BrokerError::Redis(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Connection lost",
            ))))
        });

        mock_redis.expect_set_ttl().never();
        mock_redis.expect_untrack_stream().never();

        let request = PublishRequest {
            channel_id: channel_id.to_string(),
            message_id: Uuid::new_v4().to_string(),
            client_timestamp: Some(Timestamp {
                seconds: 1736467200,
                nanos: 0,
            }),
            phase: Phase::Delta.into(),
            sequence: 2,
            payload: Some(Struct {
                fields: std::collections::BTreeMap::new(),
            }),
        };
        let body = Bytes::from(request.encode_to_vec());

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(msg, "Error occurred while publishing");
    }
}
