use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
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

const STREAM_TTL_SEC: i64 = 120; // 2 minutes
const MAX_STREAM_LEN: usize = 1200;
const MAX_MESSAGE_SIZE_BYTES: usize = 16 * 1024; // 16KB
const MAX_PUBLISH_RATE_PER_CHANNEL: usize = 20; // 20 publishes
const RATE_LIMIT_WINDOW_SEC: i64 = 1; // per 1 sec

async fn do_publish<R: RedisOperations>(
    redis: &R,
    org_id: u64,
    channel_id: Uuid,
    body: Bytes,
) -> Result<String, (StatusCode, String)> {
    if body.len() > MAX_MESSAGE_SIZE_BYTES {
        metrics::counter!("handler.publish_size_exceeded").increment(1);
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "Message size {} exceeds maximum of {} bytes",
                body.len(),
                MAX_MESSAGE_SIZE_BYTES
            ),
        ));
    }

    let channel_rate_limit_key = format!("rate_limit:channel:{}:{}", org_id, channel_id);
    let allowed = redis
        .check_rate_limit(
            &channel_rate_limit_key,
            MAX_PUBLISH_RATE_PER_CHANNEL,
            RATE_LIMIT_WINDOW_SEC
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, org_id = %org_id, channel_id = %channel_id, "Failed to check per-channel rate limit");
            metrics::counter!("handler.rate_limit_check.failed").increment(1);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to check rate limit".to_string(),
            )
        })?;

    if !allowed {
        metrics::counter!("handler.publish_rate_limited.channel").increment(1);
        tracing::warn!(
            org_id = %org_id,
            channel_id = %channel_id,
            limit = MAX_PUBLISH_RATE_PER_CHANNEL,
            window = RATE_LIMIT_WINDOW_SEC,
            "Channel rate limit exceeded"
        );
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "Rate limit exceeded: maximum {} requests per {} second(s) per channel",
                MAX_PUBLISH_RATE_PER_CHANNEL, RATE_LIMIT_WINDOW_SEC
            ),
        ));
    }

    metrics::histogram!("handler.publish.message_size_bytes").record(body.len() as f64);

    let stream_event = PublishRequest::decode(body.clone())
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid protobuf".to_string()))?;

    let stream_key = StreamKey::new(org_id, channel_id);

    // Track stream activity BEFORE publish to prevent orphaned streams.
    redis
        .track_stream_update(&stream_key, Utc::now().timestamp())
        .await
        .map_err(|_| {
            // If tracking fails, we avoid creating untracked streams.
            metrics::counter!("track_update.failed").increment(1);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to track stream update".to_string(),
            )
        })?;
    metrics::counter!("track_update.success").increment(1);

    let id = redis
        .publish(&stream_key, body.to_vec(), MAX_STREAM_LEN)
        .await
        .map_err(|_| {
            metrics::counter!("handler.publish.failed").increment(1);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to publish message to stream".to_string(),
            )
        })?;
    metrics::counter!("handler.publish.success").increment(1);

    if matches!(stream_event.phase(), Phase::End) {
        match redis.set_ttl(&stream_key, STREAM_TTL_SEC).await {
            Ok(_) => {
                // TTL is set, untrack so worker doesn't process it
                metrics::counter!("ttl.set.success").increment(1);
                if let Err(e) = redis.untrack_stream(&stream_key.as_redis_key()).await {
                    metrics::counter!("untrack.failed").increment(1);
                    tracing::error!(error = %e, stream = ?stream_key.as_redis_key(), "Failed to untrack stream")
                } else {
                    metrics::counter!("untrack.success").increment(1);
                }
            }
            Err(e) => {
                // TTL wasn't set, worker will have to clean it up
                metrics::counter!("ttl.set.failed").increment(1);
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
) -> Response {
    struct LatencyGuard {
        start: std::time::Instant,
    }
    impl Drop for LatencyGuard {
        fn drop(&mut self) {
            metrics::histogram!("handler.publish.latency_ms")
                .record(self.start.elapsed().as_millis() as f64);
        }
    }
    let _guard = LatencyGuard {
        start: std::time::Instant::now(),
    };

    let auth_header = match headers.get("authorization").and_then(|v| v.to_str().ok()) {
        Some(h) => h,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                "Missing authorization header".to_string(),
            )
                .into_response();
        }
    };

    let token = match auth_header.strip_prefix("Bearer ") {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                "Invalid authorization format".to_string(),
            )
                .into_response();
        }
    };

    if let Err(e) = validate_token(
        token,
        state.jwt_config.secret.as_bytes(),
        &state.jwt_config.expected_issuer,
        &state.jwt_config.expected_audience,
    ) {
        let message = match e {
            JwtValidationError::TokenExpired => "Token expired".to_string(),
            JwtValidationError::ValidationFailed(_) => "Token validation failed".to_string(),
        };
        return (StatusCode::UNAUTHORIZED, message).into_response();
    };

    match do_publish(&state.redis, org_id, channel_id, body).await {
        Ok(stream_id) => stream_id.into_response(),
        Err((status, message)) => {
            let mut response = (status, message).into_response();

            if status == StatusCode::TOO_MANY_REQUESTS {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
            }

            response
        }
    }
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
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

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
    async fn test_publish_rejects_oversized_raw_bytes() {
        let channel_id = Uuid::new_v4();
        let mock_redis = MockRedisOperations::new();

        let body = Bytes::from(vec![0u8; MAX_MESSAGE_SIZE_BYTES + 1000]);

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_publish_ttl_on_end() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

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
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

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
        assert_eq!(msg, "Failed to track stream update");
    }

    #[tokio::test]
    async fn test_publish_end_ttl_fails_stays_tracked() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

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
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

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
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        let body = Bytes::from(vec![255, 255, 255, 255]);

        mock_redis
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

        let result = do_publish(&mock_redis, 123, channel_id, body).await;

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
            .expect_check_rate_limit()
            .times(1)
            .returning(|_, _, _| Ok(true));

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
        assert_eq!(msg, "Failed to publish message to stream");
    }

    #[tokio::test]
    async fn test_publish_channel_rate_limit_exceeded() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_check_rate_limit()
            .withf(move |key, limit, _| {
                key.contains("rate_limit:channel:123")
                    && key.contains(&channel_id.to_string())
                    && *limit == MAX_PUBLISH_RATE_PER_CHANNEL
            })
            .times(1)
            .returning(|_, _, _| Ok(false));

        mock_redis.expect_track_stream_update().never();
        mock_redis.expect_publish().never();

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
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert!(msg.contains("Rate limit exceeded"));
    }

    #[tokio::test]
    async fn test_publish_channel_rate_limit_failed() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_check_rate_limit()
            .withf(move |key, limit, _| {
                key.contains("rate_limit:channel:123")
                    && key.contains(&channel_id.to_string())
                    && *limit == MAX_PUBLISH_RATE_PER_CHANNEL
            })
            .times(1)
            .returning(|_, _, _| {
                Err(BrokerError::Redis(redis::RedisError::from((
                    redis::ErrorKind::IoError,
                    "Connection lost",
                ))))
            });

        mock_redis.expect_track_stream_update().never();
        mock_redis.expect_publish().never();

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
        assert_eq!(msg, "Failed to check rate limit");
    }
}
