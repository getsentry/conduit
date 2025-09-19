use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
};
use broker::{RedisOperations, StreamKey};
use prost::Message;

use sentry_protos::conduit::v1alpha::PublishRequest;
use uuid::Uuid;

use crate::state::AppState;

async fn do_publish<R: RedisOperations>(
    redis: &R,
    org_id: u64,
    channel_id: Uuid,
    body: Bytes,
) -> Result<String, (StatusCode, String)> {
    let _stream_event = PublishRequest::decode(body.clone())
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid protobuf".to_string()))?;

    let stream_key = StreamKey::new(org_id, channel_id);

    let id = redis
        .publish(&stream_key, body.to_vec())
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error occurred while publishing".to_string(),
            )
        })?;

    Ok(id)
}

pub async fn publish_handler(
    State(state): State<AppState>,
    Path((org_id, channel_id)): Path<(u64, Uuid)>,
    body: Bytes,
) -> Result<String, (StatusCode, String)> {
    do_publish(&state.redis, org_id, channel_id, body).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::{Error as BrokerError, MockRedisOperations};
    use mockall::predicate::*;
    use prost_types::{Struct, Timestamp};
    use sentry_protos::conduit::v1alpha::Phase;

    #[tokio::test]
    async fn test_publish_success() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis
            .expect_publish()
            .with(
                function(move |key: &StreamKey| {
                    key.as_redis_key() == format!("stream:123:{}", channel_id)
                }),
                always(),
            )
            .times(1)
            .returning(|_, _| Ok("stream-id-123".to_string()));

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
    async fn test_publish_redis_error() {
        let channel_id = Uuid::new_v4();
        let mut mock_redis = MockRedisOperations::new();

        mock_redis.expect_publish().returning(|_, _| {
            Err(BrokerError::Redis(redis::RedisError::from((
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
