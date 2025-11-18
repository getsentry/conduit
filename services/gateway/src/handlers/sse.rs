use prost::Message;
use prost_types::value::Kind;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tracing::instrument;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        Sse,
        sse::{Event, KeepAlive},
    },
};
use broker::{RedisOperations, StreamEvents, StreamKey};
use futures_util::Stream;
use rand::{Rng, rng};
use redis::streams::StreamReadOptions;
use sentry_protos::conduit::v1alpha::PublishRequest;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    auth::{TokenValidationError, validate_token},
    state::AppState,
};

const STREAM_BLOCK_MS: usize = 1000;
const STREAM_MAX_EVENTS: usize = 20;
const MAX_CONSECUTIVE_ERRORS: u64 = 10;
const ERROR_BACKOFF_BASE_MS: u64 = 100;
const JITTER_MS_MIN: u64 = 50;
const JITTER_MS_MAX: u64 = 500;

fn proto_struct_to_json(s: Option<&prost_types::Struct>) -> serde_json::Value {
    match s {
        None => serde_json::Value::Null,
        Some(s) => {
            let mut map = serde_json::Map::new();
            for (k, v) in &s.fields {
                map.insert(k.clone(), proto_value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
    }
}

fn proto_value_to_json(v: &prost_types::Value) -> serde_json::Value {
    match &v.kind {
        Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::NumberValue(n)) => json!(n),
        Some(Kind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Kind::StructValue(s)) => proto_struct_to_json(Some(s)),
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.iter().map(proto_value_to_json).collect())
        }
        None => serde_json::Value::Null,
    }
}

#[derive(Serialize)]
struct SSEEvent {
    event_type: String,
    message_id: Uuid,
    channel_id: Uuid,
    phase: String,
    sequence: u64,
    payload: serde_json::Value,
}

pub fn create_event_stream<R: RedisOperations>(
    redis: Arc<R>,
    org_id: u64,
    channel_id: Uuid,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    let stream_key = StreamKey::new(org_id, channel_id);
    let stream_read_opts = StreamReadOptions::default()
        .block(STREAM_BLOCK_MS)
        .count(STREAM_MAX_EVENTS);
    let mut last_id = "0-0".to_string();
    let mut consecutive_errors = 0;

    async_stream::stream! {
        'outer: loop {
            let poll_start = std::time::Instant::now();
            let stream_events: StreamEvents = match redis.poll(&stream_key, &last_id, &stream_read_opts).await {
                Ok(e) => {
                    metrics::histogram!("stream.redis_poll.latency_ms").record(poll_start.elapsed().as_millis() as f64);
                    consecutive_errors = 0;
                    metrics::gauge!("stream.consecutive_errors").set(0.0);
                    e
                }
                Err(_) => {
                    metrics::histogram!("stream.redis_poll.latency_ms").record(poll_start.elapsed().as_millis() as f64);
                    metrics::counter!("stream.redis_poll.errors").increment(1);
                    consecutive_errors += 1;
                    metrics::gauge!("stream.consecutive_errors").set(consecutive_errors as f64);
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        // TODO: use a proper event schema
                        yield Ok(Event::default()
                            .event("error")
                            .data("Too many errors, closing stream"));
                        metrics::counter!("connections.closed.error_limit").increment(1);
                        metrics::gauge!("connections.active").increment(1.0);
                        break 'outer;
                    }
                    sleep(Duration::from_millis(ERROR_BACKOFF_BASE_MS * consecutive_errors)).await;
                    continue;
                }
            };
            if stream_events.events.is_empty() {
                metrics::counter!("stream.redis_poll.empty").increment(1);
                let jitter_ms = rng().random_range(JITTER_MS_MIN..=JITTER_MS_MAX);
                sleep(Duration::from_millis(jitter_ms)).await;
                continue;
            }
            metrics::histogram!("stream.redis_poll.events_per_poll").record(stream_events.events.len() as f64);
            for (event_id, event_data) in stream_events.events {
                let publish_request: PublishRequest = match PublishRequest::decode(&*event_data) {
                    Ok(pr) => pr,
                    Err(_) => {
                        tracing::error!("Failed to decode protobuf for event {}", event_id);
                        metrics::counter!("stream.protobuf_decode.failed").increment(1);
                        last_id = event_id;
                        continue;
                    }
                };

                let message_id = match Uuid::parse_str(&publish_request.message_id) {
                    Ok(id) => id,
                    Err(_) => {
                        tracing::error!("Invalid message_id UUID {}", publish_request.message_id);
                        metrics::counter!("stream.uuid_parse.failed").increment(1);
                        last_id = event_id;
                        continue;
                    }
                };

                let channel_id = match Uuid::parse_str(&publish_request.channel_id) {
                    Ok(id) => id,
                    Err(_) => {
                        tracing::error!("Invalid channel_id UUID {}", publish_request.channel_id);
                        metrics::counter!("stream.uuid_parse.failed").increment(1);
                        last_id = event_id;
                        continue;
                    }
                };

                let sse_event = SSEEvent {
                    event_type: "stream".to_string(),
                    message_id,
                    channel_id,
                    phase: publish_request.phase().as_str_name().to_string(),
                    sequence: publish_request.sequence,
                    payload: proto_struct_to_json(publish_request.payload.as_ref()),
                };

                let sse_event_json = match serde_json::to_string(&sse_event) {
                    Ok(j) => j,
                    Err(_) => {
                        tracing::error!("Failed to serialize SSE event");
                        metrics::counter!("stream.json_serialize.failed").increment(1);
                        continue;
                    }
                };

                yield Ok(Event::default()
                    .event(&sse_event.event_type)
                    .id(&event_id)
                    .data(sse_event_json)
                );
                metrics::counter!("stream.events_delivered").increment(1);

                last_id = event_id.clone();

                if matches!(publish_request.phase(), sentry_protos::conduit::v1alpha::Phase::End) {
                    metrics::counter!("connections.closed.completed").increment(1);
                    metrics::gauge!("connections.active").decrement(1.0);
                    break 'outer;
                }
            }
        };
    }
}

#[derive(Deserialize)]
pub struct SSEQuery {
    channel_id: Uuid,
    token: String,
}

#[instrument(skip_all, fields(org_id = %org_id, channel_id = %q.channel_id))]
pub async fn sse_handler(
    State(state): State<AppState>,
    Path(org_id): Path<u64>,
    Query(q): Query<SSEQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, (StatusCode, String)> {
    metrics::counter!("connections.attempts").increment(1);
    let claims = validate_token(
        &q.token,
        org_id,
        q.channel_id,
        &state.jwt_config.public_key_pem,
        &state.jwt_config.expected_issuer,
        &state.jwt_config.expected_audience,
    )
    .map_err(|e| match e {
        TokenValidationError::TokenExpired => {
            metrics::counter!("auth.token_expired").increment(1);
            (StatusCode::UNAUTHORIZED, "Token expired".to_string())
        }
        TokenValidationError::OrgIdMismatch { .. }
        | TokenValidationError::ChannelIdMismatch { .. } => {
            metrics::counter!("auth.invalid_claims").increment(1);
            (StatusCode::FORBIDDEN, "Invalid token claims".to_string())
        }
        _ => {
            metrics::counter!("auth.token_validation.failed").increment(1);
            (StatusCode::UNAUTHORIZED, "Invalid token".to_string())
        }
    })?;

    metrics::counter!("auth.token_validation.success").increment(1);

    metrics::counter!("connections.total").increment(1);
    metrics::gauge!("connections.active").increment(1.0);

    let stream = create_event_stream(Arc::new(state.redis), claims.org_id, claims.channel_id);

    Ok(Sse::new(stream).keep_alive(KeepAlive::new()))
}

#[cfg(test)]
mod tests {
    mod proto_conversion_tests {
        use super::super::*;
        use prost_types::{ListValue, Struct, Value};
        use std::collections::BTreeMap;

        #[test]
        fn test_proto_value_to_json_basic_types() {
            let v = Value {
                kind: Some(Kind::NullValue(0)),
            };
            assert_eq!(proto_value_to_json(&v), json!(null));

            let v = Value { kind: None };
            assert_eq!(proto_value_to_json(&v), json!(null));

            let v = Value {
                kind: Some(Kind::BoolValue(true)),
            };
            assert_eq!(proto_value_to_json(&v), json!(true));

            let v = Value {
                kind: Some(Kind::NumberValue(42.0)),
            };
            assert_eq!(proto_value_to_json(&v), json!(42.0));

            let v = Value {
                kind: Some(Kind::StringValue("test".to_string())),
            };
            assert_eq!(proto_value_to_json(&v), json!("test"));
        }

        #[test]
        fn test_proto_value_to_json_list() {
            let values = vec![
                Value {
                    kind: Some(Kind::NumberValue(1.0)),
                },
                Value {
                    kind: Some(Kind::StringValue("two".to_string())),
                },
                Value {
                    kind: Some(Kind::BoolValue(true)),
                },
            ];
            let v = Value {
                kind: Some(Kind::ListValue(ListValue { values })),
            };
            assert_eq!(proto_value_to_json(&v), json!([1.0, "two", true]));
        }

        #[test]
        fn test_proto_struct_to_json_null() {
            assert_eq!(proto_struct_to_json(None), json!(null));
        }

        #[test]
        fn test_proto_struct_to_json_empty() {
            let proto_struct = Struct {
                fields: BTreeMap::new(),
            };
            assert_eq!(proto_struct_to_json(Some(&proto_struct)), json!({}));
        }

        #[test]
        fn test_proto_struct_to_json_with_fields() {
            let mut fields = BTreeMap::new();
            fields.insert(
                "name".to_string(),
                Value {
                    kind: Some(Kind::StringValue("test".to_string())),
                },
            );
            fields.insert(
                "count".to_string(),
                Value {
                    kind: Some(Kind::NumberValue(42.0)),
                },
            );

            let proto_struct = Struct { fields };
            let result = proto_struct_to_json(Some(&proto_struct));

            assert_eq!(
                result,
                json!({
                    "name": "test",
                    "count": 42.0,
                })
            );
        }

        #[test]
        fn test_proto_struct_to_json_nested() {
            let mut inner_fields = BTreeMap::new();
            inner_fields.insert(
                "street".to_string(),
                Value {
                    kind: Some(Kind::StringValue("45 Fremont St".to_string())),
                },
            );
            inner_fields.insert(
                "city".to_string(),
                Value {
                    kind: Some(Kind::StringValue("San Francisco".to_string())),
                },
            );

            let mut outer_fields = BTreeMap::new();
            outer_fields.insert(
                "company".to_string(),
                Value {
                    kind: Some(Kind::StringValue("Sentry".to_string())),
                },
            );
            outer_fields.insert(
                "address".to_string(),
                Value {
                    kind: Some(Kind::StructValue(Struct {
                        fields: inner_fields,
                    })),
                },
            );
            outer_fields.insert(
                "employee_count".to_string(),
                Value {
                    kind: Some(Kind::NumberValue(300.0)),
                },
            );

            let proto_struct = Struct {
                fields: outer_fields,
            };
            let result = proto_struct_to_json(Some(&proto_struct));

            assert_eq!(
                result,
                json!({
                    "company": "Sentry",
                    "address": {
                        "street": "45 Fremont St",
                        "city": "San Francisco",
                    },
                    "employee_count": 300.0,
                })
            );
        }
    }

    mod sse_stream_tests {
        use std::collections::BTreeMap;

        use super::super::*;
        use broker::{MockRedisOperations, StreamEvents};
        use futures_util::StreamExt;
        use prost_types::{Struct, Timestamp};
        use sentry_protos::conduit::v1alpha::Phase;

        #[tokio::test]
        async fn test_create_event_stream_with_events() {
            let channel_id = Uuid::new_v4();

            let start_publish_request = PublishRequest {
                message_id: Uuid::new_v4().to_string(),
                channel_id: channel_id.clone().to_string(),
                phase: Phase::Start.into(),
                sequence: 1,
                client_timestamp: Some(Timestamp {
                    seconds: 1736467200,
                    nanos: 0,
                }),
                payload: Some(Struct {
                    fields: BTreeMap::new(),
                }),
            };
            let delta_publish_request = PublishRequest {
                message_id: Uuid::new_v4().to_string(),
                channel_id: channel_id.clone().to_string(),
                phase: Phase::Delta.into(),
                sequence: 2,
                client_timestamp: Some(Timestamp {
                    seconds: 1736467400,
                    nanos: 0,
                }),
                payload: Some(Struct {
                    fields: BTreeMap::new(),
                }),
            };
            let end_publish_request = PublishRequest {
                message_id: Uuid::new_v4().to_string(),
                channel_id: channel_id.clone().to_string(),
                phase: Phase::End.into(),
                sequence: 2,
                client_timestamp: Some(Timestamp {
                    seconds: 1736467600,
                    nanos: 0,
                }),
                payload: Some(Struct {
                    fields: BTreeMap::new(),
                }),
            };

            let mut mock_redis = MockRedisOperations::new();
            mock_redis.expect_poll().returning(move |_, _, _| {
                Ok(StreamEvents {
                    events: vec![
                        ("1-0".to_string(), start_publish_request.encode_to_vec()),
                        ("2-0".to_string(), delta_publish_request.encode_to_vec()),
                        ("3-0".to_string(), end_publish_request.encode_to_vec()),
                    ],
                })
            });

            let mut stream = Box::pin(create_event_stream(Arc::new(mock_redis), 123, channel_id));

            let mut events = vec![];
            while let Some(result) = stream.next().await {
                events.push(result.expect("Event should be Ok"));
            }

            assert_eq!(events.len(), 3);
        }

        #[tokio::test]
        async fn test_create_event_stream_errors_close_stream() {
            let mut mock_redis = MockRedisOperations::new();

            mock_redis
                .expect_poll()
                .times(MAX_CONSECUTIVE_ERRORS as usize)
                .returning(|_, _, _| {
                    let redis_err =
                        redis::RedisError::from((redis::ErrorKind::IoError, "Connection refused"));
                    Err(broker::Error::Redis(redis_err))
                });

            let mut stream = Box::pin(create_event_stream(
                Arc::new(mock_redis),
                123,
                Uuid::new_v4(),
            ));

            let mut events = vec![];
            while let Some(result) = stream.next().await {
                events.push(result.expect("Event should be Ok"));
            }

            // Only one event is expected: error
            assert_eq!(events.len(), 1);
        }

        #[tokio::test]
        async fn test_create_event_stream_invalid_events_are_skipped() {
            let channel_id = Uuid::new_v4();
            let end_publish_request = PublishRequest {
                message_id: Uuid::new_v4().to_string(),
                channel_id: channel_id.to_string(),
                phase: Phase::End.into(),
                sequence: 2,
                client_timestamp: Some(Timestamp {
                    seconds: 1736467600,
                    nanos: 0,
                }),
                payload: Some(Struct {
                    fields: BTreeMap::new(),
                }),
            };

            let mut mock_redis = MockRedisOperations::new();
            mock_redis.expect_poll().returning(move |_, _, _| {
                Ok(StreamEvents {
                    events: vec![
                        ("1-0".to_string(), vec![0xFF, 0xBA]), // Invalid proto
                        ("2-0".to_string(), end_publish_request.encode_to_vec()),
                    ],
                })
            });

            let mut stream = Box::pin(create_event_stream(Arc::new(mock_redis), 123, channel_id));

            let mut events = vec![];
            while let Some(result) = stream.next().await {
                events.push(result.expect("Event should be Ok"));
            }

            assert_eq!(events.len(), 1);
        }

        #[tokio::test]
        async fn test_create_event_stream_intermittent_errors_recovery() {
            let channel_id = Uuid::new_v4();
            let start_publish_request = PublishRequest {
                message_id: Uuid::new_v4().to_string(),
                channel_id: channel_id.clone().to_string(),
                phase: Phase::Start.into(),
                sequence: 1,
                client_timestamp: Some(Timestamp {
                    seconds: 1736467200,
                    nanos: 0,
                }),
                payload: Some(Struct {
                    fields: BTreeMap::new(),
                }),
            };
            let end_publish_request = PublishRequest {
                message_id: Uuid::new_v4().to_string(),
                channel_id: channel_id.to_string(),
                phase: Phase::End.into(),
                sequence: 2,
                client_timestamp: Some(Timestamp {
                    seconds: 1736467600,
                    nanos: 0,
                }),
                payload: Some(Struct {
                    fields: BTreeMap::new(),
                }),
            };

            let mut mock_redis = MockRedisOperations::new();
            let mut num_failures = 0;
            const EXPECTED_FAILURES: u32 = 5;
            mock_redis.expect_poll().returning(move |_, _, _| {
                if num_failures < EXPECTED_FAILURES {
                    num_failures += 1;
                    let redis_err =
                        redis::RedisError::from((redis::ErrorKind::IoError, "Connection refused"));
                    return Err(broker::Error::Redis(redis_err));
                }
                Ok(StreamEvents {
                    events: vec![
                        ("1-0".to_string(), start_publish_request.encode_to_vec()),
                        ("2-0".to_string(), end_publish_request.encode_to_vec()),
                    ],
                })
            });

            let mut stream = Box::pin(create_event_stream(Arc::new(mock_redis), 123, channel_id));

            let mut events = vec![];
            while let Some(result) = stream.next().await {
                events.push(result.expect("Event should be Ok"));
            }

            assert_eq!(events.len(), 2);
        }
    }
}
