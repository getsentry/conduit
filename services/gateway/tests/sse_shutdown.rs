//! Separate binary: elegant-departure's shutdown state is global and cannot be reset.

use std::sync::Arc;

use broker::{MockRedisOperations, StreamEvents};
use futures_util::StreamExt;
use gateway::handlers::sse::create_event_stream;
use uuid::Uuid;

async fn collect_events(redis: MockRedisOperations) -> usize {
    let stream = create_event_stream(
        Arc::new(redis),
        123,
        Uuid::new_v4(),
        None,
        Arc::new(elegant_departure::get_shutdown_guard()),
    );
    let mut stream = Box::pin(stream);

    let mut events = vec![];
    while let Some(result) = stream.next().await {
        events.push(result.expect("Event should be Ok"));
    }
    events.len()
}

#[tokio::test]
async fn test_streams_close_on_shutdown() {
    let mut redis = MockRedisOperations::new();
    redis.expect_poll().times(1).returning(|_, _, _| {
        drop(elegant_departure::shutdown());
        Ok(StreamEvents { events: vec![] })
    });
    assert_eq!(collect_events(redis).await, 2);

    let mut redis = MockRedisOperations::new();
    redis.expect_poll().never();
    assert_eq!(collect_events(redis).await, 2);
}
