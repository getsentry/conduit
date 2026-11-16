use std::sync::Arc;

use broker::MockRedisOperations;
use futures_util::StreamExt;
use gateway::handlers::sse::create_event_stream;
use uuid::Uuid;

pub async fn collect_events(redis: MockRedisOperations) -> usize {
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
