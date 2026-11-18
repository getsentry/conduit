//! Separate binary: elegant-departure's shutdown state is global and cannot be reset.

mod common;

use broker::{MockRedisOperations, StreamEvents};

/// An open stream that is waiting for events closes when shutdown starts,
/// sending retry and then shutdown.
#[tokio::test]
async fn test_open_stream_closes_on_shutdown() {
    let mut redis = MockRedisOperations::new();
    redis.expect_poll().times(1).returning(|_, _, _| {
        drop(elegant_departure::shutdown());
        Ok(StreamEvents { events: vec![] })
    });

    assert_eq!(common::collect_events(redis).await, 2);
}
