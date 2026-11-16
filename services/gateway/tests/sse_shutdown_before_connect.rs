//! Separate binary: elegant-departure's shutdown state is global and cannot be reset.

mod common;

use broker::MockRedisOperations;

/// A stream opened after shutdown has started sends retry and then shutdown,
/// without polling Redis.
#[tokio::test]
async fn test_stream_opened_during_shutdown_closes_without_polling() {
    drop(elegant_departure::shutdown());

    let mut redis = MockRedisOperations::new();
    redis.expect_poll().never();

    assert_eq!(common::collect_events(redis).await, 2);
}
