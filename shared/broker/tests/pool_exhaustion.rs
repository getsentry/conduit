//! Integration test demonstrating Redis connection pool exhaustion
//! caused by blocking XREAD calls from SSE clients.
//!
//! Requires a running Redis cluster (e.g. `devservices up`).
//!
//! Run with:
//!   REDIS_URL=redis://127.0.0.1:7100 \
//!     cargo test -p broker --test pool_exhaustion -- --ignored --nocapture

use std::sync::Arc;
use std::time::{Duration, Instant};

use broker::{Error, RedisClient, RedisOperations, StreamKey};
use redis::streams::StreamReadOptions;
use uuid::Uuid;

/// With pool_size=2 and 3 concurrent blocking polls, the third poll
/// must wait for a connection. With pool_size=2 and 4 concurrent polls,
/// the excess polls should eventually time out (default wait = 5s).
///
/// This simulates what happens when SSE clients outnumber pool connections:
/// each client's XREAD BLOCK holds a connection for up to 1s, and excess
/// clients are starved.
#[tokio::test]
#[ignore = "requires redis cluster: devservices up"]
async fn blocking_polls_exhaust_pool() {
    // SAFETY: single test in this binary, no other threads read env yet.
    unsafe {
        std::env::set_var("REDIS_POOL_MAX_SIZE", "2");
        // Keep default wait timeout (5s) to match production behavior.
    }

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:7100".to_string());
    let client = Arc::new(RedisClient::new(&redis_url).await.unwrap());

    // Spawn 2 blocking polls — they should hold both pool connections.
    let mut holders = Vec::new();
    for _ in 0..2 {
        let c = client.clone();
        holders.push(tokio::spawn(async move {
            let key = StreamKey::new(1, Uuid::new_v4());
            let opts = StreamReadOptions::default().block(1000).count(20);
            c.poll(&key, "$", &opts).await
        }));
    }

    // Give the two holders time to acquire connections and start blocking.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let status = client.pool_status();
    assert_eq!(
        status.available, 0,
        "both connections should be checked out"
    );

    // Third poll: must wait for a connection to free up.
    // The holders block for 1s, so this should succeed after ~800ms wait.
    let start = Instant::now();
    let key = StreamKey::new(1, Uuid::new_v4());
    let opts = StreamReadOptions::default().block(1000).count(20);
    let result = client.poll(&key, "$", &opts).await;
    let elapsed = start.elapsed();

    assert!(
        result.is_ok(),
        "third poll should succeed after a holder releases its connection, got: {:?}",
        result.err()
    );
    assert!(
        elapsed >= Duration::from_millis(500),
        "third poll should have waited for a connection (elapsed: {:?})",
        elapsed
    );
    println!(
        "third poll waited {:?} for a connection (pool_size=2)",
        elapsed
    );

    // Now demonstrate exhaustion: spawn 2 more blocking polls with a long
    // block so connections stay held. With pool_size=2, a subsequent poll
    // must time out waiting for a connection.
    let mut blockers = Vec::new();
    for _ in 0..2 {
        let c = client.clone();
        blockers.push(tokio::spawn(async move {
            let key = StreamKey::new(1, Uuid::new_v4());
            let opts = StreamReadOptions::default().block(10_000).count(20);
            c.poll(&key, "$", &opts).await
        }));
    }

    // Wait for blockers to acquire connections.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // This poll should time out after 5s (default wait timeout).
    let start = Instant::now();
    let key = StreamKey::new(1, Uuid::new_v4());
    let opts = StreamReadOptions::default().block(1000).count(20);
    let result = client.poll(&key, "$", &opts).await;
    let elapsed = start.elapsed();

    match result {
        Err(Error::Pool(e)) => {
            println!("pool exhaustion confirmed after {:?}: {}", elapsed, e);
            assert!(
                elapsed >= Duration::from_secs(4),
                "should have waited ~5s for timeout, got {:?}",
                elapsed
            );
        }
        other => panic!(
            "expected Pool timeout error, got: {:?}",
            other.map(|_| "Ok")
        ),
    }

    // Clean up: wait for holders and blockers to finish.
    for h in holders {
        let _ = h.await;
    }
    for b in blockers {
        let _ = b.await;
    }
}

/// Show that non-blocking operations (like publish) are also starved
/// when the pool is exhausted by blocking reads.
#[tokio::test]
#[ignore = "requires redis cluster: devservices up"]
async fn publish_starved_by_blocking_polls() {
    unsafe {
        std::env::set_var("REDIS_POOL_MAX_SIZE", "2");
    }

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:7100".to_string());
    let client = Arc::new(RedisClient::new(&redis_url).await.unwrap());

    // Occupy both connections with long-blocking XREADs.
    let mut blockers = Vec::new();
    for _ in 0..2 {
        let c = client.clone();
        blockers.push(tokio::spawn(async move {
            let key = StreamKey::new(1, Uuid::new_v4());
            let opts = StreamReadOptions::default().block(10_000).count(20);
            c.poll(&key, "$", &opts).await
        }));
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Even a simple publish (XADD) cannot get a connection.
    let start = Instant::now();
    let key = StreamKey::new(1, Uuid::new_v4());
    let result = client.publish(&key, b"test".to_vec(), 10).await;
    let elapsed = start.elapsed();

    match result {
        Err(Error::Pool(e)) => {
            println!("publish starved as expected after {:?}: {}", elapsed, e);
        }
        other => panic!(
            "expected Pool timeout error for publish, got: {:?}",
            other.map(|_| "Ok")
        ),
    }

    for b in blockers {
        let _ = b.await;
    }
}
