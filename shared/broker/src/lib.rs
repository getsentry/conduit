use std::time::Duration;

use async_trait::async_trait;
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, from_redis_value};

use mockall::automock;
use uuid::Uuid;

const STREAM_DATA_FIELD: &str = "data";

// Single sorted set tracks all stream activity. Could be a write hotspot in Redis Cluster at very high scale.
// Consider sharding the set if ZADD becomes a bottleneck.
const STREAM_TIMESTAMPS: &str = "stream_timestamps";

const DEFAULT_REDIS_POOL_MAX_SIZE: usize = 256;
const DEFAULT_REDIS_POOL_WAIT_TIMEOUT_SECS: u64 = 5;
const DEFAULT_REDIS_POOL_CREATE_TIMEOUT_SECS: u64 = 3;
const DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_SECS: u64 = 3;

pub struct StreamKey {
    org_id: u64,
    channel_id: Uuid,
}

impl StreamKey {
    pub fn new(org_id: u64, channel_id: Uuid) -> Self {
        Self { org_id, channel_id }
    }

    pub fn as_redis_key(&self) -> String {
        format!("stream:{}:{}", self.org_id, self.channel_id)
    }
}

pub struct StreamEvents {
    pub events: Vec<(String, Vec<u8>)>, // (id, data)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Pool error: {0}")]
    Pool(#[from] deadpool_redis::PoolError),

    #[error("Pool creation error: {0}")]
    CreatePool(#[from] deadpool_redis::CreatePoolError),
}

type Result<T> = std::result::Result<T, Error>;

#[automock]
#[async_trait]
pub trait RedisOperations: Send + Sync {
    /// Verifies Redis connectivity by sending a ping command.
    ///
    /// # Returns
    /// "Pong"
    async fn ping(&self) -> Result<String>;

    /// Publishes data to a stream, trimming to approximately `max_len` messages.
    ///
    /// # Returns
    /// The id of the event that was added
    async fn publish(&self, key: &StreamKey, data: Vec<u8>, max_len: usize) -> Result<String>;

    /// Polls for new events after `last_id` with the given options.
    ///
    /// # Returns
    /// Stream events after the specified ID
    async fn poll(
        &self,
        key: &StreamKey,
        last_id: &str,
        opts: &StreamReadOptions,
    ) -> Result<StreamEvents>;

    /// Sets the expiration time for a stream.
    ///
    /// # Returns
    /// Whether the expiration was set
    async fn set_ttl(&self, key: &StreamKey, seconds: i64) -> Result<bool>;

    /// Records the last activity timestamp for a stream in a sorted set.
    ///
    /// This is used by the cleanup worker to identify inactive streams. Each time
    /// a stream receives activity, this should be called to update its timestamp
    /// in the `stream_timestamps` sorted set.
    ///
    /// # Returns
    /// The number of new elements added (0 if updating existing, 1 if new)
    async fn track_stream_update(&self, key: &StreamKey, timestamp: i64) -> Result<usize>;

    /// Retrieves streams that haven't been updated since the cutoff timestamp.
    ///
    /// Queries the `stream_timestamps` sorted set for all streams with timestamps
    /// less than or equal to `cutoff_timestamp`. Used by the cleanup worker to find
    /// inactive streams eligible for deletion.
    ///
    /// # Returns
    /// Vector of stream keys as strings
    async fn get_old_streams(&self, cutoff_timestamp: i64) -> Result<Vec<String>>;

    /// Removes a stream from activity tracking without deleting the stream data.
    ///
    /// Removes the stream key from `stream_timestamps` sorted set. This should
    /// be called after deleting a stream.
    ///
    /// # Returns
    /// Number of elements removed (1 if found, 0 if not present)
    async fn untrack_stream(&self, key: &str) -> Result<usize>;

    /// Deletes the actual stream and all its messages from Redis.
    ///
    /// This removes the Redis stream key and all associated data. Typically called
    /// by the cleanup worker after a stream has been identified as inactive.
    ///
    /// # Returns
    /// Number of streams deleted
    async fn delete_stream(&self, key: &str) -> Result<usize>;
}

#[derive(Clone)]
pub struct RedisClient {
    pool: deadpool_redis::cluster::Pool,
}

impl RedisClient {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let max_size = std::env::var("REDIS_POOL_MAX_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_REDIS_POOL_MAX_SIZE);
        let wait_timeout_seconds = std::env::var("REDIS_POOL_WAIT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_REDIS_POOL_WAIT_TIMEOUT_SECS);
        let create_timeout_seconds = std::env::var("REDIS_POOL_CREATE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_REDIS_POOL_CREATE_TIMEOUT_SECS);
        let recycle_timeout_seconds = std::env::var("REDIS_POOL_RECYCLE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_REDIS_POOL_RECYCLE_TIMEOUT_SECS);
        let cfg = deadpool_redis::cluster::Config {
            urls: Some(vec![redis_url.to_string()]),
            pool: Some(deadpool_redis::PoolConfig {
                max_size,
                timeouts: deadpool_redis::Timeouts {
                    wait: Some(Duration::from_secs(wait_timeout_seconds)),
                    create: Some(Duration::from_secs(create_timeout_seconds)),
                    recycle: Some(Duration::from_secs(recycle_timeout_seconds)),
                },
                ..Default::default()
            }),
            connections: None,
            ..Default::default()
        };
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl RedisOperations for RedisClient {
    async fn ping(&self) -> Result<String> {
        let mut conn = self.pool.get().await?;
        conn.ping().await.map_err(Into::into)
    }

    async fn publish(&self, key: &StreamKey, data: Vec<u8>, max_len: usize) -> Result<String> {
        let mut conn = self.pool.get().await?;
        let id: String = conn
            .xadd_maxlen(
                key.as_redis_key(),
                StreamMaxlen::Approx(max_len),
                "*",
                &[(STREAM_DATA_FIELD, data)],
            )
            .await?;
        Ok(id)
    }

    async fn poll(
        &self,
        key: &StreamKey,
        last_id: &str,
        opts: &StreamReadOptions,
    ) -> Result<StreamEvents> {
        let mut conn = self.pool.get().await?;
        let reply: StreamReadReply = conn
            .xread_options(&[&key.as_redis_key()], &[last_id], opts)
            .await?;
        let events = reply
            .keys
            .into_iter()
            .flat_map(|stream_key| stream_key.ids)
            .map(|stream_id| {
                let data = stream_id
                    .map
                    .get(STREAM_DATA_FIELD)
                    .and_then(|v| from_redis_value::<Vec<u8>>(v).ok())
                    .unwrap_or_default();
                (stream_id.id, data)
            })
            .collect();
        Ok(StreamEvents { events })
    }

    async fn set_ttl(&self, key: &StreamKey, seconds: i64) -> Result<bool> {
        let mut conn = self.pool.get().await?;
        let res: bool = conn.expire(key.as_redis_key(), seconds).await?;
        Ok(res)
    }

    async fn track_stream_update(&self, key: &StreamKey, timestamp: i64) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let res: usize = conn
            .zadd(STREAM_TIMESTAMPS, key.as_redis_key(), timestamp)
            .await?;
        Ok(res)
    }

    async fn get_old_streams(&self, cutoff_timestamp: i64) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;
        let old_streams: Vec<String> = conn
            .zrangebyscore(STREAM_TIMESTAMPS, -f32::INFINITY, cutoff_timestamp)
            .await?;
        Ok(old_streams)
    }

    async fn untrack_stream(&self, key: &str) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let res: usize = conn.zrem(STREAM_TIMESTAMPS, key).await?;
        Ok(res)
    }

    async fn delete_stream(&self, key: &str) -> Result<usize> {
        let mut conn = self.pool.get().await?;
        let res: usize = conn.del(key).await?;
        Ok(res)
    }
}
