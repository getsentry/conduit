use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, from_redis_value};

use mockall::automock;
use uuid::Uuid;

const STREAM_DATA_FIELD: &str = "data";
const STREAM_TIMESTAMPS: &str = "stream_timestamps";

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
    conn: ConnectionManager,
}

impl RedisClient {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl RedisOperations for RedisClient {
    async fn ping(&self) -> Result<String> {
        let mut conn = self.conn.clone();
        conn.ping().await.map_err(Into::into)
    }

    async fn publish(&self, key: &StreamKey, data: Vec<u8>, max_len: usize) -> Result<String> {
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
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
        let mut conn = self.conn.clone();
        let res: bool = conn.expire(key.as_redis_key(), seconds).await?;
        Ok(res)
    }

    async fn track_stream_update(&self, key: &StreamKey, timestamp: i64) -> Result<usize> {
        let mut conn = self.conn.clone();
        let res: usize = conn
            .zadd(STREAM_TIMESTAMPS, key.as_redis_key(), timestamp)
            .await?;
        Ok(res)
    }

    async fn get_old_streams(&self, cutoff_timestamp: i64) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let old_streams: Vec<String> = conn
            .zrangebyscore(STREAM_TIMESTAMPS, -f32::INFINITY, cutoff_timestamp)
            .await?;
        Ok(old_streams)
    }

    async fn untrack_stream(&self, key: &str) -> Result<usize> {
        let mut conn = self.conn.clone();
        let res: usize = conn.zrem(STREAM_TIMESTAMPS, key).await?;
        Ok(res)
    }

    async fn delete_stream(&self, key: &str) -> Result<usize> {
        let mut conn = self.conn.clone();
        let res: usize = conn.del(key).await?;
        Ok(res)
    }
}
