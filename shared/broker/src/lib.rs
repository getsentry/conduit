use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, from_redis_value};

use mockall::automock;
use uuid::Uuid;

const STREAM_DATA_FIELD: &str = "data";

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
    async fn ping(&self) -> Result<String>;
    async fn publish(&self, key: &StreamKey, data: Vec<u8>) -> Result<String>;
    async fn poll(
        &self,
        key: &StreamKey,
        last_id: &str,
        opts: &StreamReadOptions,
    ) -> Result<StreamEvents>;
    async fn set_ttl(&self, key: &StreamKey, seconds: i64) -> Result<bool>;
    async fn trim_stream(&self, key: &StreamKey, max_len: usize) -> Result<i64>;
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

    async fn publish(&self, key: &StreamKey, data: Vec<u8>) -> Result<String> {
        let mut conn = self.conn.clone();
        let id: String = conn
            .xadd(key.as_redis_key(), "*", &[(STREAM_DATA_FIELD, data)])
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

    async fn trim_stream(&self, key: &StreamKey, max_len: usize) -> Result<i64> {
        let mut conn = self.conn.clone();
        let trimmed: i64 = conn
            .xtrim(key.as_redis_key(), StreamMaxlen::Approx(max_len))
            .await?;
        Ok(trimmed)
    }
}
