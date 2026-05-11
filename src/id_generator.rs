use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IdGeneratorConfig {
    Atomic,
    Uuid,
    Redis {
        url: String,
        #[serde(default)]
        key: Option<String>,
    },
}

impl Default for IdGeneratorConfig {
    fn default() -> Self {
        IdGeneratorConfig::Atomic
    }
}

pub fn create_id_generator(
    config: &IdGeneratorConfig,
) -> std::io::Result<Box<dyn MessageIdGenerator>> {
    match config {
        IdGeneratorConfig::Atomic => Ok(Box::new(AtomicIdGenerator::new(1))),
        IdGeneratorConfig::Uuid => Ok(Box::new(UuidIdGenerator::new())),
        IdGeneratorConfig::Redis { url, key } => {
            Ok(Box::new(RedisIdGenerator::new(url, key.as_deref())?))
        }
    }
}

#[async_trait]
pub trait MessageIdGenerator: Send + Sync {
    async fn generate(&self, prefix: &str) -> String;
}

/// Generates IDs using a local atomic counter (current default behavior).
pub struct AtomicIdGenerator {
    counter: AtomicU64,
}

impl AtomicIdGenerator {
    pub fn new(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }
}

#[async_trait]
impl MessageIdGenerator for AtomicIdGenerator {
    async fn generate(&self, prefix: &str) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}_{:012x}", prefix, n)
    }
}

/// Generates IDs using UUID v4.
pub struct UuidIdGenerator;

impl UuidIdGenerator {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MessageIdGenerator for UuidIdGenerator {
    async fn generate(&self, prefix: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        format!("{}_{}", prefix, uuid)
    }
}

/// Generates IDs using a Redis INCR counter, similar to AtomicIdGenerator
/// but with a shared counter persisted in Redis.
pub struct RedisIdGenerator {
    client: redis::Client,
    key: String,
}

impl RedisIdGenerator {
    pub fn new(redis_url: &str, key: Option<&str>) -> std::io::Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Redis connection error: {}", e),
            )
        })?;
        Ok(Self {
            client,
            key: key.unwrap_or("smpp:id_counter").to_string(),
        })
    }
}

#[async_trait]
impl MessageIdGenerator for RedisIdGenerator {
    async fn generate(&self, prefix: &str) -> String {
        use redis::AsyncCommands;
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .expect("Failed to get Redis connection for ID generation");
        let n: u64 = conn
            .incr(&self.key, 1u64)
            .await
            .expect("Failed to increment Redis ID counter");
        format!("{}_{:012x}", prefix, n)
    }
}
