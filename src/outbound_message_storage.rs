use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::Mutex;

// ============================================================
// Outbound message and batch tracking
// ============================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutboundMessage {
    pub message_id: String,
    pub status: String,
    pub source: String,
    pub destination: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_binary: Option<String>,
    /// Application-level encoding label. Possible values: "GSM7" (default), "UCS2".
    pub encoding: String,
    /// SMPP data_coding value. Common values:
    /// 0 = SMSC Default (usually GSM7), 1 = IA5 (ASCII),
    /// 2 | 4 = Octet unspecified (binary), 3 = Latin 1 (ISO-8859-1),
    /// 5 = JIS, 6 = Cyrillic (ISO-8859-5), 7 = Latin/Hebrew (ISO-8859-8),
    /// 8 = UCS2 (UTF-16 BE), 9 = Pictogram, 10 = ISO-2022-JP,
    /// 13 = Extended Kanji JIS, 14 = KS C 5601 (Korean),
    /// 192–207 / 208–223 = GSM MWI control, 240–243 = GSM message class.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_coding: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduled_delivery_time: Option<String>,
    pub parts: u32,
    pub priority: String,
    pub tags: Vec<String>,
    pub callback_url: Option<String>,
    pub batch_id: Option<String>,
    pub created_at: String,
    pub sent_at: Option<String>,
    pub delivered_at: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchInfo {
    pub batch_id: String,
    pub name: Option<String>,
    pub total_messages: u32,
    pub queued: u32,
    pub sent: u32,
    pub delivered: u32,
    pub failed: u32,
    pub expired: u32,
    pub pending: u32,
    pub created_at: String,
    pub updated_at: String,
}

// ============================================================
// OutboundMessageStorage trait and implementations
// ============================================================

/// A scheduled message entry, sorted by delivery epoch time.
#[derive(Clone, Debug)]
pub struct ScheduledMessage {
    pub delivery_epoch: u64,
    pub message_id: String,
}

/// Abstraction for outbound message persistence.
#[async_trait]
pub trait OutboundMessageStorage: Send + Sync {
    /// Save an outbound message to storage.
    async fn save(&self, message: OutboundMessage) -> std::io::Result<()>;

    /// Get a single outbound message by its ID.
    async fn get(&self, message_id: &str) -> std::io::Result<Option<OutboundMessage>>;

    /// Update the status of an outbound message. Returns the updated message if found.
    async fn update_status(
        &self,
        message_id: &str,
        status: &str,
    ) -> std::io::Result<Option<OutboundMessage>>;

    /// List all outbound messages.
    async fn list(&self) -> std::io::Result<Vec<OutboundMessage>>;

    /// Add a scheduled message entry to the sorted queue.
    async fn add_scheduled(&self, entry: ScheduledMessage) -> std::io::Result<()>;

    /// Remove and return all scheduled messages whose delivery_epoch <= now.
    async fn take_due_messages(&self, now_epoch: u64) -> std::io::Result<Vec<String>>;

    /// Save a batch info record to storage.
    async fn save_batch(&self, batch: BatchInfo) -> std::io::Result<()>;

    /// Get a batch info record by its ID.
    async fn get_batch(&self, batch_id: &str) -> std::io::Result<Option<BatchInfo>>;
}

/// In-memory implementation of `OutboundMessageStorage` backed by a `HashMap`.
pub struct MemoryOutboundMessageStorage {
    messages: Mutex<HashMap<String, OutboundMessage>>,
    scheduled: Mutex<Vec<ScheduledMessage>>,
    batches: Mutex<HashMap<String, BatchInfo>>,
}

impl MemoryOutboundMessageStorage {
    pub fn new() -> Self {
        Self {
            messages: Mutex::new(HashMap::new()),
            scheduled: Mutex::new(Vec::new()),
            batches: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl OutboundMessageStorage for MemoryOutboundMessageStorage {
    async fn save(&self, message: OutboundMessage) -> std::io::Result<()> {
        self.messages
            .lock()
            .await
            .insert(message.message_id.clone(), message);
        Ok(())
    }

    async fn get(&self, message_id: &str) -> std::io::Result<Option<OutboundMessage>> {
        Ok(self.messages.lock().await.get(message_id).cloned())
    }

    async fn update_status(
        &self,
        message_id: &str,
        status: &str,
    ) -> std::io::Result<Option<OutboundMessage>> {
        let mut msgs = self.messages.lock().await;
        match msgs.get_mut(message_id) {
            Some(msg) => {
                msg.status = status.to_string();
                Ok(Some(msg.clone()))
            }
            None => Ok(None),
        }
    }

    async fn list(&self) -> std::io::Result<Vec<OutboundMessage>> {
        Ok(self.messages.lock().await.values().cloned().collect())
    }

    async fn add_scheduled(&self, entry: ScheduledMessage) -> std::io::Result<()> {
        let mut queue = self.scheduled.lock().await;
        let pos = queue
            .binary_search_by_key(&entry.delivery_epoch, |e| e.delivery_epoch)
            .unwrap_or_else(|p| p);
        queue.insert(pos, entry);
        Ok(())
    }

    async fn take_due_messages(&self, now_epoch: u64) -> std::io::Result<Vec<String>> {
        let mut queue = self.scheduled.lock().await;
        let mut due = Vec::new();
        while let Some(front) = queue.first() {
            if front.delivery_epoch <= now_epoch {
                due.push(queue.remove(0).message_id);
            } else {
                break;
            }
        }
        Ok(due)
    }

    async fn save_batch(&self, batch: BatchInfo) -> std::io::Result<()> {
        self.batches
            .lock()
            .await
            .insert(batch.batch_id.clone(), batch);
        Ok(())
    }

    async fn get_batch(&self, batch_id: &str) -> std::io::Result<Option<BatchInfo>> {
        Ok(self.batches.lock().await.get(batch_id).cloned())
    }
}

/// Redis-backed implementation of `OutboundMessageStorage`.
///
/// Messages are stored as JSON strings in a Redis hash keyed by message_id.
pub struct RedisOutboundMessageStorage {
    client: redis::Client,
    hash_key: String,
    batch_hash_key: String,
    scheduled: Mutex<Vec<ScheduledMessage>>,
}

impl RedisOutboundMessageStorage {
    pub fn new(redis_url: &str, key_prefix: Option<&str>) -> std::io::Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;
        let prefix = key_prefix.unwrap_or("smpp");
        Ok(Self {
            client,
            hash_key: format!("{}:outbound_messages", prefix),
            batch_hash_key: format!("{}:batches", prefix),
            scheduled: Mutex::new(Vec::new()),
        })
    }

    async fn get_connection(&self) -> std::io::Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string()))
    }
}

#[async_trait]
impl OutboundMessageStorage for RedisOutboundMessageStorage {
    async fn save(&self, message: OutboundMessage) -> std::io::Result<()> {
        let json_str = serde_json::to_string(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let mut conn = self.get_connection().await?;
        redis::cmd("HSET")
            .arg(&self.hash_key)
            .arg(&message.message_id)
            .arg(&json_str)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    async fn get(&self, message_id: &str) -> std::io::Result<Option<OutboundMessage>> {
        let mut conn = self.get_connection().await?;
        let result: Option<String> = redis::cmd("HGET")
            .arg(&self.hash_key)
            .arg(message_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match result {
            Some(json_str) => {
                let msg: OutboundMessage = serde_json::from_str(&json_str).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    async fn update_status(
        &self,
        message_id: &str,
        status: &str,
    ) -> std::io::Result<Option<OutboundMessage>> {
        let mut conn = self.get_connection().await?;
        let result: Option<String> = redis::cmd("HGET")
            .arg(&self.hash_key)
            .arg(message_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match result {
            Some(json_str) => {
                let mut msg: OutboundMessage = serde_json::from_str(&json_str).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                msg.status = status.to_string();
                let updated_json = serde_json::to_string(&msg).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                redis::cmd("HSET")
                    .arg(&self.hash_key)
                    .arg(message_id)
                    .arg(&updated_json)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    async fn list(&self) -> std::io::Result<Vec<OutboundMessage>> {
        let mut conn = self.get_connection().await?;
        let items: Vec<String> = redis::cmd("HVALS")
            .arg(&self.hash_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(items
            .iter()
            .filter_map(|s| serde_json::from_str(s).ok())
            .collect())
    }

    async fn add_scheduled(&self, entry: ScheduledMessage) -> std::io::Result<()> {
        let mut queue = self.scheduled.lock().await;
        let pos = queue
            .binary_search_by_key(&entry.delivery_epoch, |e| e.delivery_epoch)
            .unwrap_or_else(|p| p);
        queue.insert(pos, entry);
        Ok(())
    }

    async fn take_due_messages(&self, now_epoch: u64) -> std::io::Result<Vec<String>> {
        let mut queue = self.scheduled.lock().await;
        let mut due = Vec::new();
        while let Some(front) = queue.first() {
            if front.delivery_epoch <= now_epoch {
                due.push(queue.remove(0).message_id);
            } else {
                break;
            }
        }
        Ok(due)
    }

    async fn save_batch(&self, batch: BatchInfo) -> std::io::Result<()> {
        let json_str = serde_json::to_string(&batch)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let mut conn = self.get_connection().await?;
        redis::cmd("HSET")
            .arg(&self.batch_hash_key)
            .arg(&batch.batch_id)
            .arg(&json_str)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    async fn get_batch(&self, batch_id: &str) -> std::io::Result<Option<BatchInfo>> {
        let mut conn = self.get_connection().await?;
        let result: Option<String> = redis::cmd("HGET")
            .arg(&self.batch_hash_key)
            .arg(batch_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        match result {
            Some(json_str) => {
                let batch: BatchInfo = serde_json::from_str(&json_str).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }
}
