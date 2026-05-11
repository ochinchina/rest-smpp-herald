use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ============================================================
// Data structures for inbound messages
// ============================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InboundMessage {
    pub message_id: String,
    pub source: String,
    pub destination: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_binary: Option<String>,
    /// SMPP data_coding value from the PDU. Common values:
    /// 0 = SMSC Default (usually GSM7), 1 = IA5 (ASCII),
    /// 2 | 4 = Octet unspecified (binary), 3 = Latin 1 (ISO-8859-1),
    /// 5 = JIS, 6 = Cyrillic (ISO-8859-5), 7 = Latin/Hebrew (ISO-8859-8),
    /// 8 = UCS2 (UTF-16 BE), 9 = Pictogram, 10 = ISO-2022-JP,
    /// 13 = Extended Kanji JIS, 14 = KS C 5601 (Korean),
    /// 192–207 / 208–223 = GSM MWI control, 240–243 = GSM message class.
    pub data_coding: u8,
    pub received_at: String,
    pub read: bool,
}

// ============================================================
// InboundMessageStorage trait and implementations
// ============================================================

/// Query parameters for filtering inbound messages in storage.
#[derive(Debug, Default)]
pub struct InboundMessageFilter {
    pub source: Option<String>,
    pub destination: Option<String>,
}

/// Abstraction for inbound message persistence.
#[async_trait]
pub trait InboundMessageStorage: Send + Sync {
    /// Save an inbound message to storage.
    async fn save(&self, message: InboundMessage) -> std::io::Result<()>;

    /// List inbound messages, optionally filtered by source/destination.
    async fn list(&self, filter: &InboundMessageFilter) -> std::io::Result<Vec<InboundMessage>>;

    /// Get a single inbound message by its ID.
    async fn get(&self, message_id: &str) -> std::io::Result<Option<InboundMessage>>;
}

/// In-memory implementation of `InboundMessageStorage` backed by a `Vec`.
pub struct MemoryInboundMessageStorage {
    messages: Mutex<Vec<InboundMessage>>,
    max_messages: usize,
}

impl MemoryInboundMessageStorage {
    pub fn new(max_messages: usize) -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
            max_messages,
        }
    }
}

#[async_trait]
impl InboundMessageStorage for MemoryInboundMessageStorage {
    async fn save(&self, message: InboundMessage) -> std::io::Result<()> {
        let mut msgs = self.messages.lock().await;
        msgs.push(message);
        if self.max_messages > 0 && msgs.len() > self.max_messages {
            let excess = msgs.len() - self.max_messages;
            msgs.drain(..excess);
        }
        Ok(())
    }

    async fn list(&self, filter: &InboundMessageFilter) -> std::io::Result<Vec<InboundMessage>> {
        let msgs = self.messages.lock().await;
        let mut result: Vec<InboundMessage> = msgs.clone();
        if let Some(ref source) = filter.source {
            result.retain(|m| &m.source == source);
        }
        if let Some(ref destination) = filter.destination {
            result.retain(|m| &m.destination == destination);
        }
        Ok(result)
    }

    async fn get(&self, message_id: &str) -> std::io::Result<Option<InboundMessage>> {
        let msgs = self.messages.lock().await;
        Ok(msgs.iter().find(|m| m.message_id == message_id).cloned())
    }
}

/// Redis-backed implementation of `InboundMessageStorage`.
///
/// Messages are stored as JSON strings in a Redis list under a configurable key.
/// Each message is also stored in a hash keyed by message_id for fast lookups.
pub struct RedisInboundMessageStorage {
    client: redis::Client,
    list_key: String,
    hash_key: String,
    max_messages: usize,
}

impl RedisInboundMessageStorage {
    pub fn new(
        redis_url: &str,
        max_messages: usize,
        key_prefix: Option<&str>,
    ) -> std::io::Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;
        let prefix = key_prefix.unwrap_or("smpp");
        Ok(Self {
            client,
            list_key: format!("{}:inbound_messages", prefix),
            hash_key: format!("{}:inbound_messages:index", prefix),
            max_messages,
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
impl InboundMessageStorage for RedisInboundMessageStorage {
    async fn save(&self, message: InboundMessage) -> std::io::Result<()> {
        let json_str = serde_json::to_string(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let mut conn = self.get_connection().await?;

        // Store in the list (newest at end) and in the hash for ID lookup
        redis::pipe()
            .cmd("RPUSH")
            .arg(&self.list_key)
            .arg(&json_str)
            .cmd("HSET")
            .arg(&self.hash_key)
            .arg(&message.message_id)
            .arg(&json_str)
            .exec_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        // Trim oldest messages if over capacity
        if self.max_messages > 0 {
            let len: usize = redis::cmd("LLEN")
                .arg(&self.list_key)
                .query_async(&mut conn)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            if len > self.max_messages {
                let excess = len - self.max_messages;
                // Remove oldest entries from front of list and from hash
                for _ in 0..excess {
                    let removed: Option<String> = redis::cmd("LPOP")
                        .arg(&self.list_key)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                        })?;
                    if let Some(json_str) = removed {
                        if let Ok(msg) = serde_json::from_str::<InboundMessage>(&json_str) {
                            let _: std::result::Result<(), _> = redis::cmd("HDEL")
                                .arg(&self.hash_key)
                                .arg(&msg.message_id)
                                .query_async(&mut conn)
                                .await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn list(&self, filter: &InboundMessageFilter) -> std::io::Result<Vec<InboundMessage>> {
        let mut conn = self.get_connection().await?;
        let items: Vec<String> = redis::cmd("LRANGE")
            .arg(&self.list_key)
            .arg(0i64)
            .arg(-1i64)
            .query_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        let mut result: Vec<InboundMessage> = items
            .iter()
            .filter_map(|s| serde_json::from_str(s).ok())
            .collect();

        if let Some(ref source) = filter.source {
            result.retain(|m| &m.source == source);
        }
        if let Some(ref destination) = filter.destination {
            result.retain(|m| &m.destination == destination);
        }
        Ok(result)
    }

    async fn get(&self, message_id: &str) -> std::io::Result<Option<InboundMessage>> {
        let mut conn = self.get_connection().await?;
        let result: Option<String> = redis::cmd("HGET")
            .arg(&self.hash_key)
            .arg(message_id)
            .query_async(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match result {
            Some(json_str) => {
                let msg: InboundMessage = serde_json::from_str(&json_str).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }
}
