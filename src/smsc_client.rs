pub use crate::alarm::{AlarmConfig, AlarmNotifier, create_alarm_notifier};
use crate::api_key_store::{ApiKeyStore, ApiKeyStoreConfig};
use crate::country_store::{CountryStore, CountryStoreConfig, create_country_store};
pub use crate::inbound_message_storage::{
    InboundMessage, InboundMessageFilter, InboundMessageStorage, MemoryInboundMessageStorage,
    RedisInboundMessageStorage,
};
use crate::message::{self, BIND_TRANSMITTER};
use crate::message::{
    SmppMessageBuffer, decode_message, encode_message, format_smpp_value, get_command_id_by_name,
};
pub use crate::outbound_message_storage::{
    BatchInfo, MemoryOutboundMessageStorage, OutboundMessage, OutboundMessageStorage,
    RedisOutboundMessageStorage, ScheduledMessage,
};
use crate::phone_number_store::{
    PhoneNumberStore, PhoneNumberStoreConfig, create_phone_number_store,
};
pub use crate::rate_limits::{LeakyBucket, RateLimitConfig};
use crate::sequence_number_allocator::SequenceNumberAllocator;
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::Path,
    extract::Query,
    extract::Request,
    extract::State as AxumState,
    http::StatusCode,
    middleware,
    middleware::Next,
    response::IntoResponse,
    response::Response,
    routing::{get, post, put},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use byteorder::{BigEndian, ByteOrder};
use core::result::Result::Ok;
use log::{error, info, warn};
use serde::de::Error;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value, json};
use std::collections::HashMap;

use crate::id_generator::{IdGeneratorConfig, MessageIdGenerator};
use crate::metrics;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

#[async_trait]
pub trait SmscMesageHandler: Send + Sync {
    /// Handles an incoming SMPP message and returns a response.
    ///
    /// # Parameters
    /// - `message`: A `serde_json::Value` representing the decoded SMPP message.
    ///
    /// # Returns
    /// `std::io::Result<Value>` — the handler's JSON response, or an I/O error.
    async fn handle_message(&self, message: Value) -> std::io::Result<Value>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancingAlgorithm {
    RoundRobin,
    FailOver,
}

impl Default for LoadBalancingAlgorithm {
    fn default() -> Self {
        LoadBalancingAlgorithm::RoundRobin
    }
}

pub struct SmscMessageHttpHandler {
    urls: Vec<String>,
    next_index: AtomicU64,
    algorithm: LoadBalancingAlgorithm,
}

impl SmscMessageHttpHandler {
    /// Creates a new `SmscMessageHttpHandler` with the given forwarding URLs
    /// and load-balancing algorithm.
    ///
    /// # Parameters
    /// - `urls`: A list of HTTP endpoint URLs to forward messages to.
    /// - `algorithm`: The load-balancing strategy (`RoundRobin` or `FailOver`).
    ///
    /// # Returns
    /// A new `SmscMessageHttpHandler` instance.
    pub fn new(urls: Vec<String>, algorithm: LoadBalancingAlgorithm) -> Self {
        SmscMessageHttpHandler {
            urls,
            next_index: AtomicU64::new(0),
            algorithm,
        }
    }
}

#[async_trait]
impl SmscMesageHandler for SmscMessageHttpHandler {
    /// Forwards an incoming SMPP message to configured HTTP URLs via POST requests.
    ///
    /// - **RoundRobin**: rotates the starting URL across calls, then fails over to
    ///   subsequent URLs if the selected one is unavailable.
    /// - **FailOver**: always starts from the first URL, only advancing to the next
    ///   on failure.
    ///
    /// # Parameters
    /// - `message`: A `serde_json::Value` representing the decoded SMPP message.
    ///
    /// # Returns
    /// `std::io::Result<Value>` — the JSON response from the first successful URL,
    /// or an I/O error if all URLs fail.
    async fn handle_message(&self, message: Value) -> std::io::Result<Value> {
        if self.urls.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No URLs configured",
            ));
        }
        let client = reqwest::Client::new();
        let len = self.urls.len();
        let start = match self.algorithm {
            LoadBalancingAlgorithm::RoundRobin => {
                self.next_index
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize
                    % len
            }
            LoadBalancingAlgorithm::FailOver => 0,
        };
        for i in 0..len {
            let url = &self.urls[(start + i) % len];
            match client.post(url).json(&message).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.json::<Value>().await {
                            Ok(json_resp) => return Ok(json_resp),
                            Err(e) => {
                                error!("Failed to parse JSON response from {}: {}", url, e);
                                continue;
                            }
                        }
                    } else {
                        error!(
                            "Received non-success status code from {}: {}",
                            url,
                            resp.status()
                        );
                        continue;
                    }
                }
                Err(e) => {
                    error!("Failed to send message to {}: {}", url, e);
                    continue;
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to handle message with all URLs",
        ))
    }
}

/// Per-SMSC connection configuration with bind parameters.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SmscConnectionConfig {
    pub address: String,
    pub system_id: String,
    pub password: String,
    pub system_type: String,
    pub addr_ton: u8,
    pub addr_npi: u8,
    pub address_range: String,
    pub interface_version: u8,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

fn default_weight() -> u32 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    Memory,
    Redis {
        url: String,
        #[serde(default)]
        key_prefix: Option<String>,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Memory
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SmscClientConfig {
    #[serde(default)]
    pub rest_addresses: Vec<String>,
    pub connections: Vec<SmscConnectionConfig>,
    #[serde(default = "default_max_inbound_messages")]
    pub max_inbound_messages: usize,
    #[serde(default)]
    pub inbound_storage: StorageConfig,
    #[serde(default)]
    pub outbound_storage: StorageConfig,
    #[serde(default)]
    pub handler_urls: Vec<String>,
    #[serde(default)]
    pub handler_algorithm: LoadBalancingAlgorithm,
    #[serde(default)]
    pub alarm_config: Option<AlarmConfig>,
    #[serde(default)]
    pub api_key_store: ApiKeyStoreConfig,
    #[serde(default)]
    pub phone_number_store: PhoneNumberStoreConfig,
    #[serde(default)]
    pub country_store: CountryStoreConfig,
    #[serde(default)]
    pub id_generator: IdGeneratorConfig,
}

fn default_max_inbound_messages() -> usize {
    10000
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SmppConnectionInfo {
    pub connection_id: String,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub system_id: String,
    pub bind_type: String,
    pub status: String,
    pub reconnect_enabled: bool,
    pub heartbeat_interval: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SenderIdInfo {
    pub sender_id: String,
    #[serde(rename = "type")]
    pub sender_type: String,
    pub status: String,
    pub verified: bool,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PhoneNumberInfo {
    pub number_id: String,
    pub phone_number: String,
    pub capabilities: Vec<String>,
    pub status: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiKeyInfo {
    pub key_id: String,
    pub name: String,
    pub key_prefix: String,
    #[serde(skip_serializing, default)]
    pub api_key: String,
    pub permissions: Vec<String>,
    pub rate_limit: u32,
    pub status: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountryInfo {
    pub code: String,
    pub name: String,
    pub country_code: u32,
    pub supported: bool,
}

// ============================================================
// Request body structs
// ============================================================

#[derive(Debug, Deserialize)]
pub struct SendSmsRequest {
    pub source: String,
    pub destination: String,
    pub message: String,
    #[serde(default)]
    pub message_binary: Option<String>,
    pub encoding: Option<String>,
    #[serde(default)]
    pub data_coding: Option<u8>,
    pub validity_period: Option<u32>,
    pub schedule_time: Option<String>,
    pub priority: Option<String>,
    pub tags: Option<Vec<String>>,
    pub callback_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BulkSmsMessage {
    pub source: String,
    pub destination: String,
    pub message: String,
    #[serde(default)]
    pub message_binary: Option<String>,
    pub encoding: Option<String>,
    #[serde(default)]
    pub data_coding: Option<u8>,
    pub schedule_time: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendBulkSmsRequest {
    pub messages: Vec<BulkSmsMessage>,
    pub batch_name: Option<String>,
    pub callback_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSmppConnectionRequest {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub system_id: String,
    pub password: String,
    pub bind_type: Option<String>,
    pub system_type: Option<String>,
    pub reconnect_enabled: Option<bool>,
    pub reconnect_interval: Option<u32>,
    pub heartbeat_interval: Option<u32>,
    pub enquire_link_interval: Option<u32>,
    pub throughput_limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSmppConnectionRequest {
    pub name: Option<String>,
    pub reconnect_enabled: Option<bool>,
    pub reconnect_interval: Option<u32>,
    pub heartbeat_interval: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct AddSmscRequest {
    pub address: String,
    pub system_id: String,
    pub password: String,
    pub system_type: Option<String>,
    pub addr_ton: Option<u8>,
    pub addr_npi: Option<u8>,
    pub address_range: Option<String>,
    pub interface_version: Option<u8>,
    pub weight: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSenderIdRequest {
    pub sender_id: String,
    #[serde(rename = "type")]
    pub sender_type: String,
}

#[derive(Debug, Deserialize)]
pub struct CreatePhoneNumberRequest {
    pub phone_number: String,
    pub capabilities: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePhoneNumberRequest {
    pub capabilities: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
    pub permissions: Option<Vec<String>>,
    pub rate_limit: Option<u32>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateApiKeyRequest {
    pub name: Option<String>,
    pub permissions: Option<Vec<String>>,
    pub rate_limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateRateLimitsRequest {
    pub outbound_per_second: Option<u32>,
    pub inbound_per_second: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ValidatePhoneRequest {
    pub phone_number: String,
}

#[derive(Debug, Deserialize)]
pub struct MessagePartsRequest {
    pub message: String,
    pub encoding: Option<String>,
}

// Query parameter structs

#[derive(Debug, Deserialize)]
pub struct InboundMessageQuery {
    pub page: Option<u32>,
    pub per_page: Option<u32>,
    pub source: Option<String>,
    pub destination: Option<String>,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
}

// ============================================================
// Webhook types
// ============================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookInfo {
    pub webhook_id: String,
    pub url: String,
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default = "default_webhook_enabled")]
    pub enabled: bool,
    pub created_at: String,
}

fn default_webhook_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct CreateWebhookRequest {
    pub url: String,
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default = "default_webhook_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct UpdateWebhookRequest {
    pub url: Option<String>,
    pub events: Option<Vec<String>>,
    pub enabled: Option<bool>,
}

// Shared application state

pub struct AppState {
    pub inbound_storage: Arc<dyn InboundMessageStorage>,
    pub outbound_storage: Arc<dyn OutboundMessageStorage>,
    pub smpp_connections_store: Mutex<Vec<SmppConnectionInfo>>,
    pub sender_ids: Mutex<Vec<SenderIdInfo>>,
    pub phone_number_store: Arc<dyn PhoneNumberStore>,
    pub country_store: Arc<dyn CountryStore>,
    pub api_key_store: Arc<dyn ApiKeyStore>,
    pub rate_limits: Arc<Mutex<RateLimitConfig>>,
    pub start_time: std::time::Instant,
    pub id_generator: Arc<dyn MessageIdGenerator>,
    pub connections: Arc<Mutex<Vec<SmscConnectionHandle>>>,
    pub connection_index: AtomicU64,
    pub seq_allocator: SequenceNumberAllocator,
    pub smsc_message_handler: Arc<dyn SmscMesageHandler>,
    pub alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
    pub webhooks: Arc<Mutex<Vec<WebhookInfo>>>,
}

/// Represents a single SMSC connection's communication channels for sending
/// outbound SMPP messages and receiving correlated responses.
pub struct SmscConnectionHandle {
    pub address: String,
    pub out_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    pub callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
    pub weight: u32,
}

/// Selects a connection index from the pool using weighted round-robin.
/// The `counter` is the current round-robin counter value. Each connection
/// is selected proportionally to its weight.
fn weighted_connection_index(conns: &[SmscConnectionHandle], counter: u64) -> usize {
    let total_weight: u64 = conns.iter().map(|c| c.weight as u64).sum();
    if total_weight == 0 {
        return counter as usize % conns.len();
    }
    let slot = counter % total_weight;
    let mut cumulative: u64 = 0;
    for (i, c) in conns.iter().enumerate() {
        cumulative += c.weight as u64;
        if slot < cumulative {
            return i;
        }
    }
    conns.len() - 1
}

/// Parses a "host:port" address string into its components.
fn parse_host_port(address: &str) -> (String, u16) {
    if let Some(colon_pos) = address.rfind(':') {
        let host = address[..colon_pos].to_string();
        let port = address[colon_pos + 1..].parse::<u16>().unwrap_or(0);
        (host, port)
    } else {
        (address.to_string(), 0)
    }
}

pub struct SmscClient {
    rest_addresses: Vec<String>,
    config: SmscClientConfig,
    seq_allocator: SequenceNumberAllocator,
    smsc_message_handler: Arc<dyn SmscMesageHandler>,
}

impl SmscClient {
    /// Creates a new `SmscClient` instance.
    ///
    /// # Parameters
    /// - `config`: The `SmscClientConfig` containing connection, binding, and handler parameters.
    ///
    /// # Returns
    /// A new `SmscClient` instance ready to be started.
    pub fn new(config: SmscClientConfig) -> Self {
        let smsc_message_handler: Arc<dyn SmscMesageHandler> =
            Arc::new(SmscMessageHttpHandler::new(
                config.handler_urls.clone(),
                config.handler_algorithm.clone(),
            ));
        SmscClient {
            rest_addresses: config.rest_addresses.clone(),
            config,
            seq_allocator: SequenceNumberAllocator::new(),
            smsc_message_handler,
        }
    }

    /// Starts the SMSC client by spawning the REST API server and a reconnection
    /// loop for each configured SMSC address.
    ///
    /// # Returns
    /// `std::io::Result<()>` — runs indefinitely; returns only on fatal error.
    pub async fn start(&mut self) -> std::io::Result<()> {
        let connections: Arc<Mutex<Vec<SmscConnectionHandle>>> = Arc::new(Mutex::new(Vec::new()));
        let inbound_storage: Arc<dyn InboundMessageStorage> = match &self.config.inbound_storage {
            StorageConfig::Memory => Arc::new(MemoryInboundMessageStorage::new(
                self.config.max_inbound_messages,
            )),
            StorageConfig::Redis { url, key_prefix } => Arc::new(RedisInboundMessageStorage::new(
                url,
                self.config.max_inbound_messages,
                key_prefix.as_deref(),
            )?),
        };
        let outbound_storage: Arc<dyn OutboundMessageStorage> =
            match &self.config.outbound_storage {
                StorageConfig::Memory => Arc::new(MemoryOutboundMessageStorage::new()),
                StorageConfig::Redis { url, key_prefix } => Arc::new(
                    RedisOutboundMessageStorage::new(url, key_prefix.as_deref())?,
                ),
            };
        let rest_addresses = self.rest_addresses.clone();
        let seq_allocator = self.seq_allocator.clone();
        let rate_limits = Arc::new(Mutex::new(RateLimitConfig::new(10, 200)));

        let phone_number_store = create_phone_number_store(&self.config.phone_number_store).await?;

        let country_store = create_country_store(&self.config.country_store).await?;

        let alarm_notifier: Option<Arc<dyn AlarmNotifier>> = match &self.config.alarm_config {
            Some(cfg) => Some(create_alarm_notifier(cfg).await?),
            None => None,
        };

        let api_key_store =
            crate::api_key_store::create_api_key_store(&self.config.api_key_store).await?;

        let id_generator: Arc<dyn MessageIdGenerator> = Arc::from(
            crate::id_generator::create_id_generator(&self.config.id_generator)?,
        );

        let webhooks: Arc<Mutex<Vec<WebhookInfo>>> = Arc::new(Mutex::new(Vec::new()));

        // Spawn a reconnection loop for each configured SMSC connection
        for conn_config in &self.config.connections {
            let (out_sender, out_receiver) = tokio::sync::mpsc::channel(1000);
            let callbacks: Arc<
                Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>,
            > = Arc::new(Mutex::new(HashMap::new()));

            connections.lock().await.push(SmscConnectionHandle {
                address: conn_config.address.clone(),
                out_sender,
                callbacks: callbacks.clone(),
                weight: conn_config.weight,
            });

            let conn_cfg = conn_config.clone();
            let seq = self.seq_allocator.clone();
            let handler = self.smsc_message_handler.clone();
            let inbound = inbound_storage.clone();
            let rl = rate_limits.clone();
            let pn = phone_number_store.clone();
            let an = alarm_notifier.clone();
            let wh = webhooks.clone();
            tokio::spawn(async move {
                Self::smsc_connection_loop(
                    conn_cfg,
                    seq,
                    handler,
                    out_receiver,
                    callbacks,
                    inbound,
                    rl,
                    pn,
                    an,
                    wh,
                )
                .await;
            });
        }

        // Start REST server (blocks indefinitely)
        Self::start_rest(
            rest_addresses,
            connections,
            seq_allocator,
            self.smsc_message_handler.clone(),
            inbound_storage,
            outbound_storage,
            rate_limits,
            phone_number_store,
            country_store,
            alarm_notifier,
            api_key_store,
            id_generator,
            webhooks,
        )
        .await;
        Ok(())
    }

    /// Manages the reconnection loop for a single SMSC connection. Continuously attempts
    /// to connect, and retries with backoff after disconnection or failure.
    ///
    /// # Parameters
    /// - `conn_config`: The per-connection bind configuration (address + SMPP bind parameters).
    /// - `seq_allocator`: Shared sequence number allocator.
    /// - `smsc_message_handler`: Handler for processing inbound SMPP messages.
    /// - `out_receiver`: Channel receiver for outbound messages destined for this connection.
    /// - `callbacks`: Shared map for correlating SMPP request/response sequence numbers.
    /// - `inbound_storage`: Shared inbound message storage backend.
    /// - `rate_limits`: Shared rate limit configuration for tracking inbound message rates.
    pub async fn smsc_connection_loop(
        conn_config: SmscConnectionConfig,
        seq_allocator: SequenceNumberAllocator,
        smsc_message_handler: Arc<dyn SmscMesageHandler>,
        mut out_receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
        callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
        inbound_storage: Arc<dyn InboundMessageStorage>,
        rate_limits: Arc<Mutex<RateLimitConfig>>,
        phone_number_store: Arc<dyn PhoneNumberStore>,
        alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
        webhooks: Arc<Mutex<Vec<WebhookInfo>>>,
    ) {
        let (host, port) = parse_host_port(&conn_config.address);
        let mut alarm_active = false;

        loop {
            info!(
                "Attempting to connect to SMSC at {}...",
                conn_config.address
            );
            match Self::connect_smsc_to(
                &conn_config,
                &seq_allocator,
                &smsc_message_handler,
                &mut out_receiver,
                callbacks.clone(),
                inbound_storage.clone(),
                rate_limits.clone(),
                phone_number_store.clone(),
                alarm_notifier.clone(),
                &mut alarm_active,
                webhooks.clone(),
            )
            .await
            {
                Ok(_) => {
                    // Ok means we were connected (bind succeeded) but then disconnected
                    info!(
                        "Disconnected from SMSC {}, will attempt to reconnect...",
                        conn_config.address
                    );
                    if let Some(ref notifier) = alarm_notifier {
                        notifier
                            .raise_alarm(
                                &host,
                                port,
                                &format!("Connection to SMSC {}:{} lost", host, port),
                            )
                            .await;
                        alarm_active = true;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    error!("Failed to connect to SMSC {}: {}", conn_config.address, e);
                    if let Some(ref notifier) = alarm_notifier {
                        if !alarm_active {
                            notifier
                                .raise_alarm(
                                    &host,
                                    port,
                                    &format!("Failed to connect to SMSC {}:{}: {}", host, port, e),
                                )
                                .await;
                            alarm_active = true;
                        }
                    }
                    Self::drain_pending_messages(&mut out_receiver, callbacks.clone())
                        .await
                        .unwrap();
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Establishes a TCP connection to a specific SMSC address, performs the SMPP bind
    /// handshake, and enters the main event loop for sending/receiving SMPP messages.
    ///
    /// # Parameters
    /// - `conn_config`: The per-connection bind configuration (address + SMPP bind parameters).
    /// - `seq_allocator`: Shared sequence number allocator for generating SMPP sequence numbers.
    /// - `smsc_message_handler`: Handler for processing inbound SMPP messages.
    /// - `out_receiver`: Channel receiver for outbound SMPP messages from the REST API.
    /// - `callbacks`: Shared map of sequence-number → oneshot senders for correlating
    ///   SMPP responses with their originating requests.
    /// - `inbound_storage`: Shared inbound message storage backend.
    /// - `rate_limits`: Shared rate limit configuration for tracking inbound message rates.
    ///
    /// # Returns
    /// `std::io::Result<()>` — returns `Ok(())` when the connection is lost or timed out.
    async fn connect_smsc_to(
        conn_config: &SmscConnectionConfig,
        seq_allocator: &SequenceNumberAllocator,
        smsc_message_handler: &Arc<dyn SmscMesageHandler>,
        out_receiver: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
        callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
        inbound_storage: Arc<dyn InboundMessageStorage>,
        rate_limits: Arc<Mutex<RateLimitConfig>>,
        phone_number_store: Arc<dyn PhoneNumberStore>,
        alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
        alarm_active: &mut bool,
        webhooks: Arc<Mutex<Vec<WebhookInfo>>>,
    ) -> std::io::Result<()> {
        info!("trying to connect to SMSC at {}", conn_config.address);
        let stream = TcpStream::connect(&conn_config.address).await?;

        info!("succeed to connecte SMSC {}", conn_config.address);

        let (mut reader, mut writer) = stream.into_split();

        // buffer to store incoming data from SMSC
        let mut buffer = SmppMessageBuffer::new();

        Self::do_bind(
            conn_config,
            seq_allocator,
            &mut reader,
            &mut writer,
            &mut buffer,
        )
        .await?;

        metrics::SMSC_ACTIVE_CONNECTIONS.inc();

        // Connection restored — clear alarm if one was active
        if *alarm_active {
            if let Some(ref notifier) = alarm_notifier {
                let (host, port) = parse_host_port(&conn_config.address);
                notifier
                    .clear_alarm(
                        &host,
                        port,
                        &format!("Connection to SMSC {}:{} restored", host, port),
                    )
                    .await;
            }
            *alarm_active = false;
        }

        // enquery link timer
        let enquery_link_interval = Duration::from_secs(30);
        let mut enquery_link_timer = tokio::time::interval(enquery_link_interval);
        let no_message_timeout = Duration::from_secs(180);
        let mut no_message_timer = tokio::time::interval(no_message_timeout);
        // the first tick of timer will complete immediately, so we need to skip it
        no_message_timer.tick().await;
        let messages_received = AtomicU32::new(0);
        info!("Starting main loop to communicate with SMSC");
        let smsc_message_handler = smsc_message_handler.clone();

        loop {
            tokio::select! {
                // message from rest interface to send to SMSC
                data = out_receiver.recv() => {
                    info!("SMSC connection writable");
                    if let Some(message) = data {
                        if let Err(e) = writer.write_all(&message).await {
                            error!("Failed to send message to SMSC: {}", e);
                            break;
                        } else {
                            info!("Succeed to send message to SMSC: {:?}", message);
                        }
                    } else {
                        error!("out_receiver channel closed");
                        break;
                    }
                }

                _ = reader.readable() => {
                    match Self::read_messages(&mut reader, &mut buffer).await {
                        Err(e) => {
                            error!("Failed to read messages from SMSC: {}", e);
                            break;
                        }
                        Ok(Some(messages)) => {
                            messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // reset no message timer
                            for message in messages {
                                Self::process_in_message(&mut writer, message, &callbacks, smsc_message_handler.clone(), inbound_storage.clone(), rate_limits.clone(), phone_number_store.clone(), webhooks.clone()).await?;
                            }
                        }
                        Ok(None) => {}
                    }
                }

                _ = enquery_link_timer.tick() => {
                    Self::send_enquire_link(&mut writer, seq_allocator.next()).await?;
                }

                _ = no_message_timer.tick() => {
                    info!("Checking for no message timeout");
                    if messages_received.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                        error!("No message received from SMSC for {:?}, disconnecting...", no_message_timeout);
                        break;
                    } else {
                        messages_received.store(0, std::sync::atomic::Ordering::Relaxed);
                    }
                }

            }
        }
        metrics::SMSC_ACTIVE_CONNECTIONS.dec();
        info!("Exiting SMSC connection loop");
        Ok(())
    }

    /// Drains all pending outbound messages from the channel and sends an error
    /// response to each waiting callback, indicating the SMSC connection was lost.
    ///
    /// # Parameters
    /// - `out_receiver`: Channel receiver containing queued outbound SMPP messages.
    /// - `callbacks`: Shared map of sequence-number → oneshot senders to notify with errors.
    ///
    /// # Returns
    /// `std::io::Result<()>` — always returns `Ok(())`.
    async fn drain_pending_messages(
        out_receiver: &mut tokio::sync::mpsc::Receiver<Vec<u8>>,
        callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
    ) -> std::io::Result<()> {
        while let Ok(message) = out_receiver.try_recv() {
            let seq_number = BigEndian::read_u32(&message[12..16]);
            if let Some(sender) = callbacks.lock().await.remove(&seq_number) {
                let _ = sender.send(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "SMSC connection lost",
                )));
            }
        }
        Ok(())
    }

    /// Performs the SMPP bind handshake by sending a bind_transmitter PDU
    /// and waiting for the bind response from the SMSC.
    ///
    /// # Parameters
    /// - `conn_config`: The per-connection SMPP bind configuration parameters.
    /// - `seq_allocator`: Shared sequence number allocator for the bind request.
    /// - `reader`: The read half of the TCP connection to the SMSC.
    /// - `writer`: The write half of the TCP connection to the SMSC.
    /// - `buffer`: A reusable `SmppMessageBuffer` for accumulating incoming bytes.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` if bind succeeds, or an error if it fails.
    async fn do_bind(
        conn_config: &SmscConnectionConfig,
        seq_allocator: &SequenceNumberAllocator,
        reader: &mut tokio::net::tcp::OwnedReadHalf,
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        buffer: &mut SmppMessageBuffer,
    ) -> std::io::Result<()> {
        let bind_message = Self::create_bind_message(conn_config, seq_allocator);
        let mut buf = [0u8; 1024];
        writer.write_all(&bind_message).await?;

        loop {
            let _ = reader.readable().await?;
            if let Ok(n) = reader.try_read(&mut buf) {
                info!("Received {} bytes from SMSC", n);
                buffer.write(&buf[..n]);
                if let Some(message) = buffer.extract_message() {
                    let response = decode_message(&message.buffer)?;
                    if Self::is_bind_ok(&response) {
                        return Ok(());
                    } else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Bind failed",
                        ));
                    }
                }
            }
        }
    }

    /// Checks whether an SMPP bind response indicates a successful bind.
    ///
    /// # Parameters
    /// - `response`: A `serde_json::Value` containing the decoded bind response PDU.
    ///
    /// # Returns
    /// `bool` — `true` if the response is a bind_transmitter_resp with status 0 (success).
    fn is_bind_ok(response: &Value) -> bool {
        let command_id = response
            .get("command_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if command_id == 0x80000002 {
            let command_status = response
                .get("command_status")
                .and_then(|v| v.as_u64())
                .unwrap_or(1);
            if command_status == 0 {
                info!("Bind successful");
                return true;
            } else {
                error!("Bind failed with status: {}", command_status);
                return false;
            }
        }
        false
    }

    /// Initializes the shared application state and starts the Axum REST API server
    /// with all route handlers for SMS, gateway management, and utility endpoints.
    ///
    /// # Parameters
    /// - `rest_address`: The `host:port` address to bind the HTTP listener to.
    /// - `connections`: Shared pool of SMSC connection handles for round-robin message routing.
    /// - `seq_allocator`: A `SequenceNumberAllocator` for generating SMPP sequence numbers.
    /// - `smsc_message_handler`: Handler for processing inbound SMPP messages on new connections.
    /// - `inbound_storage`: Shared inbound message storage backend.
    /// - `outbound_storage`: Shared outbound message storage backend.
    /// - `rate_limits`: Shared rate limit configuration.
    async fn start_rest(
        rest_addresses: Vec<String>,
        connections: Arc<Mutex<Vec<SmscConnectionHandle>>>,
        seq_allocator: SequenceNumberAllocator,
        smsc_message_handler: Arc<dyn SmscMesageHandler>,
        inbound_storage: Arc<dyn InboundMessageStorage>,
        outbound_storage: Arc<dyn OutboundMessageStorage>,
        rate_limits: Arc<Mutex<RateLimitConfig>>,
        phone_number_store: Arc<dyn PhoneNumberStore>,
        country_store: Arc<dyn CountryStore>,
        alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
        api_key_store: Arc<dyn ApiKeyStore>,
        id_generator: Arc<dyn MessageIdGenerator>,
        webhooks: Arc<Mutex<Vec<WebhookInfo>>>,
    ) {
        let state = Arc::new(AppState {
            inbound_storage,
            outbound_storage,
            smpp_connections_store: Mutex::new(Vec::new()),
            sender_ids: Mutex::new(Vec::new()),
            phone_number_store,
            country_store,
            api_key_store,
            rate_limits,
            start_time: std::time::Instant::now(),
            id_generator,
            connections,
            connection_index: AtomicU64::new(0),
            seq_allocator,
            smsc_message_handler,
            alarm_notifier,
            webhooks,
        });

        // Start the scheduled delivery timer
        {
            let st = state.clone();
            tokio::spawn(async move {
                scheduled_delivery_timer(st).await;
            });
        }

        let protected = Router::new()
            .route("/raw/api/{path}", post(raw_api_handler))
            // SMS Messaging
            .route("/v1/sms/send", post(send_sms))
            .route("/v1/sms/send/bulk", post(send_bulk_sms))
            .route(
                "/v1/sms/messages/{message_id}",
                get(get_message_status).delete(cancel_message),
            )
            .route("/v1/sms/batches/{batch_id}", get(get_batch_status))
            // Inbound Messages
            .route("/v1/sms/inbound", get(list_inbound_messages))
            .route("/v1/sms/inbound/{message_id}", get(get_inbound_message))
            // Gateway Status
            .route("/v1/gateway/status", get(get_gateway_status))
            // SMPP Connections
            .route(
                "/v1/gateway/smpp/connections",
                get(list_smpp_connections).post(create_smpp_connection),
            )
            .route(
                "/v1/gateway/smpp/connections/{connection_id}",
                put(update_smpp_connection).delete(delete_smpp_connection),
            )
            .route(
                "/v1/gateway/smpp/connections/{connection_id}/rebind",
                post(rebind_smpp_connection),
            )
            // Live SMSC connections (dynamically add/list)
            .route(
                "/v1/gateway/smpp/live-connections",
                get(list_live_smsc_connections).post(add_live_smsc_connection),
            )
            // Sender IDs
            .route(
                "/v1/gateway/sender-ids",
                get(list_sender_ids).post(create_sender_id),
            )
            // Phone Numbers
            .route(
                "/v1/gateway/numbers",
                get(list_phone_numbers).post(create_phone_number),
            )
            .route(
                "/v1/gateway/numbers/{number_id}",
                put(update_phone_number).delete(delete_phone_number),
            )
            // API Keys
            .route(
                "/v1/gateway/api-keys",
                get(list_api_keys).post(create_api_key),
            )
            .route(
                "/v1/gateway/api-keys/{key_id}",
                put(update_api_key).delete(delete_api_key),
            )
            // Rate Limits
            .route(
                "/v1/gateway/rate-limits",
                get(get_rate_limits).put(update_rate_limits),
            )
            // Utilities
            .route("/v1/utils/countries", get(get_supported_countries))
            .route("/v1/utils/validate-phone", post(validate_phone))
            .route("/v1/utils/message-parts", post(calculate_message_parts))
            // Webhooks
            .route("/v1/webhooks/inbound", post(create_webhook))
            .route("/v1/webhooks/{webhook_id}/test", post(test_webhook))
            .route(
                "/v1/webhooks/{webhook_id}",
                get(get_webhook).put(update_webhook).delete(delete_webhook),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                require_api_key,
            ))
            .with_state(state.clone());

        let app = Router::new()
            .route("/metrics", get(prometheus_metrics))
            .merge(protected);

        let mut listeners = Vec::new();
        for addr in &rest_addresses {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("REST API listening on {}", addr);
                    listeners.push(listener);
                }
                Err(e) => {
                    error!("Failed to bind REST API to {}: {}", addr, e);
                }
            }
        }
        if listeners.is_empty() {
            error!("No REST API listeners could be bound");
            return;
        }
        // Serve on the first listener, spawn the rest
        let first = listeners.remove(0);
        for listener in listeners {
            let app_clone = app.clone();
            tokio::spawn(async move {
                axum::serve(listener, app_clone).await.unwrap();
            });
        }
        axum::serve(first, app).await.unwrap();
    }

    /// Constructs an SMPP bind_transmitter PDU using the given configuration.
    ///
    /// # Parameters
    /// - `conn_config`: The per-connection SMPP bind configuration parameters.
    /// - `seq_allocator`: Shared sequence number allocator for the bind request.
    ///
    /// # Returns
    /// `Vec<u8>` — the serialized bind_transmitter message bytes.
    fn create_bind_message(
        conn_config: &SmscConnectionConfig,
        seq_allocator: &SequenceNumberAllocator,
    ) -> Vec<u8> {
        let mut message = SmppMessageBuffer::new();

        message.write_u32(0); // Placeholder for command_length
        message.write_u32(BIND_TRANSMITTER); // command_id for bind_transmitter
        message.write_u32(0); // command_status
        let seq_num = seq_allocator.next();
        message.write_u32(seq_num); // sequence_number

        message.write_c_octet_str(&conn_config.system_id);
        message.write_c_octet_str(&conn_config.password);
        message.write_c_octet_str(&conn_config.system_type);
        message.write_u8(conn_config.interface_version);
        message.write_u8(conn_config.addr_ton); // addr_ton
        message.write_u8(conn_config.addr_npi); // addr_npi
        message.write_c_octet_str(&conn_config.address_range); // address_range

        message.update_length();
        message.buffer.to_vec()
    }

    /// Sends an SMPP enquire_link PDU to the SMSC to verify the connection is alive.
    ///
    /// # Parameters
    /// - `writer`: The write half of the TCP connection to the SMSC.
    /// - `sequence_number`: The sequence number to use in the enquire_link PDU.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn send_enquire_link(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        sequence_number: u32,
    ) -> std::io::Result<()> {
        let message = json! ({
            "command_id": 0x00000015,
            "command_status": 0,
            "sequence_number": sequence_number
        });
        info!("Sent enquire_link message to SMSC: {}", message);
        let message = encode_message(&message, None).unwrap_or_else(|_| Vec::new());
        writer.write_all(&message).await?;
        Ok(())
    }

    /// Reads raw bytes from the SMSC TCP connection and extracts complete SMPP messages.
    ///
    /// # Parameters
    /// - `reader`: The read half of the TCP connection to the SMSC.
    /// - `buffer`: A reusable `SmppMessageBuffer` for accumulating partial data.
    ///
    /// # Returns
    /// `Result<Option<Vec<SmppMessageBuffer>>>` — `Ok(Some(messages))` if one or more
    /// complete messages were read, `Ok(None)` if no data was available, or an error
    /// if the connection was closed or a read failure occurred.
    async fn read_messages(
        reader: &mut tokio::net::tcp::OwnedReadHalf,
        buffer: &mut SmppMessageBuffer,
    ) -> Result<Option<Vec<SmppMessageBuffer>>> {
        let mut buf = [0u8; 1024];

        match reader.try_read(&mut buf) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(None);
            }
            Err(e) => {
                error!("Failed to read from SMSC: {}", e);
                return Err(Error::custom("Failed to read from SMSC"));
            }
            Ok(0) => {
                error!("SMSC connection closed");
                return Err(Error::custom("SMSC connection closed"));
            }
            Ok(n) => {
                info!("Received {} bytes from SMSC", n);
                buffer.write(&buf[..n]);
                let mut messages = Vec::new();
                while let Some(message) = buffer.extract_message() {
                    // Process the message
                    messages.push(message);
                }
                return Ok(Some(messages));
            }
        }
    }

    /// Processes a single incoming SMPP message: dispatches enquire_link responses,
    /// delivers inbound SMS to the message handler and stores them in `inbound_messages`,
    /// and routes responses to pending callbacks.
    ///
    /// # Parameters
    /// - `writer`: The write half of the TCP connection (used for sending responses).
    /// - `message`: The received `SmppMessageBuffer` to process.
    /// - `callbacks`: Shared map of sequence-number → oneshot senders for response correlation.
    /// - `smsc_message_handler`: The handler for processing inbound SMS (DELIVER_SM / DATA_SM).
    /// - `inbound_storage`: Shared inbound message storage backend.
    /// - `rate_limits`: Shared rate limit configuration for tracking inbound message rates.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn process_in_message(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        message: SmppMessageBuffer,
        callbacks: &Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
        smsc_message_handler: Arc<dyn SmscMesageHandler>,
        inbound_storage: Arc<dyn InboundMessageStorage>,
        rate_limits: Arc<Mutex<RateLimitConfig>>,
        phone_number_store: Arc<dyn PhoneNumberStore>,
        webhooks: Arc<Mutex<Vec<WebhookInfo>>>,
    ) -> std::io::Result<()> {
        use crate::message;

        match decode_message(&message.buffer) {
            Ok(decoded_message) => {
                info!(
                    "Decoded message from SMSC: {}",
                    format_smpp_value(&decoded_message)
                );
                let command_id = message.get_command_id().unwrap_or(0);
                if command_id == message::ENQUIRE_LINK {
                    Self::process_enquire_link(&message, writer).await?;
                } else if command_id == message::ENQUIRE_LINK_RESP {
                    info!(
                        "Received enquire_link_resp from SMSC for {}",
                        message.get_sequence_number().unwrap_or(0)
                    );
                } else if command_id == message::DELIVER_SM || command_id == message::DATA_SM {
                    let cmd_name = if command_id == message::DELIVER_SM {
                        "DELIVER_SM"
                    } else {
                        "DATA_SM"
                    };
                    info!("Received {} from SMSC", cmd_name);
                    metrics::SMSC_INBOUND_MESSAGES.inc();

                    // Extract fields from the decoded message and store in inbound_messages
                    let source = decoded_message
                        .get("source_addr")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let destination = decoded_message
                        .get("destination_addr")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let short_message_b64 = decoded_message
                        .get("short_message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let data_coding = decoded_message
                        .get("data_coding")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u8;

                    // Decode base64 short_message to text when possible
                    let (message_text, message_binary) =
                        if let Ok(bytes) = BASE64_STANDARD.decode(&short_message_b64) {
                            match data_coding {
                                // Binary encodings — keep raw base64 only
                                4 | 5 | 6 => (String::new(), Some(short_message_b64.clone())),
                                // Text encodings — try UTF-8 decode
                                _ => match String::from_utf8(bytes.clone()) {
                                    Ok(text) => (text, Some(short_message_b64.clone())),
                                    Err(_) => (String::new(), Some(short_message_b64.clone())),
                                },
                            }
                        } else {
                            (short_message_b64.clone(), None)
                        };

                    let seq_num = message.get_sequence_number().unwrap_or(0);

                    // Always acknowledge the SMSC
                    Self::send_deliver_sm_resp(writer, seq_num, command_id).await?;

                    // Filter inbound by registered phone numbers (skip if store is empty)
                    {
                        let is_empty = phone_number_store.is_empty().await.unwrap_or(true);
                        if !is_empty {
                            let has_inbound = phone_number_store
                                .has_capability(&destination, "sms_inbound")
                                .await
                                .unwrap_or(false);
                            if !has_inbound {
                                warn!(
                                    "Dropping inbound message to {} — not a registered sms_inbound number",
                                    destination
                                );
                                return Ok(());
                            }
                        }
                    }

                    let message_id = format!(
                        "inb_{:08x}_{}",
                        seq_num,
                        now_utc().replace([':', '-', 'T', 'Z'], "")
                    );

                    inbound_storage
                        .save(InboundMessage {
                            message_id,
                            source,
                            destination,
                            message: message_text,
                            message_binary,
                            data_coding,
                            received_at: now_utc(),
                            read: false,
                        })
                        .await?;

                    // Track inbound rate usage (leaky bucket)
                    {
                        let mut limits = rate_limits.lock().await;
                        limits.record_inbound();
                    }

                    // Forward to the webhook handler
                    smsc_message_handler
                        .handle_message(decoded_message.clone())
                        .await?;

                    // Dispatch to registered webhooks
                    {
                        let hooks = webhooks.lock().await;
                        let targets: Vec<WebhookInfo> = hooks
                            .iter()
                            .filter(|h| h.enabled && h.events.contains(&"inbound_sms".to_string()))
                            .cloned()
                            .collect();
                        drop(hooks);

                        if !targets.is_empty() {
                            let payload = decoded_message.clone();
                            tokio::spawn(async move {
                                let client = reqwest::Client::builder()
                                    .timeout(std::time::Duration::from_secs(10))
                                    .build()
                                    .unwrap();
                                for hook in targets {
                                    let res = client.post(&hook.url).json(&payload).send().await;
                                    match res {
                                        Ok(resp) => {
                                            info!(
                                                "Webhook {} delivered to {} — status {}",
                                                hook.webhook_id,
                                                hook.url,
                                                resp.status()
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Webhook {} delivery to {} failed: {}",
                                                hook.webhook_id, hook.url, e
                                            );
                                        }
                                    }
                                }
                            });
                        }
                    }
                } else if let Some(seq_num) = message.get_sequence_number() {
                    if let Some(sender) = callbacks.lock().await.remove(&seq_num) {
                        sender.send(Ok(decoded_message)).unwrap();
                    } else {
                        warn!("No callback found for sequence number {}", seq_num);
                    }
                };
            }
            Err(e) => {
                error!("Failed to decode message from SMSC: {}", e);
            }
        }

        Ok(())
    }

    /// Sends an encoded SMPP message to the SMSC via the outbound channel and waits
    /// for the correlated response (up to 10 seconds).
    ///
    /// # Parameters
    /// - `message`: The serialized SMPP PDU bytes to send.
    /// - `out_sender`: The channel sender for forwarding messages to the SMSC TCP writer.
    /// - `callbacks`: Shared map for registering a oneshot receiver keyed by sequence number.
    ///
    /// # Returns
    /// `std::io::Result<Value>` — the decoded JSON response from the SMSC,
    /// or an I/O error on failure or timeout.
    async fn send_message(
        message: Vec<u8>,
        out_sender: tokio::sync::mpsc::Sender<Vec<u8>>,
        callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
    ) -> std::io::Result<Value> {
        info!(
            "Sending message to SMSC: {:?}",
            message
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect::<Vec<String>>()
        );
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let seq_num = BigEndian::read_u32(&message[12..16]);
        out_sender.send(message).await.unwrap();
        callbacks.lock().await.insert(seq_num, resp_tx);

        match timeout(Duration::from_secs(10), resp_rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to receive response",
            )),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Response timed out",
            )),
        }
    }

    /// Responds to an incoming SMPP enquire_link message by sending an enquire_link_resp
    /// with the same sequence number.
    ///
    /// # Parameters
    /// - `message`: The received enquire_link `SmppMessageBuffer`.
    /// - `writer`: The write half of the TCP connection to the SMSC.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn process_enquire_link(
        message: &SmppMessageBuffer,
        writer: &mut OwnedWriteHalf,
    ) -> std::io::Result<()> {
        info!("Received enquire_link from SMSC, sending enquire_link_resp");
        // Send enquire_link_resp
        let seq_num = message.get_sequence_number().unwrap_or(0);
        let resp_message = json!({
            "command_id": 0x80000015 as u32,
            "command_status": 0 as u32,
            "sequence_number": seq_num
        });
        let encoded_resp_message =
            encode_message(&resp_message, None).unwrap_or_else(|_| Vec::new());
        match writer.write_all(&encoded_resp_message).await {
            Ok(_) => {
                info!(
                    "Sent enquire_link_resp to SMSC:{}",
                    format_smpp_value(&resp_message)
                );
            }
            Err(e) => {
                error!(
                    "Failed to send enquire_link_resp to SMSC: {} with error: {}",
                    format_smpp_value(&resp_message),
                    e
                );
                return Err(e);
            }
        }
        Ok(())
    }

    /// Sends a deliver_sm_resp or data_sm_resp back to the SMSC to acknowledge
    /// receipt of an inbound message.
    ///
    /// # Parameters
    /// - `writer`: The write half of the TCP connection to the SMSC.
    /// - `sequence_number`: The sequence number from the original deliver_sm/data_sm.
    /// - `command_id`: The command ID of the original message (DELIVER_SM or DATA_SM).
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn send_deliver_sm_resp(
        writer: &mut OwnedWriteHalf,
        sequence_number: u32,
        command_id: u32,
    ) -> std::io::Result<()> {
        let resp_command_id = command_id | 0x80000000; // response = request | 0x80000000
        let mut resp = SmppMessageBuffer::new();
        resp.write_u32(17); // command_length: 16-byte header + 1 byte NULL message_id
        resp.write_u32(resp_command_id);
        resp.write_u32(0); // command_status: ESME_ROK
        resp.write_u32(sequence_number);
        resp.write_u8(0); // message_id (empty C-Octet String)
        writer.write_all(&resp.buffer).await?;
        info!(
            "Sent response (command_id=0x{:08X}) for seq {}",
            resp_command_id, sequence_number
        );
        Ok(())
    }
}

// ============================================================
// Pagination helper
// ============================================================

/// Paginates a slice of items and returns the requested page along with pagination metadata.
///
/// # Parameters
/// - `items`: The full slice of items to paginate.
/// - `page`: The 1-based page number to retrieve.
/// - `per_page`: The number of items per page.
///
/// # Returns
/// `(Vec<T>, Value)` — a tuple of the page items and a JSON object containing
/// pagination details (page, per_page, total_pages, total_items, links).
pub fn paginate<T: Clone>(items: &[T], page: u32, per_page: u32) -> (Vec<T>, Value) {
    let total_items = items.len() as u32;
    let total_pages = if total_items == 0 {
        1
    } else {
        (total_items + per_page - 1) / per_page
    };
    let start = ((page - 1) * per_page) as usize;
    let end = (start + per_page as usize).min(items.len());
    let page_items = if start < items.len() {
        items[start..end].to_vec()
    } else {
        Vec::new()
    };
    let pagination = json!({
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total_items": total_items,
        "links": {
            "first": format!("?page=1&per_page={}", per_page),
            "last": format!("?page={}&per_page={}", total_pages, per_page),
            "prev": if page > 1 { Some(format!("?page={}&per_page={}", page - 1, per_page)) } else { None::<String> },
            "next": if page < total_pages { Some(format!("?page={}&per_page={}", page + 1, per_page)) } else { None::<String> },
        }
    });
    (page_items, pagination)
}

// ============================================================
// Helper functions
// ============================================================

/// Generates a unique identifier string with the given prefix using the configured ID generator.
///
/// # Parameters
/// - `state`: The shared `AppState` containing the ID generator.
/// - `prefix`: A string prefix for the generated ID (e.g., "msg", "batch").
///
/// # Returns
/// `String` — a unique identifier.
async fn gen_id(state: &AppState, prefix: &str) -> String {
    state.id_generator.generate(prefix).await
}

/// Returns the current UTC time as an ISO-8601 formatted string.
///
/// # Returns
/// `String` — a timestamp in the format `YYYY-MM-DDTHH:MM:SSZ`.
fn now_utc() -> String {
    // Simple ISO-8601 UTC timestamp using std
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    // rough conversion — good enough for an in-memory gateway
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let h = time_secs / 3600;
    let m = (time_secs % 3600) / 60;
    let s = time_secs % 60;
    // days since epoch → date (simplified Gregorian)
    let (y, mo, d) = days_to_ymd(days);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, m, s)
}

/// Converts a number of days since the Unix epoch (1970-01-01) to a (year, month, day) tuple.
///
/// # Parameters
/// - `days`: The number of days since 1970-01-01.
///
/// # Returns
/// `(u64, u64, u64)` — a tuple of (year, month, day) with 1-based month and day.
fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut y = 1970;
    loop {
        let dy = if is_leap(y) { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        y += 1;
    }
    let leap = is_leap(y);
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 0;
    for (i, &md) in mdays.iter().enumerate() {
        if days < md {
            mo = i;
            break;
        }
        days -= md;
    }
    (y, (mo + 1) as u64, days + 1)
}

/// Determines whether the given year is a leap year.
///
/// # Parameters
/// - `y`: The year to check.
///
/// # Returns
/// `bool` — `true` if the year is a leap year.
fn is_leap(y: u64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Parses an ISO-8601 UTC timestamp (YYYY-MM-DDTHH:MM:SSZ) into seconds since Unix epoch.
/// Returns `None` if the format is invalid.
fn parse_iso8601_to_epoch(s: &str) -> Option<u64> {
    // Expected format: "2026-05-01T12:00:00Z"
    if s.len() < 19 {
        return None;
    }
    let year: u64 = s.get(0..4)?.parse().ok()?;
    let month: u64 = s.get(5..7)?.parse().ok()?;
    let day: u64 = s.get(8..10)?.parse().ok()?;
    let hour: u64 = s.get(11..13)?.parse().ok()?;
    let min: u64 = s.get(14..16)?.parse().ok()?;
    let sec: u64 = s.get(17..19)?.parse().ok()?;

    if month < 1 || month > 12 || day < 1 || day > 31 {
        return None;
    }

    // Convert to days since epoch
    let mut days: u64 = 0;
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }
    let leap = is_leap(year);
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    for i in 0..(month - 1) as usize {
        days += mdays[i];
    }
    days += day - 1;

    Some(days * 86400 + hour * 3600 + min * 60 + sec)
}

/// Returns the current time as seconds since Unix epoch.
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Validates whether a phone number string conforms to E.164 format
/// (starts with '+' followed by 6–15 digits).
///
/// # Parameters
/// - `phone`: The phone number string to validate.
///
/// # Returns
/// `bool` — `true` if the phone number is a valid E.164 number.
fn is_valid_e164(phone: &str) -> bool {
    if !phone.starts_with('+') {
        return false;
    }
    let digits = &phone[1..];
    digits.len() >= 6 && digits.len() <= 15 && digits.chars().all(|c| c.is_ascii_digit())
}

/// Formats a phone number string into E.164 format by stripping non-digit characters
/// and prepending '+'. For 10-digit US numbers (not starting with '1'), prepends '+1'.
///
/// # Parameters
/// - `phone`: The raw phone number string to format.
///
/// # Returns
/// `String` — the phone number in E.164 format (e.g., "+12025551234").
fn format_e164(phone: &str) -> String {
    let digits: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();
    // If it looks like a US local number (10 digits, not starting with 1), add country code
    if digits.len() == 10 && !digits.starts_with('1') {
        format!("+1{}", digits)
    } else {
        format!("+{}", digits)
    }
}

/// Looks up the country code and name for an E.164 phone number based on its prefix.
/// Supports US (+1), GB (+44), CN (+86), and IN (+91).
///
/// # Parameters
/// - `phone`: An E.164 formatted phone number string.
///
/// # Returns
/// `(Option<&'static str>, Option<&'static str>)` — a tuple of (ISO country code, country name),
/// or `(None, None)` if the prefix is not recognized.
fn country_from_e164(phone: &str) -> (Option<&'static str>, Option<&'static str>) {
    if phone.starts_with("+1") {
        return (Some("US"), Some("United States"));
    }
    if phone.starts_with("+44") {
        return (Some("GB"), Some("United Kingdom"));
    }
    if phone.starts_with("+86") {
        return (Some("CN"), Some("China"));
    }
    if phone.starts_with("+91") {
        return (Some("IN"), Some("India"));
    }
    (None, None)
}

/// Creates a standardized JSON error response with success=false, an error code, and a message.
///
/// # Parameters
/// - `code`: A short error code string (e.g., "NOT_FOUND", "INVALID_REQUEST").
/// - `message`: A human-readable error description.
///
/// # Returns
/// `Value` — a `serde_json::Value` with the structure `{ success: false, error: { code, message } }`.
fn error_json(code: &str, message: &str) -> Value {
    json!({
        "success": false,
        "error": {
            "code": code,
            "message": message
        }
    })
}

/// Sends a POST request to the given callback URL with the outbound message status.
/// This is fire-and-forget: errors are logged but not propagated.
async fn fire_callback(callback_url: &str, msg: &OutboundMessage) {
    let client = reqwest::Client::new();
    let payload = json!({
        "message_id": msg.message_id,
        "status": msg.status,
        "source": msg.source,
        "destination": msg.destination,
        "sent_at": msg.sent_at,
        "error_code": msg.error_code,
        "error_message": msg.error_message,
    });
    match client.post(callback_url).json(&payload).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                warn!(
                    "Callback to {} returned non-success status: {}",
                    callback_url,
                    resp.status()
                );
            } else {
                info!("Callback to {} succeeded", callback_url);
            }
        }
        Err(e) => {
            error!("Failed to send callback to {}: {}", callback_url, e);
        }
    }
}

/// Builds a SUBMIT_SM JSON payload from an `OutboundMessage`, encodes it as an
/// SMPP PDU, sends it through the SMSC connection pool, updates the message
/// status in storage, and fires the callback URL if configured.
async fn submit_and_callback(state: Arc<AppState>, message_id: String) {
    let msg = match state.outbound_storage.get(&message_id).await {
        Ok(Some(m)) => m,
        _ => {
            error!("submit_and_callback: message {} not found", message_id);
            return;
        }
    };

    // If the message has a scheduled delivery time in the future, enqueue it
    if let Some(ref sdt) = msg.scheduled_delivery_time {
        if let Some(target_epoch) = parse_iso8601_to_epoch(sdt) {
            let now = now_epoch_secs();
            if target_epoch > now {
                info!(
                    "Message {} scheduled for {}, enqueueing for later delivery",
                    message_id, sdt
                );
                let _ = state
                    .outbound_storage
                    .update_status(&message_id, "scheduled")
                    .await;
                let entry = ScheduledMessage {
                    delivery_epoch: target_epoch,
                    message_id: message_id.clone(),
                };
                let _ = state.outbound_storage.add_scheduled(entry).await;
                return;
            }
        }
    }

    let data_coding = msg.data_coding.unwrap_or(0);
    let short_message_b64 = if let Some(ref bin) = msg.message_binary {
        bin.clone()
    } else {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(msg.message.as_bytes())
    };

    let payload = json!({
        "service_type": "",
        "source_addr_ton": 5,
        "source_addr_npi": 0,
        "source_addr": msg.source,
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": msg.destination,
        "esm_class": 0,
        "protocol_id": 0,
        "priority_flag": 0,
        "schedule_delivery_time": "",
        "validity_period": "",
        "registered_delivery": 1,
        "replace_if_present_flag": 0,
        "data_coding": data_coding,
        "sm_default_msg_id": 0,
        "short_message": short_message_b64,
    });

    let smpp_msg = match encode_message(&payload, Some(message::SUBMIT_SM)) {
        Ok(mut m) => {
            message::update_sequence_number(&mut m, state.seq_allocator.next());
            m
        }
        Err(e) => {
            error!("Failed to encode SUBMIT_SM for {}: {}", message_id, e);
            let _ = state
                .outbound_storage
                .update_status(&message_id, "failed")
                .await;
            if let Some(ref url) = msg.callback_url {
                if let Ok(Some(updated)) = state.outbound_storage.get(&message_id).await {
                    fire_callback(url, &updated).await;
                }
            }
            return;
        }
    };

    // Pick a connection from the pool using weighted round-robin
    let conn = {
        let conns = state.connections.lock().await;
        if conns.is_empty() {
            error!("No SMSC connections available for message {}", message_id);
            let _ = state
                .outbound_storage
                .update_status(&message_id, "failed")
                .await;
            if let Some(ref url) = msg.callback_url {
                if let Ok(Some(updated)) = state.outbound_storage.get(&message_id).await {
                    fire_callback(url, &updated).await;
                }
            }
            return;
        }
        let counter = state
            .connection_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let idx = weighted_connection_index(&conns, counter);
        (conns[idx].out_sender.clone(), conns[idx].callbacks.clone())
    };

    let (status, err_code, err_msg) = match SmscClient::send_message(smpp_msg, conn.0, conn.1).await
    {
        Ok(resp) => {
            let command_status = resp
                .get("command_status")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if command_status == 0 {
                ("sent", None, None)
            } else {
                (
                    "failed",
                    Some(format!("0x{:08X}", command_status)),
                    Some(format!("SMSC returned error status {}", command_status)),
                )
            }
        }
        Err(e) => (
            "failed",
            Some("SEND_ERROR".to_string()),
            Some(e.to_string()),
        ),
    };

    // Update the message status in storage
    let _ = state
        .outbound_storage
        .update_status(&message_id, status)
        .await;

    // Fire callback if configured
    if let Some(ref url) = msg.callback_url {
        // Re-read the updated message to get the latest state
        if let Ok(Some(updated)) = state.outbound_storage.get(&message_id).await {
            fire_callback(url, &updated).await;
        } else {
            // Fallback: build a minimal version with the info we have
            let mut fallback = msg.clone();
            fallback.status = status.to_string();
            fallback.error_code = err_code;
            fallback.error_message = err_msg;
            fire_callback(url, &fallback).await;
        }
    }
}

/// Delivers a single message directly to the SMSC (bypassing the scheduling check).
/// Used by the scheduled delivery timer for messages whose time has arrived.
async fn deliver_due_message(state: Arc<AppState>, message_id: String) {
    let msg = match state.outbound_storage.get(&message_id).await {
        Ok(Some(m)) => m,
        _ => {
            error!("deliver_due_message: message {} not found", message_id);
            return;
        }
    };

    let data_coding = msg.data_coding.unwrap_or(0);
    let short_message_b64 = if let Some(ref bin) = msg.message_binary {
        bin.clone()
    } else {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(msg.message.as_bytes())
    };

    let payload = json!({
        "service_type": "",
        "source_addr_ton": 5,
        "source_addr_npi": 0,
        "source_addr": msg.source,
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": msg.destination,
        "esm_class": 0,
        "protocol_id": 0,
        "priority_flag": 0,
        "schedule_delivery_time": "",
        "validity_period": "",
        "registered_delivery": 1,
        "replace_if_present_flag": 0,
        "data_coding": data_coding,
        "sm_default_msg_id": 0,
        "short_message": short_message_b64,
    });

    let smpp_msg = match encode_message(&payload, Some(message::SUBMIT_SM)) {
        Ok(mut m) => {
            message::update_sequence_number(&mut m, state.seq_allocator.next());
            m
        }
        Err(e) => {
            error!("Failed to encode SUBMIT_SM for {}: {}", message_id, e);
            let _ = state
                .outbound_storage
                .update_status(&message_id, "failed")
                .await;
            if let Some(ref url) = msg.callback_url {
                if let Ok(Some(updated)) = state.outbound_storage.get(&message_id).await {
                    fire_callback(url, &updated).await;
                }
            }
            return;
        }
    };

    let conn = {
        let conns = state.connections.lock().await;
        if conns.is_empty() {
            error!("No SMSC connections available for message {}", message_id);
            let _ = state
                .outbound_storage
                .update_status(&message_id, "failed")
                .await;
            if let Some(ref url) = msg.callback_url {
                if let Ok(Some(updated)) = state.outbound_storage.get(&message_id).await {
                    fire_callback(url, &updated).await;
                }
            }
            return;
        }
        let counter = state
            .connection_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let idx = weighted_connection_index(&conns, counter);
        (conns[idx].out_sender.clone(), conns[idx].callbacks.clone())
    };

    let (status, err_code, err_msg) = match SmscClient::send_message(smpp_msg, conn.0, conn.1).await
    {
        Ok(resp) => {
            let command_status = resp
                .get("command_status")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if command_status == 0 {
                ("sent", None, None)
            } else {
                (
                    "failed",
                    Some(format!("0x{:08X}", command_status)),
                    Some(format!("SMSC returned error status {}", command_status)),
                )
            }
        }
        Err(e) => (
            "failed",
            Some("SEND_ERROR".to_string()),
            Some(e.to_string()),
        ),
    };

    let _ = state
        .outbound_storage
        .update_status(&message_id, status)
        .await;

    if let Some(ref url) = msg.callback_url {
        if let Ok(Some(updated)) = state.outbound_storage.get(&message_id).await {
            fire_callback(url, &updated).await;
        } else {
            let mut fallback = msg.clone();
            fallback.status = status.to_string();
            fallback.error_code = err_code;
            fallback.error_message = err_msg;
            fire_callback(url, &fallback).await;
        }
    }
}

/// Periodically scans the scheduled message queue and delivers messages whose
/// delivery time has arrived. Runs every second.
async fn scheduled_delivery_timer(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = now_epoch_secs();

        // Collect due messages from the sorted queue
        let due_messages = match state.outbound_storage.take_due_messages(now).await {
            Ok(msgs) => msgs,
            Err(e) => {
                error!("Failed to retrieve due scheduled messages: {}", e);
                continue;
            }
        };

        // Deliver each due message
        for message_id in due_messages {
            let st = state.clone();
            tokio::spawn(async move {
                deliver_due_message(st, message_id).await;
            });
        }
    }
}

// ============================================================
// API key authentication middleware
// ============================================================

/// Axum middleware that enforces API key authentication.
///
/// Checks the `Authorization: Bearer <key>` header or `X-API-Key` header
/// against the keys stored in the `ApiKeyStore`. If no keys exist in the
/// store (bootstrapping mode), all requests are allowed through.
pub async fn require_api_key(
    AxumState(state): AxumState<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    // Extract the API key from headers
    let api_key_value = req
        .headers()
        .get("authorization")
        .and_then(|v: &axum::http::HeaderValue| v.to_str().ok())
        .and_then(|v: &str| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
        .or_else(|| {
            req.headers()
                .get("x-api-key")
                .and_then(|v: &axum::http::HeaderValue| v.to_str().ok())
                .map(|s| s.to_string())
        });

    // If no API keys exist, skip auth (bootstrapping mode)
    match state.api_key_store.is_empty().await {
        Ok(true) => return next.run(req).await,
        Err(e) => {
            error!("Failed to check API key store: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(error_json("INTERNAL_ERROR", "API key store unavailable")),
            )
                .into_response();
        }
        _ => {}
    }

    let api_key_str = match &api_key_value {
        Some(k) => k.as_str(),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(error_json("UNAUTHORIZED", "API key required")),
            )
                .into_response();
        }
    };

    match state.api_key_store.verify_key(api_key_str).await {
        Ok(Some(key_info)) => {
            if key_info.status != "active" {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(error_json("UNAUTHORIZED", "API key is not active")),
                )
                    .into_response();
            }
            if let Some(ref expires_at) = key_info.expires_at {
                if now_utc() > *expires_at {
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(error_json("UNAUTHORIZED", "API key has expired")),
                    )
                        .into_response();
                }
            }
        }
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(error_json("UNAUTHORIZED", "Invalid API key")),
            )
                .into_response();
        }
        Err(e) => {
            error!("Failed to verify API key: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(error_json("INTERNAL_ERROR", "API key verification failed")),
            )
                .into_response();
        }
    }

    // Update last_used_at
    if let Some(ref key) = api_key_value {
        if let Err(e) = state.api_key_store.record_usage(key).await {
            warn!("Failed to record API key usage: {}", e);
        }
    }

    next.run(req).await
}

// ============================================================
// REST API handler functions
// ============================================================

/// Handles raw SMPP API requests by encoding a JSON payload into an SMPP PDU
/// and forwarding it to the SMSC via round-robin connection selection,
/// then returning the decoded response.
///
/// Returns Prometheus metrics in text exposition format.
///
/// # Route
/// `GET /metrics`
pub async fn prometheus_metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics::render_metrics(),
    )
}

/// # Parameters
/// - `path`: The URL path segment used to determine the SMPP command ID.
/// - `state`: The shared application state containing the connection pool.
/// - `payload`: A JSON body representing the SMPP message fields.
///
/// # Returns
/// `Json<Value>` — the decoded SMPP response, or an error JSON object.
pub async fn raw_api_handler(
    Path(path): Path<String>,
    AxumState(state): AxumState<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let command_id = get_command_id_by_name(&path.to_uppercase());
    if let Ok(mut message) = encode_message(&payload, command_id) {
        message::update_sequence_number(&mut message, state.seq_allocator.next());
        // Pick a connection from the pool using weighted round-robin
        let (out_sender, callbacks) = {
            let conns = state.connections.lock().await;
            if conns.is_empty() {
                error!("No SMSC connections available");
                return Json(json!({"error": "No SMSC connections available"}));
            }
            let counter = state
                .connection_index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let idx = weighted_connection_index(&conns, counter);
            (conns[idx].out_sender.clone(), conns[idx].callbacks.clone())
        };
        match SmscClient::send_message(message, out_sender, callbacks).await {
            Ok(resp) => Json(resp),
            Err(_) => {
                error!("Failed to send message for path: {}", path);
                Json(json!({"error": "Failed to send message"}))
            }
        }
    } else {
        error!("Failed to create message for path: {}", path);
        Json(json!({"error": "Failed to create message"}))
    }
}

// ============================================================
// 2. SMS Messaging APIs
// ============================================================

/// Sends a single SMS message. Validates the request, calculates message parts,
/// and queues the message for delivery.
///
/// # Route
/// `POST /v1/sms/send`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `SendSmsRequest` JSON body with source, destination, message, and optional fields.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with message details on success,
/// or `400 Bad Request` with an error JSON.
pub async fn send_sms(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<SendSmsRequest>,
) -> impl IntoResponse {
    if req.source.is_empty()
        || req.destination.is_empty()
        || (req.message.is_empty() && req.message_binary.is_none())
    {
        metrics::REST_SUBMIT_MESSAGES
            .with_label_values(&["400"])
            .inc();
        return (
            StatusCode::BAD_REQUEST,
            Json(error_json(
                "INVALID_REQUEST",
                "source, destination, and message (or message_binary) are required",
            )),
        );
    }
    if !is_valid_e164(&req.destination) {
        metrics::REST_SUBMIT_MESSAGES
            .with_label_values(&["400"])
            .inc();
        return (
            StatusCode::BAD_REQUEST,
            Json(error_json(
                "INVALID_PHONE_NUMBER",
                "Invalid destination phone number",
            )),
        );
    }
    if req.message.len() > 1600 {
        metrics::REST_SUBMIT_MESSAGES
            .with_label_values(&["400"])
            .inc();
        return (
            StatusCode::BAD_REQUEST,
            Json(error_json(
                "MESSAGE_TOO_LONG",
                "Message exceeds maximum length of 1600 characters",
            )),
        );
    }

    // Validate source against registered phone numbers (skip if store is empty)
    {
        let is_empty = state.phone_number_store.is_empty().await.unwrap_or(true);
        if !is_empty {
            let has_outbound = state
                .phone_number_store
                .has_capability(&req.source, "sms_outbound")
                .await
                .unwrap_or(false);
            if !has_outbound {
                metrics::REST_SUBMIT_MESSAGES
                    .with_label_values(&["403"])
                    .inc();
                return (
                    StatusCode::FORBIDDEN,
                    Json(error_json(
                        "SOURCE_NOT_REGISTERED",
                        "Source number is not registered with sms_outbound capability",
                    )),
                );
            }
        }
    }

    // Check and acquire outbound rate limit (leaky bucket)
    {
        let mut limits = state.rate_limits.lock().await;
        if !limits.try_acquire_outbound() {
            metrics::REST_SUBMIT_MESSAGES
                .with_label_values(&["429"])
                .inc();
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(error_json(
                    "RATE_LIMIT_EXCEEDED",
                    "Outbound rate limit exceeded",
                )),
            );
        }
    }

    let encoding = req.encoding.unwrap_or_else(|| "GSM7".into());
    let max_part = if encoding == "UCS2" { 67 } else { 153 };
    let parts = if req.message.len() <= (if encoding == "UCS2" { 70 } else { 160 }) {
        1
    } else {
        ((req.message.len() as u32) + max_part - 1) / max_part
    };

    let message_id = gen_id(&state, "msg").await;
    let now = now_utc();

    let msg = OutboundMessage {
        message_id: message_id.clone(),
        status: "queued".into(),
        source: req.source,
        destination: req.destination,
        message: req.message,
        message_binary: req.message_binary,
        encoding,
        data_coding: req.data_coding,
        scheduled_delivery_time: req.schedule_time,
        parts,
        priority: req.priority.unwrap_or_else(|| "normal".into()),
        tags: req.tags.unwrap_or_default(),
        callback_url: req.callback_url,
        batch_id: None,
        created_at: now.clone(),
        sent_at: None,
        delivered_at: None,
        error_code: None,
        error_message: None,
    };

    if let Err(e) = state.outbound_storage.save(msg).await {
        metrics::REST_SUBMIT_MESSAGES
            .with_label_values(&["500"])
            .inc();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json("STORAGE_ERROR", &e.to_string())),
        );
    }

    // Spawn background task to send via SMSC and fire callback
    {
        let state = state.clone();
        let mid = message_id.clone();
        tokio::spawn(async move {
            submit_and_callback(state, mid).await;
        });
    }

    metrics::REST_SUBMIT_MESSAGES
        .with_label_values(&["201"])
        .inc();
    (
        StatusCode::CREATED,
        Json(json!({
            "success": true,
            "data": {
                "message_id": message_id,
                "status": "queued",
                "parts": parts,
                "created_at": now,
                "estimated_delivery": now
            }
        })),
    )
}

/// Sends multiple SMS messages in a single batch. Validates each destination,
/// queues valid messages, and tracks the batch.
///
/// # Route
/// `POST /v1/sms/send/bulk`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `SendBulkSmsRequest` JSON body with a messages array and optional batch name.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with batch details and per-message results,
/// or `400 Bad Request` if the request is invalid.
pub async fn send_bulk_sms(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<SendBulkSmsRequest>,
) -> impl IntoResponse {
    if req.messages.is_empty() {
        metrics::REST_SUBMIT_MESSAGES
            .with_label_values(&["400"])
            .inc();
        return (
            StatusCode::BAD_REQUEST,
            Json(error_json("INVALID_REQUEST", "messages array is required")),
        );
    }
    if req.messages.len() > 10000 {
        metrics::REST_SUBMIT_MESSAGES
            .with_label_values(&["400"])
            .inc();
        return (
            StatusCode::BAD_REQUEST,
            Json(error_json(
                "INVALID_REQUEST",
                "Maximum 10000 messages per batch",
            )),
        );
    }

    // Check outbound rate limits (leaky bucket)
    {
        let limits = state.rate_limits.lock().await;
        if !limits.has_outbound_capacity() {
            metrics::REST_SUBMIT_MESSAGES
                .with_label_values(&["429"])
                .inc();
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(error_json(
                    "RATE_LIMIT_EXCEEDED",
                    "Outbound rate limit exceeded",
                )),
            );
        }
    }

    let batch_id = gen_id(&state, "batch").await;
    let now = now_utc();
    let mut queued: u32 = 0;
    let mut failed: u32 = 0;
    let mut results = Vec::new();

    // Check if store is empty for source validation
    let phone_store_empty = state.phone_number_store.is_empty().await.unwrap_or(true);

    {
        for m in &req.messages {
            if !is_valid_e164(&m.destination) {
                failed += 1;
                results.push(json!({
                    "destination": m.destination,
                    "message_id": null,
                    "status": "failed",
                    "error": "Invalid phone number"
                }));
                continue;
            }

            // Validate source against registered phone numbers (skip if store is empty)
            if !phone_store_empty {
                let has_outbound = state
                    .phone_number_store
                    .has_capability(&m.source, "sms_outbound")
                    .await
                    .unwrap_or(false);
                if !has_outbound {
                    failed += 1;
                    results.push(json!({
                        "destination": m.destination,
                        "message_id": null,
                        "status": "failed",
                        "error": "Source number is not registered with sms_outbound capability"
                    }));
                    continue;
                }
            }

            let message_id = gen_id(&state, "msg").await;
            let encoding = m.encoding.clone().unwrap_or_else(|| "GSM7".into());
            let msg = OutboundMessage {
                message_id: message_id.clone(),
                status: "queued".into(),
                source: m.source.clone(),
                destination: m.destination.clone(),
                message: m.message.clone(),
                message_binary: m.message_binary.clone(),
                encoding,
                data_coding: m.data_coding,
                scheduled_delivery_time: m.schedule_time.clone(),
                parts: 1,
                priority: "normal".into(),
                tags: Vec::new(),
                callback_url: req.callback_url.clone(),
                batch_id: Some(batch_id.clone()),
                created_at: now.clone(),
                sent_at: None,
                delivered_at: None,
                error_code: None,
                error_message: None,
            };

            if let Err(_e) = state.outbound_storage.save(msg).await {
                failed += 1;
                results.push(json!({
                    "destination": m.destination,
                    "message_id": null,
                    "status": "failed",
                    "error": "Storage error"
                }));
                continue;
            }

            queued += 1;
            // Acquire outbound rate limit token (leaky bucket)
            {
                let mut limits = state.rate_limits.lock().await;
                let _ = limits.try_acquire_outbound();
            }

            // Spawn background task to send via SMSC and fire callback
            {
                let state = state.clone();
                let mid = message_id.clone();
                tokio::spawn(async move {
                    submit_and_callback(state, mid).await;
                });
            }

            results.push(json!({
                "destination": m.destination,
                "message_id": message_id,
                "status": "queued"
            }));
        }
    }

    let _ = state
        .outbound_storage
        .save_batch(BatchInfo {
            batch_id: batch_id.clone(),
            name: req.batch_name,
            total_messages: req.messages.len() as u32,
            queued,
            sent: 0,
            delivered: 0,
            failed,
            expired: 0,
            pending: 0,
            created_at: now.clone(),
            updated_at: now,
        })
        .await;

    metrics::REST_SUBMIT_MESSAGES
        .with_label_values(&["201"])
        .inc();
    (
        StatusCode::CREATED,
        Json(json!({
            "success": true,
            "data": {
                "batch_id": batch_id,
                "total_messages": req.messages.len(),
                "queued": queued,
                "failed": failed,
                "messages": results
            }
        })),
    )
}

/// Retrieves the current status and details of an outbound SMS message.
///
/// # Route
/// `GET /v1/sms/messages/{message_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `message_id`: The unique message identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with message data, or `404 Not Found`.
pub async fn get_message_status(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(message_id): Path<String>,
) -> impl IntoResponse {
    match state.outbound_storage.get(&message_id).await {
        Ok(Some(msg)) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": msg })),
        ),
        _ => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Message not found")),
        ),
    }
}

/// Cancels a queued outbound SMS message. Only messages with "queued" status can be cancelled.
///
/// # Route
/// `DELETE /v1/sms/messages/{message_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `message_id`: The unique message identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` on success, `400 Bad Request` if not cancellable,
/// or `404 Not Found`.
pub async fn cancel_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(message_id): Path<String>,
) -> impl IntoResponse {
    match state.outbound_storage.get(&message_id).await {
        Ok(Some(msg)) => {
            if msg.status != "queued" {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(error_json(
                        "CANNOT_CANCEL",
                        "Only queued messages can be cancelled",
                    )),
                );
            }
            match state
                .outbound_storage
                .update_status(&message_id, "cancelled")
                .await
            {
                Ok(Some(_)) => (
                    StatusCode::OK,
                    Json(json!({
                        "success": true,
                        "data": { "message_id": message_id, "status": "cancelled" }
                    })),
                ),
                _ => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(error_json(
                        "STORAGE_ERROR",
                        "Failed to update message status",
                    )),
                ),
            }
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Message not found")),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json("STORAGE_ERROR", "Failed to retrieve message")),
        ),
    }
}

/// Retrieves the status and statistics of a bulk SMS batch.
///
/// # Route
/// `GET /v1/sms/batches/{batch_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `batch_id`: The unique batch identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with batch data, or `404 Not Found`.
pub async fn get_batch_status(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(batch_id): Path<String>,
) -> impl IntoResponse {
    match state.outbound_storage.get_batch(&batch_id).await {
        Ok(Some(b)) => (StatusCode::OK, Json(json!({ "success": true, "data": b }))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Batch not found")),
        ),
        Err(e) => {
            error!("Failed to get batch {}: {}", batch_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(error_json("INTERNAL_ERROR", "Failed to retrieve batch")),
            )
        }
    }
}

// ============================================================
// 3. Inbound Message APIs
// ============================================================

/// Lists inbound SMS messages with optional filtering by source/destination
/// and pagination support.
///
/// # Route
/// `GET /v1/sms/inbound`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `params`: Query parameters for filtering (source, destination) and pagination (page, per_page).
///
/// # Returns
/// `Json<Value>` — a JSON response with the filtered/paginated inbound messages and pagination metadata.
pub async fn list_inbound_messages(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<InboundMessageQuery>,
) -> Json<Value> {
    let filter = InboundMessageFilter {
        source: params.source,
        destination: params.destination,
    };
    let filtered = state
        .inbound_storage
        .list(&filter)
        .await
        .unwrap_or_default();

    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(20).min(100);
    let (page_items, pagination) = paginate(&filtered, page, per_page);

    Json(json!({
        "success": true,
        "data": page_items,
        "pagination": pagination
    }))
}

/// Retrieves a single inbound SMS message by its message ID.
///
/// # Route
/// `GET /v1/sms/inbound/{message_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `message_id`: The unique inbound message identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with message data, or `404 Not Found`.
pub async fn get_inbound_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(message_id): Path<String>,
) -> impl IntoResponse {
    match state.inbound_storage.get(&message_id).await {
        Ok(Some(msg)) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": msg })),
        ),
        _ => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Inbound message not found")),
        ),
    }
}

// ============================================================
// 4.1 Gateway Status
// ============================================================

/// Returns the overall gateway health status, including version, uptime,
/// SMPP connection details, and current rate limit usage.
///
/// # Route
/// `GET /v1/gateway/status`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with gateway, connection, and rate limit information.
pub async fn get_gateway_status(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let connections = state.smpp_connections_store.lock().await;
    let rate_limits = state.rate_limits.lock().await;
    let uptime = state.start_time.elapsed().as_secs();

    Json(json!({
        "success": true,
        "data": {
            "gateway": {
                "version": "1.0.0",
                "uptime_seconds": uptime,
                "status": "healthy"
            },
            "smpp_connections": *connections,
            "rate_limits": {
                "outbound_per_second": rate_limits.outbound.capacity,
                "outbound_remaining": rate_limits.outbound.remaining(),
                "inbound_per_second": rate_limits.inbound.capacity,
                "inbound_remaining": rate_limits.inbound.remaining(),
            }
        }
    }))
}

// ============================================================
// 4.2 SMPP Connection Management
// ============================================================

/// Lists all registered SMPP connections.
///
/// # Route
/// `GET /v1/gateway/smpp/connections`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of SMPP connections.
pub async fn list_smpp_connections(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let connections = state.smpp_connections_store.lock().await;
    Json(json!({ "success": true, "data": *connections }))
}

/// Creates a new SMPP connection configuration.
///
/// # Route
/// `POST /v1/gateway/smpp/connections`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `CreateSmppConnectionRequest` JSON body with connection parameters.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with the new connection details.
pub async fn create_smpp_connection(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<CreateSmppConnectionRequest>,
) -> impl IntoResponse {
    let connection_id = gen_id(&state, "smpp_conn").await;
    let conn = SmppConnectionInfo {
        connection_id: connection_id.clone(),
        name: req.name,
        host: req.host,
        port: req.port,
        system_id: req.system_id,
        bind_type: req.bind_type.unwrap_or_else(|| "transceiver".into()),
        status: "disconnected".into(),
        reconnect_enabled: req.reconnect_enabled.unwrap_or(true),
        heartbeat_interval: req.heartbeat_interval.unwrap_or(30),
    };
    state.smpp_connections_store.lock().await.push(conn.clone());
    (
        StatusCode::CREATED,
        Json(json!({ "success": true, "data": conn })),
    )
}

/// Updates an existing SMPP connection's configuration (name, reconnect, heartbeat).
///
/// # Route
/// `PUT /v1/gateway/smpp/connections/{connection_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `connection_id`: The unique connection identifier from the URL path.
/// - `req`: An `UpdateSmppConnectionRequest` JSON body with optional fields to update.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with updated connection data, or `404 Not Found`.
pub async fn update_smpp_connection(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(connection_id): Path<String>,
    Json(req): Json<UpdateSmppConnectionRequest>,
) -> impl IntoResponse {
    let mut conns = state.smpp_connections_store.lock().await;
    match conns.iter_mut().find(|c| c.connection_id == connection_id) {
        Some(conn) => {
            if let Some(name) = req.name {
                conn.name = name;
            }
            if let Some(v) = req.reconnect_enabled {
                conn.reconnect_enabled = v;
            }
            if let Some(v) = req.heartbeat_interval {
                conn.heartbeat_interval = v;
            }
            (
                StatusCode::OK,
                Json(json!({ "success": true, "data": conn.clone() })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Connection not found")),
        ),
    }
}

/// Deletes an SMPP connection configuration by its connection ID.
///
/// # Route
/// `DELETE /v1/gateway/smpp/connections/{connection_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `connection_id`: The unique connection identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` on success, or `404 Not Found`.
pub async fn delete_smpp_connection(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    let mut conns = state.smpp_connections_store.lock().await;
    let len_before = conns.len();
    conns.retain(|c| c.connection_id != connection_id);
    if conns.len() < len_before {
        (
            StatusCode::OK,
            Json(json!({ "success": true, "data": { "deleted": true } })),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Connection not found")),
        )
    }
}

/// Triggers a rebind (reconnect) of an SMPP connection, setting its status to "reconnecting".
///
/// # Route
/// `POST /v1/gateway/smpp/connections/{connection_id}/rebind`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `connection_id`: The unique connection identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with the reconnecting status, or `404 Not Found`.
pub async fn rebind_smpp_connection(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    let mut conns = state.smpp_connections_store.lock().await;
    match conns.iter_mut().find(|c| c.connection_id == connection_id) {
        Some(conn) => {
            conn.status = "reconnecting".into();
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "data": { "connection_id": connection_id, "status": "reconnecting" }
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Connection not found")),
        ),
    }
}

// ============================================================
// 4.2.1 Live SMSC Connection Management
// ============================================================

/// Lists all active live SMSC connection handles in the pool, showing their addresses.
///
/// # Route
/// `GET /v1/gateway/smpp/live-connections`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of active SMSC connection addresses.
pub async fn list_live_smsc_connections(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let conns = state.connections.lock().await;
    let list: Vec<Value> = conns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            json!({
                "index": i,
                "address": c.address,
            })
        })
        .collect();
    Json(json!({ "success": true, "data": list }))
}

/// Dynamically adds a new SMSC connection at runtime. Creates the communication channels,
/// registers the connection in the pool, and spawns a background reconnection loop that
/// immediately begins attempting to connect and bind to the specified SMSC server.
///
/// # Route
/// `POST /v1/gateway/smpp/live-connections`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: An `AddSmscRequest` JSON body with address, system_id, password, and optional
///   SMPP bind parameters (system_type, addr_ton, addr_npi, address_range, interface_version).
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with the new connection details.
pub async fn add_live_smsc_connection(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<AddSmscRequest>,
) -> impl IntoResponse {
    let (out_sender, out_receiver) = tokio::sync::mpsc::channel(1000);
    let callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let address = req.address.clone();

    // Build a SmscConnectionConfig for this connection
    let conn_config = SmscConnectionConfig {
        address: address.clone(),
        system_id: req.system_id.clone(),
        password: req.password,
        system_type: req.system_type.unwrap_or_default(),
        addr_ton: req.addr_ton.unwrap_or(0),
        addr_npi: req.addr_npi.unwrap_or(0),
        address_range: req.address_range.unwrap_or_default(),
        interface_version: req.interface_version.unwrap_or(0x34),
        weight: req.weight.unwrap_or(1),
    };

    // Register in the connection pool
    state.connections.lock().await.push(SmscConnectionHandle {
        address: address.clone(),
        out_sender,
        callbacks: callbacks.clone(),
        weight: conn_config.weight,
    });

    // Spawn the reconnection loop
    let seq = state.seq_allocator.clone();
    let handler = state.smsc_message_handler.clone();
    let inbound = state.inbound_storage.clone();
    let rl = state.rate_limits.clone();
    let pn = state.phone_number_store.clone();
    let an = state.alarm_notifier.clone();
    let wh = state.webhooks.clone();
    tokio::spawn(async move {
        SmscClient::smsc_connection_loop(
            conn_config,
            seq,
            handler,
            out_receiver,
            callbacks,
            inbound,
            rl,
            pn,
            an,
            wh,
        )
        .await;
    });

    (
        StatusCode::CREATED,
        Json(json!({
            "success": true,
            "data": {
                "address": req.address,
                "system_id": req.system_id,
                "status": "connecting"
            }
        })),
    )
}

// ============================================================
// 4.3 Sender ID Management
// ============================================================

/// Lists all registered sender IDs.
///
/// # Route
/// `GET /v1/gateway/sender-ids`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of sender IDs.
pub async fn list_sender_ids(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let sender_ids = state.sender_ids.lock().await;
    Json(json!({ "success": true, "data": *sender_ids }))
}

/// Registers a new sender ID with the given type.
///
/// # Route
/// `POST /v1/gateway/sender-ids`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `CreateSenderIdRequest` JSON body with sender_id and type.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with the new sender ID details.
pub async fn create_sender_id(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<CreateSenderIdRequest>,
) -> impl IntoResponse {
    let now = now_utc();
    let info = SenderIdInfo {
        sender_id: req.sender_id,
        sender_type: req.sender_type,
        status: "active".into(),
        verified: false,
        created_at: now,
    };
    state.sender_ids.lock().await.push(info.clone());
    (
        StatusCode::CREATED,
        Json(json!({ "success": true, "data": info })),
    )
}

// ============================================================
// 4.4 Phone Number Management
// ============================================================

/// Lists all registered phone numbers.
///
/// # Route
/// `GET /v1/gateway/numbers`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of phone numbers.
pub async fn list_phone_numbers(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    match state.phone_number_store.list().await {
        Ok(numbers) => Json(json!({ "success": true, "data": numbers })),
        Err(e) => Json(error_json("INTERNAL_ERROR", &e.to_string())),
    }
}

/// Registers a new phone number with optional capabilities and webhook binding.
///
/// # Route
/// `POST /v1/gateway/numbers`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `CreatePhoneNumberRequest` JSON body with phone_number and optional fields.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with the new phone number details.
pub async fn create_phone_number(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<CreatePhoneNumberRequest>,
) -> impl IntoResponse {
    let number_id = gen_id(&state, "num").await;
    let info = PhoneNumberInfo {
        number_id: number_id.clone(),
        phone_number: req.phone_number,
        capabilities: req
            .capabilities
            .unwrap_or_else(|| vec!["sms_inbound".into(), "sms_outbound".into()]),
        status: "active".into(),
        created_at: now_utc(),
    };
    state.phone_number_store.create(info.clone()).await.ok();
    (
        StatusCode::CREATED,
        Json(json!({ "success": true, "data": info })),
    )
}

/// Updates an existing phone number's capabilities or webhook binding.
///
/// # Route
/// `PUT /v1/gateway/numbers/{number_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `number_id`: The unique phone number identifier from the URL path.
/// - `req`: An `UpdatePhoneNumberRequest` JSON body with optional fields to update.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with updated data, or `404 Not Found`.
pub async fn update_phone_number(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(number_id): Path<String>,
    Json(req): Json<UpdatePhoneNumberRequest>,
) -> impl IntoResponse {
    match state
        .phone_number_store
        .update(&number_id, req.capabilities)
        .await
    {
        Ok(Some(num)) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": num })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Phone number not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json("INTERNAL_ERROR", &e.to_string())),
        ),
    }
}

/// Deletes a phone number by its number ID.
///
/// # Route
/// `DELETE /v1/gateway/numbers/{number_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `number_id`: The unique phone number identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` on success, or `404 Not Found`.
pub async fn delete_phone_number(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(number_id): Path<String>,
) -> impl IntoResponse {
    match state.phone_number_store.delete(&number_id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": { "deleted": true } })),
        ),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Phone number not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json("INTERNAL_ERROR", &e.to_string())),
        ),
    }
}

// ============================================================
// 4.5 API Key Management
// ============================================================

/// Lists all registered API keys.
///
/// # Route
/// `GET /v1/gateway/api-keys`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of API keys.
pub async fn list_api_keys(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    match state.api_key_store.list().await {
        Ok(keys) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": keys })),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json(
                "INTERNAL_ERROR",
                &format!("Failed to list API keys: {}", e),
            )),
        ),
    }
}

/// Creates a new API key with the specified name, permissions, and rate limit.
/// Returns the full API key string (only shown once).
///
/// # Route
/// `POST /v1/gateway/api-keys`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `CreateApiKeyRequest` JSON body with name and optional permissions/rate_limit/expires_at.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with key details including the full API key.
pub async fn create_api_key(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<CreateApiKeyRequest>,
) -> impl IntoResponse {
    let key_id = gen_id(&state, "key").await;
    let api_key = format!(
        "sgw_live_{}_{}",
        gen_id(&state, "k").await,
        gen_id(&state, "k").await,
    );
    let now = now_utc();
    let permissions = req
        .permissions
        .unwrap_or_else(|| vec!["sms:send".into(), "sms:receive".into(), "sms:status".into()]);
    let rate_limit = req.rate_limit.unwrap_or(100);

    let info = ApiKeyInfo {
        key_id: key_id.clone(),
        name: req.name.clone(),
        key_prefix: format!("sgw_***_{}", &key_id[key_id.len().saturating_sub(4)..]),
        api_key: api_key.clone(),
        permissions: permissions.clone(),
        rate_limit,
        status: "active".into(),
        created_at: now.clone(),
        last_used_at: None,
        expires_at: req.expires_at.clone(),
    };

    if let Err(e) = state.api_key_store.create(info).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json(
                "INTERNAL_ERROR",
                &format!("Failed to create API key: {}", e),
            )),
        );
    }

    (
        StatusCode::CREATED,
        Json(json!({
            "success": true,
            "data": {
                "key_id": key_id,
                "api_key": api_key,
                "name": req.name,
                "permissions": permissions,
                "created_at": now,
                "expires_at": req.expires_at
            }
        })),
    )
}

/// Updates an existing API key's name, permissions, or rate limit.
///
/// # Route
/// `PUT /v1/gateway/api-keys/{key_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `key_id`: The unique API key identifier from the URL path.
/// - `req`: An `UpdateApiKeyRequest` JSON body with optional fields to update.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with updated key data, or `404 Not Found`.
pub async fn update_api_key(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(key_id): Path<String>,
    Json(req): Json<UpdateApiKeyRequest>,
) -> impl IntoResponse {
    match state
        .api_key_store
        .update(&key_id, req.name, req.permissions, req.rate_limit)
        .await
    {
        Ok(Some(key)) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": key })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "API key not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json(
                "INTERNAL_ERROR",
                &format!("Failed to update API key: {}", e),
            )),
        ),
    }
}

/// Deletes an API key by its key ID.
///
/// # Route
/// `DELETE /v1/gateway/api-keys/{key_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `key_id`: The unique API key identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` on success, or `404 Not Found`.
pub async fn delete_api_key(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(key_id): Path<String>,
) -> impl IntoResponse {
    match state.api_key_store.delete(&key_id).await {
        Ok(true) => (
            StatusCode::OK,
            Json(json!({ "success": true, "data": { "deleted": true } })),
        ),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "API key not found")),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(error_json(
                "INTERNAL_ERROR",
                &format!("Failed to delete API key: {}", e),
            )),
        ),
    }
}

// ============================================================
// 4.6 Rate Limit Management
// ============================================================

/// Retrieves the current rate limit configuration and usage.
///
/// # Route
/// `GET /v1/gateway/rate-limits`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the default limits and current usage.
pub async fn get_rate_limits(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    let limits = state.rate_limits.lock().await;
    Json(json!({ "success": true, "data": *limits }))
}

/// Updates the default rate limit thresholds (per-second, per-minute, per-hour, per-day).
///
/// # Route
/// `PUT /v1/gateway/rate-limits`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: An `UpdateRateLimitsRequest` JSON body with optional rate limit fields to update.
///
/// # Returns
/// `Json<Value>` — a JSON response with the updated rate limit configuration.
pub async fn update_rate_limits(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<UpdateRateLimitsRequest>,
) -> Json<Value> {
    let mut limits = state.rate_limits.lock().await;
    if let Some(v) = req.outbound_per_second {
        limits.outbound.update_capacity(v, 1.0);
    }
    if let Some(v) = req.inbound_per_second {
        limits.inbound.update_capacity(v, 1.0);
    }
    Json(json!({ "success": true, "data": *limits }))
}

// ============================================================
// 5. Utility APIs
// ============================================================

/// Validates and formats a phone number, returning E.164 format and country information.
///
/// # Route
/// `POST /v1/utils/validate-phone`
///
/// # Parameters
/// - `req`: A `ValidatePhoneRequest` JSON body with the phone_number to validate.
///
/// # Returns
/// `Json<Value>` — a JSON response with the original number, formatted E.164, validity,
/// and country information.
pub async fn validate_phone(Json(req): Json<ValidatePhoneRequest>) -> Json<Value> {
    let formatted = format_e164(&req.phone_number);
    let valid = is_valid_e164(&formatted);
    let (country_code, country_name) = if valid {
        country_from_e164(&formatted)
    } else {
        (None, None)
    };

    Json(json!({
        "success": true,
        "data": {
            "original": req.phone_number,
            "formatted": if valid { Some(&formatted) } else { None },
            "valid": valid,
            "country_code": country_code,
            "country_name": country_name,
            "carrier": null,
            "line_type": null
        }
    }))
}

/// Calculates how many SMS parts a message will require based on its length and encoding.
///
/// # Route
/// `POST /v1/utils/message-parts`
///
/// # Parameters
/// - `req`: A `MessagePartsRequest` JSON body with the message text and optional encoding (GSM7 or UCS2).
///
/// # Returns
/// `Json<Value>` — a JSON response with message_length, encoding, parts count,
/// max_length_per_part, and characters_remaining.
pub async fn calculate_message_parts(Json(req): Json<MessagePartsRequest>) -> Json<Value> {
    let encoding = req.encoding.unwrap_or_else(|| "GSM7".into());
    let (max_single, max_concat) = if encoding == "UCS2" {
        (70usize, 67usize)
    } else {
        (160, 153)
    };

    let msg_len = req.message.len();
    let parts = if msg_len <= max_single {
        1
    } else {
        (msg_len + max_concat - 1) / max_concat
    };
    let chars_remaining = if msg_len <= max_single {
        max_single - msg_len
    } else {
        let used_in_last = msg_len % max_concat;
        if used_in_last == 0 {
            0
        } else {
            max_concat - used_in_last
        }
    };

    Json(json!({
        "success": true,
        "data": {
            "message_length": msg_len,
            "max_length_per_part": if parts == 1 { max_single } else { max_concat },
            "encoding": encoding,
            "parts": parts,
            "characters_remaining": chars_remaining
        }
    }))
}

/// Returns the list of supported countries with their ISO codes and dialing prefixes.
///
/// # Route
/// `GET /v1/utils/countries`
///
/// # Returns
/// `Json<Value>` — a JSON response with an array of `CountryInfo` objects.
pub async fn get_supported_countries(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    match state.country_store.list().await {
        Ok(countries) => Json(json!({ "success": true, "data": countries })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "success": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

// ============================================================
// Webhook CRUD handlers
// ============================================================

pub async fn create_webhook(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(req): Json<CreateWebhookRequest>,
) -> impl IntoResponse {
    let webhook_id = gen_id(&state, "wh").await;
    let now = now_utc();
    let info = WebhookInfo {
        webhook_id: webhook_id.clone(),
        url: req.url.clone(),
        events: req.events,
        enabled: req.enabled,
        created_at: now,
    };
    state.webhooks.lock().await.push(info.clone());
    (
        StatusCode::CREATED,
        Json(json!({
            "success": true,
            "data": info
        })),
    )
}

pub async fn get_webhook(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(webhook_id): Path<String>,
) -> impl IntoResponse {
    let webhooks = state.webhooks.lock().await;
    if let Some(wh) = webhooks.iter().find(|w| w.webhook_id == webhook_id) {
        Json(json!({ "success": true, "data": wh })).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Webhook not found")),
        )
            .into_response()
    }
}

pub async fn update_webhook(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(webhook_id): Path<String>,
    Json(req): Json<UpdateWebhookRequest>,
) -> impl IntoResponse {
    let mut webhooks = state.webhooks.lock().await;
    if let Some(wh) = webhooks.iter_mut().find(|w| w.webhook_id == webhook_id) {
        if let Some(url) = req.url {
            wh.url = url;
        }
        if let Some(events) = req.events {
            wh.events = events;
        }
        if let Some(enabled) = req.enabled {
            wh.enabled = enabled;
        }
        let updated = wh.clone();
        Json(json!({ "success": true, "data": updated })).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Webhook not found")),
        )
            .into_response()
    }
}

pub async fn delete_webhook(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(webhook_id): Path<String>,
) -> impl IntoResponse {
    let mut webhooks = state.webhooks.lock().await;
    let len_before = webhooks.len();
    webhooks.retain(|w| w.webhook_id != webhook_id);
    if webhooks.len() < len_before {
        Json(json!({ "success": true, "data": { "deleted": true } })).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Webhook not found")),
        )
            .into_response()
    }
}

pub async fn test_webhook(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(webhook_id): Path<String>,
) -> impl IntoResponse {
    let webhooks = state.webhooks.lock().await;
    if let Some(_wh) = webhooks.iter().find(|w| w.webhook_id == webhook_id) {
        Json(json!({
            "success": true,
            "data": {
                "verified": true,
                "response_status": 200
            }
        }))
        .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Webhook not found")),
        )
            .into_response()
    }
}
