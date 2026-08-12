use crate::esm_class::MessageMode;
use crate::id_generator::IdGeneratorConfig;
use crate::{alarm::AlarmConfig, ip_whitelist::IpWhitelistConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancingAlgorithm {
    RoundRobin,
    FailOver,
}

impl LoadBalancingAlgorithm {
    pub fn from(s: &String) -> Self {
        match s.to_lowercase().as_str() {
            "roundrobin" | "round_robin" | "ro" | "round-robin" => {
                LoadBalancingAlgorithm::RoundRobin
            }
            "failover" | "fail_over" | "fo" | "fail-over" => LoadBalancingAlgorithm::FailOver,
            _ => LoadBalancingAlgorithm::RoundRobin,
        }
    }
}
impl Default for LoadBalancingAlgorithm {
    fn default() -> Self {
        LoadBalancingAlgorithm::RoundRobin
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestAddressConfig {
    /// The address to listen on, e.g., "127.0.0.1:8080"
    pub address: String,
    #[serde(rename = "key-file")]
    pub key_file: Option<String>,
    #[serde(rename = "cert-file")]
    pub cert_file: Option<String>,
    #[serde(rename = "ca-cert-file")]
    pub ca_cert_file: Option<String>,
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
    #[serde(default = "get_default_bind_type")]
    pub bind_type: String,
    #[serde(default = "get_default_session_init_timer")]
    pub session_init_timer: u32,
    #[serde(default = "get_default_enquire_link_timer")]
    pub enquire_link_timer: u32,
    #[serde(default = "get_default_inactive_timer")]
    pub inactive_timer: u32,
    #[serde(default = "get_default_response_timer")]
    pub response_timer: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestHandlerConfig {
    // Optional list of destination addresses to match against.
    pub dest_addresses: Option<Vec<String>>,
    // Optional list of destination address patterns (regex) to match against.
    pub dest_address_patterns: Option<Vec<String>>,
    // List of URLs to send the message to.
    pub urls: Vec<String>,
    // Optional load balancing algorithm to use when sending messages to multiple URLs.
    pub algorithm: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    #[serde(default = "get_default_bind_type")]
    pub bind_type: String,
    #[serde(default)]
    pub rest_addresses: Vec<RestAddressConfig>,
    pub connections: Vec<SmscConnectionConfig>,
    pub load_balancing_algorithm: Option<String>,
    #[serde(default)]
    pub request_handlers: Option<Vec<RequestHandlerConfig>>,
    #[serde(default)]
    pub alarm_config: Option<AlarmConfig>,
    #[serde(default)]
    pub id_generator: Option<IdGeneratorConfig>,
    #[serde(default = "get_default_session_init_timer")]
    pub session_init_timer: u32,
    #[serde(default = "get_default_enquire_link_timer")]
    pub enquire_link_timer: u32,
    #[serde(default = "get_default_inactive_timer")]
    pub inactive_timer: u32,
    #[serde(default = "get_default_response_timer")]
    pub response_timer: u32,
}

fn get_default_session_init_timer() -> u32 {
    30
}

fn get_default_enquire_link_timer() -> u32 {
    30
}

fn get_default_inactive_timer() -> u32 {
    300
}

fn get_default_response_timer() -> u32 {
    30
}

fn get_default_bind_type() -> String {
    "transceiver".to_string()
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCredential {
    pub system_id: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsersConfig {
    pub users: Vec<UserCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub ton: u8,
    pub npi: u8,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionListConfig {
    pub name: String,
    pub addresses: Vec<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageStorageConfig {
    InMemory,
    Sqlite {
        db_path: String,
    },
    Postgres {
        url: String,
    },
    Redis {
        url: String,
        key_prefix: Option<String>,
    },
    RabbitMQ {
        url: String,
        queue_name: String,
    },
    MySQL {
        url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAuthConfig {
    #[serde(default)]
    pub users_file: Option<String>,
    #[serde(default)]
    pub auth_url: Option<String>,
}

impl Default for UserAuthConfig {
    fn default() -> Self {
        UserAuthConfig {
            users_file: None,
            auth_url: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub listen_addresses: Vec<String>,
    pub interface_version: u8,
    pub rest_addresses: Vec<RestAddressConfig>,
    #[serde(default)]
    pub request_handlers: Option<Vec<RequestHandlerConfig>>,
    #[serde(default)]
    pub ip_whitelist: IpWhitelistConfig,
    #[serde(default)]
    pub user_auth: UserAuthConfig,
    #[serde(default)]
    pub session_init_timer: Option<u32>,
    #[serde(default)]
    pub enquire_link_timer: Option<u32>,
    #[serde(default)]
    pub inactive_timer: Option<u32>,
    #[serde(default)]
    pub response_timer: Option<u32>,
    #[serde(default)]
    pub id_generator: Option<IdGeneratorConfig>,
    #[serde(default)]
    pub distribution_list: Option<Vec<DistributionListConfig>>,
    #[serde(default)]
    pub message_store: Option<MessageStorageConfig>,
    #[serde(default)]
    pub default_validity_period: Option<u32>, // default validity_period for a message
    #[serde(default)]
    pub default_mc_mode: Option<String>, // default mc_mode for a message
    #[serde(default)]
    pub predefined_messages: Option<Vec<String>>, // predefined messages for quick sending
    pub default_service_type: Option<String>, // default service_type for a message
}

impl ServerConfig {
    pub fn get_default_validity_period(&self) -> u32 {
        self.default_validity_period.unwrap_or(3600)
    }

    pub fn get_default_mc_mode(&self) -> MessageMode {
        let message_mode = MessageMode::from_string(
            &self
                .default_mc_mode
                .clone()
                .unwrap_or_else(|| "storeandforward".to_string()),
        )
        .unwrap_or(MessageMode::StoreAndForward);
        if message_mode == MessageMode::Default {
            MessageMode::StoreAndForward
        } else {
            message_mode
        }
    }

    pub fn get_predefined_message(&self, index: usize) -> Option<&String> {
        if let Some(predefined_messages) = &self.predefined_messages {
            if index - 1 < predefined_messages.len() {
                return Some(&predefined_messages[index - 1]);
            }
        }
        None
    }

    pub fn get_default_service_type(&self) -> String {
        self.default_service_type.clone().unwrap_or("".to_string())
    }
}

pub fn load_server_config(path: &str) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let config_data = std::fs::read_to_string(path)?;
    let config: ServerConfig = if path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_str(&config_data)?
    } else {
        serde_json::from_str(&config_data)?
    };
    Ok(config)
}

pub fn is_empty_file(filename: &str) -> bool {
    match std::fs::metadata(filename) {
        Ok(metadata) => metadata.len() == 0,
        Err(_) => true,
    }
}
