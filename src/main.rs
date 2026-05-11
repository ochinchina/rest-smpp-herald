pub mod alarm;
pub mod api_key_store;
pub mod command_ids;
pub mod command_status;
pub mod country_store;
pub mod field_codec;
pub mod id_generator;
pub mod inbound_message_storage;
pub mod ip_whitelist;
pub mod message;
pub mod message_handler;
pub mod metrics;
pub mod outbound_message_storage;
pub mod phone_number_store;
pub mod rate_limits;
pub mod sequence_number_allocator;
pub mod smsc_client;
pub mod smsc_server;
pub mod user_authentication;

use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use log::info;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio;

#[derive(Debug, Parser, Clone)]
#[clap(
    name = "SMPP Router",
    version = "1.0",
    author = "OpenAI",
    about = "An SMPP Router implemented in Rust"
)]
struct Args {
    #[clap(subcommand)]
    config: AppConfig,

    /// Log output destination: stdout or a file path
    #[arg(long, default_value = "stdout")]
    log_output: String,

    /// Log format
    #[arg(long, default_value = "text", value_enum)]
    log_format: LogFormat,

    /// Log timestamp format
    #[arg(long, default_value = "local", value_enum)]
    log_timestamp: LogTimestamp,
}

#[derive(Debug, Clone, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, ValueEnum)]
enum LogTimestamp {
    Local,
    Iso8601,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SmscConnection {
    address: String,
    system_id: String,
    password: String,
    system_type: String,
    addr_ton: u8,
    addr_npi: u8,
    address_range: String,
    interface_version: u8,
    #[serde(default = "default_weight")]
    weight: u32,
}

fn default_weight() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientConfig {
    #[serde(default)]
    rest_addresses: Vec<String>,
    connections: Vec<SmscConnection>,
    #[serde(default = "default_max_inbound_messages")]
    max_inbound_messages: usize,
    #[serde(default)]
    inbound_storage: smsc_client::StorageConfig,
    #[serde(default)]
    outbound_storage: smsc_client::StorageConfig,
    #[serde(default)]
    handler_urls: Vec<String>,
    #[serde(default)]
    handler_algorithm: smsc_client::LoadBalancingAlgorithm,
    #[serde(default)]
    alarm_config: Option<smsc_client::AlarmConfig>,
    #[serde(default)]
    api_key_store: api_key_store::ApiKeyStoreConfig,
    #[serde(default)]
    phone_number_store: phone_number_store::PhoneNumberStoreConfig,
    #[serde(default)]
    country_store: country_store::CountryStoreConfig,
    #[serde(default)]
    id_generator: id_generator::IdGeneratorConfig,
}

fn default_max_inbound_messages() -> usize {
    10000
}

/// Loads the SMPP client configuration from a JSON file at the given path.
///
/// # Parameters
/// - `path`: The file system path to the JSON configuration file.
///
/// # Returns
/// `Result<ClientConfig, Box<dyn std::error::Error>>` — the parsed client
/// configuration, or an error if the file cannot be read or contains invalid JSON.
///
/// # JSON Format
/// ```json
/// {
///     "connections": [
///         {
///             "address": "127.0.0.1:2775",
///             "system_id": "my_system",
///             "password": "secret",
///             "system_type": "",
///             "addr_ton": 0,
///             "addr_npi": 0,
///             "address_range": "",
///             "interface_version": 52
///         },
///         {
///             "address": "127.0.0.1:2776",
///             "system_id": "other_system",
///             "password": "other_secret",
///             "system_type": "",
///             "addr_ton": 1,
///             "addr_npi": 1,
///             "address_range": "",
///             "interface_version": 52
///         }
///     ],
///     "max_inbound_messages": 10000
/// }
/// ```
///
/// All fields are required except `max_inbound_messages`, which defaults to `10000`
/// if omitted.
fn load_client_config(path: &str) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let config_data = std::fs::read_to_string(path)?;
    let config: ClientConfig = if path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_str(&config_data)?
    } else {
        serde_json::from_str(&config_data)?
    };
    Ok(config)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerConfig {
    #[serde(default)]
    listen_addresses: Vec<String>,
    interface_version: u8,
    #[serde(default)]
    handler_urls: Vec<String>,
    #[serde(default)]
    handler_algorithm: smsc_server::LoadBalancingAlgorithm,
    #[serde(default)]
    ip_whitelist: ip_whitelist::IpWhitelistConfig,
    #[serde(default)]
    users_file: Option<String>,
    #[serde(default)]
    auth_url: Option<String>,
}

fn load_server_config(path: &str) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    let config_data = std::fs::read_to_string(path)?;
    let config: ServerConfig = if path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_str(&config_data)?
    } else {
        serde_json::from_str(&config_data)?
    };
    Ok(config)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserCredential {
    system_id: String,
    password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UsersConfig {
    users: Vec<UserCredential>,
}

fn load_users_config(path: &str) -> Result<UsersConfig, Box<dyn std::error::Error>> {
    let config_data = std::fs::read_to_string(path)?;
    let config: UsersConfig = if path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_str(&config_data)?
    } else {
        serde_json::from_str(&config_data)?
    };
    Ok(config)
}

fn init_logging(output: &str, format: &LogFormat, timestamp: &LogTimestamp) {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let time_format = match timestamp {
        LogTimestamp::Local => "%Y-%m-%d %H:%M:%S%.3f".to_string(),
        LogTimestamp::Iso8601 => "%Y-%m-%dT%H:%M:%S%.3f%:z".to_string(),
    };

    match output {
        "stdout" => match format {
            LogFormat::Json => {
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .json()
                    .with_target(true)
                    .with_timer(fmt::time::ChronoLocal::new(time_format))
                    .init();
            }
            LogFormat::Text => {
                fmt::Subscriber::builder()
                    .with_env_filter(env_filter)
                    .with_target(true)
                    .with_timer(fmt::time::ChronoLocal::new(time_format))
                    .init();
            }
        },
        file_path => {
            let file_appender = tracing_appender::rolling::never(
                std::path::Path::new(file_path)
                    .parent()
                    .unwrap_or(std::path::Path::new(".")),
                std::path::Path::new(file_path)
                    .file_name()
                    .unwrap_or(std::ffi::OsStr::new("smpp.log")),
            );
            match format {
                LogFormat::Json => {
                    fmt::Subscriber::builder()
                        .with_env_filter(env_filter)
                        .json()
                        .with_target(true)
                        .with_timer(fmt::time::ChronoLocal::new(time_format))
                        .with_writer(file_appender)
                        .with_ansi(false)
                        .init();
                }
                LogFormat::Text => {
                    fmt::Subscriber::builder()
                        .with_env_filter(env_filter)
                        .with_target(true)
                        .with_timer(fmt::time::ChronoLocal::new(time_format))
                        .with_writer(file_appender)
                        .with_ansi(false)
                        .init();
                }
            }
        }
    }

    // Bridge log crate macros to tracing
    tracing_log::LogTracer::init().ok();
}

#[derive(Debug, Clone, Subcommand)]
enum AppConfig {
    Client {
        #[arg(long, help = "the configuration file")]
        config_file: String,
    },
    Server {
        #[arg(long, help = "the configuration file")]
        config_file: String,
    },
}

async fn start_client(config_file: String) {
    use smsc_client::SmscClient;
    use smsc_client::SmscClientConfig;
    use smsc_client::SmscConnectionConfig;
    let config_data = load_client_config(&config_file).unwrap();
    let connections: Vec<SmscConnectionConfig> = config_data
        .connections
        .into_iter()
        .map(|c| SmscConnectionConfig {
            address: c.address,
            system_id: c.system_id,
            password: c.password,
            system_type: c.system_type,
            addr_ton: c.addr_ton,
            addr_npi: c.addr_npi,
            address_range: c.address_range,
            interface_version: c.interface_version,
            weight: c.weight,
        })
        .collect();
    let config = SmscClientConfig {
        rest_addresses: config_data.rest_addresses,
        connections,
        max_inbound_messages: config_data.max_inbound_messages,
        inbound_storage: config_data.inbound_storage,
        outbound_storage: config_data.outbound_storage,
        handler_urls: config_data.handler_urls,
        handler_algorithm: config_data.handler_algorithm,
        alarm_config: config_data.alarm_config,
        api_key_store: config_data.api_key_store,
        phone_number_store: config_data.phone_number_store,
        country_store: config_data.country_store,
        id_generator: config_data.id_generator,
    };

    let _ = tokio::spawn(async move {
        let mut client = SmscClient::new(config);
        client.start().await.unwrap();
    })
    .await;
}

async fn start_server(config_file: String) {
    use smsc_server::SmscServer;
    use smsc_server::SmscServerConfig;
    let config_data = load_server_config(&config_file).unwrap();

    let config = SmscServerConfig {
        listen_addresses: config_data.listen_addresses,
        interface_version: config_data.interface_version,
        ip_whitelist: config_data.ip_whitelist,
    };

    let user_auth: Arc<dyn smsc_server::UserAuthentication> =
        if let Some(url) = config_data.auth_url {
            Arc::new(smsc_server::HttpUserAuthentication::new(url))
        } else if let Some(file) = config_data.users_file {
            let users_data = load_users_config(&file).unwrap();
            let valid_users: Vec<(String, String)> = users_data
                .users
                .into_iter()
                .map(|u| (u.system_id, u.password))
                .collect();
            Arc::new(smsc_server::SimpleUserAuthentication::new(valid_users))
        } else {
            panic!("Either users-file or auth-url must be provided");
        };

    let server = SmscServer::new(config.clone());
    let message_handler = Arc::new(smsc_server::HttpMessageHandler::with_algorithm(
        config_data.handler_urls,
        config_data.handler_algorithm,
    ));
    server.start(user_auth, message_handler).await.unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    metrics::register_metrics();
    let args = Args::parse();

    init_logging(&args.log_output, &args.log_format, &args.log_timestamp);

    let config = args.config.clone();
    info!("Starting SMPP Router with config: {:?}", args.config);

    match config {
        AppConfig::Client { config_file } => {
            info!("Starting SMPP Client with config file: {}", config_file);
            start_client(config_file).await;
        }
        AppConfig::Server { config_file } => {
            info!("Starting SMPP Server with config file: {}", config_file);
            start_server(config_file).await;
        }
    }
}
