use clap::{Parser, Subcommand, ValueEnum};
use log::info;
use rest_smpp_herald::config::{ClientConfig, ServerConfig, UsersConfig, load_server_config};
use rest_smpp_herald::message_handler::{
    MessageHandler, NoOpMessageHandler, create_request_handler,
};
use rest_smpp_herald::metrics;
use rest_smpp_herald::smsc_client::smsc_client::SmscClient;

use rest_smpp_herald::smsc_server::smsc_server::SmscServer;
use rest_smpp_herald::user_authentication::{
    HttpUserAuthentication, SimpleUserAuthentication, UserAuthentication,
};
use serde_json;
use std::sync::Arc;
use tokio;

#[derive(Debug, Clone, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum LogTimestamp {
    Local,
    Iso8601,
}

#[derive(Debug, Clone, ValueEnum, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn to_str(&self) -> &'static str {
        match *self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }
}

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

    #[arg(long, default_value = "info")]
    log_level: LogLevel,
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

fn load_users_config(path: &str) -> Result<UsersConfig, Box<dyn std::error::Error>> {
    let config_data = std::fs::read_to_string(path)?;
    let config: UsersConfig = if path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_str(&config_data)?
    } else {
        serde_json::from_str(&config_data)?
    };
    Ok(config)
}

fn init_logging(output: &str, format: &LogFormat, timestamp: &LogTimestamp, log_level: &LogLevel) {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level.to_str()));

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
                        .with_level(true)
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
        #[arg(long, help = "the configuration file", short = 'c')]
        config_file: String,
    },
    Server {
        #[arg(long, help = "the configuration file", short = 'c')]
        config_file: String,
    },
}

async fn start_client(config_file: String) {
    let config: ClientConfig = load_client_config(&config_file).unwrap();

    let mut client = SmscClient::new(config);

    match client.start().await {
        Ok(_) => info!("SMPP Client started successfully"),
        Err(e) => {
            eprintln!("Error starting SMPP Client: {}", e);
            std::process::exit(1);
        }
    }
}

async fn start_server(config_file: String) {
    let config: ServerConfig = load_server_config(&config_file).unwrap();
    let config_clone = config.clone(); // Clone the config to move into the async block

    let user_auth: Arc<dyn UserAuthentication> = if let Some(url) = config.user_auth.auth_url {
        Arc::new(HttpUserAuthentication::new(url))
    } else if let Some(file) = config.user_auth.users_file {
        let users_data = load_users_config(&file).unwrap();
        let valid_users: Vec<(String, String)> = users_data
            .users
            .into_iter()
            .map(|u| (u.system_id, u.password))
            .collect();
        Arc::new(SimpleUserAuthentication::new(valid_users))
    } else {
        panic!("Either users-file or auth-url must be provided");
    };

    let message_handler: Box<dyn MessageHandler> = config.request_handlers.map_or(
        Box::new(NoOpMessageHandler::new()) as Box<dyn MessageHandler>,
        |handler_configs| create_request_handler(&handler_configs) as Box<dyn MessageHandler>,
    );

    let server = SmscServer::new(config_clone).await;

    match server.start(user_auth, Arc::new(message_handler)).await {
        Ok(_) => info!("SMPP Server started successfully"),
        Err(e) => {
            eprintln!("Error starting SMPP Server: {}", e);
            std::process::exit(1);
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    metrics::register_metrics();
    let args = Args::parse();

    init_logging(
        &args.log_output,
        &args.log_format,
        &args.log_timestamp,
        &args.log_level,
    );

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
