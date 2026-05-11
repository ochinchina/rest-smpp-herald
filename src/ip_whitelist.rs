use async_trait::async_trait;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// ============================================================
// IpWhitelist trait
// ============================================================

/// Abstraction for IP whitelist verification.
#[async_trait]
pub trait IpWhitelist: Send + Sync {
    /// Returns true if the whitelist is empty (i.e. all IPs are allowed).
    async fn is_empty(&self) -> std::io::Result<bool>;

    /// Check whether the given IP address is allowed.
    async fn is_allowed(&self, ip: &str) -> std::io::Result<bool>;
}

// ============================================================
// Configuration
// ============================================================

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IpWhitelistConfig {
    /// No whitelist — all IPs are allowed.
    None,
    /// In-memory whitelist, optionally loaded from a file.
    Memory {
        #[serde(default)]
        file: Option<String>,
        #[serde(default)]
        ips: Vec<String>,
    },
    /// Database-backed whitelist (PostgreSQL or SQLite via sqlx).
    Database { url: String },
    /// HTTP/HTTPS service that verifies IPs.
    Http { url: String },
}

impl Default for IpWhitelistConfig {
    fn default() -> Self {
        IpWhitelistConfig::None
    }
}

/// Build an `IpWhitelist` from the given configuration.
pub async fn create_ip_whitelist(
    config: &IpWhitelistConfig,
) -> std::io::Result<std::sync::Arc<dyn IpWhitelist>> {
    match config {
        IpWhitelistConfig::None => {
            Ok(std::sync::Arc::new(EmptyIpWhitelist) as std::sync::Arc<dyn IpWhitelist>)
        }
        IpWhitelistConfig::Memory { file, ips } => {
            let store = MemoryIpWhitelist::new(ips.clone());
            if let Some(path) = file {
                store.load_from_file(path)?;
            }
            Ok(std::sync::Arc::new(store) as std::sync::Arc<dyn IpWhitelist>)
        }
        IpWhitelistConfig::Database { url } => {
            let store = DatabaseIpWhitelist::new(url).await?;
            Ok(std::sync::Arc::new(store) as std::sync::Arc<dyn IpWhitelist>)
        }
        IpWhitelistConfig::Http { url } => {
            let store = HttpIpWhitelist::new(url);
            Ok(std::sync::Arc::new(store) as std::sync::Arc<dyn IpWhitelist>)
        }
    }
}

// ============================================================
// EmptyIpWhitelist (no filtering)
// ============================================================

pub struct EmptyIpWhitelist;

#[async_trait]
impl IpWhitelist for EmptyIpWhitelist {
    async fn is_empty(&self) -> std::io::Result<bool> {
        Ok(true)
    }

    async fn is_allowed(&self, _ip: &str) -> std::io::Result<bool> {
        Ok(true)
    }
}

// ============================================================
// MemoryIpWhitelist
// ============================================================

/// In-memory IP whitelist. Can be pre-populated via config or loaded from a file.
///
/// The file should contain one IP address per line (blank lines and lines
/// starting with `#` are ignored). JSON and YAML array formats are also supported.
pub struct MemoryIpWhitelist {
    ips: Mutex<Vec<String>>,
}

impl MemoryIpWhitelist {
    pub fn new(ips: Vec<String>) -> Self {
        Self {
            ips: Mutex::new(ips),
        }
    }

    /// Load IPs from a file. Supports:
    /// - Plain text (one IP per line)
    /// - JSON array of strings
    /// - YAML array of strings
    pub fn load_from_file(&self, path: &str) -> std::io::Result<()> {
        let data = std::fs::read_to_string(path)?;

        let ips: Vec<String> = if path.ends_with(".json") {
            serde_json::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_yaml::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else {
            // Plain text: one IP per line
            data.lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(|l| l.to_string())
                .collect()
        };

        info!("Loaded {} IP(s) from whitelist file {}", ips.len(), path);
        match self.ips.try_lock() {
            Ok(mut guard) => {
                guard.extend(ips);
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to lock IPs during file load",
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl IpWhitelist for MemoryIpWhitelist {
    async fn is_empty(&self) -> std::io::Result<bool> {
        Ok(self.ips.lock().await.is_empty())
    }

    async fn is_allowed(&self, ip: &str) -> std::io::Result<bool> {
        let ips = self.ips.lock().await;
        if ips.is_empty() {
            return Ok(true);
        }
        Ok(ips.iter().any(|allowed| allowed == ip))
    }
}

// ============================================================
// DatabaseIpWhitelist
// ============================================================

/// Database-backed IP whitelist using sqlx.
///
/// On each incoming connection, the server queries the database to check
/// whether the client IP is present in the `ip_whitelist` table.
pub struct DatabaseIpWhitelist {
    pool: sqlx::AnyPool,
}

impl DatabaseIpWhitelist {
    pub async fn new(url: &str) -> std::io::Result<Self> {
        sqlx::any::install_default_drivers();

        let pool = sqlx::AnyPool::connect(url).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ip_whitelist (
                ip TEXT PRIMARY KEY
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        info!("Database IP whitelist store initialised ({})", url);
        Ok(Self { pool })
    }
}

#[async_trait]
impl IpWhitelist for DatabaseIpWhitelist {
    async fn is_empty(&self) -> std::io::Result<bool> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ip_whitelist")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(row.0 == 0)
    }

    async fn is_allowed(&self, ip: &str) -> std::io::Result<bool> {
        // If table is empty, allow all
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ip_whitelist")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        if count.0 == 0 {
            return Ok(true);
        }

        let row = sqlx::query("SELECT ip FROM ip_whitelist WHERE ip = $1")
            .bind(ip)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(row.is_some())
    }
}

// ============================================================
// HttpIpWhitelist
// ============================================================

/// HTTP-backed IP whitelist.
///
/// On each incoming connection, sends a GET request to the configured URL
/// with the client IP as a query parameter:
///   `GET {base_url}/verify?ip={client_ip}`
///
/// Expects a JSON response: `{ "allowed": true }` or `{ "allowed": false }`.
pub struct HttpIpWhitelist {
    base_url: String,
    client: reqwest::Client,
}

impl HttpIpWhitelist {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl IpWhitelist for HttpIpWhitelist {
    async fn is_empty(&self) -> std::io::Result<bool> {
        // HTTP whitelist is never considered empty — always verifies
        Ok(false)
    }

    async fn is_allowed(&self, ip: &str) -> std::io::Result<bool> {
        let url = format!("{}/verify?ip={}", self.base_url, urlencoding::encode(ip));
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<serde_json::Value>().await {
                        Ok(val) => {
                            let allowed = val
                                .get("allowed")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            Ok(allowed)
                        }
                        Err(e) => {
                            warn!("Failed to parse IP whitelist verify response: {}", e);
                            Ok(false)
                        }
                    }
                } else if resp.status() == reqwest::StatusCode::FORBIDDEN
                    || resp.status() == reqwest::StatusCode::NOT_FOUND
                {
                    Ok(false)
                } else {
                    warn!("IP whitelist verify returned status: {}", resp.status());
                    Ok(false)
                }
            }
            Err(e) => {
                error!("Failed to verify IP via HTTP whitelist: {}", e);
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            }
        }
    }
}
