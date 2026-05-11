use async_trait::async_trait;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::smsc_client::CountryInfo;

// ============================================================
// CountryStore trait
// ============================================================

/// Abstraction for loading supported country information.
#[async_trait]
pub trait CountryStore: Send + Sync {
    /// List all supported countries.
    async fn list(&self) -> std::io::Result<Vec<CountryInfo>>;
}

// ============================================================
// Configuration
// ============================================================

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CountryStoreConfig {
    /// In-memory store, optionally loaded from a file.
    Memory {
        #[serde(default)]
        file: Option<String>,
    },
    /// Database-backed store (PostgreSQL or SQLite via sqlx).
    Database { url: String },
    /// HTTP/HTTPS service that provides the country list.
    Http { url: String },
}

impl Default for CountryStoreConfig {
    fn default() -> Self {
        CountryStoreConfig::Memory { file: None }
    }
}

/// Build a `CountryStore` from the given configuration.
pub async fn create_country_store(
    config: &CountryStoreConfig,
) -> std::io::Result<std::sync::Arc<dyn CountryStore>> {
    match config {
        CountryStoreConfig::Memory { file } => {
            let store = MemoryCountryStore::new();
            if let Some(path) = file {
                store.load_from_file(path)?;
            }
            Ok(std::sync::Arc::new(store) as std::sync::Arc<dyn CountryStore>)
        }
        CountryStoreConfig::Database { url } => {
            let store = DatabaseCountryStore::new(url).await?;
            Ok(std::sync::Arc::new(store) as std::sync::Arc<dyn CountryStore>)
        }
        CountryStoreConfig::Http { url } => {
            let store = HttpCountryStore::new(url);
            Ok(std::sync::Arc::new(store) as std::sync::Arc<dyn CountryStore>)
        }
    }
}

// ============================================================
// MemoryCountryStore
// ============================================================

/// In-memory country store. Can be loaded from a JSON or YAML file.
pub struct MemoryCountryStore {
    countries: Mutex<Vec<CountryInfo>>,
}

impl MemoryCountryStore {
    pub fn new() -> Self {
        Self {
            countries: Mutex::new(Vec::new()),
        }
    }

    /// Load countries from a file. Supports JSON and YAML formats.
    /// The file should contain an array of CountryInfo objects.
    pub fn load_from_file(&self, path: &str) -> std::io::Result<()> {
        let data = std::fs::read_to_string(path)?;

        let countries: Vec<CountryInfo> = if path.ends_with(".json") {
            serde_json::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_yaml::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else {
            serde_json::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        };

        info!("Loaded {} countries from file {}", countries.len(), path);
        match self.countries.try_lock() {
            Ok(mut guard) => {
                *guard = countries;
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to lock countries during file load",
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl CountryStore for MemoryCountryStore {
    async fn list(&self) -> std::io::Result<Vec<CountryInfo>> {
        Ok(self.countries.lock().await.clone())
    }
}

// ============================================================
// DatabaseCountryStore
// ============================================================

/// Database-backed country store using sqlx.
pub struct DatabaseCountryStore {
    pool: sqlx::AnyPool,
}

impl DatabaseCountryStore {
    pub async fn new(url: &str) -> std::io::Result<Self> {
        sqlx::any::install_default_drivers();

        let pool = sqlx::AnyPool::connect(url).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS countries (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                country_code INTEGER NOT NULL,
                supported BOOLEAN NOT NULL DEFAULT TRUE
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        info!("Database country store initialised ({})", url);
        Ok(Self { pool })
    }
}

#[async_trait]
impl CountryStore for DatabaseCountryStore {
    async fn list(&self) -> std::io::Result<Vec<CountryInfo>> {
        let rows: Vec<(String, String, i32, bool)> =
            sqlx::query_as("SELECT code, name, country_code, supported FROM countries")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(code, name, country_code, supported)| CountryInfo {
                code,
                name,
                country_code: country_code as u32,
                supported,
            })
            .collect())
    }
}

// ============================================================
// HttpCountryStore
// ============================================================

/// HTTP-backed country store.
///
/// Sends a GET request to the configured URL and expects a JSON array
/// of CountryInfo objects in the response.
pub struct HttpCountryStore {
    url: String,
    client: reqwest::Client,
}

impl HttpCountryStore {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl CountryStore for HttpCountryStore {
    async fn list(&self) -> std::io::Result<Vec<CountryInfo>> {
        match self.client.get(&self.url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<Vec<CountryInfo>>().await {
                        Ok(countries) => Ok(countries),
                        Err(e) => {
                            warn!("Failed to parse country list response: {}", e);
                            Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                e.to_string(),
                            ))
                        }
                    }
                } else {
                    error!(
                        "Country store HTTP request returned status: {}",
                        resp.status()
                    );
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("HTTP status: {}", resp.status()),
                    ))
                }
            }
            Err(e) => {
                error!("Failed to fetch countries via HTTP: {}", e);
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            }
        }
    }
}
