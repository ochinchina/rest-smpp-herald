use async_trait::async_trait;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::smsc_client::PhoneNumberInfo;

// ============================================================
// PhoneNumberStore trait
// ============================================================

/// Abstraction for phone number management and verification.
#[async_trait]
pub trait PhoneNumberStore: Send + Sync {
    /// Check if the store has no registered numbers (skip validation when empty).
    async fn is_empty(&self) -> std::io::Result<bool>;

    /// Check whether a phone number is registered with a given capability.
    /// Returns `true` if the number is active and has the requested capability.
    async fn has_capability(&self, phone_number: &str, capability: &str) -> std::io::Result<bool>;

    /// List all registered phone numbers.
    async fn list(&self) -> std::io::Result<Vec<PhoneNumberInfo>>;

    /// Create (register) a new phone number.
    async fn create(&self, info: PhoneNumberInfo) -> std::io::Result<()>;

    /// Update an existing phone number's capabilities. Returns the updated info if found.
    async fn update(
        &self,
        number_id: &str,
        capabilities: Option<Vec<String>>,
    ) -> std::io::Result<Option<PhoneNumberInfo>>;

    /// Delete a phone number. Returns true if it was found and deleted.
    async fn delete(&self, number_id: &str) -> std::io::Result<bool>;
}

// ============================================================
// Configuration
// ============================================================

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PhoneNumberStoreConfig {
    /// In-memory store, optionally pre-loaded from a JSON/YAML file.
    Memory {
        #[serde(default)]
        file: Option<String>,
    },
    /// Database-backed store (PostgreSQL or SQLite via sqlx).
    Database { url: String },
    /// HTTP/HTTPS proxy that delegates verification to a remote REST service.
    Http { url: String },
}

impl Default for PhoneNumberStoreConfig {
    fn default() -> Self {
        PhoneNumberStoreConfig::Memory { file: None }
    }
}

/// Build a `PhoneNumberStore` from the given configuration.
pub async fn create_phone_number_store(
    config: &PhoneNumberStoreConfig,
) -> std::io::Result<std::sync::Arc<dyn PhoneNumberStore>> {
    match config {
        PhoneNumberStoreConfig::Memory { file } => {
            let store = MemoryPhoneNumberStore::new();
            if let Some(path) = file {
                store.load_from_file(path)?;
            }
            Ok(std::sync::Arc::new(store))
        }
        PhoneNumberStoreConfig::Database { url } => {
            let store = DatabasePhoneNumberStore::new(url).await?;
            Ok(std::sync::Arc::new(store))
        }
        PhoneNumberStoreConfig::Http { url } => {
            let store = HttpPhoneNumberStore::new(url);
            Ok(std::sync::Arc::new(store))
        }
    }
}

// ============================================================
// MemoryPhoneNumberStore
// ============================================================

/// In-memory implementation of `PhoneNumberStore`.
///
/// Optionally loads initial phone numbers from a JSON or YAML file at startup.
pub struct MemoryPhoneNumberStore {
    numbers: Mutex<Vec<PhoneNumberInfo>>,
}

impl MemoryPhoneNumberStore {
    pub fn new() -> Self {
        Self {
            numbers: Mutex::new(Vec::new()),
        }
    }

    /// Load phone numbers from a JSON or YAML file.
    pub fn load_from_file(&self, path: &str) -> std::io::Result<()> {
        let data = std::fs::read_to_string(path)?;
        let numbers: Vec<PhoneNumberInfo> = if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_yaml::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else {
            serde_json::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        };
        info!("Loaded {} phone number(s) from {}", numbers.len(), path);
        match self.numbers.try_lock() {
            Ok(mut guard) => {
                *guard = numbers;
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to lock numbers during file load",
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl PhoneNumberStore for MemoryPhoneNumberStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        Ok(self.numbers.lock().await.is_empty())
    }

    async fn has_capability(&self, phone_number: &str, capability: &str) -> std::io::Result<bool> {
        let numbers = self.numbers.lock().await;
        Ok(numbers.iter().any(|n| {
            n.phone_number == phone_number
                && n.status == "active"
                && n.capabilities.iter().any(|c| c == capability)
        }))
    }

    async fn list(&self) -> std::io::Result<Vec<PhoneNumberInfo>> {
        Ok(self.numbers.lock().await.clone())
    }

    async fn create(&self, info: PhoneNumberInfo) -> std::io::Result<()> {
        self.numbers.lock().await.push(info);
        Ok(())
    }

    async fn update(
        &self,
        number_id: &str,
        capabilities: Option<Vec<String>>,
    ) -> std::io::Result<Option<PhoneNumberInfo>> {
        let mut numbers = self.numbers.lock().await;
        match numbers.iter_mut().find(|n| n.number_id == number_id) {
            Some(num) => {
                if let Some(caps) = capabilities {
                    num.capabilities = caps;
                }
                Ok(Some(num.clone()))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, number_id: &str) -> std::io::Result<bool> {
        let mut numbers = self.numbers.lock().await;
        let before = numbers.len();
        numbers.retain(|n| n.number_id != number_id);
        Ok(numbers.len() < before)
    }
}

// ============================================================
// DatabasePhoneNumberStore
// ============================================================

/// Database-backed implementation of `PhoneNumberStore` using sqlx.
///
/// Supports PostgreSQL (`postgres://…`) and SQLite (`sqlite://…`) connection URLs.
/// On each submit message, the application checks the database for registration.
pub struct DatabasePhoneNumberStore {
    pool: sqlx::AnyPool,
}

impl DatabasePhoneNumberStore {
    pub async fn new(url: &str) -> std::io::Result<Self> {
        sqlx::any::install_default_drivers();

        let pool = sqlx::AnyPool::connect(url).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;

        // Create table if it doesn't exist.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS phone_numbers (
                number_id    TEXT PRIMARY KEY,
                phone_number TEXT NOT NULL,
                capabilities TEXT NOT NULL,
                status       TEXT NOT NULL,
                created_at   TEXT NOT NULL
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        info!("Database phone number store initialised ({})", url);
        Ok(Self { pool })
    }

    fn row_to_info(row: &sqlx::any::AnyRow) -> std::io::Result<PhoneNumberInfo> {
        use sqlx::Row;
        let capabilities_json: String = row
            .try_get("capabilities")
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let capabilities: Vec<String> =
            serde_json::from_str(&capabilities_json).unwrap_or_default();

        Ok(PhoneNumberInfo {
            number_id: row
                .try_get("number_id")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            phone_number: row
                .try_get("phone_number")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            capabilities,
            status: row
                .try_get("status")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
        })
    }
}

#[async_trait]
impl PhoneNumberStore for DatabasePhoneNumberStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM phone_numbers")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(row.0 == 0)
    }

    async fn has_capability(&self, phone_number: &str, capability: &str) -> std::io::Result<bool> {
        let row = sqlx::query(
            "SELECT capabilities FROM phone_numbers WHERE phone_number = $1 AND status = 'active'",
        )
        .bind(phone_number)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match row {
            Some(r) => {
                use sqlx::Row;
                let capabilities_json: String = r.try_get("capabilities").map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;
                let capabilities: Vec<String> =
                    serde_json::from_str(&capabilities_json).unwrap_or_default();
                Ok(capabilities.iter().any(|c| c == capability))
            }
            None => Ok(false),
        }
    }

    async fn list(&self) -> std::io::Result<Vec<PhoneNumberInfo>> {
        let rows = sqlx::query("SELECT * FROM phone_numbers")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        rows.iter().map(Self::row_to_info).collect()
    }

    async fn create(&self, info: PhoneNumberInfo) -> std::io::Result<()> {
        let capabilities_json = serde_json::to_string(&info.capabilities)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO phone_numbers (number_id, phone_number, capabilities, status, created_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&info.number_id)
        .bind(&info.phone_number)
        .bind(&capabilities_json)
        .bind(&info.status)
        .bind(&info.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    async fn update(
        &self,
        number_id: &str,
        capabilities: Option<Vec<String>>,
    ) -> std::io::Result<Option<PhoneNumberInfo>> {
        if let Some(caps) = capabilities {
            let capabilities_json = serde_json::to_string(&caps)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

            let result =
                sqlx::query("UPDATE phone_numbers SET capabilities = $1 WHERE number_id = $2")
                    .bind(&capabilities_json)
                    .bind(number_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

            if result.rows_affected() == 0 {
                return Ok(None);
            }
        }

        // Re-fetch the updated record
        let row = sqlx::query("SELECT * FROM phone_numbers WHERE number_id = $1")
            .bind(number_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match row {
            Some(r) => Ok(Some(Self::row_to_info(&r)?)),
            None => Ok(None),
        }
    }

    async fn delete(&self, number_id: &str) -> std::io::Result<bool> {
        let result = sqlx::query("DELETE FROM phone_numbers WHERE number_id = $1")
            .bind(number_id)
            .execute(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(result.rows_affected() > 0)
    }
}

// ============================================================
// HttpPhoneNumberStore
// ============================================================

/// HTTP-backed implementation of `PhoneNumberStore`.
///
/// Delegates all phone number operations to a remote REST service.
/// For each submit message, phone number verification is forwarded
/// to the configured HTTP/HTTPS RESTful interface.
///
/// Expected remote API endpoints:
/// - `GET  /phone-numbers`                          → list all numbers
/// - `POST /phone-numbers`                          → create a number
/// - `PUT  /phone-numbers/{number_id}`              → update a number
/// - `DELETE /phone-numbers/{number_id}`            → delete a number
/// - `GET  /phone-numbers/verify?phone_number=X&capability=Y` → verify capability
pub struct HttpPhoneNumberStore {
    base_url: String,
    client: reqwest::Client,
}

impl HttpPhoneNumberStore {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl PhoneNumberStore for HttpPhoneNumberStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        let url = format!("{}/phone-numbers", self.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<serde_json::Value>().await {
                        Ok(val) => {
                            let data = val.get("data").and_then(|d| d.as_array());
                            Ok(data.map(|a| a.is_empty()).unwrap_or(true))
                        }
                        Err(e) => {
                            warn!("Failed to parse phone numbers list response: {}", e);
                            Ok(true)
                        }
                    }
                } else {
                    warn!(
                        "Phone number store HTTP list returned status: {}",
                        resp.status()
                    );
                    Ok(true)
                }
            }
            Err(e) => {
                error!("Failed to contact phone number store at {}: {}", url, e);
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            }
        }
    }

    async fn has_capability(&self, phone_number: &str, capability: &str) -> std::io::Result<bool> {
        let url = format!(
            "{}/phone-numbers/verify?phone_number={}&capability={}",
            self.base_url,
            urlencoding::encode(phone_number),
            urlencoding::encode(capability)
        );
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<serde_json::Value>().await {
                        Ok(val) => {
                            let verified = val
                                .get("verified")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            Ok(verified)
                        }
                        Err(e) => {
                            warn!("Failed to parse verify response: {}", e);
                            Ok(false)
                        }
                    }
                } else if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    Ok(false)
                } else {
                    warn!("Phone number verify returned status: {}", resp.status());
                    Ok(false)
                }
            }
            Err(e) => {
                error!("Failed to verify phone number via HTTP: {}", e);
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    e.to_string(),
                ))
            }
        }
    }

    async fn list(&self) -> std::io::Result<Vec<PhoneNumberInfo>> {
        let url = format!("{}/phone-numbers", self.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<serde_json::Value>().await {
                        Ok(val) => {
                            let data = val.get("data").unwrap_or(&val);
                            let numbers: Vec<PhoneNumberInfo> =
                                serde_json::from_value(data.clone()).unwrap_or_default();
                            Ok(numbers)
                        }
                        Err(e) => Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        )),
                    }
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("HTTP {} from phone number store", resp.status()),
                    ))
                }
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                e.to_string(),
            )),
        }
    }

    async fn create(&self, info: PhoneNumberInfo) -> std::io::Result<()> {
        let url = format!("{}/phone-numbers", self.base_url);
        match self.client.post(&url).json(&info).send().await {
            Ok(resp) => {
                if resp.status().is_success() || resp.status() == reqwest::StatusCode::CREATED {
                    Ok(())
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("HTTP {} creating phone number", resp.status()),
                    ))
                }
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                e.to_string(),
            )),
        }
    }

    async fn update(
        &self,
        number_id: &str,
        capabilities: Option<Vec<String>>,
    ) -> std::io::Result<Option<PhoneNumberInfo>> {
        let url = format!("{}/phone-numbers/{}", self.base_url, number_id);
        let body = serde_json::json!({ "capabilities": capabilities });
        match self.client.put(&url).json(&body).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<serde_json::Value>().await {
                        Ok(val) => {
                            let data = val.get("data").unwrap_or(&val);
                            let info: Option<PhoneNumberInfo> =
                                serde_json::from_value(data.clone()).ok();
                            Ok(info)
                        }
                        Err(_) => Ok(None),
                    }
                } else if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    Ok(None)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("HTTP {} updating phone number", resp.status()),
                    ))
                }
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                e.to_string(),
            )),
        }
    }

    async fn delete(&self, number_id: &str) -> std::io::Result<bool> {
        let url = format!("{}/phone-numbers/{}", self.base_url, number_id);
        match self.client.delete(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    Ok(true)
                } else if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    Ok(false)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("HTTP {} deleting phone number", resp.status()),
                    ))
                }
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                e.to_string(),
            )),
        }
    }
}
