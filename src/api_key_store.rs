use async_trait::async_trait;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::smsc_client::ApiKeyInfo;

// ============================================================
// ApiKeyStore trait
// ============================================================

/// Abstraction for API key persistence and verification.
#[async_trait]
pub trait ApiKeyStore: Send + Sync {
    /// Check if the store has no keys (bootstrapping mode — skip auth).
    async fn is_empty(&self) -> std::io::Result<bool>;

    /// Verify an API key and return the key info if valid.
    async fn verify_key(&self, api_key: &str) -> std::io::Result<Option<ApiKeyInfo>>;

    /// Record that a key was used (update `last_used_at`).
    async fn record_usage(&self, api_key: &str) -> std::io::Result<()>;

    /// List all API keys.
    async fn list(&self) -> std::io::Result<Vec<ApiKeyInfo>>;

    /// Create a new API key.
    async fn create(&self, info: ApiKeyInfo) -> std::io::Result<()>;

    /// Update an existing API key. Returns the updated key if found.
    async fn update(
        &self,
        key_id: &str,
        name: Option<String>,
        permissions: Option<Vec<String>>,
        rate_limit: Option<u32>,
    ) -> std::io::Result<Option<ApiKeyInfo>>;

    /// Delete an API key. Returns true if it was found and deleted.
    async fn delete(&self, key_id: &str) -> std::io::Result<bool>;
}

// ============================================================
// Configuration
// ============================================================

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ApiKeyStoreConfig {
    /// No API key verification — all requests are allowed through.
    Empty,
    /// In-memory store, optionally pre-loaded from a JSON/YAML file.
    Memory {
        #[serde(default)]
        file: Option<String>,
    },
    /// Database-backed store (PostgreSQL or SQLite via sqlx).
    Database { url: String },
    /// HTTP/HTTPS proxy that delegates all operations to a remote REST service.
    Http { url: String },
}

impl Default for ApiKeyStoreConfig {
    fn default() -> Self {
        ApiKeyStoreConfig::Memory { file: None }
    }
}

/// Build an `ApiKeyStore` from the given configuration.
pub async fn create_api_key_store(
    config: &ApiKeyStoreConfig,
) -> std::io::Result<std::sync::Arc<dyn ApiKeyStore>> {
    match config {
        ApiKeyStoreConfig::Empty => Ok(std::sync::Arc::new(EmptyApiKeyStore)),
        ApiKeyStoreConfig::Memory { file } => {
            let store = MemoryApiKeyStore::new();
            if let Some(path) = file {
                store.load_from_file(path)?;
            }
            Ok(std::sync::Arc::new(store))
        }
        ApiKeyStoreConfig::Database { url } => {
            let store = DatabaseApiKeyStore::new(url).await?;
            Ok(std::sync::Arc::new(store))
        }
        ApiKeyStoreConfig::Http { url } => {
            let store = HttpApiKeyStore::new(url);
            Ok(std::sync::Arc::new(store))
        }
    }
}

// ============================================================
// EmptyApiKeyStore
// ============================================================

/// A no-op implementation of `ApiKeyStore` that disables API key verification.
///
/// `is_empty()` always returns `true`, so the authentication middleware will
/// skip key checks entirely (bootstrapping mode). All write operations are
/// silently ignored.
pub struct EmptyApiKeyStore;

#[async_trait]
impl ApiKeyStore for EmptyApiKeyStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        Ok(true)
    }

    async fn verify_key(&self, _api_key: &str) -> std::io::Result<Option<ApiKeyInfo>> {
        Ok(Some(ApiKeyInfo {
            key_id: String::new(),
            name: String::new(),
            key_prefix: String::new(),
            api_key: String::new(),
            permissions: Vec::new(),
            rate_limit: 0,
            status: "active".into(),
            created_at: String::new(),
            last_used_at: None,
            expires_at: None,
        }))
    }

    async fn record_usage(&self, _api_key: &str) -> std::io::Result<()> {
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<ApiKeyInfo>> {
        Ok(Vec::new())
    }

    async fn create(&self, _info: ApiKeyInfo) -> std::io::Result<()> {
        Ok(())
    }

    async fn update(
        &self,
        _key_id: &str,
        _name: Option<String>,
        _permissions: Option<Vec<String>>,
        _rate_limit: Option<u32>,
    ) -> std::io::Result<Option<ApiKeyInfo>> {
        Ok(None)
    }

    async fn delete(&self, _key_id: &str) -> std::io::Result<bool> {
        Ok(false)
    }
}

// ============================================================
// MemoryApiKeyStore
// ============================================================

/// In-memory implementation of `ApiKeyStore`.
///
/// Optionally loads initial keys from a JSON or YAML file at startup.
/// Supports full CRUD through the REST API.
pub struct MemoryApiKeyStore {
    keys: Mutex<Vec<ApiKeyInfo>>,
}

impl MemoryApiKeyStore {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(Vec::new()),
        }
    }

    /// Load API keys from a JSON or YAML file.
    pub fn load_from_file(&self, path: &str) -> std::io::Result<()> {
        let data = std::fs::read_to_string(path)?;
        let keys: Vec<ApiKeyInfo> = if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_yaml::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        } else {
            serde_json::from_str(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        };
        info!("Loaded {} API key(s) from {}", keys.len(), path);
        // We are in sync context during startup, use try_lock.
        match self.keys.try_lock() {
            Ok(mut guard) => {
                *guard = keys;
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to lock keys during file load",
                ));
            }
        }
        Ok(())
    }
}

fn now_utc() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

#[async_trait]
impl ApiKeyStore for MemoryApiKeyStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        Ok(self.keys.lock().await.is_empty())
    }

    async fn verify_key(&self, api_key: &str) -> std::io::Result<Option<ApiKeyInfo>> {
        let keys = self.keys.lock().await;
        Ok(keys.iter().find(|k| k.api_key == api_key).cloned())
    }

    async fn record_usage(&self, api_key: &str) -> std::io::Result<()> {
        let mut keys = self.keys.lock().await;
        if let Some(k) = keys.iter_mut().find(|k| k.api_key == api_key) {
            k.last_used_at = Some(now_utc());
        }
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<ApiKeyInfo>> {
        Ok(self.keys.lock().await.clone())
    }

    async fn create(&self, info: ApiKeyInfo) -> std::io::Result<()> {
        self.keys.lock().await.push(info);
        Ok(())
    }

    async fn update(
        &self,
        key_id: &str,
        name: Option<String>,
        permissions: Option<Vec<String>>,
        rate_limit: Option<u32>,
    ) -> std::io::Result<Option<ApiKeyInfo>> {
        let mut keys = self.keys.lock().await;
        match keys.iter_mut().find(|k| k.key_id == key_id) {
            Some(key) => {
                if let Some(n) = name {
                    key.name = n;
                }
                if let Some(p) = permissions {
                    key.permissions = p;
                }
                if let Some(r) = rate_limit {
                    key.rate_limit = r;
                }
                Ok(Some(key.clone()))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, key_id: &str) -> std::io::Result<bool> {
        let mut keys = self.keys.lock().await;
        let before = keys.len();
        keys.retain(|k| k.key_id != key_id);
        Ok(keys.len() < before)
    }
}

// ============================================================
// DatabaseApiKeyStore
// ============================================================

/// Database-backed implementation of `ApiKeyStore` using sqlx.
///
/// Supports PostgreSQL (`postgres://…`) and SQLite (`sqlite://…`) connection URLs.
pub struct DatabaseApiKeyStore {
    pool: sqlx::AnyPool,
}

impl DatabaseApiKeyStore {
    pub async fn new(url: &str) -> std::io::Result<Self> {
        sqlx::any::install_default_drivers();

        let pool = sqlx::AnyPool::connect(url).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;

        // Create table if it doesn't exist.
        // Use SQL compatible with both PostgreSQL and SQLite.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS api_keys (
                key_id       TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                key_prefix   TEXT NOT NULL,
                api_key      TEXT NOT NULL,
                permissions  TEXT NOT NULL,
                rate_limit   INTEGER NOT NULL,
                status       TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                last_used_at TEXT,
                expires_at   TEXT
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        info!("Database API key store initialised ({})", url);
        Ok(Self { pool })
    }

    fn row_to_info(row: &sqlx::any::AnyRow) -> std::io::Result<ApiKeyInfo> {
        use sqlx::Row;
        let permissions_json: String = row
            .try_get("permissions")
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let permissions: Vec<String> = serde_json::from_str(&permissions_json).unwrap_or_default();

        Ok(ApiKeyInfo {
            key_id: row
                .try_get("key_id")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            name: row
                .try_get("name")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            key_prefix: row
                .try_get("key_prefix")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            api_key: row
                .try_get("api_key")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            permissions,
            rate_limit: row
                .try_get::<i32, _>("rate_limit")
                .map(|v| v as u32)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            status: row
                .try_get("status")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            last_used_at: row
                .try_get("last_used_at")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            expires_at: row
                .try_get("expires_at")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
        })
    }
}

#[async_trait]
impl ApiKeyStore for DatabaseApiKeyStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM api_keys")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(row.0 == 0)
    }

    async fn verify_key(&self, api_key: &str) -> std::io::Result<Option<ApiKeyInfo>> {
        let row = sqlx::query("SELECT * FROM api_keys WHERE api_key = $1")
            .bind(api_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        match row {
            Some(r) => Ok(Some(Self::row_to_info(&r)?)),
            None => Ok(None),
        }
    }

    async fn record_usage(&self, api_key: &str) -> std::io::Result<()> {
        let now = now_utc();
        sqlx::query("UPDATE api_keys SET last_used_at = $1 WHERE api_key = $2")
            .bind(&now)
            .bind(api_key)
            .execute(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<ApiKeyInfo>> {
        let rows = sqlx::query("SELECT * FROM api_keys")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        rows.iter().map(Self::row_to_info).collect()
    }

    async fn create(&self, info: ApiKeyInfo) -> std::io::Result<()> {
        let permissions_json = serde_json::to_string(&info.permissions)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO api_keys (key_id, name, key_prefix, api_key, permissions, rate_limit, status, created_at, last_used_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(&info.key_id)
        .bind(&info.name)
        .bind(&info.key_prefix)
        .bind(&info.api_key)
        .bind(&permissions_json)
        .bind(info.rate_limit as i32)
        .bind(&info.status)
        .bind(&info.created_at)
        .bind(&info.last_used_at)
        .bind(&info.expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    async fn update(
        &self,
        key_id: &str,
        name: Option<String>,
        permissions: Option<Vec<String>>,
        rate_limit: Option<u32>,
    ) -> std::io::Result<Option<ApiKeyInfo>> {
        // Fetch existing row first
        let row = sqlx::query("SELECT * FROM api_keys WHERE key_id = $1")
            .bind(key_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        let row = match row {
            Some(r) => r,
            None => return Ok(None),
        };
        let mut info = Self::row_to_info(&row)?;

        if let Some(n) = name {
            info.name = n;
        }
        if let Some(p) = permissions {
            info.permissions = p;
        }
        if let Some(r) = rate_limit {
            info.rate_limit = r;
        }

        let permissions_json = serde_json::to_string(&info.permissions)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;

        sqlx::query(
            "UPDATE api_keys SET name = $1, permissions = $2, rate_limit = $3 WHERE key_id = $4",
        )
        .bind(&info.name)
        .bind(&permissions_json)
        .bind(info.rate_limit as i32)
        .bind(key_id)
        .execute(&self.pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(Some(info))
    }

    async fn delete(&self, key_id: &str) -> std::io::Result<bool> {
        let result = sqlx::query("DELETE FROM api_keys WHERE key_id = $1")
            .bind(key_id)
            .execute(&self.pool)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(result.rows_affected() > 0)
    }
}

// ============================================================
// HttpApiKeyStore
// ============================================================

/// HTTP/HTTPS-backed implementation of `ApiKeyStore`.
///
/// Delegates all operations to a remote REST service.
///
/// Expected remote API:
/// - `POST  {base_url}/verify`       — body `{"api_key":"…"}`, returns `ApiKeyInfo` or 401
/// - `GET   {base_url}/keys`          — returns `[ApiKeyInfo, …]`
/// - `POST  {base_url}/keys`          — body `ApiKeyInfo`, creates a key
/// - `PUT   {base_url}/keys/{key_id}` — body `{"name":…,"permissions":…,"rate_limit":…}`
/// - `DELETE {base_url}/keys/{key_id}` — deletes a key
pub struct HttpApiKeyStore {
    base_url: String,
    client: reqwest::Client,
}

impl HttpApiKeyStore {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl ApiKeyStore for HttpApiKeyStore {
    async fn is_empty(&self) -> std::io::Result<bool> {
        // Ask the remote service for the key list and check emptiness.
        let keys = self.list().await?;
        Ok(keys.is_empty())
    }

    async fn verify_key(&self, api_key: &str) -> std::io::Result<Option<ApiKeyInfo>> {
        let url = format!("{}/verify", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "api_key": api_key }))
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        if resp.status() == reqwest::StatusCode::UNAUTHORIZED
            || resp.status() == reqwest::StatusCode::NOT_FOUND
        {
            return Ok(None);
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!("HTTP key store verify returned {}: {}", status, body);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("verify request failed with status {}", status),
            ));
        }

        let info: ApiKeyInfo = resp
            .json()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some(info))
    }

    async fn record_usage(&self, api_key: &str) -> std::io::Result<()> {
        // Usage recording is handled implicitly by the verify call on the remote side.
        let _ = api_key;
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<ApiKeyInfo>> {
        let url = format!("{}/keys", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        if !resp.status().is_success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("list keys request failed with status {}", resp.status()),
            ));
        }

        let keys: Vec<ApiKeyInfo> = resp
            .json()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(keys)
    }

    async fn create(&self, info: ApiKeyInfo) -> std::io::Result<()> {
        let url = format!("{}/keys", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&info)
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        if !resp.status().is_success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("create key request failed with status {}", resp.status()),
            ));
        }
        Ok(())
    }

    async fn update(
        &self,
        key_id: &str,
        name: Option<String>,
        permissions: Option<Vec<String>>,
        rate_limit: Option<u32>,
    ) -> std::io::Result<Option<ApiKeyInfo>> {
        let url = format!("{}/keys/{}", self.base_url, key_id);
        let body = serde_json::json!({
            "name": name,
            "permissions": permissions,
            "rate_limit": rate_limit,
        });
        let resp = self
            .client
            .put(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("update key request failed with status {}", resp.status()),
            ));
        }

        let info: ApiKeyInfo = resp
            .json()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some(info))
    }

    async fn delete(&self, key_id: &str) -> std::io::Result<bool> {
        let url = format!("{}/keys/{}", self.base_url, key_id);
        let resp = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !resp.status().is_success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("delete key request failed with status {}", resp.status()),
            ));
        }
        Ok(true)
    }
}
