use async_trait::async_trait;
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

// ============================================================
// AlarmNotifier trait
// ============================================================

/// Abstraction for alarm notification backends.
#[async_trait]
pub trait AlarmNotifier: Send + Sync {
    async fn raise_alarm(&self, host: &str, port: u16, description: &str);
    async fn clear_alarm(&self, host: &str, port: u16, description: &str);
}

// ============================================================
// Configuration
// ============================================================

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AlarmConfig {
    Http {
        notify_url: String,
        #[serde(default = "default_alarm_id")]
        alarm_id: String,
    },
    Database {
        url: String,
        #[serde(default = "default_alarm_id")]
        alarm_id: String,
    },
}

fn default_alarm_id() -> String {
    "smsc_connection_failure".to_string()
}

/// Creates an `AlarmNotifier` implementation based on the provided configuration.
pub async fn create_alarm_notifier(
    config: &AlarmConfig,
) -> std::io::Result<Arc<dyn AlarmNotifier>> {
    match config {
        AlarmConfig::Http {
            notify_url,
            alarm_id,
        } => Ok(Arc::new(HttpAlarmNotifier::new(
            notify_url.clone(),
            alarm_id.clone(),
        ))),
        AlarmConfig::Database { url, alarm_id } => {
            let notifier = DatabaseAlarmNotifier::new(url, alarm_id.clone()).await?;
            Ok(Arc::new(notifier))
        }
    }
}

// ============================================================
// HTTP implementation
// ============================================================

pub struct HttpAlarmNotifier {
    notify_url: String,
    alarm_id: String,
    client: reqwest::Client,
}

impl HttpAlarmNotifier {
    pub fn new(notify_url: String, alarm_id: String) -> Self {
        Self {
            notify_url,
            alarm_id,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl AlarmNotifier for HttpAlarmNotifier {
    async fn raise_alarm(&self, host: &str, port: u16, description: &str) {
        let payload = json!({
            "alarm_id": self.alarm_id,
            "status": "raised",
            "host": host,
            "port": port,
            "description": description,
        });
        if let Err(e) = self
            .client
            .post(&self.notify_url)
            .json(&payload)
            .send()
            .await
        {
            error!("Failed to send alarm notification: {}", e);
        }
    }

    async fn clear_alarm(&self, host: &str, port: u16, description: &str) {
        let payload = json!({
            "alarm_id": self.alarm_id,
            "status": "cleared",
            "host": host,
            "port": port,
            "description": description,
        });
        if let Err(e) = self
            .client
            .post(&self.notify_url)
            .json(&payload)
            .send()
            .await
        {
            error!("Failed to send alarm clear notification: {}", e);
        }
    }
}

// ============================================================
// Database implementation (PostgreSQL / SQLite via sqlx)
// ============================================================

pub struct DatabaseAlarmNotifier {
    pool: sqlx::AnyPool,
    alarm_id: String,
}

impl DatabaseAlarmNotifier {
    pub async fn new(url: &str, alarm_id: String) -> std::io::Result<Self> {
        let pool = sqlx::AnyPool::connect(url).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;

        // Auto-create the alarms table if it doesn't exist
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS alarms (
                id INTEGER PRIMARY KEY,
                alarm_id TEXT NOT NULL,
                status TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                description TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )",
        )
        .execute(&pool)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(Self { pool, alarm_id })
    }
}

#[async_trait]
impl AlarmNotifier for DatabaseAlarmNotifier {
    async fn raise_alarm(&self, host: &str, port: u16, description: &str) {
        if let Err(e) = sqlx::query(
            "INSERT INTO alarms (alarm_id, status, host, port, description) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&self.alarm_id)
        .bind("raised")
        .bind(host)
        .bind(port as i32)
        .bind(description)
        .execute(&self.pool)
        .await
        {
            error!("Failed to save raised alarm to database: {}", e);
        }
    }

    async fn clear_alarm(&self, host: &str, port: u16, description: &str) {
        if let Err(e) = sqlx::query(
            "INSERT INTO alarms (alarm_id, status, host, port, description) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&self.alarm_id)
        .bind("cleared")
        .bind(host)
        .bind(port as i32)
        .bind(description)
        .execute(&self.pool)
        .await
        {
            error!("Failed to save cleared alarm to database: {}", e);
        }
    }
}
