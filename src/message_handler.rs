use async_trait::async_trait;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::AtomicU64;

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

#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle_message(&self, decoded_message: Value) -> std::io::Result<Value>;
}

pub struct HttpMessageHandler {
    urls: Vec<String>,
    next_index: AtomicU64,
    algorithm: LoadBalancingAlgorithm,
}

impl HttpMessageHandler {
    pub fn new(urls: Vec<String>) -> Self {
        HttpMessageHandler {
            urls,
            next_index: AtomicU64::new(0),
            algorithm: LoadBalancingAlgorithm::default(),
        }
    }

    pub fn with_algorithm(urls: Vec<String>, algorithm: LoadBalancingAlgorithm) -> Self {
        HttpMessageHandler {
            urls,
            next_index: AtomicU64::new(0),
            algorithm,
        }
    }
}

#[async_trait]
impl MessageHandler for HttpMessageHandler {
    async fn handle_message(&self, decoded_message: Value) -> std::io::Result<Value> {
        if self.urls.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No handler URLs configured",
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
            match client.post(url).json(&decoded_message).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.json::<Value>().await {
                            Ok(json_resp) => {
                                info!("Successfully sent message to {}", url);
                                return Ok(json_resp);
                            }
                            Err(e) => {
                                warn!("Failed to parse JSON response from {}: {}", url, e);
                                continue;
                            }
                        }
                    } else {
                        warn!(
                            "Received non-success status from {}: {}",
                            url,
                            resp.status()
                        );
                        continue;
                    }
                }
                Err(e) => {
                    warn!("Failed to send message to {}: {}", url, e);
                    continue;
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "All handler URLs failed",
        ))
    }
}
