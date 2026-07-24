use crate::config::{LoadBalancingAlgorithm, RequestHandlerConfig};
use async_trait::async_trait;

use log::{info, warn};
use regex::Regex;
use serde_json::Value;
use std::sync::atomic::AtomicU64;

#[async_trait]
pub trait MessageHandler: Send + Sync {
    fn is_my_request_address(&self, address: &str) -> bool;
    async fn handle_message(&self, decoded_message: &Value) -> std::io::Result<Value>;
}

pub struct HttpMessageHandler {
    pub dest_addresses: Vec<String>,
    pub dest_address_patterns: Vec<Regex>,
    pub urls: Vec<String>,
    next_index: AtomicU64,
    pub algorithm: LoadBalancingAlgorithm,
}

impl HttpMessageHandler {
    pub fn new(
        dest_addresses: Vec<String>,
        dest_address_patterns: Vec<String>,
        urls: Vec<String>,
        algorithm: LoadBalancingAlgorithm,
    ) -> Self {
        let dest_address_patterns = dest_address_patterns
            .into_iter()
            .filter_map(|pattern| match Regex::new(&pattern) {
                Ok(regex) => Some(regex),
                Err(e) => {
                    warn!("Invalid regex pattern '{}': {}", pattern, e);
                    None
                }
            })
            .collect();

        HttpMessageHandler {
            dest_addresses,
            dest_address_patterns,
            //distribution_list,
            urls,
            next_index: AtomicU64::new(0),
            algorithm,
        }
    }

    pub fn create_from(config: &RequestHandlerConfig) -> Self {
        let algorithm = config
            .algorithm
            .as_ref()
            .map_or(LoadBalancingAlgorithm::RoundRobin, |alg| {
                LoadBalancingAlgorithm::from(alg)
            });
        HttpMessageHandler::new(
            config.dest_addresses.clone().unwrap_or_default(),
            config.dest_address_patterns.clone().unwrap_or_default(),
            config.urls.clone(),
            algorithm,
        )
    }

    pub fn match_any_pattern(&self, dest_address: &str) -> bool {
        self.dest_address_patterns
            .iter()
            .any(|pattern| pattern.is_match(dest_address))
    }

    pub fn match_any_address(&self, dest_address: &str) -> bool {
        self.dest_addresses.contains(&dest_address.to_string())
    }
}

#[async_trait]
impl MessageHandler for HttpMessageHandler {
    fn is_my_request_address(&self, address: &str) -> bool {
        if self.dest_addresses.is_empty() && self.dest_address_patterns.is_empty() {
            return true;
        }

        self.match_any_address(&address) || self.match_any_pattern(&address)
    }

    async fn handle_message(&self, decoded_message: &Value) -> std::io::Result<Value> {
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

pub struct CompositeMessageHandler {
    handlers: Vec<Box<dyn MessageHandler>>,
}

impl CompositeMessageHandler {
    pub fn new(handlers: Vec<Box<dyn MessageHandler>>) -> Self {
        CompositeMessageHandler { handlers }
    }
}

#[async_trait]
impl MessageHandler for CompositeMessageHandler {
    fn is_my_request_address(&self, address: &str) -> bool {
        self.handlers
            .iter()
            .any(|handler| handler.is_my_request_address(address))
    }

    async fn handle_message(&self, decoded_message: &Value) -> std::io::Result<Value> {
        let destination_address = decoded_message
            .get("destination_addr")
            .and_then(|addr| addr.as_str())
            .unwrap_or("");
        for handler in &self.handlers {
            if handler.is_my_request_address(destination_address) {
                return handler.handle_message(decoded_message).await;
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "No suitable handler found",
        ))
    }
}

pub fn create_request_handler(configs: &[RequestHandlerConfig]) -> Box<dyn MessageHandler> {
    let handlers: Vec<Box<dyn MessageHandler>> = configs
        .iter()
        .map(|config| Box::new(HttpMessageHandler::create_from(config)) as Box<dyn MessageHandler>)
        .collect();
    Box::new(CompositeMessageHandler::new(handlers)) as Box<dyn MessageHandler>
}

pub struct NoOpMessageHandler;

impl NoOpMessageHandler {
    pub fn new() -> Self {
        NoOpMessageHandler
    }
}

#[async_trait]
impl MessageHandler for NoOpMessageHandler {
    fn is_my_request_address(&self, _address: &str) -> bool {
        false
    }

    async fn handle_message(&self, _decoded_message: &Value) -> std::io::Result<Value> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "NoOpMessageHandler cannot handle messages",
        ))
    }
}
