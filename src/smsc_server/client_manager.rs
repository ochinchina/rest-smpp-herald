use log::{debug, error, info, warn};
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
/**
 * Manages connected clients and their associated address ranges and message senders.
 */
pub struct ClientManager {
    // A thread-safe map of address ranges to their corresponding regex and message sender.
    connections: Arc<Mutex<HashMap<String, (Regex, tokio::sync::mpsc::Sender<Vec<u8>>)>>>,
}

impl ClientManager {
    pub(crate) fn new() -> Self {
        ClientManager {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn add_client(
        &self,
        address_range: String,
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) {
        if address_range.is_empty() {
            warn!("Attempted to add client with empty address range");
            return;
        }

        let address_range = if !address_range.starts_with("^") && address_range.ends_with("$") {
            format!(".*{}", address_range)
        } else {
            address_range
        };
        let address_range = if address_range.starts_with("^") && !address_range.ends_with("$") {
            format!("{}.*", address_range)
        } else {
            address_range
        };

        match Regex::new(&address_range) {
            Ok(regex) => {
                self.connections
                    .lock()
                    .await
                    .insert(address_range.clone(), (regex, sender));
                info!("Added client for address range: {}", address_range);
            }
            Err(e) => {
                error!(
                    "Failed to compile regex for address range '{}': {}",
                    address_range, e
                );
            }
        }
    }

    pub(crate) async fn remove_client(&self, address_range: &str) {
        self.connections.lock().await.remove(address_range);
    }

    pub(crate) async fn send_message(
        &self,
        address: &str,
        message: &Vec<u8>,
    ) -> std::io::Result<()> {
        if let Some(sender) = self.get_writer(address).await {
            if let Err(e) = sender.send(message.clone()).await {
                error!("Failed to send message to client {}: {}", address, e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to send message",
                ));
            }
            debug!(
                "Succeed to send message to client {}: {}",
                address,
                hex::encode(&message)
            );
            Ok(())
        } else {
            error!("Client {} not found", address);
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Client not found",
            ))
        }
    }

    pub(crate) async fn get_writer(
        &self,
        address: &str,
    ) -> Option<tokio::sync::mpsc::Sender<Vec<u8>>> {
        let guard = self.connections.lock().await;
        guard.iter().find_map(|(_, (regex, sender))| {
            if regex.is_match(address) {
                Some(sender.clone())
            } else {
                None
            }
        })
    }
}
