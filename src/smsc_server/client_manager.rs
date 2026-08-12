use log::{debug, error, info, warn};
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

#[derive(Clone)]
/**
 * Manages connected clients and their associated address ranges and message senders.
 */
pub struct ClientManager {
    /// A thread-safe map of address ranges to their corresponding regex and message sender.
    /// key is peer_address, value is a tuple of (range address regex, sender)
    connections: Arc<Mutex<HashMap<String, (Regex, Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>)>>>,
}

impl ClientManager {
    pub(crate) fn new() -> Self {
        ClientManager {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds a new client to the manager with the specified peer address, address range, and message sender.
    /// # Arguments
    /// * `peer_address` - The address of the peer client.
    /// * `address_range` - The address range associated with the client, which will be converted to a regex.
    /// * `writer` - The message sender associated with the client.
    pub(crate) async fn add_client(
        &self,
        peer_address: String,
        address_range: String,
        writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
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
                    .entry(peer_address.clone())
                    .or_insert((regex, writer));
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

    /// Removes a client from the manager based on the specified peer address.
    /// # Arguments
    /// * `peer_address` - The address of the peer client to be removed.
    pub(crate) async fn remove_client(&self, peer_address: String) {
        self.connections.lock().await.remove(&peer_address);
    }

    /// Sends a message to the client associated with the specified address.
    /// # Arguments
    /// * `address` - The address of the client to which the message should be sent
    /// * `message` - The message to be sent to the client.
    /// # Returns
    /// * `std::io::Result<()>` - Returns Ok(()) if the message was sent successfully, or an error if the client was not found or if there was an issue sending the message.
    pub(crate) async fn send_message(
        &self,
        address: &str,
        message: &Vec<u8>,
    ) -> std::io::Result<()> {
        let writers = self.get_writer(address).await;
        if writers.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Client not found",
            ));
        }

        for writer in writers {
            let mut writer = writer.lock().await;
            if let Ok(_) = writer.write_all(message).await {
                if let Ok(_) = writer.flush().await {
                    debug!(
                        "Succeed to send message to client {}: {}",
                        address,
                        hex::encode(&message)
                    );
                    return Ok(());
                }
            }
        }

        error!(
            "Failed to send message to client {}: {}",
            address,
            hex::encode(&message)
        );
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send message to client",
        ));
    }

    pub(crate) async fn get_writer(
        &self,
        address: &str,
    ) -> Vec<Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>> {
        let guard = self.connections.lock().await;
        guard
            .iter()
            .filter_map(|(_, (regex, writer))| {
                if regex.is_match(address) {
                    Some(writer.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}
