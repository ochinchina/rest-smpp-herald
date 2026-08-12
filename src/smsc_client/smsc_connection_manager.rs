use crate::config::LoadBalancingAlgorithm;
use crate::message::SMPPMessage;
use crate::smsc_client::smsc_connection::SmscConnection;
use log::{error, info};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SmscConnectionManager {
    connections: Arc<Mutex<Vec<Arc<SmscConnection>>>>,
    algorithm: LoadBalancingAlgorithm,
    index: Arc<AtomicU32>,
}

impl SmscConnectionManager {
    pub fn new(algorithm: LoadBalancingAlgorithm) -> Self {
        SmscConnectionManager {
            connections: Arc::new(Mutex::new(Vec::new())),
            algorithm,
            index: Arc::new(AtomicU32::new(0)),
        }
    }

    pub(crate) async fn get_connection_addresses(&self) -> Vec<String> {
        let conns = self.connections.lock().await;
        conns.iter().map(|conn| conn.get_address()).collect()
    }

    /// Add a new SMSC connection to the manager.
    ///
    /// # Arguments
    /// * `connection` - The `SmscConnection` to be added.
    pub async fn add_connection(&self, connection: SmscConnection) {
        let mut conns = self.connections.lock().await;
        conns.push(Arc::new(connection));
    }

    /// Remove an SMSC connection from the manager by its address.
    ///
    /// # Arguments
    /// * `address` - The address of the SMSC connection to be removed.
    pub async fn remove_connection(&self, address: &str) {
        let mut conns = self.connections.lock().await;
        conns.retain(|conn| conn.config.address != address);
    }

    /// Send a message to one of the available SMSC connections based on the load balancing algorithm.
    /// If the message is sent successfully, it waits for a response from the SMSC.
    /// If no response is received within the timeout, it tries the next available connection.
    /// If all connections fail, it returns an error.
    ///
    /// # Arguments
    /// * `message` - The SMPP message to be sent.
    ///
    /// # Returns
    /// `std::io::Result<Value>` - The response from the SMSC or an error if all connections fail.
    pub async fn send_message(&self, message: &SMPPMessage) -> std::io::Result<Value> {
        let connections = self.get_connections().await;

        if connections.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No active SMSC connections available",
            ));
        }

        for connection in connections.iter() {
            info!("Sending message to SMSC at {}", connection.config.address);
            if let Ok(response) = connection.send_message(message).await {
                if let Some(inner_response) = response {
                    return Ok(inner_response);
                }
            } else {
                error!(
                    "Failed to send message to SMSC at {}",
                    connection.config.address
                );
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send message to any SMSC connection",
        ))
    }

    /// Get the list of SMSC connections in the order determined by the load balancing algorithm.
    /// For RoundRobin, it returns the connections starting from the next index.
    /// For other algorithms, it returns the connections in their current order.
    ///
    /// # Returns
    /// Vec<Arc<SmscConnection>> - The list of SMSC connections in the order determined by the load balancing algorithm.
    async fn get_connections(&self) -> Vec<Arc<SmscConnection>> {
        let mut result = vec![];

        let conns = self.connections.lock().await;
        let n = conns.len();
        let index = if self.algorithm == LoadBalancingAlgorithm::RoundRobin {
            self.index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize
                % n
        } else {
            0
        };

        for i in 0..n {
            result.push(conns[(index + i) % n].clone());
        }
        result
    }
}
