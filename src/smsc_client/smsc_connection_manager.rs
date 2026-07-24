use crate::config::LoadBalancingAlgorithm;
use crate::message::SMPPMessage;
use crate::smsc_client::response_manager::ResponseManager;
use crate::smsc_client::smsc_connection::SmscConnection;
use log::{error, info};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use tokio::sync::Mutex;

use tokio::time::Duration;

#[derive(Clone)]
pub struct SmscConnectionManager {
    connections: Arc<Mutex<Vec<Arc<SmscConnection>>>>,
    algorithm: LoadBalancingAlgorithm,
    index: Arc<AtomicU32>,
    response_manager: ResponseManager,
    per_connection_timeout: Duration,
}

impl SmscConnectionManager {
    pub fn new(algorithm: LoadBalancingAlgorithm) -> Self {
        SmscConnectionManager {
            connections: Arc::new(Mutex::new(Vec::new())),
            algorithm,
            index: Arc::new(AtomicU32::new(0)),
            response_manager: ResponseManager::new(),
            per_connection_timeout: Duration::from_secs(5),
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

    /// Notify the manager that a response has been received for a specific sequence number.
    ///
    /// # Arguments
    /// * `sequence_number` - The sequence number of the message for which the response was received.
    /// * `response` - The response value received from the SMSC.
    pub async fn response_received(&self, sequence_number: u32, response: Value) {
        self.response_manager
            .response_received(sequence_number, response)
            .await;
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

        let sequence_number = message.get_sequence_number().unwrap_or(0);

        let mut response_receiver = self
            .response_manager
            .register_callback(sequence_number)
            .await;

        for connection in connections.iter() {
            info!("Sending message to SMSC at {}", connection.config.address);
            if let Ok(_) = connection.send_message(message).await {
                if let Ok(response) = self.wait_response(&mut response_receiver).await {
                    return Ok(response);
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

    /// Wait for a response from the SMSC for a specific sequence number.
    /// If a response is received within the timeout, it returns the response.
    /// If the timeout is reached without a response, it returns a timeout error.
    ///
    /// # Arguments
    /// * `smsc_address` - The address of the SMSC from which the response is expected.
    /// * `receiver` - The oneshot receiver to wait for the response.
    /// * `timeout` - The duration to wait for the response before timing out.
    ///
    /// # Returns
    /// `std::io::Result<Value>` - The response from the SMSC or an error if the timeout is reached or the channel is closed.
    async fn wait_response(
        &self,
        receiver: &mut tokio::sync::oneshot::Receiver<std::io::Result<Value>>,
    ) -> std::io::Result<Value> {
        match tokio::time::timeout(self.per_connection_timeout, receiver).await {
            Ok(result) => match result {
                Ok(response) => response,
                Err(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Response channel closed",
                )),
            },
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Response timed out",
            )),
        }
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
