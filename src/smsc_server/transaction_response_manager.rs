use log::warn;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::select;
use tokio::sync::Mutex;

/// Manages the mapping of sequence numbers to their corresponding response channels.
#[derive(Clone)]
pub struct TransactionResponseManager {
    transactions: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<Value>>>>, // Maps sequence numbers to response channels
}

impl TransactionResponseManager {
    pub fn new() -> Self {
        TransactionResponseManager {
            transactions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Called when a response is received for a specific sequence number. It looks up the registered callback for that sequence number and invokes it with the received response.
    /// # Arguments
    /// * `sequence_number` - The sequence number for which the response was received.
    /// * `response` - The response value received for the sequence number.
    pub async fn response_received(&self, sequence_number: u32, response: Value) {
        if let Some(sender) = self.transactions.lock().await.remove(&sequence_number) {
            let _ = sender.send(response);
        } else {
            warn!(
                "No transaction found for sequence number {} to send response",
                sequence_number
            );
        }
    }

    /// Waits for a response for a specific sequence number. If the response is not received within the timeout duration, it returns None.
    /// # Arguments
    /// * `sequence_number` - The sequence number for which the response is being awaited.
    /// * `timeout` - The duration to wait for the response before timing out.
    /// # Returns
    /// An `Option<Value>` containing the response if received, or None if timed out.
    pub async fn wait_response(&self, sequence_number: u32) -> Option<Value> {
        let (sender, receiver) = tokio::sync::oneshot::channel::<Value>();
        {
            let mut transactions = self.transactions.lock().await;
            transactions.insert(sequence_number, sender);
        }

        select! {
            res = receiver => {
                match res {
                    Ok(response) => Some(response),
                    Err(_) => {
                        warn!("Response channel closed for sequence number {}", sequence_number);
                        None
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                warn!("Timeout waiting for response for sequence number {}", sequence_number);
                self.transactions.lock().await.remove(&sequence_number);
                None
            }
        }
    }
}
