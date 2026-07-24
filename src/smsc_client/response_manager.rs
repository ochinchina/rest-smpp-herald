use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct ResponseManager {
    callbacks: Arc<Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>>,
}

impl ResponseManager {
    pub fn new() -> Self {
        ResponseManager {
            callbacks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a callback for a specific sequence number. When a response is received for that sequence number, the callback will be invoked.
    ///
    /// # Arguments
    /// * `sequence_number` - The sequence number for which the callback is being registered.
    /// # Returns
    /// A `tokio::sync::oneshot::Receiver` that will receive the response when it is available.
    pub async fn register_callback(
        &self,
        sequence_number: u32,
    ) -> tokio::sync::oneshot::Receiver<std::io::Result<Value>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.callbacks.lock().await.insert(sequence_number, sender);
        receiver
    }

    pub async fn wait_response(
        &self,
        sequence_number: u32,
        timeout: tokio::time::Duration,
    ) -> std::io::Result<Value> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            self.callbacks.lock().await.insert(sequence_number, sender);
        }

        match tokio::time::timeout(timeout, receiver).await {
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

    /// Called when a response is received for a specific sequence number. It looks up the registered callback for that sequence number and invokes it with the received response.
    /// # Arguments
    /// * `sequence_number` - The sequence number for which the response was received.
    /// * `response` - The response received for the sequence number.
    pub async fn response_received(&self, sequence_number: u32, response: Value) {
        self.callbacks
            .lock()
            .await
            .remove(&sequence_number)
            .map(|sender| {
                let _ = sender.send(Ok(response));
            });
    }
}
