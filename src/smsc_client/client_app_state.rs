use crate::alarm::AlarmNotifier;
use crate::config::SmppConnectionInfo;
use crate::id_generator::MessageIdGenerator;
use crate::message_handler::MessageHandler;
use crate::sequence_number_allocator::SequenceNumberAllocator;
use crate::smsc_client::delivery_receipt_manager::DeliveryReceiptManager;
use crate::smsc_client::smsc_connection_manager::SmscConnectionManager;
use std::sync::{Arc, atomic::AtomicU64};
use tokio::sync::Mutex;

/// Represents the shared application state for the SMPP Router, including storage backends,
pub struct ClientAppState {
    pub smpp_connections_store: Mutex<Vec<SmppConnectionInfo>>,
    pub start_time: std::time::Instant,
    pub id_generator: Arc<dyn MessageIdGenerator>,
    pub smsc_connection_manager: Arc<SmscConnectionManager>,
    pub connection_index: AtomicU64,
    pub seq_allocator: SequenceNumberAllocator,
    pub smsc_message_handler: Arc<Box<dyn MessageHandler>>,
    pub alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
    pub delivery_receipt_manager: Arc<DeliveryReceiptManager>,
}

impl ClientAppState {
    pub async fn gen_id(&self, prefix: &str) -> String {
        let id_generator = self.id_generator.clone();
        let prefix = prefix.to_string();
        id_generator.generate(&prefix).await
    }
}
