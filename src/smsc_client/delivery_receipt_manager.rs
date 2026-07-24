use crate::esm_class::{EsmClass, MessageType};
use crate::registered_delivery::{IntermediateNotification, RegisteredDelivery};
use chrono::{DateTime, Local};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct DeliveryReceiptManager {
    /// Maps message_id to a tuple of (receipt_url, registered_delivery, esm_class, expire_time)
    receipt_urls:
        Arc<Mutex<HashMap<String, (String, RegisteredDelivery, EsmClass, DateTime<Local>)>>>,
    next_clean_time: Arc<Mutex<DateTime<Local>>>,
}

impl DeliveryReceiptManager {
    pub fn new() -> Self {
        DeliveryReceiptManager {
            receipt_urls: Arc::new(Mutex::new(HashMap::new())),
            next_clean_time: Arc::new(Mutex::new(Local::now() + chrono::Duration::minutes(5))),
        }
    }

    pub async fn register_receipt_url(
        &self,
        message_id: String,
        receipt_url: String,
        registered_delivery: RegisteredDelivery,
    ) {
        if registered_delivery.delivery_receipt
            == crate::registered_delivery::DeliveryReceipt::NoRequested
            && registered_delivery.intermediate_notification
                == crate::registered_delivery::IntermediateNotification::NoRequested
            && registered_delivery.sme_acked
                == crate::registered_delivery::SMEOriginatedAcknowledgement::NoRequested
        {
            info!(
                "No receipts requested for message_id {}, not registering receipt URL.",
                message_id
            );
            return;
        }
        self.clean_expired_entries().await;

        let mut urls = self.receipt_urls.lock().await;
        urls.insert(
            message_id,
            (
                receipt_url,
                registered_delivery,
                EsmClass::default(),
                Local::now() + chrono::Duration::hours(1),
            ),
        );
    }

    pub async fn get_receipt_url(&self, message_id: &str, esm_class: &EsmClass) -> Option<String> {
        let mut receipt_urls = self.receipt_urls.lock().await;
        let urls = receipt_urls.get_mut(message_id);
        if let Some(urls) = urls {
            let (receipt_url, registered_delivery, existing_esm_class, expire_time) = urls;
            let url = if *expire_time > Local::now() {
                Some(receipt_url.clone())
            } else {
                None
            };

            existing_esm_class.merge_from(esm_class);

            if Self::is_all_receipt_requested(registered_delivery, esm_class) {
                info!(
                    "All receipts requested for message_id {}, registered_delivery: {}, esm_class: {}",
                    message_id,
                    registered_delivery.to_string(),
                    existing_esm_class.to_string()
                );
                receipt_urls.remove(message_id);
            }

            return url;
        }
        return None;
    }

    fn is_all_receipt_requested(
        registered_delivery: &RegisteredDelivery,
        esm_class: &EsmClass,
    ) -> bool {
        // Check if the registered_delivery and esm_class indicate that all receipts are requested
        if registered_delivery
            .delivery_receipt
            .is_delivery_receipt_requested()
            && esm_class.message_type != MessageType::DeliveryReceipt
        {
            return false;
        }

        if registered_delivery.intermediate_notification == IntermediateNotification::Requested
            && esm_class.message_type != MessageType::IntermediateDeliveryNotification
        {
            return false;
        }

        if registered_delivery.sme_acked.is_sme_ack_requested()
            && !esm_class.ansi41_specific.is_delivery_ack()
        {
            return false;
        }

        true
    }

    pub async fn remove_receipt_info(&self, message_id: &str) {
        let mut urls = self.receipt_urls.lock().await;
        urls.remove(message_id);
    }

    async fn clean_expired_entries(&self) {
        let now = Local::now();
        {
            let mut next_clean_time = self.next_clean_time.lock().await;
            if *next_clean_time > now {
                return;
            }
            *next_clean_time = Local::now() + chrono::Duration::minutes(5);
        }
        let mut urls = self.receipt_urls.lock().await;

        urls.retain(|_, (_, _, _, expire_time)| *expire_time > now);
    }
}
