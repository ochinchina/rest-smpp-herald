use crate::sequence_number_allocator::SequenceNumberAllocator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Maps new sequence numbers to original sequence numbers with timestamp
#[derive(Clone)]
pub struct SequenceNumberMapper {
    seq_allocator: SequenceNumberAllocator,
    sequence_number_map: Arc<Mutex<HashMap<u32, (u32, std::time::Instant)>>>, // Maps new sequence numbers to original sequence numbers with timestamp
    last_cleanup: Arc<Mutex<std::time::Instant>>, // Timestamp of the last cleanup
    expiration_duration: std::time::Duration,     // Duration after which the mapping expires
}

impl SequenceNumberMapper {
    pub fn new(seq_allocator: SequenceNumberAllocator) -> Self {
        SequenceNumberMapper {
            seq_allocator,
            sequence_number_map: Arc::new(Mutex::new(HashMap::new())),
            last_cleanup: Arc::new(Mutex::new(std::time::Instant::now())),
            expiration_duration: std::time::Duration::from_secs(60), // 1 minute expiration
        }
    }

    pub async fn map_sequence_number(&self, original_seq: u32) -> u32 {
        self.clean_expired_mappings().await;

        let new_seq = self.seq_allocator.next();
        self.sequence_number_map
            .lock()
            .await
            .insert(new_seq, (original_seq, std::time::Instant::now()));
        new_seq
    }

    pub async fn get_original_sequence_number(&self, new_seq: u32) -> Option<u32> {
        self.sequence_number_map
            .lock()
            .await
            .get(&new_seq)
            .map(|(orig, _)| *orig)
    }

    async fn clean_expired_mappings(&self) {
        if !self.is_time_to_cleanup().await {
            return;
        }
        let now = std::time::Instant::now();
        {
            let mut last_cleanup = self.last_cleanup.lock().await;
            *last_cleanup = now;
        }
        let mut map = self.sequence_number_map.lock().await;
        map.retain(|_, (_, timestamp)| now.duration_since(*timestamp) < self.expiration_duration);
    }

    async fn is_time_to_cleanup(&self) -> bool {
        let last_cleanup = self.last_cleanup.lock().await;
        last_cleanup.elapsed() >= self.expiration_duration
    }
}
