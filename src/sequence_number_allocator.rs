use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone)]
pub struct SequenceNumberAllocator {
    current: Arc<AtomicU32>,
}

impl Default for SequenceNumberAllocator {
    fn default() -> Self {
        SequenceNumberAllocator::new()
    }
}

impl SequenceNumberAllocator {
    pub fn new() -> Self {
        SequenceNumberAllocator {
            current: Arc::new(AtomicU32::new(0)),
        }
    }

    /**
     * Get the next sequence number in a thread-safe manner.
     */
    pub fn next(&self) -> u32 {
        self.current.fetch_add(1, Ordering::Relaxed)
    }
}
