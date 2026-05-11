use serde::Serialize;
use serde_json::json;

/// A leaky bucket rate limiter. Tokens (representing requests) are added
/// when a request arrives and leak out at a constant rate. When the bucket
/// is full the request is rejected.
#[derive(Debug)]
pub struct LeakyBucket {
    pub capacity: u32,
    tokens: f64,
    leak_rate: f64,
    last_checked: std::time::Instant,
}

impl LeakyBucket {
    pub fn new(capacity: u32, window_secs: f64) -> Self {
        Self {
            capacity,
            tokens: 0.0,
            leak_rate: capacity as f64 / window_secs,
            last_checked: std::time::Instant::now(),
        }
    }

    fn leak(&mut self) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_checked).as_secs_f64();
        self.last_checked = now;
        self.tokens = (self.tokens - elapsed * self.leak_rate).max(0.0);
    }

    fn has_capacity(&self) -> bool {
        let elapsed = self.last_checked.elapsed().as_secs_f64();
        let current = (self.tokens - elapsed * self.leak_rate).max(0.0);
        current < self.capacity as f64
    }

    pub fn try_acquire(&mut self) -> bool {
        self.leak();
        if self.tokens < self.capacity as f64 {
            self.tokens += 1.0;
            true
        } else {
            false
        }
    }

    pub fn remaining(&self) -> u32 {
        let elapsed = self.last_checked.elapsed().as_secs_f64();
        let current = (self.tokens - elapsed * self.leak_rate).max(0.0);
        (self.capacity as f64 - current).max(0.0) as u32
    }

    pub fn current_usage(&self) -> u32 {
        let elapsed = self.last_checked.elapsed().as_secs_f64();
        let current = (self.tokens - elapsed * self.leak_rate).max(0.0);
        current.ceil() as u32
    }

    pub fn update_capacity(&mut self, capacity: u32, window_secs: f64) {
        self.capacity = capacity;
        self.leak_rate = capacity as f64 / window_secs;
    }
}

/// Holds per-direction leaky-bucket rate limiters (one outbound, one inbound).
#[derive(Debug)]
pub struct RateLimitConfig {
    pub outbound: LeakyBucket,
    pub inbound: LeakyBucket,
}

impl RateLimitConfig {
    pub fn new(outbound_per_second: u32, inbound_per_second: u32) -> Self {
        Self {
            outbound: LeakyBucket::new(outbound_per_second, 1.0),
            inbound: LeakyBucket::new(inbound_per_second, 1.0),
        }
    }

    /// Checks the outbound bucket and, if it has capacity, acquires a token.
    /// Returns `true` when the request is allowed.
    pub fn try_acquire_outbound(&mut self) -> bool {
        self.outbound.try_acquire()
    }

    /// Non-mutating capacity check for the outbound bucket.
    pub fn has_outbound_capacity(&self) -> bool {
        self.outbound.has_capacity()
    }

    /// Records one inbound message.
    pub fn record_inbound(&mut self) {
        self.inbound.leak();
        self.inbound.tokens += 1.0;
    }
}

impl Serialize for RateLimitConfig {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;

        let limits = json!({
            "outbound_per_second": self.outbound.capacity,
            "inbound_per_second": self.inbound.capacity,
        });

        let current_usage = json!({
            "outbound": self.outbound.current_usage(),
            "inbound": self.inbound.current_usage(),
        });

        map.serialize_entry("limits", &limits)?;
        map.serialize_entry("current_usage", &current_usage)?;
        map.end()
    }
}
