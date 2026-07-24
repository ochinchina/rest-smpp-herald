pub mod alarm;
pub mod command_ids;
pub mod command_status;
pub mod config;

pub mod esm_class;
pub mod field_codec;
pub mod id_generator;
pub mod ip_whitelist;
pub mod message;
pub mod message_handler;
pub mod message_state;
pub mod message_storage;
pub mod metrics;
pub mod npi;
pub mod registered_delivery;
pub mod sequence_number_allocator;
pub mod service_type;
pub mod smsc_client;
pub mod smsc_server;
pub mod ton;
pub mod user_authentication;

/// Parses an ISO-8601 UTC timestamp (YYYY-MM-DDTHH:MM:SSZ) into seconds since Unix epoch.
/// Returns `None` if the format is invalid.
pub fn parse_iso8601_to_epoch(s: &str) -> Option<u64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        Some(dt.timestamp() as u64)
    } else {
        None
    }
}

/// Returns the current time as seconds since Unix epoch.
pub fn now_epoch_secs() -> u64 {
    chrono::Local::now().timestamp() as u64
    /*std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs()*/
}
