use lazy_static::lazy_static;
use prometheus::{CounterVec, Encoder, IntCounter, IntGauge, Opts, Registry, TextEncoder};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    /// Counts REST API submit_sm requests, labeled by response status.
    pub static ref REST_SUBMIT_MESSAGES: CounterVec = CounterVec::new(
        Opts::new("smpp_rest_submit_messages_total", "Total submit messages received from REST API"),
        &["status"],
    )
    .unwrap();

    /// Tracks the number of active SMSC connections.
    pub static ref SMSC_ACTIVE_CONNECTIONS: IntGauge = IntGauge::new(
        "smpp_smsc_active_connections",
        "Number of active connections to the SMSC server",
    )
    .unwrap();

    /// Counts inbound messages received from the SMSC server.
    pub static ref SMSC_INBOUND_MESSAGES: IntCounter = IntCounter::new(
        "smpp_smsc_inbound_messages_total",
        "Total inbound messages received from the SMSC server",
    )
    .unwrap();
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(REST_SUBMIT_MESSAGES.clone()))
        .unwrap();
    REGISTRY
        .register(Box::new(SMSC_ACTIVE_CONNECTIONS.clone()))
        .unwrap();
    REGISTRY
        .register(Box::new(SMSC_INBOUND_MESSAGES.clone()))
        .unwrap();
}

pub fn render_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
