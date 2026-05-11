use async_trait::async_trait;
use axum::Router;
use axum::middleware;
use axum::routing::{get, post, put};
use http_body_util::BodyExt;
use hyper::StatusCode;
use rest_smpp_herald::api_key_store::MemoryApiKeyStore;
use rest_smpp_herald::country_store::MemoryCountryStore;
use rest_smpp_herald::id_generator::AtomicIdGenerator;
use rest_smpp_herald::phone_number_store::MemoryPhoneNumberStore;
use rest_smpp_herald::sequence_number_allocator::SequenceNumberAllocator;
use rest_smpp_herald::smsc_client::*;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::Mutex;
use tower::ServiceExt;

/// A no-op message handler for testing.
struct NoopMessageHandler;

#[async_trait]
impl SmscMesageHandler for NoopMessageHandler {
    async fn handle_message(&self, _message: Value) -> std::io::Result<Value> {
        Ok(json!({}))
    }
}

/// Build a test router with shared AppState pre-populated as needed.
fn build_test_app(state: Arc<AppState>) -> Router {
    Router::new()
        // SMS Messaging
        .route("/v1/sms/send", post(send_sms))
        .route("/v1/sms/send/bulk", post(send_bulk_sms))
        .route(
            "/v1/sms/messages/{message_id}",
            get(get_message_status).delete(cancel_message),
        )
        .route("/v1/sms/batches/{batch_id}", get(get_batch_status))
        // Inbound
        .route("/v1/sms/inbound", get(list_inbound_messages))
        .route("/v1/sms/inbound/{message_id}", get(get_inbound_message))
        // Gateway
        .route("/v1/gateway/status", get(get_gateway_status))
        .route(
            "/v1/gateway/smpp/connections",
            get(list_smpp_connections).post(create_smpp_connection),
        )
        .route(
            "/v1/gateway/smpp/connections/{connection_id}",
            put(update_smpp_connection).delete(delete_smpp_connection),
        )
        .route(
            "/v1/gateway/smpp/connections/{connection_id}/rebind",
            post(rebind_smpp_connection),
        )
        .route(
            "/v1/gateway/smpp/live-connections",
            get(list_live_smsc_connections).post(add_live_smsc_connection),
        )
        .route(
            "/v1/gateway/sender-ids",
            get(list_sender_ids).post(create_sender_id),
        )
        .route(
            "/v1/gateway/numbers",
            get(list_phone_numbers).post(create_phone_number),
        )
        .route(
            "/v1/gateway/numbers/{number_id}",
            put(update_phone_number).delete(delete_phone_number),
        )
        .route(
            "/v1/gateway/api-keys",
            get(list_api_keys).post(create_api_key),
        )
        .route(
            "/v1/gateway/api-keys/{key_id}",
            put(update_api_key).delete(delete_api_key),
        )
        .route(
            "/v1/gateway/rate-limits",
            get(get_rate_limits).put(update_rate_limits),
        )
        .route("/v1/utils/countries", get(get_supported_countries))
        .route("/v1/utils/validate-phone", post(validate_phone))
        .route("/v1/utils/message-parts", post(calculate_message_parts))
        // Webhooks
        .route("/v1/webhooks/inbound", post(create_webhook))
        .route("/v1/webhooks/{webhook_id}/test", post(test_webhook))
        .route(
            "/v1/webhooks/{webhook_id}",
            get(get_webhook).put(update_webhook).delete(delete_webhook),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ))
        .with_state(state)
}

/// Create a default empty AppState for testing.
fn create_test_state() -> Arc<AppState> {
    let (out_sender, _out_receiver) = tokio::sync::mpsc::channel(100);
    let connections = Arc::new(Mutex::new(vec![SmscConnectionHandle {
        address: "test:2775".into(),
        out_sender,
        callbacks: Arc::new(Mutex::new(HashMap::new())),
        weight: 1,
    }]));
    Arc::new(AppState {
        inbound_storage: Arc::new(MemoryInboundMessageStorage::new(10000)),
        outbound_storage: Arc::new(MemoryOutboundMessageStorage::new()),
        smpp_connections_store: Mutex::new(Vec::new()),
        sender_ids: Mutex::new(Vec::new()),
        phone_number_store: Arc::new(MemoryPhoneNumberStore::new()),
        country_store: Arc::new(MemoryCountryStore::new()),
        api_key_store: Arc::new(MemoryApiKeyStore::new()),
        rate_limits: Arc::new(Mutex::new(RateLimitConfig::new(10, 200))),
        start_time: std::time::Instant::now(),
        id_generator: Arc::new(AtomicIdGenerator::new(1)),
        connections,
        connection_index: AtomicU64::new(0),
        seq_allocator: SequenceNumberAllocator::new(),
        smsc_message_handler: Arc::new(NoopMessageHandler),
        alarm_notifier: None,
        webhooks: Arc::new(Mutex::new(Vec::new())),
    })
}

async fn send_get(app: &Router, uri: &str) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("GET")
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

async fn send_post(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

async fn send_put(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

async fn send_delete(app: &Router, uri: &str) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

async fn send_get_auth(app: &Router, uri: &str, api_key: &str) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("GET")
        .uri(uri)
        .header("x-api-key", api_key)
        .body(axum::body::Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

async fn send_put_auth(app: &Router, uri: &str, body: Value, api_key: &str) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .header("x-api-key", api_key)
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

async fn send_delete_auth(app: &Router, uri: &str, api_key: &str) -> (StatusCode, Value) {
    let req = hyper::Request::builder()
        .method("DELETE")
        .uri(uri)
        .header("x-api-key", api_key)
        .body(axum::body::Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

// ============================================================
// GET /v1/sms/inbound
// ============================================================

#[tokio::test]
async fn test_list_inbound_messages_empty() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/inbound").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert!(body["data"].as_array().unwrap().is_empty());
    assert_eq!(body["pagination"]["total_items"], 0);
    assert_eq!(body["pagination"]["page"], 1);
    assert_eq!(body["pagination"]["per_page"], 20);
}

#[tokio::test]
async fn test_list_inbound_messages_with_data() {
    let state = create_test_state();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_001".into(),
            source: "+0987654321".into(),
            destination: "+1234567890".into(),
            message: "Hello".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-15T10:30:00Z".into(),
            read: false,
        })
        .await
        .unwrap();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_002".into(),
            source: "+1111111111".into(),
            destination: "+1234567890".into(),
            message: "World".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-15T11:00:00Z".into(),
            read: true,
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/inbound").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["message_id"], "inb_001");
    assert_eq!(data[1]["message_id"], "inb_002");
    assert_eq!(body["pagination"]["total_items"], 2);
}

#[tokio::test]
async fn test_list_inbound_messages_filter_by_source() {
    let state = create_test_state();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_001".into(),
            source: "+0987654321".into(),
            destination: "+1234567890".into(),
            message: "Hello".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-15T10:30:00Z".into(),
            read: false,
        })
        .await
        .unwrap();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_002".into(),
            source: "+1111111111".into(),
            destination: "+1234567890".into(),
            message: "World".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-15T11:00:00Z".into(),
            read: true,
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/inbound?source=%2B0987654321").await;

    assert_eq!(status, StatusCode::OK);
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["message_id"], "inb_001");
}

#[tokio::test]
async fn test_list_inbound_messages_filter_by_destination() {
    let state = create_test_state();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_001".into(),
            source: "+0987654321".into(),
            destination: "+1234567890".into(),
            message: "Hello".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-15T10:30:00Z".into(),
            read: false,
        })
        .await
        .unwrap();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_002".into(),
            source: "+1111111111".into(),
            destination: "+9999999999".into(),
            message: "World".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-15T11:00:00Z".into(),
            read: true,
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/inbound?destination=%2B9999999999").await;

    assert_eq!(status, StatusCode::OK);
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["message_id"], "inb_002");
}

#[tokio::test]
async fn test_list_inbound_messages_pagination() {
    let state = create_test_state();
    for i in 0..25 {
        state
            .inbound_storage
            .save(InboundMessage {
                message_id: format!("inb_{:03}", i),
                source: "+0987654321".into(),
                destination: "+1234567890".into(),
                message: format!("Message {}", i),
                message_binary: None,
                data_coding: 0,
                received_at: "2024-01-15T10:30:00Z".into(),
                read: false,
            })
            .await
            .unwrap();
    }
    let app = build_test_app(state);

    // Page 1
    let (_, body) = send_get(&app, "/v1/sms/inbound?page=1&per_page=10").await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 10);
    assert_eq!(body["pagination"]["total_items"], 25);
    assert_eq!(body["pagination"]["total_pages"], 3);
    assert_eq!(body["pagination"]["page"], 1);

    // Page 3 (last page, 5 items)
    let (_, body) = send_get(&app, "/v1/sms/inbound?page=3&per_page=10").await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 5);

    // per_page capped at 100
    let (_, body) = send_get(&app, "/v1/sms/inbound?per_page=200").await;
    assert_eq!(body["pagination"]["per_page"], 100);
}

// ============================================================
// GET /v1/gateway/status
// ============================================================

#[tokio::test]
async fn test_get_gateway_status_empty() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/status").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let data = &body["data"];
    assert_eq!(data["gateway"]["version"], "1.0.0");
    assert_eq!(data["gateway"]["status"], "healthy");
    assert!(data["gateway"]["uptime_seconds"].as_u64().is_some());
    assert!(data["smpp_connections"].as_array().unwrap().is_empty());
    assert_eq!(data["rate_limits"]["outbound_per_second"], 10);
    assert_eq!(data["rate_limits"]["outbound_remaining"], 10);
    assert_eq!(data["rate_limits"]["inbound_per_second"], 200);
    assert_eq!(data["rate_limits"]["inbound_remaining"], 200);
}

#[tokio::test]
async fn test_get_gateway_status_with_connections() {
    let state = create_test_state();
    {
        let mut conns = state.smpp_connections_store.lock().await;
        conns.push(SmppConnectionInfo {
            connection_id: "smpp_conn_1".into(),
            name: "Primary SMSC".into(),
            host: "smsc.carrier.com".into(),
            port: 2775,
            system_id: "gateway_client".into(),
            bind_type: "transceiver".into(),
            status: "bound".into(),
            reconnect_enabled: true,
            heartbeat_interval: 30,
        });
    }
    let app = build_test_app(state);

    let (_, body) = send_get(&app, "/v1/gateway/status").await;
    let conns = body["data"]["smpp_connections"].as_array().unwrap();
    assert_eq!(conns.len(), 1);
    assert_eq!(conns[0]["connection_id"], "smpp_conn_1");
    assert_eq!(conns[0]["status"], "bound");
}

#[tokio::test]
async fn test_get_gateway_status_rate_limit_remaining() {
    let state = create_test_state();
    {
        let mut limits = state.rate_limits.lock().await;
        // Consume 3 tokens via the leaky bucket
        for _ in 0..3 {
            limits.try_acquire_outbound();
        }
    }
    let app = build_test_app(state);

    let (_, body) = send_get(&app, "/v1/gateway/status").await;
    // 10 - 3 = 7
    assert_eq!(body["data"]["rate_limits"]["outbound_remaining"], 7);
}

// ============================================================
// GET /v1/gateway/smpp/connections
// ============================================================

#[tokio::test]
async fn test_list_smpp_connections_empty() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/smpp/connections").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert!(body["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_smpp_connections_with_data() {
    let state = create_test_state();
    {
        let mut conns = state.smpp_connections_store.lock().await;
        conns.push(SmppConnectionInfo {
            connection_id: "smpp_conn_1".into(),
            name: "Primary SMSC".into(),
            host: "smsc.carrier.com".into(),
            port: 2775,
            system_id: "gateway_client".into(),
            bind_type: "transceiver".into(),
            status: "bound".into(),
            reconnect_enabled: true,
            heartbeat_interval: 30,
        });
        conns.push(SmppConnectionInfo {
            connection_id: "smpp_conn_2".into(),
            name: "Secondary SMSC".into(),
            host: "smsc2.carrier.com".into(),
            port: 2776,
            system_id: "gateway_client_2".into(),
            bind_type: "receiver".into(),
            status: "disconnected".into(),
            reconnect_enabled: false,
            heartbeat_interval: 60,
        });
    }
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/smpp/connections").await;

    assert_eq!(status, StatusCode::OK);
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["name"], "Primary SMSC");
    assert_eq!(data[0]["port"], 2775);
    assert_eq!(data[0]["bind_type"], "transceiver");
    assert_eq!(data[1]["name"], "Secondary SMSC");
    assert_eq!(data[1]["reconnect_enabled"], false);
}

// ============================================================
// GET /v1/gateway/sender-ids
// ============================================================

#[tokio::test]
async fn test_list_sender_ids_empty() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/sender-ids").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert!(body["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_sender_ids_with_data() {
    let state = create_test_state();
    {
        let mut ids = state.sender_ids.lock().await;
        ids.push(SenderIdInfo {
            sender_id: "+1234567890".into(),
            sender_type: "msisdn".into(),
            status: "active".into(),
            verified: true,
            created_at: "2024-01-01T00:00:00Z".into(),
        });
        ids.push(SenderIdInfo {
            sender_id: "MyCompany".into(),
            sender_type: "alphanumeric".into(),
            status: "active".into(),
            verified: false,
            created_at: "2024-01-10T00:00:00Z".into(),
        });
    }
    let app = build_test_app(state);

    let (_, body) = send_get(&app, "/v1/gateway/sender-ids").await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["sender_id"], "+1234567890");
    assert_eq!(data[0]["type"], "msisdn");
    assert_eq!(data[0]["verified"], true);
    assert_eq!(data[1]["sender_id"], "MyCompany");
    assert_eq!(data[1]["type"], "alphanumeric");
    assert_eq!(data[1]["verified"], false);
}

// ============================================================
// GET /v1/gateway/numbers
// ============================================================

#[tokio::test]
async fn test_list_phone_numbers_empty() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/numbers").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert!(body["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_phone_numbers_with_data() {
    let state = create_test_state();
    state
        .phone_number_store
        .create(PhoneNumberInfo {
            number_id: "num_123".into(),
            phone_number: "+1234567890".into(),
            capabilities: vec!["sms_inbound".into(), "sms_outbound".into()],
            status: "active".into(),
            created_at: "2024-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (_, body) = send_get(&app, "/v1/gateway/numbers").await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["number_id"], "num_123");
    assert_eq!(data[0]["phone_number"], "+1234567890");
    let caps = data[0]["capabilities"].as_array().unwrap();
    assert_eq!(caps.len(), 2);
    assert_eq!(caps[0], "sms_inbound");
    assert_eq!(caps[1], "sms_outbound");
}

#[tokio::test]
async fn test_list_phone_numbers_null_webhook() {
    let state = create_test_state();
    state
        .phone_number_store
        .create(PhoneNumberInfo {
            number_id: "num_456".into(),
            phone_number: "+9876543210".into(),
            capabilities: vec!["sms_outbound".into()],
            status: "active".into(),
            created_at: "2024-01-02T00:00:00Z".into(),
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (_, body) = send_get(&app, "/v1/gateway/numbers").await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data[0]["phone_number"], "+9876543210");
}

// ============================================================
// GET /v1/gateway/api-keys
// ============================================================

#[tokio::test]
async fn test_list_api_keys_empty() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/api-keys").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert!(body["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_list_api_keys_with_data() {
    let state = create_test_state();
    state
        .api_key_store
        .create(ApiKeyInfo {
            key_id: "key_abc123".into(),
            name: "Production Key".into(),
            key_prefix: "sgw_live_***".into(),
            api_key: "sgw_live_test_key_abc123".into(),
            permissions: vec!["sms:send".into(), "sms:receive".into(), "sms:status".into()],
            rate_limit: 100,
            status: "active".into(),
            created_at: "2024-01-01T00:00:00Z".into(),
            last_used_at: Some("2024-01-15T10:30:00Z".into()),
            expires_at: None,
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (_, body) = send_get_auth(&app, "/v1/gateway/api-keys", "sgw_live_test_key_abc123").await;
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["key_id"], "key_abc123");
    assert_eq!(data[0]["name"], "Production Key");
    assert_eq!(data[0]["key_prefix"], "sgw_live_***");
    assert_eq!(data[0]["rate_limit"], 100);
    assert_eq!(data[0]["status"], "active");
    let perms = data[0]["permissions"].as_array().unwrap();
    assert_eq!(perms.len(), 3);
    assert!(data[0]["expires_at"].is_null());
    // last_used_at is updated by the auth middleware on each request
    assert!(data[0]["last_used_at"].is_string());
}

// ============================================================
// GET /v1/gateway/rate-limits
// ============================================================

#[tokio::test]
async fn test_get_rate_limits_defaults() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/rate-limits").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let data = &body["data"];
    assert_eq!(data["limits"]["outbound_per_second"], 10);
    assert_eq!(data["limits"]["inbound_per_second"], 200);
    assert_eq!(data["current_usage"]["outbound"], 0);
    assert_eq!(data["current_usage"]["inbound"], 0);
}

#[tokio::test]
async fn test_get_rate_limits_with_usage() {
    let state = create_test_state();
    {
        let mut limits = state.rate_limits.lock().await;
        // Consume tokens via the leaky bucket
        for _ in 0..5 {
            limits.outbound.try_acquire();
        }
    }
    let app = build_test_app(state);

    let (_, body) = send_get(&app, "/v1/gateway/rate-limits").await;
    assert_eq!(body["data"]["current_usage"]["outbound"], 5);
}

// ============================================================
// GET /v1/utils/countries
// ============================================================

#[tokio::test]
async fn test_get_supported_countries() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/utils/countries").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let data = body["data"].as_array().unwrap();
    // Empty memory store returns no countries
    assert_eq!(data.len(), 0);
}

// ============================================================
// Response format consistency tests
// ============================================================

#[tokio::test]
async fn test_all_list_endpoints_return_success_true() {
    let state = create_test_state();
    let app = build_test_app(state);

    let endpoints = vec![
        "/v1/sms/inbound",
        "/v1/gateway/status",
        "/v1/gateway/smpp/connections",
        "/v1/gateway/sender-ids",
        "/v1/gateway/numbers",
        "/v1/gateway/api-keys",
        "/v1/gateway/rate-limits",
        "/v1/utils/countries",
    ];

    for endpoint in endpoints {
        let (status, body) = send_get(&app, endpoint).await;
        assert_eq!(status, StatusCode::OK, "Failed for endpoint: {}", endpoint);
        assert_eq!(
            body["success"], true,
            "success not true for endpoint: {}",
            endpoint
        );
        assert!(
            body.get("data").is_some(),
            "Missing 'data' field for endpoint: {}",
            endpoint
        );
    }
}

#[tokio::test]
async fn test_paginated_endpoints_have_pagination_field() {
    let state = create_test_state();
    let app = build_test_app(state);

    let paginated_endpoints = vec!["/v1/sms/inbound"];

    for endpoint in paginated_endpoints {
        let (_, body) = send_get(&app, endpoint).await;
        assert!(
            body.get("pagination").is_some(),
            "Missing 'pagination' field for endpoint: {}",
            endpoint
        );
        let pagination = &body["pagination"];
        assert!(
            pagination.get("page").is_some(),
            "Missing page for {}",
            endpoint
        );
        assert!(
            pagination.get("per_page").is_some(),
            "Missing per_page for {}",
            endpoint
        );
        assert!(
            pagination.get("total_pages").is_some(),
            "Missing total_pages for {}",
            endpoint
        );
        assert!(
            pagination.get("total_items").is_some(),
            "Missing total_items for {}",
            endpoint
        );
        assert!(
            pagination.get("links").is_some(),
            "Missing links for {}",
            endpoint
        );
    }
}

// ============================================================
// POST /v1/sms/send
// ============================================================

#[tokio::test]
async fn test_send_sms_success() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": "Hello, world!"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["success"], true);
    assert!(
        body["data"]["message_id"]
            .as_str()
            .unwrap()
            .starts_with("msg_")
    );
    assert_eq!(body["data"]["status"], "queued");
    assert_eq!(body["data"]["parts"], 1);
}

#[tokio::test]
async fn test_send_sms_invalid_destination() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "invalid",
            "message": "Hello"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["success"], false);
    assert_eq!(body["error"]["code"], "INVALID_PHONE_NUMBER");
}

#[tokio::test]
async fn test_send_sms_message_too_long() {
    let state = create_test_state();
    let app = build_test_app(state);

    let long_msg = "x".repeat(1601);
    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": long_msg
        }),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "MESSAGE_TOO_LONG");
}

#[tokio::test]
async fn test_send_sms_multipart() {
    let state = create_test_state();
    let app = build_test_app(state);

    let long_msg = "x".repeat(300);
    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": long_msg,
            "encoding": "GSM7"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert!(body["data"]["parts"].as_u64().unwrap() > 1);
}

// ============================================================
// POST /v1/sms/send/bulk
// ============================================================

#[tokio::test]
async fn test_send_bulk_sms() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send/bulk",
        json!({
            "messages": [
                { "source": "+1234567890", "destination": "+0987654321", "message": "Hello 1" },
                { "source": "+1234567890", "destination": "+1111111111", "message": "Hello 2" }
            ],
            "batch_name": "Test batch"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["success"], true);
    assert!(
        body["data"]["batch_id"]
            .as_str()
            .unwrap()
            .starts_with("batch_")
    );
    assert_eq!(body["data"]["total_messages"], 2);
    assert_eq!(body["data"]["queued"], 2);
    assert_eq!(body["data"]["failed"], 0);
}

#[tokio::test]
async fn test_send_bulk_sms_with_invalid_numbers() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send/bulk",
        json!({
            "messages": [
                { "source": "+1234567890", "destination": "+0987654321", "message": "Valid" },
                { "source": "+1234567890", "destination": "invalid", "message": "Invalid" }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["queued"], 1);
    assert_eq!(body["data"]["failed"], 1);
}

#[tokio::test]
async fn test_send_sms_source_not_registered() {
    let state = create_test_state();
    // Register a phone number with sms_outbound capability
    state
        .phone_number_store
        .create(PhoneNumberInfo {
            number_id: "num_1".into(),
            phone_number: "+1111111111".into(),
            capabilities: vec!["sms_outbound".into()],
            status: "active".into(),
            created_at: "2025-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+9999999999",
            "destination": "+0987654321",
            "message": "Hello"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], "SOURCE_NOT_REGISTERED");
}

#[tokio::test]
async fn test_send_sms_source_registered() {
    let state = create_test_state();
    state
        .phone_number_store
        .create(PhoneNumberInfo {
            number_id: "num_1".into(),
            phone_number: "+1234567890".into(),
            capabilities: vec!["sms_outbound".into()],
            status: "active".into(),
            created_at: "2025-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": "Hello"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_send_sms_source_inactive_rejected() {
    let state = create_test_state();
    state
        .phone_number_store
        .create(PhoneNumberInfo {
            number_id: "num_1".into(),
            phone_number: "+1234567890".into(),
            capabilities: vec!["sms_outbound".into()],
            status: "inactive".into(),
            created_at: "2025-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": "Hello"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], "SOURCE_NOT_REGISTERED");
}

#[tokio::test]
async fn test_send_bulk_sms_source_not_registered() {
    let state = create_test_state();
    state
        .phone_number_store
        .create(PhoneNumberInfo {
            number_id: "num_1".into(),
            phone_number: "+1111111111".into(),
            capabilities: vec!["sms_outbound".into()],
            status: "active".into(),
            created_at: "2025-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/sms/send/bulk",
        json!({
            "messages": [
                { "source": "+1111111111", "destination": "+0987654321", "message": "Valid" },
                { "source": "+9999999999", "destination": "+0987654322", "message": "Invalid source" }
            ]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["queued"], 1);
    assert_eq!(body["data"]["failed"], 1);
}

// ============================================================
// GET /v1/sms/messages/{message_id} & DELETE
// ============================================================

#[tokio::test]
async fn test_get_message_status() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    // Send a message first
    let (_, send_body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": "Test"
        }),
    )
    .await;
    let msg_id = send_body["data"]["message_id"].as_str().unwrap();

    let (status, body) = send_get(&app, &format!("/v1/sms/messages/{}", msg_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["message_id"], msg_id);
    assert_eq!(body["data"]["status"], "queued");
}

#[tokio::test]
async fn test_get_message_status_not_found() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/messages/msg_nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

#[tokio::test]
async fn test_cancel_message() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, send_body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": "Cancel me"
        }),
    )
    .await;
    let msg_id = send_body["data"]["message_id"].as_str().unwrap();

    let (status, body) = send_delete(&app, &format!("/v1/sms/messages/{}", msg_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], "cancelled");

    // Verify status changed
    let (_, body) = send_get(&app, &format!("/v1/sms/messages/{}", msg_id)).await;
    assert_eq!(body["data"]["status"], "cancelled");
}

// ============================================================
// GET /v1/sms/batches/{batch_id}
// ============================================================

#[tokio::test]
async fn test_get_batch_status() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, bulk_body) = send_post(
        &app,
        "/v1/sms/send/bulk",
        json!({
            "messages": [
                { "source": "+1234567890", "destination": "+0987654321", "message": "Hello" }
            ],
            "batch_name": "My batch"
        }),
    )
    .await;
    let batch_id = bulk_body["data"]["batch_id"].as_str().unwrap();

    let (status, body) = send_get(&app, &format!("/v1/sms/batches/{}", batch_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["batch_id"], batch_id);
    assert_eq!(body["data"]["name"], "My batch");
    assert_eq!(body["data"]["total_messages"], 1);
}

#[tokio::test]
async fn test_get_batch_status_not_found() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, _) = send_get(&app, "/v1/sms/batches/batch_nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ============================================================
// GET /v1/sms/inbound/{message_id}
// ============================================================

#[tokio::test]
async fn test_get_inbound_message() {
    let state = create_test_state();
    state
        .inbound_storage
        .save(InboundMessage {
            message_id: "inb_001".into(),
            source: "+111".into(),
            destination: "+222".into(),
            message: "Hello".into(),
            message_binary: None,
            data_coding: 0,
            received_at: "2024-01-01T00:00:00Z".into(),
            read: false,
        })
        .await
        .unwrap();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/inbound/inb_001").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["message_id"], "inb_001");
}

#[tokio::test]
async fn test_get_inbound_message_not_found() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/sms/inbound/inb_nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

// ============================================================
// SMPP Connection CRUD
// ============================================================

#[tokio::test]
async fn test_create_smpp_connection() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/gateway/smpp/connections",
        json!({
            "name": "Test SMSC",
            "host": "smsc.test.com",
            "port": 2775,
            "system_id": "testuser",
            "password": "testpass"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["name"], "Test SMSC");
    assert_eq!(body["data"]["host"], "smsc.test.com");
    assert_eq!(body["data"]["port"], 2775);
    assert_eq!(body["data"]["status"], "disconnected");
    assert_eq!(body["data"]["bind_type"], "transceiver");
}

#[tokio::test]
async fn test_update_smpp_connection() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/smpp/connections",
        json!({
            "name": "Original", "host": "h", "port": 2775, "system_id": "u", "password": "p"
        }),
    )
    .await;
    let conn_id = create_body["data"]["connection_id"].as_str().unwrap();

    let (status, body) = send_put(
        &app,
        &format!("/v1/gateway/smpp/connections/{}", conn_id),
        json!({
            "name": "Updated"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["name"], "Updated");
}

#[tokio::test]
async fn test_delete_smpp_connection() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/smpp/connections",
        json!({
            "name": "Del", "host": "h", "port": 2775, "system_id": "u", "password": "p"
        }),
    )
    .await;
    let conn_id = create_body["data"]["connection_id"].as_str().unwrap();

    let (status, body) =
        send_delete(&app, &format!("/v1/gateway/smpp/connections/{}", conn_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["deleted"], true);

    // Verify it's gone
    let (_, list_body) = send_get(&app, "/v1/gateway/smpp/connections").await;
    assert!(list_body["data"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_rebind_smpp_connection() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/smpp/connections",
        json!({
            "name": "Rebind", "host": "h", "port": 2775, "system_id": "u", "password": "p"
        }),
    )
    .await;
    let conn_id = create_body["data"]["connection_id"].as_str().unwrap();

    let (status, body) = send_post(
        &app,
        &format!("/v1/gateway/smpp/connections/{}/rebind", conn_id),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["status"], "reconnecting");
}

// ============================================================
// Sender ID CRUD
// ============================================================

#[tokio::test]
async fn test_create_sender_id() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/gateway/sender-ids",
        json!({
            "sender_id": "MyCompany",
            "type": "alphanumeric"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["sender_id"], "MyCompany");
    assert_eq!(body["data"]["type"], "alphanumeric");
    assert_eq!(body["data"]["status"], "active");
    assert_eq!(body["data"]["verified"], false);
}

// ============================================================
// Phone Number CRUD
// ============================================================

#[tokio::test]
async fn test_create_phone_number() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/gateway/numbers",
        json!({
            "phone_number": "+1234567890",
            "capabilities": ["sms_inbound", "sms_outbound"]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["phone_number"], "+1234567890");
    assert_eq!(body["data"]["status"], "active");
}

#[tokio::test]
async fn test_update_phone_number() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/numbers",
        json!({
            "phone_number": "+1234567890"
        }),
    )
    .await;
    let num_id = create_body["data"]["number_id"].as_str().unwrap();

    let (status, body) = send_put(
        &app,
        &format!("/v1/gateway/numbers/{}", num_id),
        json!({
            "capabilities": ["sms_outbound"]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let caps = body["data"]["capabilities"].as_array().unwrap();
    assert_eq!(caps.len(), 1);
    assert_eq!(caps[0], "sms_outbound");
}

#[tokio::test]
async fn test_delete_phone_number() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/numbers",
        json!({
            "phone_number": "+1234567890"
        }),
    )
    .await;
    let num_id = create_body["data"]["number_id"].as_str().unwrap();

    let (status, body) = send_delete(&app, &format!("/v1/gateway/numbers/{}", num_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["deleted"], true);
}

// ============================================================
// API Key CRUD
// ============================================================

#[tokio::test]
async fn test_create_api_key() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/gateway/api-keys",
        json!({
            "name": "Test Key",
            "permissions": ["sms:send"],
            "rate_limit": 50
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["name"], "Test Key");
    assert!(
        body["data"]["api_key"]
            .as_str()
            .unwrap()
            .starts_with("sgw_live_")
    );
    assert!(body["data"]["key_id"].as_str().unwrap().starts_with("key_"));
}

#[tokio::test]
async fn test_update_api_key() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/api-keys",
        json!({
            "name": "Original"
        }),
    )
    .await;
    let key_id = create_body["data"]["key_id"].as_str().unwrap();
    let api_key = create_body["data"]["api_key"].as_str().unwrap();

    let (status, body) = send_put_auth(
        &app,
        &format!("/v1/gateway/api-keys/{}", key_id),
        json!({
            "name": "Updated Key"
        }),
        api_key,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["name"], "Updated Key");
}

#[tokio::test]
async fn test_delete_api_key() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/gateway/api-keys",
        json!({
            "name": "Delete me"
        }),
    )
    .await;
    let key_id = create_body["data"]["key_id"].as_str().unwrap();
    let api_key = create_body["data"]["api_key"].as_str().unwrap();

    let (status, body) =
        send_delete_auth(&app, &format!("/v1/gateway/api-keys/{}", key_id), api_key).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["deleted"], true);
}

// ============================================================
// Rate Limits PUT
// ============================================================

#[tokio::test]
async fn test_update_rate_limits() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_put(
        &app,
        "/v1/gateway/rate-limits",
        json!({
            "outbound_per_second": 20
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["limits"]["outbound_per_second"], 20);
    // Inbound unchanged
    assert_eq!(body["data"]["limits"]["inbound_per_second"], 200);
}

// ============================================================
// Webhook CRUD
// ============================================================

#[tokio::test]
async fn test_create_webhook() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/webhooks/inbound",
        json!({
            "url": "https://example.com/webhook",
            "events": ["inbound_sms"],
            "enabled": true
        }),
    )
    .await;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["url"], "https://example.com/webhook");
    assert_eq!(body["data"]["enabled"], true);
}

#[tokio::test]
async fn test_get_webhook() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/webhooks/inbound",
        json!({
            "url": "https://example.com/wh"
        }),
    )
    .await;
    let wh_id = create_body["data"]["webhook_id"].as_str().unwrap();

    let (status, body) = send_get(&app, &format!("/v1/webhooks/{}", wh_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["webhook_id"], wh_id);
}

#[tokio::test]
async fn test_update_webhook() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/webhooks/inbound",
        json!({
            "url": "https://example.com/old"
        }),
    )
    .await;
    let wh_id = create_body["data"]["webhook_id"].as_str().unwrap();

    let (status, body) = send_put(
        &app,
        &format!("/v1/webhooks/{}", wh_id),
        json!({
            "url": "https://example.com/new",
            "enabled": false
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["url"], "https://example.com/new");
    assert_eq!(body["data"]["enabled"], false);
}

#[tokio::test]
async fn test_delete_webhook() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/webhooks/inbound",
        json!({
            "url": "https://example.com/del"
        }),
    )
    .await;
    let wh_id = create_body["data"]["webhook_id"].as_str().unwrap();

    let (status, body) = send_delete(&app, &format!("/v1/webhooks/{}", wh_id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["deleted"], true);
}

#[tokio::test]
async fn test_test_webhook() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (_, create_body) = send_post(
        &app,
        "/v1/webhooks/inbound",
        json!({
            "url": "https://example.com/test"
        }),
    )
    .await;
    let wh_id = create_body["data"]["webhook_id"].as_str().unwrap();

    let (status, body) = send_post(&app, &format!("/v1/webhooks/{}/test", wh_id), json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["verified"], true);
    assert_eq!(body["data"]["response_status"], 200);
}

// ============================================================
// Utility APIs
// ============================================================

#[tokio::test]
async fn test_validate_phone_valid() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/utils/validate-phone",
        json!({
            "phone_number": "+1234567890"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["valid"], true);
    assert_eq!(body["data"]["formatted"], "+1234567890");
    assert_eq!(body["data"]["country_code"], "US");
}

#[tokio::test]
async fn test_validate_phone_invalid() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/utils/validate-phone",
        json!({
            "phone_number": "123"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["valid"], false);
    assert!(body["data"]["formatted"].is_null());
}

#[tokio::test]
async fn test_calculate_message_parts_single() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/utils/message-parts",
        json!({
            "message": "Short message",
            "encoding": "GSM7"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["parts"], 1);
    assert_eq!(body["data"]["encoding"], "GSM7");
    assert_eq!(body["data"]["max_length_per_part"], 160);
}

#[tokio::test]
async fn test_calculate_message_parts_multi() {
    let state = create_test_state();
    let app = build_test_app(state);

    let long_msg = "x".repeat(300);
    let (status, body) = send_post(
        &app,
        "/v1/utils/message-parts",
        json!({
            "message": long_msg,
            "encoding": "GSM7"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["parts"], 2); // 300 chars Ã¢â€ â€™ ceil(300/153) = 2
    assert_eq!(body["data"]["max_length_per_part"], 153);
}

#[tokio::test]
async fn test_calculate_message_parts_ucs2() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_post(
        &app,
        "/v1/utils/message-parts",
        json!({
            "message": "Short",
            "encoding": "UCS2"
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["parts"], 1);
    assert_eq!(body["data"]["max_length_per_part"], 70);
}

// ============================================================
// Not Found for CRUD on missing resources
// ============================================================

#[tokio::test]
async fn test_update_nonexistent_smpp_connection() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_put(
        &app,
        "/v1/gateway/smpp/connections/smpp_conn_fake",
        json!({
            "name": "No such"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

#[tokio::test]
async fn test_delete_nonexistent_phone_number() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_delete(&app, "/v1/gateway/numbers/num_fake").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

#[tokio::test]
async fn test_delete_nonexistent_webhook() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_delete(&app, "/v1/webhooks/wh_fake").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

#[tokio::test]
async fn test_get_webhook_not_found() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/webhooks/wh_nonexist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
}

#[tokio::test]
async fn test_cancel_already_sent_message() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    // Send, then manually change status
    let (_, send_body) = send_post(
        &app,
        "/v1/sms/send",
        json!({
            "source": "+1234567890",
            "destination": "+0987654321",
            "message": "Hello"
        }),
    )
    .await;
    let msg_id = send_body["data"]["message_id"]
        .as_str()
        .unwrap()
        .to_string();
    {
        state
            .outbound_storage
            .update_status(&msg_id, "sent")
            .await
            .unwrap();
    }

    let (status, body) = send_delete(&app, &format!("/v1/sms/messages/{}", msg_id)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "CANNOT_CANCEL");
}

// ============================================================
// Live SMSC Connection Management
// ============================================================

#[tokio::test]
async fn test_list_live_smsc_connections() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/smpp/live-connections").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    // The test state starts with one connection handle ("test:2775")
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["address"], "test:2775");
}

#[tokio::test]
async fn test_add_live_smsc_connection() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (status, body) = send_post(
        &app,
        "/v1/gateway/smpp/live-connections",
        json!({
            "address": "10.0.0.1:2775",
            "system_id": "test_user",
            "password": "secret"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["address"], "10.0.0.1:2775");
    assert_eq!(body["data"]["system_id"], "test_user");
    assert_eq!(body["data"]["status"], "connecting");

    // Verify the connection pool now has 2 entries
    let conns = state.connections.lock().await;
    assert_eq!(conns.len(), 2);
    assert_eq!(conns[1].address, "10.0.0.1:2775");
}

#[tokio::test]
async fn test_add_live_smsc_connection_with_optional_fields() {
    let state = create_test_state();
    let app = build_test_app(state.clone());

    let (status, body) = send_post(
        &app,
        "/v1/gateway/smpp/live-connections",
        json!({
            "address": "10.0.0.2:2776",
            "system_id": "user2",
            "password": "pass2",
            "system_type": "VMA",
            "addr_ton": 1,
            "addr_npi": 1,
            "address_range": "44",
            "interface_version": 52
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["address"], "10.0.0.2:2776");
    assert_eq!(body["data"]["system_id"], "user2");
}
