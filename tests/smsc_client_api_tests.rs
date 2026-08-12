use axum::Router;
use axum::routing::{get, post, put};
use http_body_util::BodyExt;
use hyper::StatusCode;
use rest_smpp_herald::config::SmppConnectionInfo;
use rest_smpp_herald::id_generator::AtomicIdGenerator;
use rest_smpp_herald::message_handler::NoOpMessageHandler;
use rest_smpp_herald::sequence_number_allocator::SequenceNumberAllocator;
use rest_smpp_herald::smsc_client::client_app_state::ClientAppState;
use rest_smpp_herald::smsc_client::delivery_receipt_manager::DeliveryReceiptManager;
use rest_smpp_herald::smsc_client::smsc_client::{
    add_live_smsc_connection, create_smpp_connection, delete_smpp_connection, get_gateway_status,
    list_live_smsc_connections, list_smpp_connections, rebind_smpp_connection,
    update_smpp_connection,
};
use rest_smpp_herald::smsc_client::smsc_connection_manager::SmscConnectionManager;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn build_test_app(state: Arc<ClientAppState>) -> Router {
    Router::new()
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
        .with_state(state)
}

fn create_test_state() -> Arc<ClientAppState> {
    let smsc_connection_manager =
        SmscConnectionManager::new(rest_smpp_herald::config::LoadBalancingAlgorithm::RoundRobin);

    Arc::new(ClientAppState {
        smpp_connections_store: Mutex::new(Vec::new()),
        start_time: std::time::Instant::now(),
        id_generator: Arc::new(AtomicIdGenerator::new(1)),
        smsc_connection_manager: Arc::new(smsc_connection_manager),
        connection_index: AtomicU64::new(0),
        seq_allocator: SequenceNumberAllocator::new(),
        smsc_message_handler: Arc::new(Box::new(NoOpMessageHandler)),
        alarm_notifier: None,
        delivery_receipt_manager: Arc::new(DeliveryReceiptManager::new()),
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
    let data = body["data"].as_array().unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["name"], "Primary SMSC");
    assert_eq!(data[0]["port"], 2775);
    assert_eq!(data[0]["bind_type"], "transceiver");
    assert_eq!(data[1]["name"], "Secondary SMSC");
    assert_eq!(data[1]["reconnect_enabled"], false);
}

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
async fn test_list_live_smsc_connections() {
    let state = create_test_state();
    let app = build_test_app(state);

    let (status, body) = send_get(&app, "/v1/gateway/smpp/live-connections").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 0);
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
