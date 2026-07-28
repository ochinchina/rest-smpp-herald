pub use crate::alarm::{AlarmConfig, AlarmNotifier, create_alarm_notifier};
use crate::config::{
    AddSmscRequest, ClientConfig, CreateSmppConnectionRequest, LoadBalancingAlgorithm,
    RestAddressConfig, SmppConnectionInfo, SmscConnectionConfig, UpdateSmppConnectionRequest,
};
use crate::message::{CommandId, SMPPMessage};
use crate::sequence_number_allocator::SequenceNumberAllocator;
use crate::smsc_client::client_app_state::ClientAppState;
use crate::smsc_client::delivery_receipt_manager::DeliveryReceiptManager;
use crate::smsc_client::smsc_connection::SmscConnection;
use crate::smsc_client::smsc_connection_manager::SmscConnectionManager;
use axum::{
    Json, Router,
    extract::Path,
    extract::State as AxumState,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
};

use core::result::Result::Ok;
use log::{error, info, warn};

use serde_json::{Value, json};

use crate::id_generator::{MessageIdGenerator, create_id_generator};
use crate::message_handler::{MessageHandler, NoOpMessageHandler, create_request_handler};

use crate::metrics;

use std::clone::Clone;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::Mutex;

pub struct SmscClient {
    rest_addresses: Vec<RestAddressConfig>,
    config: ClientConfig,
    seq_allocator: SequenceNumberAllocator,
    message_handler: Arc<Box<dyn MessageHandler>>,
    delivery_receipt_manager: Arc<DeliveryReceiptManager>,
    smsc_connection_manager: Arc<SmscConnectionManager>,
}

impl SmscClient {
    /// Creates a new `SmscClient` instance.
    ///
    /// # Parameters
    /// - `config`: The `SmscClientConfig` containing connection, binding, and handler parameters.
    ///
    /// # Returns
    /// A new `SmscClient` instance ready to be started.
    pub fn new(config: ClientConfig) -> Self {
        let config_clone = config.clone();

        let message_handler: Arc<Box<dyn MessageHandler>> =
            if config_clone.request_handlers.is_some() {
                Arc::new(create_request_handler(
                    &config_clone.request_handlers.unwrap(),
                ))
            } else {
                let handler = Box::new(NoOpMessageHandler::new()) as Box<dyn MessageHandler>;
                Arc::new(handler)
            };
        let load_balancing_algorithm = LoadBalancingAlgorithm::from(
            &config_clone
                .load_balancing_algorithm
                .clone()
                .unwrap_or_else(|| "round_robin".into()),
        );
        SmscClient {
            rest_addresses: config.rest_addresses.clone(),
            config,
            seq_allocator: SequenceNumberAllocator::new(),
            message_handler,
            delivery_receipt_manager: Arc::new(DeliveryReceiptManager::new()),
            smsc_connection_manager: Arc::new(SmscConnectionManager::new(load_balancing_algorithm)),
        }
    }

    /// Starts the SMSC client by spawning the REST API server and a reconnection
    /// loop for each configured SMSC address.
    ///
    /// # Returns
    /// `std::io::Result<()>` — runs indefinitely; returns only on fatal error.
    pub async fn start(&mut self) -> std::io::Result<()> {
        let rest_addresses = self.rest_addresses.clone();
        let seq_allocator = self.seq_allocator.clone();

        let alarm_notifier: Option<Arc<dyn AlarmNotifier>> = match &self.config.alarm_config {
            Some(cfg) => Some(create_alarm_notifier(cfg).await?),
            None => None,
        };

        let id_generator = Arc::from(create_id_generator(&self.config.id_generator));

        // Spawn a reconnection loop for each configured SMSC connection
        for conn_config in &self.config.connections {
            let connection = SmscConnection::new(
                self.config.bind_type.clone(),
                self.config.session_init_timer,
                self.config.enquire_link_timer,
                self.config.inactive_timer,
                self.config.response_timer,
                conn_config.clone(),
                self.seq_allocator.clone(),
                self.message_handler.clone(),
                alarm_notifier.clone(),
                self.delivery_receipt_manager.clone(),
            );
            self.smsc_connection_manager
                .add_connection(connection.clone())
                .await;

            tokio::spawn(async move {
                connection.run().await;
            });
        }

        // Start REST server (blocks indefinitely)
        Self::start_rest(
            rest_addresses,
            self.smsc_connection_manager.clone(),
            seq_allocator,
            self.message_handler.clone(),
            alarm_notifier,
            id_generator,
            self.delivery_receipt_manager.clone(),
        )
        .await;
        Ok(())
    }

    /// Initializes the shared application state and starts the Axum REST API server
    /// with all route handlers for SMS, gateway management, and utility endpoints.
    ///
    /// # Parameters
    /// - `rest_address`: The `host:port` address to bind the HTTP listener to.
    /// - `smsc_connection_manager`: Shared pool of SMSC connection handles for round-robin message routing.
    /// - `seq_allocator`: A `SequenceNumberAllocator` for generating SMPP sequence numbers.
    /// - `smsc_message_handler`: Handler for processing inbound SMPP messages on new connections.
    /// - `inbound_storage`: Shared inbound message storage backend.
    /// - `outbound_storage`: Shared outbound message storage backend.
    async fn start_rest(
        rest_addresses: Vec<RestAddressConfig>,
        smsc_connection_manager: Arc<SmscConnectionManager>,
        seq_allocator: SequenceNumberAllocator,
        smsc_message_handler: Arc<Box<dyn MessageHandler>>,
        alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
        id_generator: Arc<dyn MessageIdGenerator>,
        delivery_receipt_manager: Arc<DeliveryReceiptManager>,
    ) {
        let state = Arc::new(ClientAppState {
            smpp_connections_store: Mutex::new(Vec::new()),
            start_time: std::time::Instant::now(),
            id_generator,
            smsc_connection_manager: smsc_connection_manager,
            connection_index: AtomicU64::new(0),
            seq_allocator,
            smsc_message_handler,
            alarm_notifier,
            delivery_receipt_manager,
        });

        let protected = Router::new()
            .route("/v1/smpp", post(smpp_api_handler))
            // SMS Messaging
            .route("/v1/gateway/status", get(get_gateway_status))
            // SMPP Connections
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
            // Live SMSC connections (dynamically add/list)
            .route(
                "/v1/gateway/smpp/live-connections",
                get(list_live_smsc_connections).post(add_live_smsc_connection),
            )
            .with_state(state.clone());

        let app = Router::new()
            .route("/metrics", get(prometheus_metrics))
            .merge(protected);

        let mut plain_listeners = Vec::new();
        let mut tls_configs: Vec<(
            tokio::net::TcpListener,
            axum_server::tls_rustls::RustlsConfig,
        )> = Vec::new();

        for addr in &rest_addresses {
            // Check if TLS is configured and certificate files exist
            let tls_config = match (&addr.cert_file, &addr.key_file) {
                (Some(cert_file), Some(key_file)) => {
                    let cert_path = std::path::Path::new(cert_file);
                    let key_path = std::path::Path::new(key_file);
                    if cert_path.exists() && key_path.exists() {
                        match axum_server::tls_rustls::RustlsConfig::from_pem_file(
                            cert_path, key_path,
                        )
                        .await
                        {
                            Ok(config) => Some(config),
                            Err(e) => {
                                error!("Failed to load TLS config for {}: {}", addr.address, e);
                                None
                            }
                        }
                    } else {
                        warn!(
                            "TLS configured for {} but cert/key files not found (cert: {}, key: {})",
                            addr.address, cert_file, key_file
                        );
                        None
                    }
                }
                _ => None,
            };

            if let Some(rustls_config) = tls_config {
                match tokio::net::TcpListener::bind(&addr.address).await {
                    Ok(listener) => {
                        info!("REST API (TLS) listening on {}", addr.address);
                        tls_configs.push((listener, rustls_config));
                    }
                    Err(e) => {
                        error!("Failed to bind REST API (TLS) to {}: {}", addr.address, e);
                    }
                }
            } else {
                match tokio::net::TcpListener::bind(&addr.address).await {
                    Ok(listener) => {
                        info!("REST API listening on {}", addr.address);
                        plain_listeners.push(listener);
                    }
                    Err(e) => {
                        error!("Failed to bind REST API to {}: {}", addr.address, e);
                    }
                }
            }
        }

        if plain_listeners.is_empty() && tls_configs.is_empty() {
            error!("No REST API listeners could be bound");
            return;
        }

        // Spawn TLS listeners
        for (listener, rustls_config) in tls_configs {
            let app_clone = app.clone();
            tokio::spawn(async move {
                let addr = listener.local_addr().unwrap();
                axum_server::from_tcp_rustls(listener.into_std().unwrap(), rustls_config)
                    .serve(app_clone.into_make_service())
                    .await
                    .unwrap_or_else(|e| {
                        error!("TLS server on {} failed: {}", addr, e);
                    });
            });
        }

        // Serve plain listeners
        if !plain_listeners.is_empty() {
            let first = plain_listeners.remove(0);
            for listener in plain_listeners {
                let app_clone = app.clone();
                tokio::spawn(async move {
                    axum::serve(listener, app_clone).await.unwrap();
                });
            }
            axum::serve(first, app).await.unwrap();
        } else {
            // All listeners are TLS — block on a pending future so we don't exit
            std::future::pending::<()>().await;
        }
    }
}

/// Creates a standardized JSON error response with success=false, an error code, and a message.
///
/// # Parameters
/// - `code`: A short error code string (e.g., "NOT_FOUND", "INVALID_REQUEST").
/// - `message`: A human-readable error description.
///
/// # Returns
/// `Value` — a `serde_json::Value` with the structure `{ success: false, error: { code, message } }`.
fn error_json(code: &str, message: &str) -> Value {
    json!({
        "success": false,
        "error": {
            "code": code,
            "message": message
        }
    })
}

// ============================================================
// REST API handler functions
// ============================================================

/// Handles raw SMPP API requests by encoding a JSON payload into an SMPP PDU
/// and forwarding it to the SMSC via round-robin connection selection,
/// then returning the decoded response.
///
/// Returns Prometheus metrics in text exposition format.
///
/// # Route
/// `GET /metrics`
pub async fn prometheus_metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics::render_metrics(),
    )
}

/// # Parameters
/// - `path`: The URL path segment used to determine the SMPP command ID.
/// - `state`: The shared application state containing the connection pool.
/// - `payload`: A JSON body representing the SMPP message fields.
///
/// # Returns
/// `Json<Value>` — the decoded SMPP response, or an error JSON object.
pub async fn smpp_api_handler(
    AxumState(state): AxumState<Arc<ClientAppState>>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    if let Ok(mut message) = SMPPMessage::from_json(&payload) {
        let command_id = CommandId::from_u32(message.get_command_id().unwrap_or(0));
        if command_id.is_none() {
            error!(
                "Unknown command_id in request: {}",
                serde_json::to_string(&payload).unwrap_or_default()
            );
            return Json(json!({"error": "Unknown command_id"}));
        }

        if !command_id.unwrap().is_client_request() {
            error!(
                "Command_id is not a client request: {}",
                serde_json::to_string(&payload).unwrap_or_default()
            );
            return Json(json!({"error": "Command_id is not a client request"}));
        }

        let sequence_number = state.seq_allocator.next();

        message.update_sequence_number(sequence_number);

        match state.smsc_connection_manager.send_message(&message).await {
            Ok(response) => Json(response),
            Err(e) => {
                error!(
                    "Failed to send message from: {} with error: {}",
                    serde_json::to_string(&payload).unwrap_or_default(),
                    e
                );
                Json(json!({"error": "Failed to send message"}))
            }
        }
    } else {
        error!(
            "Failed to create message from: {}",
            serde_json::to_string(&payload).unwrap_or_default()
        );
        Json(json!({"error": "Failed to create message"}))
    }
}

// ============================================================
// 4.1 Gateway Status
// ============================================================

/// Returns the overall gateway health status, including version, uptime,
/// SMPP connection details, and current rate limit usage.
///
/// # Route
/// `GET /v1/gateway/status`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with gateway, connection, and rate limit information.
pub async fn get_gateway_status(AxumState(state): AxumState<Arc<ClientAppState>>) -> Json<Value> {
    let connections = state.smpp_connections_store.lock().await;
    let uptime = state.start_time.elapsed().as_secs();

    Json(json!({
        "success": true,
        "data": {
            "gateway": {
                "version": "1.0.0",
                "uptime_seconds": uptime,
                "status": "healthy"
            },
            "smpp_connections": *connections,
        }
    }))
}

// ============================================================
// 4.2 SMPP Connection Management
// ============================================================

/// Lists all registered SMPP connections.
///
/// # Route
/// `GET /v1/gateway/smpp/connections`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of SMPP connections.
pub async fn list_smpp_connections(
    AxumState(state): AxumState<Arc<ClientAppState>>,
) -> Json<Value> {
    let connections = state.smpp_connections_store.lock().await;
    Json(json!({ "success": true, "data": *connections }))
}

/// Creates a new SMPP connection configuration.
///
/// # Route
/// `POST /v1/gateway/smpp/connections`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: A `CreateSmppConnectionRequest` JSON body with connection parameters.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with the new connection details.
pub async fn create_smpp_connection(
    AxumState(state): AxumState<Arc<ClientAppState>>,
    Json(req): Json<CreateSmppConnectionRequest>,
) -> impl IntoResponse {
    let connection_id = state.gen_id("smpp_conn-").await;
    let conn = SmppConnectionInfo {
        connection_id: connection_id.clone(),
        name: req.name,
        host: req.host,
        port: req.port,
        system_id: req.system_id,
        bind_type: req.bind_type.unwrap_or_else(|| "transceiver".into()),
        status: "disconnected".into(),
        reconnect_enabled: req.reconnect_enabled.unwrap_or(true),
        heartbeat_interval: req.heartbeat_interval.unwrap_or(30),
    };
    state.smpp_connections_store.lock().await.push(conn.clone());
    (
        StatusCode::CREATED,
        Json(json!({ "success": true, "data": conn })),
    )
}

/// Updates an existing SMPP connection's configuration (name, reconnect, heartbeat).
///
/// # Route
/// `PUT /v1/gateway/smpp/connections/{connection_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `connection_id`: The unique connection identifier from the URL path.
/// - `req`: An `UpdateSmppConnectionRequest` JSON body with optional fields to update.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with updated connection data, or `404 Not Found`.
pub async fn update_smpp_connection(
    AxumState(state): AxumState<Arc<ClientAppState>>,
    Path(connection_id): Path<String>,
    Json(req): Json<UpdateSmppConnectionRequest>,
) -> impl IntoResponse {
    let mut conns = state.smpp_connections_store.lock().await;
    match conns.iter_mut().find(|c| c.connection_id == connection_id) {
        Some(conn) => {
            if let Some(name) = req.name {
                conn.name = name;
            }
            if let Some(v) = req.reconnect_enabled {
                conn.reconnect_enabled = v;
            }
            if let Some(v) = req.heartbeat_interval {
                conn.heartbeat_interval = v;
            }
            (
                StatusCode::OK,
                Json(json!({ "success": true, "data": conn.clone() })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Connection not found")),
        ),
    }
}

/// Deletes an SMPP connection configuration by its connection ID.
///
/// # Route
/// `DELETE /v1/gateway/smpp/connections/{connection_id}`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `connection_id`: The unique connection identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` on success, or `404 Not Found`.
pub async fn delete_smpp_connection(
    AxumState(state): AxumState<Arc<ClientAppState>>,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    let mut conns = state.smpp_connections_store.lock().await;
    let len_before = conns.len();
    conns.retain(|c| c.connection_id != connection_id);
    if conns.len() < len_before {
        (
            StatusCode::OK,
            Json(json!({ "success": true, "data": { "deleted": true } })),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Connection not found")),
        )
    }
}

/// Triggers a rebind (reconnect) of an SMPP connection, setting its status to "reconnecting".
///
/// # Route
/// `POST /v1/gateway/smpp/connections/{connection_id}/rebind`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `connection_id`: The unique connection identifier from the URL path.
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `200 OK` with the reconnecting status, or `404 Not Found`.
pub async fn rebind_smpp_connection(
    AxumState(state): AxumState<Arc<ClientAppState>>,
    Path(connection_id): Path<String>,
) -> impl IntoResponse {
    let mut conns = state.smpp_connections_store.lock().await;
    match conns.iter_mut().find(|c| c.connection_id == connection_id) {
        Some(conn) => {
            conn.status = "reconnecting".into();
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "data": { "connection_id": connection_id, "status": "reconnecting" }
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(error_json("NOT_FOUND", "Connection not found")),
        ),
    }
}

// ============================================================
// 4.2.1 Live SMSC Connection Management
// ============================================================

/// Lists all active live SMSC connection handles in the pool, showing their addresses.
///
/// # Route
/// `GET /v1/gateway/smpp/live-connections`
///
/// # Parameters
/// - `state`: The shared application state.
///
/// # Returns
/// `Json<Value>` — a JSON response with the list of active SMSC connection addresses.
pub async fn list_live_smsc_connections(
    AxumState(state): AxumState<Arc<ClientAppState>>,
) -> Json<Value> {
    let conn_addresses = state
        .smsc_connection_manager
        .get_connection_addresses()
        .await;
    let list: Vec<Value> = conn_addresses
        .iter()
        .enumerate()
        .map(|(i, address)| {
            json!({
                "index": i,
                "address": address,
            })
        })
        .collect();
    Json(json!({ "success": true, "data": list }))
}

/// Dynamically adds a new SMSC connection at runtime. Creates the communication channels,
/// registers the connection in the pool, and spawns a background reconnection loop that
/// immediately begins attempting to connect and bind to the specified SMSC server.
///
/// # Route
/// `POST /v1/gateway/smpp/live-connections`
///
/// # Parameters
/// - `state`: The shared application state.
/// - `req`: An `AddSmscRequest` JSON body with address, system_id, password, and optional
///   SMPP bind parameters (system_type, addr_ton, addr_npi, address_range, interface_version).
///
/// # Returns
/// `(StatusCode, Json<Value>)` — `201 Created` with the new connection details.
pub async fn add_live_smsc_connection(
    AxumState(state): AxumState<Arc<ClientAppState>>,
    Json(req): Json<AddSmscRequest>,
) -> impl IntoResponse {
    let address = req.address.clone();

    // Build a SmscConnection for this connection
    let conn_config = SmscConnectionConfig {
        address: address.clone(),
        system_id: req.system_id.clone(),
        password: req.password,
        system_type: req.system_type.unwrap_or_default(),
        addr_ton: req.addr_ton.unwrap_or(0),
        addr_npi: req.addr_npi.unwrap_or(0),
        address_range: req.address_range.unwrap_or_default(),
        interface_version: req.interface_version.unwrap_or(0x34),
    };

    // Spawn the reconnection loop
    let seq_allocator = state.seq_allocator.clone();
    let handler = state.smsc_message_handler.clone();
    let alarm_notifier = state.alarm_notifier.clone();
    let delivery_receipt_manager = state.delivery_receipt_manager.clone();
    let connection = SmscConnection::new(
        req.bind_type.clone(),
        req.session_init_timer,
        req.enquire_link_timer,
        req.inactive_timer,
        req.response_timer,
        conn_config.clone(),
        seq_allocator,
        handler,
        alarm_notifier,
        delivery_receipt_manager,
    );

    tokio::spawn(async move {
        connection.run().await;
    });

    (
        StatusCode::CREATED,
        Json(json!({
            "success": true,
            "data": {
                "address": req.address,
                "system_id": req.system_id,
                "status": "connecting"
            }
        })),
    )
}
