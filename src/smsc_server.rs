use crate::command_status::{ESME_RBINDFAIL, ESME_ROK, ESME_RUNKNOWNERR};
use crate::ip_whitelist::{IpWhitelist, IpWhitelistConfig, create_ip_whitelist};
use crate::message::{
    BIND_RECEIVER, BIND_TRANSCEIVER, BIND_TRANSMITTER, ENQUIRE_LINK, SUBMIT_SM, SmppMessageBuffer,
    UNBIND, decode_message, encode_message, format_smpp_value,
};
pub use crate::message_handler::{HttpMessageHandler, LoadBalancingAlgorithm, MessageHandler};
use crate::sequence_number_allocator::SequenceNumberAllocator;
pub use crate::user_authentication::{
    HttpUserAuthentication, SimpleUserAuthentication, UserAuthentication,
};

use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SmscServerConfig {
    pub listen_addresses: Vec<String>,
    pub interface_version: u8,
    /// Optional IP whitelist configuration.
    #[serde(default)]
    pub ip_whitelist: IpWhitelistConfig,
}

pub struct MessageSender {
    writer: tokio::net::tcp::OwnedWriteHalf,
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
}

impl MessageSender {
    pub fn new(
        writer: tokio::net::tcp::OwnedWriteHalf,
        receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        MessageSender { writer, receiver }
    }

    pub async fn run(&mut self) -> std::io::Result<()> {
        while let Some(message) = self.receiver.recv().await {
            self.writer.write_all(&message).await?;
        }

        Ok(())
    }
}

fn create_request_message(
    command_id: u32,
    sequence_number: u32,
    additional_fields: Vec<(String, Value)>,
) -> Value {
    let mut request = serde_json::Map::new();
    request.insert("command_id".to_string(), Value::from(command_id));
    request.insert("sequence_number".to_string(), Value::from(sequence_number));
    for (key, value) in additional_fields {
        request.insert(key, value);
    }
    Value::Object(request)
}

fn create_response_message(
    command_id: u32,
    sequence_number: u32,
    command_status: u32,
    additional_fields: Vec<(String, Value)>,
) -> Value {
    let mut response = serde_json::Map::new();
    response.insert(
        "command_id".to_string(),
        Value::from((0x80000000 | command_id) as u32),
    );
    response.insert("command_status".to_string(), Value::from(command_status));
    response.insert("sequence_number".to_string(), Value::from(sequence_number));
    for (key, value) in additional_fields {
        response.insert(key, value);
    }
    Value::Object(response)
}

pub struct SmscServer {
    config: SmscServerConfig,
    sequence_number_allocator: SequenceNumberAllocator,
}

impl SmscServer {
    pub fn new(config: SmscServerConfig) -> Self {
        SmscServer {
            config,
            sequence_number_allocator: SequenceNumberAllocator::new(),
        }
    }

    pub async fn start(
        &self,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<dyn MessageHandler>,
    ) -> std::io::Result<()> {
        let ip_whitelist = create_ip_whitelist(&self.config.ip_whitelist).await?;

        let mut listeners = Vec::new();
        for addr in &self.config.listen_addresses {
            let listener = TcpListener::bind(addr).await?;
            info!("SMSC server listening on {}", addr);
            listeners.push(listener);
        }
        if listeners.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "No listen addresses configured",
            ));
        }

        // Spawn a task per listener, all sharing the same config/auth/handler
        let config = Arc::new(self.config.clone());
        let seq = self.sequence_number_allocator.clone();
        let ip_wl: Arc<dyn IpWhitelist> = ip_whitelist;

        let mut handles = Vec::new();
        for listener in listeners {
            let config = config.clone();
            let seq = seq.clone();
            let ip_wl = ip_wl.clone();
            let ua = user_authentication.clone();
            let mh = message_handler.clone();
            handles.push(tokio::spawn(async move {
                Self::accept_loop(listener, config, seq, ip_wl, ua, mh).await
            }));
        }

        // Wait for all — if any returns an error, propagate it
        for handle in handles {
            handle
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))??;
        }
        Ok(())
    }

    async fn accept_loop(
        listener: TcpListener,
        config: Arc<SmscServerConfig>,
        sequence_number_allocator: SequenceNumberAllocator,
        ip_whitelist: Arc<dyn IpWhitelist>,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<dyn MessageHandler>,
    ) -> std::io::Result<()> {
        loop {
            let (socket, addr) = listener.accept().await?;

            // Check IP whitelist if configured
            if !ip_whitelist.is_empty().await.unwrap_or(true) {
                let peer_ip = addr.ip().to_string();
                match ip_whitelist.is_allowed(&peer_ip).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!("Rejected connection from {} — not in IP whitelist", peer_ip);
                        drop(socket);
                        continue;
                    }
                    Err(e) => {
                        warn!("IP whitelist check failed for {}: {}", peer_ip, e);
                        drop(socket);
                        continue;
                    }
                }
            }

            let config = (*config).clone();
            let sequence_number_allocator = sequence_number_allocator.clone();
            let t = user_authentication.clone();
            let m = message_handler.clone();
            tokio::spawn(async move {
                // Handle incoming connections
                Self::handle_connection(socket, config, sequence_number_allocator, t, m)
                    .await
                    .unwrap_or(());
            });
        }
    }

    async fn handle_connection(
        socket: TcpStream,
        config: SmscServerConfig,
        sequence_number_allocator: SequenceNumberAllocator,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<dyn MessageHandler>,
    ) -> std::io::Result<()> {
        info!("New connection from {:?}", socket.peer_addr());
        let mut buffer = SmppMessageBuffer::new();
        let (reader, writer) = socket.into_split();
        let (mut sender, receiver) = tokio::sync::mpsc::channel(100);
        let mut message_sender = MessageSender::new(writer, receiver);
        let mut buf = [0u8; 1024];

        tokio::spawn(async move {
            message_sender.run().await.unwrap_or(());
        });

        loop {
            let _ = reader.readable().await.unwrap();
            match reader.try_read(&mut buf) {
                Ok(0) => {
                    info!("Client disconnected");
                    break;
                }
                Ok(n) => {
                    info!("Received {} bytes from client", n);
                    buffer.write(&buf[..n]);
                    while let Some(message) = buffer.extract_message() {
                        if let Ok(decoded_message) = decode_message(&message.buffer) {
                            info!(
                                "Decoded message from client: {}",
                                format_smpp_value(&decoded_message)
                            );
                            Self::handle_message(
                                &decoded_message,
                                &mut sender,
                                config.clone(),
                                sequence_number_allocator.clone(),
                                user_authentication.clone(),
                                message_handler.clone(),
                            )
                            .await
                            .unwrap();
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available, continue to the next iteration
                    continue;
                }
                Err(e) => {
                    warn!("Error reading from client: {:?}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_message(
        decoded_message: &Value,
        sender: &mut tokio::sync::mpsc::Sender<Vec<u8>>,
        config: SmscServerConfig,
        sequence_number_allocator: SequenceNumberAllocator,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<dyn MessageHandler>,
    ) -> std::io::Result<()> {
        // Placeholder for handling a decoded message
        info!("Handling message: {}", format_smpp_value(decoded_message));
        let command_id = decoded_message
            .get("command_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let sequence_number = decoded_message
            .get("sequence_number")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        if command_id == ENQUIRE_LINK {
            Self::handle_enquire_link(command_id, sequence_number, decoded_message, sender).await?;
        } else if command_id == BIND_TRANSMITTER
            || command_id == BIND_RECEIVER
            || command_id == BIND_TRANSCEIVER
        {
            Self::handle_bind(
                command_id,
                sequence_number,
                decoded_message,
                sender,
                config,
                sequence_number_allocator,
                user_authentication.clone(),
            )
            .await?;
        } else if command_id == UNBIND {
            Self::handle_unbind(command_id, sequence_number, decoded_message, sender).await?;
        } else if command_id == SUBMIT_SM {
            Self::handle_submit_sm(
                command_id,
                sequence_number,
                decoded_message,
                sender,
                message_handler.clone(),
            )
            .await?;
        } else if command_id == (0x80000000 | ENQUIRE_LINK) {
        } else {
            warn!("Unknown command_id: 0x{:08X}", command_id);
        }
        // Implement your message handling logic here
        Ok(())
    }

    async fn handle_bind(
        command_id: u32,
        sequence_number: u32,
        decoded_message: &Value,
        sender: &mut tokio::sync::mpsc::Sender<Vec<u8>>,
        config: SmscServerConfig,
        sequence_number_allocator: SequenceNumberAllocator,
        user_authentication: Arc<dyn UserAuthentication>,
    ) -> std::io::Result<()> {
        let system_id = decoded_message
            .get("system_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let password = decoded_message
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let _system_type = decoded_message
            .get("system_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let mut status_code = ESME_RBINDFAIL;
        if user_authentication
            .authenticate(system_id.to_string(), password.to_string())
            .await
        {
            info!(
                "Authentication successful for system_id: {} with password: {}",
                system_id, password
            );
            status_code = ESME_ROK;
        } else {
            warn!(
                "Authentication failed for system_id: {} with password: {}",
                system_id, password
            );
        }

        // Bind Transmitter
        let response = create_response_message(
            command_id,
            sequence_number,
            status_code,
            vec![
                (
                    "system_id".to_string(),
                    Value::String(system_id.to_string()),
                ),
                (
                    "sc_interface_version".to_string(),
                    Value::from(config.interface_version),
                ),
            ],
        );
        let encoded_response = encode_message(&response, None).unwrap();
        if status_code == ESME_ROK {
            info!("Bind successful for system_id: {}", system_id);
            let sender = sender.clone();
            let sequence_number_allocator = sequence_number_allocator.clone();
            tokio::spawn(async move {
                Self::start_enquire_link_task(sender, sequence_number_allocator).await;
            });
        } else {
            error!("Bind failed for system_id: {}", system_id);
        }
        sender.send(encoded_response).await.unwrap();

        Ok(())
    }

    async fn start_enquire_link_task(
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
        sequence_number_allocator: SequenceNumberAllocator,
    ) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            let enquire_link =
                create_request_message(ENQUIRE_LINK, sequence_number_allocator.next(), vec![]);
            let encoded_enquire_link = encode_message(&enquire_link, None).unwrap();

            info!("Sending enquire_link to client: {:}", enquire_link);
            sender.send(encoded_enquire_link).await.unwrap();
            info!("Sent enquire_link to client");
        }
    }
    async fn handle_enquire_link(
        command_id: u32,
        sequence_number: u32,
        _decoded_message: &Value,
        sender: &mut tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> std::io::Result<()> {
        let response = create_response_message(command_id, sequence_number, ESME_ROK, vec![]);
        let encoded_response = encode_message(&response, None).unwrap();
        sender.send(encoded_response).await.unwrap();

        Ok(())
    }

    async fn handle_unbind(
        command_id: u32,
        sequence_number: u32,
        _decoded_message: &Value,
        sender: &mut tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> std::io::Result<()> {
        let response = create_response_message(command_id, sequence_number, ESME_ROK, vec![]);
        let response = encode_message(&response, None).unwrap();
        sender.send(response).await.unwrap();
        Ok(())
    }

    async fn handle_submit_sm(
        command_id: u32,
        sequence_number: u32,
        decoded_message: &Value,
        sender: &mut tokio::sync::mpsc::Sender<Vec<u8>>,
        message_handler: Arc<dyn MessageHandler>,
    ) -> std::io::Result<()> {
        info!(
            "Handling submit_sm command: {}",
            format_smpp_value(decoded_message)
        );
        let response = message_handler
            .handle_message(decoded_message.clone())
            .await;
        match response {
            Err(e) => {
                warn!("Message handler failed: {:?}", e);
                let error_response = create_response_message(
                    command_id,
                    sequence_number,
                    ESME_RUNKNOWNERR,
                    vec![("message_id".to_string(), Value::String("".to_string()))],
                );
                let encoded_error_response = encode_message(&error_response, None).unwrap();
                sender.send(encoded_error_response).await.unwrap();
                Ok(())
            }
            Ok(handler_resp) => {
                // Build submit_sm_resp: success status, extract message_id
                // and any valid TLV fields from the handler JSON response.
                let message_id = handler_resp
                    .get("message_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let mut fields: Vec<(String, Value)> =
                    vec![("message_id".to_string(), Value::String(message_id))];

                // Extract valid TLV fields from the handler response
                if let Value::Object(map) = &handler_resp {
                    for (key, value) in map {
                        if crate::message::is_valid_tlv_field(key) {
                            fields.push((key.clone(), value.clone()));
                        }
                    }
                }

                let resp = create_response_message(command_id, sequence_number, ESME_ROK, fields);
                match encode_message(&resp, None) {
                    Ok(encoded_response) => {
                        info!("Encoded submit_sm_resp: {:?}", encoded_response);
                        sender.send(encoded_response).await.unwrap();
                        Ok(())
                    }
                    Err(e) => {
                        warn!("Failed to encode submit_sm_resp: {:?}", e);
                        let error_response = create_response_message(
                            command_id,
                            sequence_number,
                            ESME_RUNKNOWNERR,
                            vec![("message_id".to_string(), Value::String("".to_string()))],
                        );
                        let encoded_error_response = encode_message(&error_response, None).unwrap();
                        sender.send(encoded_error_response).await.unwrap();
                        Ok(())
                    }
                }
            }
        }
    }
}
