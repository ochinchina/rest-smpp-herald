use crate::command_status::CommandStatus;
use crate::config::{Address, MessageStorageConfig, ServerConfig};
use crate::esm_class::{EsmClass, MessageMode};
use crate::ip_whitelist::{IpWhitelist, create_ip_whitelist};
use crate::message::{CommandId, DistrubutionList, SMPPMessage, SmppMessageBuffer};
pub use crate::message_handler::{HttpMessageHandler, MessageHandler};
use crate::message_storage::{
    MessageStorage, ScheduleDeliveryMessageHandler, create_message_storage,
    get_schedule_delivery_time, get_validity_period,
};
use crate::sequence_number_allocator::SequenceNumberAllocator;
pub use crate::user_authentication::{
    HttpUserAuthentication, SimpleUserAuthentication, UserAuthentication,
};

use axum::{
    Router,
    extract::{Json, State as AxumState},
    routing::{get, post},
};
use chrono::Local;

use crate::id_generator::{MessageIdGenerator, create_id_generator};
use crate::smsc_server::client_manager::ClientManager;
use crate::smsc_server::server_app_state::ServerAppState;
use crate::smsc_server::transaction_response_manager::TransactionResponseManager;
use log::{debug, error, info, warn};
use serde_json::{Value, json};
use std::collections::HashMap;

use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

pub(crate) struct SmscServerConnection {
    peer_address: String,
    config: Arc<ServerConfig>,
    id_generator: Arc<Box<dyn MessageIdGenerator>>,
    sequence_number_allocator: SequenceNumberAllocator,
    pending_messages: Arc<Box<dyn MessageStorage>>,
    transaction_response_manager: TransactionResponseManager,
    distribution_list: Arc<Box<DistrubutionList>>,
    user_authentication: Arc<dyn UserAuthentication>,
    message_handler: Arc<Box<dyn MessageHandler>>,
    client_manager: ClientManager,
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
}

async fn forward_message_to_client(
    decoded_message: &Value,
    client_manager: &ClientManager,
) -> std::io::Result<()> {
    match decoded_message
        .get("destination_addr")
        .and_then(|v| v.as_str())
    {
        Some(dest_addr) => {
            info!(
                "Forwarding message to client with destination address: {}",
                dest_addr
            );

            match client_manager
                .send_message(
                    dest_addr,
                    &SMPPMessage::from_json(&decoded_message).unwrap().to_vec(),
                )
                .await
            {
                Ok(_) => {
                    info!("Message forwarded to client successfully");
                }
                _ => {
                    error!(
                        "Failed to forward message to client with destination address: {}",
                        dest_addr
                    );
                }
            }
        }
        _ => {
            warn!("No destination address found in message, cannot forward");
        }
    }
    Ok(())
}

impl SmscServerConnection {
    pub fn new(
        config: Arc<ServerConfig>,
        id_generator: Arc<Box<dyn MessageIdGenerator>>,
        sequence_number_allocator: SequenceNumberAllocator,
        pending_messages: Arc<Box<dyn MessageStorage>>,
        transaction_response_manager: TransactionResponseManager,
        distribution_list: Arc<Box<DistrubutionList>>,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<Box<dyn MessageHandler>>,
        client_manager: ClientManager,
        stream: TcpStream,
    ) -> Self {
        let peer_address = stream
            .peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let (reader, writer) = stream.into_split();

        SmscServerConnection {
            peer_address,
            config,
            id_generator,
            sequence_number_allocator,
            pending_messages,
            transaction_response_manager,
            distribution_list,
            user_authentication,
            message_handler,
            client_manager,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub async fn run(&mut self) -> std::io::Result<()> {
        let mut buffer = SmppMessageBuffer::new();
        let mut buf = [0u8; 1024];

        loop {
            match self.reader.readable().await {
                Ok(_) => match self.reader.try_read(&mut buf) {
                    Ok(0) => {
                        info!("Client disconnected");
                        break;
                    }
                    Ok(n) => {
                        debug!("Received {} bytes from client {}", n, self.peer_address);
                        buffer.write(&buf[..n]);
                        while let Some(message) = buffer.extract_message() {
                            debug!(
                                "receive message {} from client: {}",
                                message.to_hex(),
                                self.peer_address
                            );
                            if let Ok(mut decoded_message) = message.to_json() {
                                debug!(
                                    "Decoded message: {} from client {}",
                                    decoded_message, self.peer_address
                                );
                                if let Err(e) = self.handle_message(&mut decoded_message).await {
                                    error!("Error handling message: {:?}", e);
                                    break;
                                }
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
                },
                Err(e) => {
                    warn!("Error waiting for readable: {:?}", e);
                    break;
                }
            }
        }

        // Remove the client from the manager when the connection is closed
        self.client_manager
            .remove_client(self.peer_address.clone())
            .await;

        Ok(())
    }

    async fn handle_message(&self, decoded_message: &mut Value) -> std::io::Result<()> {
        let command_id = SMPPMessage::extract_command_id(decoded_message);

        let sequence_number = decoded_message
            .get("sequence_number")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        match command_id {
            Some(CommandId::BindTransmitter)
            | Some(CommandId::BindReceiver)
            | Some(CommandId::BindTransceiver) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                match self
                    .handle_bind(
                        command_id.unwrap().as_u32(),
                        sequence_number,
                        decoded_message,
                    )
                    .await
                {
                    Ok(_) => {
                        let address_range = decoded_message
                            .get("address_range")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        self.client_manager
                            .add_client(
                                self.peer_address.clone(),
                                address_range,
                                self.writer.clone(),
                            )
                            .await;
                    }
                    Err(e) => {
                        warn!("Error handling bind message: {:?}", e);
                    }
                };
            }
            Some(CommandId::Unbind) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                self.handle_unbind(command_id.unwrap().as_u32(), sequence_number)
                    .await?;
            }
            Some(CommandId::SubmitSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                self.handle_data_submit_sm(
                    "submit_sm",
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    decoded_message,
                )
                .await?;
            }
            Some(CommandId::SubmitMulti) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                self.handle_submit_multi(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    decoded_message,
                )
                .await?;
            }
            Some(CommandId::DataSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                self.handle_data_submit_sm(
                    "data_sm",
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    decoded_message,
                )
                .await?;
            }
            Some(CommandId::QuerySm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                let message_id = decoded_message
                    .get("message_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let response = SMPPMessage::create_json_response(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    CommandStatus::EsmeRQueryFail.as_u32(),
                    vec![
                        ("message_id".to_string(), Value::String(message_id)),
                        ("final_date".to_string(), Value::String("".to_string())),
                        ("message_state".to_string(), Value::from(5)),
                        ("error_code".to_string(), Value::from(0)),
                    ],
                );
                let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                self.send(&data).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                })?;
            }
            Some(CommandId::CancelSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                let result: bool = match self.pending_messages.cancel_message(decoded_message).await
                {
                    Ok(r) => r,
                    Err(_) => false,
                };
                let response = SMPPMessage::create_json_response(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    if result {
                        CommandStatus::EsmeRok.as_u32()
                    } else {
                        CommandStatus::EsmeRCancelFail.as_u32()
                    },
                    vec![],
                );
                let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                self.send(&data).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                })?;
            }
            Some(CommandId::ReplaceSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                let result = match self.pending_messages.replace_message(decoded_message).await {
                    Ok(r) => r,
                    Err(_) => false,
                };
                let response = SMPPMessage::create_json_response(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    if result {
                        CommandStatus::EsmeRok.as_u32()
                    } else {
                        CommandStatus::EsmeRReplaceFail.as_u32()
                    },
                    vec![],
                );
                let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                self.send(&data).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                })?;
            }
            Some(CommandId::BroadcastSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                warn!("BroadcastSm command not implemented");
                let response = SMPPMessage::create_json_response(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    CommandStatus::EsmeRBcastFail.as_u32(),
                    vec![],
                );
                let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                self.send(&data).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                })?;
            }
            Some(CommandId::QueryBroadcastSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                warn!("QueryBroadcastSm command not implemented");
                let response = SMPPMessage::create_json_response(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    CommandStatus::EsmeRBcastQueryFail.as_u32(),
                    vec![],
                );
                let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                self.send(&data).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                })?;
            }
            Some(CommandId::CancelBroadcastSm) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                warn!("CancelBroadcastSm command not implemented");
                let response = SMPPMessage::create_json_response(
                    command_id.unwrap().as_u32(),
                    sequence_number,
                    CommandStatus::EsmeRBcastCancelFail.as_u32(),
                    vec![],
                );
                let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                self.send(&data).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                })?;
            }
            Some(CommandId::EnquireLink) => {
                self.handle_enquire_link(command_id.unwrap().as_u32(), sequence_number)
                    .await?;
            }
            Some(CommandId::EnquireLinkResp) => {
                debug!(
                    "Received {} from client {}",
                    decoded_message, self.peer_address
                );
            }
            Some(CommandId::DeliverSmResp) => {
                info!(
                    "Received message {} from client {}",
                    decoded_message, self.peer_address
                );
                let response_sequence_number = decoded_message
                    .get("sequence_number")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32;
                self.transaction_response_manager
                    .response_received(response_sequence_number, decoded_message.clone())
                    .await;
            }
            Some(cmd_id) => {
                info!(
                    "received message {} from client {}",
                    decoded_message, self.peer_address
                );
                if cmd_id.is_request() {
                    let response = SMPPMessage::create_json_response(
                        cmd_id.as_u32(),
                        sequence_number,
                        CommandStatus::EsmeRInvCmdId.as_u32(),
                        vec![],
                    );
                    let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
                    self.send(&data).await.map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to send response: {}", e),
                        )
                    })?;
                }
            }
            None => warn!(
                "No Command ID found in message for client {}",
                self.peer_address
            ),
        }

        Ok(())
    }

    async fn handle_enquire_link(
        &self,
        command_id: u32,
        sequence_number: u32,
    ) -> std::io::Result<()> {
        let response = SMPPMessage::create_json_response(
            command_id,
            sequence_number,
            CommandStatus::EsmeRok.as_u32(),
            vec![],
        );
        let encoded_response = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
        self.send(&encoded_response).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to send enquire link response: {}", e),
            )
        })?;

        Ok(())
    }

    async fn handle_bind(
        &self,
        command_id: u32,
        sequence_number: u32,
        decoded_message: &Value,
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

        let mut status_code = CommandStatus::EsmeRBindFail.as_u32();
        if self
            .user_authentication
            .authenticate(system_id.to_string(), password.to_string())
            .await
        {
            info!(
                "Authentication successful with system_id: {} and password *** for client {}",
                system_id, self.peer_address
            );
            status_code = CommandStatus::EsmeRok.as_u32();
        } else {
            warn!(
                "Authentication failed with system_id: {} and password *** for client {}",
                system_id, self.peer_address
            );
        }

        // Bind Transmitter
        let response = SMPPMessage::create_json_response(
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
                    Value::from(self.config.interface_version),
                ),
            ],
        );
        let encoded_response = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
        if status_code == CommandStatus::EsmeRok.as_u32() {
            info!(
                "Bind successful with system_id: {} for client {}",
                system_id, self.peer_address
            );

            let peer_address_clone = self.peer_address.to_string();
            let writer = self.writer.clone();
            let sequence_number_allocator = self.sequence_number_allocator.clone();
            let enquire_link_timer = self.config.enquire_link_timer.unwrap_or(30) as u64;
            tokio::spawn(async move {
                if let Err(e) = Self::start_enquire_link_task(
                    peer_address_clone,
                    writer,
                    sequence_number_allocator,
                    enquire_link_timer,
                )
                .await
                {
                    error!("Failed to start enquire link task: {:?}", e);
                }
            });
        } else {
            error!(
                "Bind failed with system_id: {} for client {}",
                system_id, self.peer_address
            );
        }
        self.send(&encoded_response).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to send bind response: {}", e),
            )
        })?;

        Ok(())
    }

    async fn handle_unbind(&self, command_id: u32, sequence_number: u32) -> std::io::Result<()> {
        let response = SMPPMessage::create_json_response(
            command_id,
            sequence_number,
            CommandStatus::EsmeRok.as_u32(),
            vec![],
        );
        let response = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
        self.send(&response).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to send unbind response: {}", e),
            )
        })?;
        self.close().await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to close connection: {}", e),
            )
        })?;
        Ok(())
    }

    async fn handle_data_submit_sm(
        &self,
        command_name: &str,
        command_id: u32,
        sequence_number: u32,
        decoded_message: &mut Value,
    ) -> std::io::Result<()> {
        info!(
            "Handling {} command: {}",
            command_name,
            SMPPMessage::format_smpp_value(decoded_message)
        );
        let message_id = self.id_generator.generate("msg-").await;

        self.replace_sm_default_msg_id_with_predefined_message(decoded_message)?;
        self.set_service_type(decoded_message);

        let new_sequence_number = self.sequence_number_allocator.next();
        decoded_message.as_object_mut().map(|obj| {
            obj.insert(
                "sequence_number".to_string(),
                Value::from(new_sequence_number),
            )
        });

        let dest_address = decoded_message
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if self.message_handler.is_my_request_address(dest_address) {
            info!("Message is my request, processing with message handler");
            match self
                .process_local_data_submit_message(
                    command_name,
                    command_id,
                    sequence_number,
                    decoded_message,
                )
                .await
            {
                Ok(response) => self.send(&response).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to send response: {}", e),
                    )
                }),
                Err(e) => Err(e),
            }
        } else {
            let message_mode = self.get_message_mode(decoded_message);

            let response: Option<std::result::Result<Value, Value>> = match message_mode {
                MessageMode::Datagram => {
                    info!("Message mode is Datagram, forwarding immediately");
                    Some(
                        self.send_message_in_datagram_mode(
                            command_id,
                            sequence_number,
                            &message_id,
                            decoded_message,
                        )
                        .await,
                    )
                }
                MessageMode::Transaction => {
                    info!("Message mode is Transaction, forwarding immediately");
                    Some(
                        self.send_message_in_transaction_mode(
                            command_id,
                            sequence_number,
                            &message_id,
                            decoded_message,
                        )
                        .await,
                    )
                }
                MessageMode::StoreAndForward => {
                    info!(
                        "Message mode is Store and Forward, checking schedule delivery time and validity period"
                    );
                    Some(
                        self.store_and_forward_message(
                            command_id,
                            sequence_number,
                            &message_id,
                            decoded_message,
                        )
                        .await,
                    )
                }
                _ => {
                    info!("Message mode is Default, using default configuration");
                    None
                }
            };
            let response = match response {
                Some(Ok(resp)) => resp,
                Some(Err(err_resp)) => err_resp,
                None => SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRok.as_u32(),
                    vec![("message_id".to_string(), Value::String(message_id.clone()))],
                ),
            };
            let data = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
            self.send(&data).await.map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to send response: {}", e),
                )
            })
        }
    }

    fn replace_sm_default_msg_id_with_predefined_message(
        &self,
        decoded_message: &mut Value,
    ) -> std::result::Result<(), std::io::Error> {
        let sm_default_msg_id = decoded_message
            .get("sm_default_msg_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        if sm_default_msg_id != 0 {
            info!(
                "sm_default_msg_id is not zero ({}), processing with message handler",
                sm_default_msg_id
            );
            if let Some(predefined_message) = self
                .config
                .get_predefined_message(sm_default_msg_id as usize)
            {
                decoded_message
                    .get_mut("sm_default_msg_id")
                    .map(|v| *v = Value::from(0));

                SMPPMessage::replace_message_with_predefined(
                    decoded_message,
                    predefined_message.as_str(),
                )?;
            }
        }
        Ok(())
    }

    fn set_service_type(&self, decoded_message: &mut Value) {
        let service_type = decoded_message
            .get("service_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if service_type.is_empty() {
            let default_service_type = self.config.get_default_service_type();
            if !default_service_type.is_empty() {
                decoded_message
                    .get_mut("service_type")
                    .map(|v| *v = Value::from(default_service_type));
            }
        }
    }

    fn get_message_mode(&self, decoded_message: &Value) -> MessageMode {
        let esm_class = decoded_message
            .get("esm_class")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;

        let esm_class = EsmClass::from_u8(esm_class).unwrap_or_default();

        match esm_class.message_mode {
            MessageMode::Default => self.config.get_default_mc_mode(),
            MessageMode::Datagram => MessageMode::Datagram,
            MessageMode::Transaction => MessageMode::Transaction,
            MessageMode::StoreAndForward => MessageMode::StoreAndForward,
        }
    }

    async fn process_local_data_submit_message(
        &self,
        command_name: &str,
        command_id: u32,
        sequence_number: u32,
        decoded_message: &Value,
    ) -> std::io::Result<Vec<u8>> {
        let response = match self.message_handler.handle_message(decoded_message).await {
            Ok(value) => {
                info!(
                    "Message handler response: {}",
                    SMPPMessage::format_smpp_value(&value)
                );
                let message_id = value
                    .get("message_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let mut fields: Vec<(String, Value)> =
                    vec![("message_id".to_string(), Value::String(message_id))];
                Self::extract_tlv_fields_to(&value, &mut fields);

                SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRok.as_u32(),
                    fields,
                )
            }
            Err(e) => {
                warn!("Message handler failed: {:?}", e);
                let message_id = self.id_generator.generate("msg-").await;
                SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRUnknownErr.as_u32(),
                    vec![("message_id".to_string(), Value::String(message_id.clone()))],
                )
            }
        };

        info!(
            "{} response: {}",
            command_name,
            SMPPMessage::format_smpp_value(&response)
        );

        return match SMPPMessage::from_json(&response) {
            Ok(encoded_response) => {
                let encoded_response = encoded_response.to_vec();
                info!(
                    "Encoded {}_resp: {:?}",
                    command_name,
                    hex::encode(&encoded_response)
                );
                Ok(encoded_response)
            }
            Err(e) => {
                warn!("Failed to encode {}_resp: {:?}", command_name, e);
                let error_response = SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRUnknownErr.as_u32(),
                    vec![
                        (
                            "message_id".to_string(),
                            Value::String(self.id_generator.generate("msg-").await),
                        ),
                        (
                            "command_status".to_string(),
                            Value::from(CommandStatus::EsmeRUnknownErr.as_u32()),
                        ),
                    ],
                );
                let encoded_error_response =
                    SMPPMessage::from_json(&error_response).map_or(vec![], |v| v.to_vec());
                info!(
                    "Encoded {}_resp (error): {:?}",
                    command_name,
                    hex::encode(&encoded_error_response)
                );
                Ok(encoded_error_response)
            }
        };
    }

    async fn send_message_in_datagram_mode(
        &self,
        command_id: u32,
        sequence_number: u32,
        message_id: &String,
        decoded_message: &Value,
    ) -> std::result::Result<Value, Value> {
        let new_sequence_number = self.sequence_number_allocator.next();

        let mut decoded_message = decoded_message.clone();
        // Update the command_id to DeliverSm for forwarding
        decoded_message.as_object_mut().map(|obj| {
            obj.get_mut("command_id")
                .map(|v| *v = Value::from(CommandId::DeliverSm.as_u32()));
            obj.get_mut("sequence_number")
                .map(|v| *v = Value::from(new_sequence_number));
        });

        let client_manager = self.client_manager.clone();

        tokio::spawn({
            async move {
                if let Err(e) = forward_message_to_client(&decoded_message, &client_manager).await {
                    error!("Failed to forward message to client: {}", e);
                }
            }
        });

        let response = SMPPMessage::create_json_response(
            command_id,
            sequence_number,
            CommandStatus::EsmeRok.as_u32(),
            vec![("message_id".to_string(), Value::String(message_id.clone()))],
        );
        return Ok(response);
    }

    async fn send_message_in_transaction_mode(
        &self,
        command_id: u32,
        sequence_number: u32,
        message_id: &String,
        decoded_message: &Value,
    ) -> std::result::Result<Value, Value> {
        let mut decoded_message = decoded_message.clone();
        let new_sequence_number = self.sequence_number_allocator.next();
        // Update the command_id to DeliverSm for forwarding
        decoded_message.as_object_mut().map(|obj| {
            obj.get_mut("command_id")
                .map(|v| *v = Value::from(CommandId::DeliverSm.as_u32()));
            obj.get_mut("sequence_number")
                .map(|v| *v = Value::from(new_sequence_number));
        });
        match forward_message_to_client(&decoded_message, &self.client_manager).await {
            Ok(_) => {
                return match self
                    .transaction_response_manager
                    .wait_response(new_sequence_number)
                    .await
                {
                    Some(response) => {
                        let command_status = response
                            .get("command_status")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(CommandStatus::EsmeRUnknownErr.as_u32() as u64)
                            as u32;
                        let response = SMPPMessage::create_json_response(
                            command_id,
                            sequence_number,
                            command_status,
                            vec![("message_id".to_string(), Value::String(message_id.clone()))],
                        );
                        if command_status == CommandStatus::EsmeRok.as_u32() {
                            Ok(response)
                        } else {
                            Err(response)
                        }
                    }
                    None => Err(SMPPMessage::create_json_response(
                        command_id,
                        sequence_number,
                        CommandStatus::EsmeRSubmitFail.as_u32(),
                        vec![("message_id".to_string(), Value::String(message_id.clone()))],
                    )),
                };
            }
            Err(e) => {
                warn!("Failed to forward message to client: {:?}", e);
                let response = SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRUnknownErr.as_u32(),
                    vec![("message_id".to_string(), Value::String(message_id.clone()))],
                );
                return Err(response);
            }
        }
    }

    async fn store_and_forward_message(
        &self,
        command_id: u32,
        sequence_number: u32,
        message_id: &String,
        decoded_message: &Value,
    ) -> std::result::Result<Value, Value> {
        let schedule_deliver_time = match get_schedule_delivery_time(decoded_message) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_deliver_time.is_some() && schedule_deliver_time.unwrap() < Local::now() {
            error!("schedule delivery time in message is in the past");
            let response = SMPPMessage::create_json_response(
                command_id,
                sequence_number,
                CommandStatus::EsmeRInvSched.as_u32(),
                vec![(
                    "message_id".to_string(),
                    Value::String("invalid-schedule-delivery-time".to_string()),
                )],
            );
            return Err(response);
        };

        let validity_period =
            match get_validity_period(decoded_message, self.config.get_default_validity_period()) {
                Ok(time) => Some(time),
                Err(_) => None,
            };

        if validity_period.is_some() && validity_period.unwrap() < Local::now() {
            error!("validity period in message is in the past");
            let response = SMPPMessage::create_json_response(
                command_id,
                sequence_number,
                CommandStatus::EsmeRInvExpiry.as_u32(),
                vec![(
                    "message_id".to_string(),
                    Value::String("invalid-validity-period".to_string()),
                )],
            );
            return Err(response);
        }

        if schedule_deliver_time.is_some()
            && validity_period.is_some()
            && schedule_deliver_time.unwrap() > validity_period.unwrap()
        {
            error!("schedule delivery time is after validity period");
            let response = SMPPMessage::create_json_response(
                command_id,
                sequence_number,
                CommandStatus::EsmeRInvSched.as_u32(),
                vec![(
                    "message_id".to_string(),
                    Value::String("schedule-delivery-time-after-validity-period".to_string()),
                )],
            );
            return Err(response);
        }

        info!("Message has schedule delivery time, storing for later processing");

        match self
            .pending_messages
            .save(message_id, decoded_message)
            .await
        {
            Ok(_) => {
                info!("Message stored successfully with ID: {}", message_id);
                let response = SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRok.as_u32(),
                    vec![("message_id".to_string(), Value::String(message_id.clone()))],
                );
                return Ok(response);
            }
            Err(e) => {
                warn!("Failed to store message: {:?}", e);
                let response = SMPPMessage::create_json_response(
                    command_id,
                    sequence_number,
                    CommandStatus::EsmeRUnknownErr.as_u32(),
                    vec![(
                        "message_id".to_string(),
                        Value::String("fail-to-store".to_string()),
                    )],
                );
                return Err(response);
            }
        }
    }

    async fn handle_submit_multi(
        &self,
        command_id: u32,
        sequence_number: u32,
        decoded_message: &mut Value,
    ) -> std::io::Result<()> {
        let mut failed_addresses = Vec::new();
        let message_id = self.id_generator.generate("msg-").await;
        let message_mode = self.get_message_mode(decoded_message);
        self.replace_sm_default_msg_id_with_predefined_message(decoded_message)?;
        self.set_service_type(decoded_message);

        if let Ok(dest_addresses) =
            SMPPMessage::extract_destionation_addresses(decoded_message, &self.distribution_list)
        {
            for dest_addr in dest_addresses {
                // Check if the message handler can handle this destination address
                if let Ok(mut submit_sm) =
                    self.create_submit_sm_from_submit_multi(decoded_message, &dest_addr)
                {
                    let new_sequence_number = self.sequence_number_allocator.next();
                    submit_sm.as_object_mut().map(|obj| {
                        obj.insert(
                            "sequence_number".to_string(),
                            Value::from(new_sequence_number),
                        )
                    });

                    if self
                        .message_handler
                        .is_my_request_address(&dest_addr.address)
                    {
                        match self
                            .process_local_data_submit_message(
                                "submit_sm",
                                CommandId::SubmitSm.as_u32(),
                                sequence_number,
                                &submit_sm,
                            )
                            .await
                        {
                            Ok(_) => {
                                info!(
                                    "SubmitSm message processed locally for destination address: {}",
                                    dest_addr.address
                                );
                            }
                            Err(_) => {
                                failed_addresses.push(dest_addr.clone());
                            }
                        }
                    } else {
                        match message_mode {
                            MessageMode::Datagram => {
                                info!("Message mode is Datagram, forwarding immediately");
                                if let Err(_) = self
                                    .send_message_in_datagram_mode(
                                        command_id,
                                        sequence_number,
                                        &message_id,
                                        &submit_sm,
                                    )
                                    .await
                                {
                                    failed_addresses.push(dest_addr.clone());
                                }
                            }
                            MessageMode::Transaction => {
                                info!("Message mode is Transaction, forwarding immediately");
                                if let Err(_) = self
                                    .send_message_in_transaction_mode(
                                        command_id,
                                        sequence_number,
                                        &message_id,
                                        &submit_sm,
                                    )
                                    .await
                                {
                                    failed_addresses.push(dest_addr.clone());
                                }
                            }
                            MessageMode::StoreAndForward | MessageMode::Default => {
                                info!(
                                    "Message mode is Store and Forward, checking schedule delivery time and validity period"
                                );
                                if let Err(_) = self
                                    .store_and_forward_message(
                                        CommandId::SubmitSm.as_u32(),
                                        sequence_number,
                                        &message_id,
                                        &submit_sm,
                                    )
                                    .await
                                {
                                    failed_addresses.push(dest_addr.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
        let response = if failed_addresses.is_empty() {
            SMPPMessage::create_json_response(
                CommandId::SubmitMulti.as_u32(),
                sequence_number,
                CommandStatus::EsmeRok.as_u32(),
                vec![
                    ("message_id".to_string(), Value::String(message_id.clone())),
                    ("unsuccess_smes".to_string(), Value::Array(vec![])),
                ],
            )
        } else {
            let failed_addresses = failed_addresses
                .iter()
                .map(|addr| {
                    let mut map = serde_json::Map::new();
                    map.insert("dest_addr_ton".to_string(), Value::Number(addr.ton.into()));
                    map.insert("dest_addr_npi".to_string(), Value::Number(addr.npi.into()));
                    map.insert(
                        "destination_addr".to_string(),
                        Value::String(addr.address.clone()),
                    );
                    map.insert(
                        "error_status_code".to_string(),
                        Value::Number(CommandStatus::EsmeRUnknownErr.as_u32().into()),
                    );
                    Value::Object(map)
                })
                .collect::<Vec<Value>>();

            SMPPMessage::create_json_response(
                CommandId::SubmitMulti.as_u32(),
                sequence_number,
                CommandStatus::EsmeRUnknownErr.as_u32(),
                vec![
                    ("message_id".to_string(), Value::String(message_id.clone())),
                    ("unsuccess_smes".to_string(), Value::Array(failed_addresses)),
                ],
            )
        };
        let encoded_response = SMPPMessage::from_json(&response).map_or(vec![], |v| v.to_vec());
        self.send(&encoded_response).await.unwrap();
        Ok(())
    }

    fn create_submit_sm_from_submit_multi(
        &self,
        decoded_message: &Value,
        dest_addr: &Address,
    ) -> Result<Value, std::io::Error> {
        let mut submit_sm = serde_json::Map::new();
        submit_sm.insert(
            "command_id".to_string(),
            Value::from(CommandId::SubmitSm.as_u32()),
        );
        submit_sm.insert(
            "sequence_number".to_string(),
            decoded_message
                .get("sequence_number")
                .unwrap_or(&Value::Number(0.into()))
                .clone(),
        );
        submit_sm.insert("command_status".to_string(), Value::Number(0.into()));
        submit_sm.insert(
            "dest_addr_ton".to_string(),
            Value::Number(dest_addr.ton.into()),
        );
        submit_sm.insert(
            "dest_addr_npi".to_string(),
            Value::Number(dest_addr.npi.into()),
        );
        submit_sm.insert(
            "destination_addr".to_string(),
            Value::String(dest_addr.address.clone()),
        );

        for field in [
            "service_type",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "esm_class",
            "protocol_id",
            "priority_flag",
            "schedule_delivery_time",
            "validity_period",
            "registered_delivery",
            "replace_if_present_flag",
            "data_coding",
            "sm_default_msg_id",
            "short_message",
        ] {
            if let Some(value) = decoded_message.get(field) {
                submit_sm.insert(field.to_string(), value.clone());
            }
        }

        Ok(Value::Object(submit_sm))
    }

    fn extract_tlv_fields_to(response: &Value, fields: &mut Vec<(String, Value)>) {
        if let Value::Object(map) = response {
            for (key, value) in map {
                if crate::message::is_valid_tlv_field(key) {
                    fields.push((key.clone(), value.clone()));
                }
            }
        }
    }

    async fn send(&self, message: &Vec<u8>) -> std::io::Result<()> {
        let mut writer = self.writer.lock().await;
        writer.write_all(message).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn close(&self) -> std::io::Result<()> {
        let mut writer = self.writer.lock().await;
        writer.shutdown().await?;
        Ok(())
    }

    async fn start_enquire_link_task(
        peer_address: String,
        writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
        sequence_number_allocator: SequenceNumberAllocator,
        enquire_link_timer: u64,
    ) -> std::io::Result<()> {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(enquire_link_timer));
        loop {
            interval.tick().await;
            let sequence_number = sequence_number_allocator.next();
            let enquire_link_message = SMPPMessage::create_json_request(
                CommandId::EnquireLink.as_u32(),
                sequence_number,
                vec![],
            );
            let encoded_message =
                SMPPMessage::from_json(&enquire_link_message).map_or(vec![], |v| v.to_vec());

            if let Err(e) = {
                let mut writer_lock = writer.lock().await;
                writer_lock.write_all(&encoded_message).await?;
                writer_lock.flush().await?;
                Ok::<(), std::io::Error>(())
            } {
                error!(
                    "Failed to send EnquireLink message to client {}: {}",
                    peer_address, e
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to send EnquireLink message: {}", e),
                ));
            }
            debug!("Sent EnquireLink message to client {}", peer_address);
        }
    }
}
#[derive(Clone)]
pub struct SmscServer {
    config: Arc<ServerConfig>,
    id_generator: Arc<Box<dyn MessageIdGenerator>>,
    sequence_number_allocator: SequenceNumberAllocator,
    distribution_list: Arc<Box<DistrubutionList>>,
    pending_messages: Arc<Box<dyn MessageStorage>>,
    client_manager: ClientManager,
    transaction_response_manager: TransactionResponseManager,
}

#[async_trait::async_trait]
impl ScheduleDeliveryMessageHandler for SmscServer {
    async fn handle_schedule_delivery_message(&self, message: Value) -> std::io::Result<()> {
        let messages = match SMPPMessage::extract_command_id(&message) {
            Some(CommandId::SubmitSm) | Some(CommandId::DataSm) => {
                info!("Handling SubmitSm message: {}", message);
                let mut decoded_message = message.clone();
                let new_sequence_number = self.sequence_number_allocator.next();

                // Update the command_id to DeliverSm for forwarding
                decoded_message.as_object_mut().map(|obj| {
                    obj.get_mut("command_id")
                        .map(|v| *v = Value::from(CommandId::DeliverSm.as_u32()));
                    obj.get_mut("sequence_number")
                        .map(|v| *v = Value::from(new_sequence_number));
                });
                Some(decoded_message)
            }

            Some(_) => {
                info!("Handling other message: {}", message);
                None
            }
            None => {
                warn!("Received delivery message without command ID: {}", message);
                None
            }
        };

        if let Some(messages) = messages {
            if let Err(e) = forward_message_to_client(&messages, &self.client_manager).await {
                error!("Failed to forward message to client: {}", e);
            }
        }
        Ok(())
    }
}

impl SmscServer {
    pub async fn new(config: ServerConfig) -> Self {
        let default_validity_period = config.get_default_validity_period();
        let message_storage_config = config
            .message_store
            .clone()
            .unwrap_or(MessageStorageConfig::InMemory);

        let pending_messages =
            create_message_storage(&message_storage_config, default_validity_period).await;
        let sequence_number_allocator = SequenceNumberAllocator::new();
        let smsc_server = SmscServer {
            config: Arc::new(config.clone()),
            id_generator: Arc::new(create_id_generator(&config.id_generator)),
            sequence_number_allocator: sequence_number_allocator.clone(),
            distribution_list: Arc::new(Box::new(
                config
                    .distribution_list
                    .map_or(DistrubutionList::new(HashMap::new()), |dl| {
                        DistrubutionList::create_from(&dl)
                    }),
            )),
            pending_messages: Arc::new(pending_messages),
            client_manager: ClientManager::new(),
            transaction_response_manager: TransactionResponseManager::new(),
        };

        let smsc_server_clone = smsc_server.clone();
        let smsc_server_handler = smsc_server.clone();
        let deliver_message_handler =
            Box::new(smsc_server_handler) as Box<dyn ScheduleDeliveryMessageHandler>;

        smsc_server_clone
            .pending_messages
            .set_schedule_delivery_message_handler(deliver_message_handler)
            .await;

        smsc_server
    }

    pub async fn start(
        &self,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<Box<dyn MessageHandler>>,
    ) -> std::io::Result<()> {
        let self_clone = self.clone();

        tokio::spawn(async move {
            if let Err(e) = self_clone
                .start_listener(user_authentication, message_handler)
                .await
            {
                error!("SMSC server listener failed: {}", e);
            }
        });

        self.start_rest_listener().await?;
        Ok(())
    }

    async fn start_listener(
        &self,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<Box<dyn MessageHandler>>,
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
        let ip_wl: Arc<dyn IpWhitelist> = ip_whitelist;

        let mut handles = Vec::new();
        for listener in listeners {
            let ip_wl = ip_wl.clone();
            let ua = user_authentication.clone();
            let mh = message_handler.clone();
            let self_clone = self.clone();

            handles.push(tokio::spawn(async move {
                self_clone.accept_loop(listener, ip_wl, ua, mh).await
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

    async fn start_rest_listener(&self) -> std::io::Result<()> {
        let mut plain_listeners = Vec::new();
        let mut tls_configs: Vec<(
            tokio::net::TcpListener,
            axum_server::tls_rustls::RustlsConfig,
        )> = Vec::new();

        let state = Arc::new(ServerAppState::new(self.client_manager.clone()));
        let router = Router::new()
            .route("/v1/smpp", post(Self::handle_rest_request))
            .route("/v1/healthz", get(Self::handle_healthz))
            .with_state(state);

        for addr in &self.config.rest_addresses {
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
            return Ok(());
        }

        // Spawn TLS listeners
        for (listener, rustls_config) in tls_configs {
            let router_clone = router.clone();
            tokio::spawn(async move {
                let addr = listener.local_addr().unwrap();
                axum_server::from_tcp_rustls(listener.into_std().unwrap(), rustls_config)
                    .serve(router_clone.into_make_service())
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
                let router_clone = router.clone();
                tokio::spawn(async move {
                    axum::serve(listener, router_clone).await.unwrap();
                });
            }
            axum::serve(first, router).await.unwrap();
        } else {
            // All listeners are TLS — block on a pending future so we don't exit
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn handle_rest_request(
        AxumState(state): AxumState<Arc<ServerAppState>>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        info!("Received REST request: {}", payload);
        match SMPPMessage::from_json(&payload) {
            Ok(smpp_message) => {
                if let Some(command_id) = smpp_message.get_command_id() {
                    let command_id = CommandId::from_u32(command_id);
                    match command_id {
                        Some(CommandId::DeliverSm) => {
                            info!("Received SMPP command: {}", command_id.unwrap().to_str());
                            let dest_address = payload
                                .get("destination_addr")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();

                            state
                                .client_manager
                                .send_message(&dest_address, &smpp_message.to_vec())
                                .await
                                .unwrap_or_else(|e| {
                                    error!("Failed to send message to client: {}", e);
                                });
                        }
                        _ => {
                            warn!(
                                "Received unsupported SMPP command: {}",
                                command_id.map_or("Unsupport", |cmd_id| cmd_id.to_str())
                            );
                        }
                    }
                }

                info!("Received valid SMPP message: {}", smpp_message.to_hex());
                Json(json!({"status": "ok", "message": "SMPP message received"}))
            }
            Err(e) => {
                warn!("Failed to decode SMPP message from REST request: {}", e);
                return Json(
                    json!({"status": "error", "message": format!("Failed to decode SMPP message: {}", e)}),
                );
            }
        }
    }

    async fn handle_healthz(AxumState(_state): AxumState<Arc<ServerAppState>>) -> Json<Value> {
        info!("Received health check request");
        Json(serde_json::json!({"status": "healthy"}))
    }
    async fn accept_loop(
        &self,
        listener: TcpListener,
        ip_whitelist: Arc<dyn IpWhitelist>,
        user_authentication: Arc<dyn UserAuthentication>,
        message_handler: Arc<Box<dyn MessageHandler>>,
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

            let mut smsc_server_connection = SmscServerConnection::new(
                self.config.clone(),
                self.id_generator.clone(),
                self.sequence_number_allocator.clone(),
                self.pending_messages.clone(),
                self.transaction_response_manager.clone(),
                self.distribution_list.clone(),
                user_authentication.clone(),
                message_handler.clone(),
                self.client_manager.clone(),
                socket,
            );

            tokio::spawn(async move {
                smsc_server_connection.run().await.unwrap_or_else(|e| {
                    error!("Error in connection handler: {}", e);
                });
            });
        }
    }
}
