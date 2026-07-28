use crate::alarm::AlarmNotifier;
use crate::command_status::CommandStatus;
use crate::config::SmscConnectionConfig;
use crate::esm_class::EsmClass;
use crate::message::{CommandId, SMPPMessage, SmppMessageBuffer};
use crate::message_handler::MessageHandler;
use crate::metrics;
use crate::sequence_number_allocator::SequenceNumberAllocator;
use crate::smsc_client::delivery_receipt_manager::DeliveryReceiptManager;
use bytes::BytesMut;
use log::{debug, error, info, warn};
use serde_json::{Value, json};
use std::io::Result;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::Mutex;
use tokio::time::Duration;
/// Parses a "host:port" address string into its components.
fn parse_host_port(address: &str) -> (String, u16) {
    if let Some(colon_pos) = address.rfind(':') {
        let host = address[..colon_pos].to_string();
        let port = address[colon_pos + 1..].parse::<u16>().unwrap_or(0);
        (host, port)
    } else {
        (address.to_string(), 0)
    }
}

#[derive(Clone)]
pub struct SmscConnection {
    bind_type: String,
    session_init_timer: u32,
    enquire_link_timer: u32,
    inactive_timer: u32,
    response_timer: u32,
    pub(crate) config: SmscConnectionConfig,
    pub seq_allocator: SequenceNumberAllocator,
    pub smsc_message_handler: Arc<Box<dyn MessageHandler>>,
    pub alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
    pub delivery_receipt_manager: Arc<DeliveryReceiptManager>,
    pub writer: Arc<Mutex<Option<tokio::net::tcp::OwnedWriteHalf>>>,
    response_manager: Arc<crate::smsc_client::response_manager::ResponseManager>,
}

impl SmscConnection {
    pub fn new(
        bind_type: String,
        session_init_timer: u32,
        enquire_link_timer: u32,
        inactive_timer: u32,
        response_timer: u32,
        config: SmscConnectionConfig,
        seq_allocator: SequenceNumberAllocator,
        smsc_message_handler: Arc<Box<dyn MessageHandler>>,
        alarm_notifier: Option<Arc<dyn AlarmNotifier>>,
        delivery_receipt_manager: Arc<DeliveryReceiptManager>,
    ) -> Self {
        SmscConnection {
            bind_type,
            session_init_timer,
            enquire_link_timer,
            inactive_timer,
            response_timer,
            config,
            seq_allocator,
            smsc_message_handler,
            alarm_notifier,
            delivery_receipt_manager,
            writer: Arc::new(Mutex::new(None)),
            response_manager: Arc::new(crate::smsc_client::response_manager::ResponseManager::new()),
        }
    }

    pub(crate) fn get_address(&self) -> String {
        self.config.address.clone()
    }

    pub async fn run(&self) {
        //let (out_sender, mut out_receiver) = tokio::sync::mpsc::channel(1000);

        // let callbacks: Arc<
        //     Mutex<HashMap<u32, tokio::sync::oneshot::Sender<std::io::Result<Value>>>>,
        // > = Arc::new(Mutex::new(HashMap::new()));
        let (host, port) = parse_host_port(&self.config.address);
        let mut alarm_active = false;

        loop {
            info!(
                "Attempting to connect to SMSC at {}...",
                self.config.address
            );
            match self.connect_smsc_to(&mut alarm_active).await {
                Ok(_) => {
                    // Ok means we were connected (bind succeeded) but then disconnected
                    info!(
                        "Disconnected from SMSC {}, will attempt to reconnect...",
                        self.config.address
                    );
                    if let Some(ref notifier) = self.alarm_notifier {
                        notifier
                            .raise_alarm(
                                &host,
                                port,
                                &format!("Connection to SMSC {}:{} lost", host, port),
                            )
                            .await;
                        alarm_active = true;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    error!("Failed to connect to SMSC {}: {}", self.config.address, e);
                    if let Some(ref notifier) = self.alarm_notifier {
                        if !alarm_active {
                            notifier
                                .raise_alarm(
                                    &host,
                                    port,
                                    &format!("Failed to connect to SMSC {}:{}: {}", host, port, e),
                                )
                                .await;
                            alarm_active = true;
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn connect_smsc_to(&self, alarm_active: &mut bool) -> std::io::Result<()> {
        info!("trying to connect to SMSC at {}", self.config.address);
        let stream = TcpStream::connect(&self.config.address).await?;

        info!("succeed to connecte SMSC {}", self.config.address);

        let (mut reader, writer) = stream.into_split();
        self.writer.lock().await.replace(writer);

        // buffer to store incoming data from SMSC
        let mut buffer = SmppMessageBuffer::new();

        self.do_bind(&mut reader, &mut buffer).await?;

        metrics::SMSC_ACTIVE_CONNECTIONS.inc();

        // Connection restored — clear alarm if one was active
        if *alarm_active {
            if let Some(ref notifier) = self.alarm_notifier {
                let (host, port) = parse_host_port(&self.config.address);
                notifier
                    .clear_alarm(
                        &host,
                        port,
                        &format!("Connection to SMSC {}:{} restored", host, port),
                    )
                    .await;
            }
            *alarm_active = false;
        }

        // enquery link timer
        let enquery_link_interval = Duration::from_secs(self.enquire_link_timer as u64);
        let mut enquery_link_timer = tokio::time::interval(enquery_link_interval);
        let inactive_timer_timeout = Duration::from_secs(self.inactive_timer as u64);
        let mut inactive_timer = tokio::time::interval(inactive_timer_timeout);
        // the first tick of timer will complete immediately, so we need to skip it
        inactive_timer.tick().await;
        // the first tick of timer will complete immediately, so we need to skip it
        enquery_link_timer.tick().await;

        let messages_received = AtomicU32::new(0);

        info!(
            "Starting main loop to communicate with SMSC {}",
            self.config.address
        );

        loop {
            tokio::select! {

                _ = reader.readable() => {
                    match self.read_messages(&mut reader, &mut buffer).await {
                        Err(e) => {
                            error!("Failed to read messages from SMSC: {}", e);
                            break;
                        }
                        Ok(Some(messages)) => {
                            messages_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            // reset no message timer
                            for message in messages {
                                match self.process_in_message(message).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Error processing incoming message: {}", e);
                                    }
                                }
                            }
                        }
                        Ok(None) => {}
                    }
                }

                _ = enquery_link_timer.tick() => {
                    self.send_enquire_link().await?;
                }

                _ = inactive_timer.tick() => {
                    info!("Checking for no message timeout");
                    if messages_received.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                        error!("No message received from SMSC for {:?}, disconnecting...", inactive_timer_timeout);
                        break;
                    } else {
                        messages_received.store(0, std::sync::atomic::Ordering::Relaxed);
                    }
                }

            }
        }
        metrics::SMSC_ACTIVE_CONNECTIONS.dec();
        info!("Exiting SMSC connection loop");
        Ok(())
    }

    /// Performs the SMPP bind handshake by sending a bind_transmitter PDU
    /// and waiting for the bind response from the SMSC.
    ///
    /// # Parameters
    /// - `conn_config`: The per-connection SMPP bind configuration parameters.
    /// - `seq_allocator`: Shared sequence number allocator for the bind request.
    /// - `reader`: The read half of the TCP connection to the SMSC.
    /// - `writer`: The write half of the TCP connection to the SMSC.
    /// - `buffer`: A reusable `SmppMessageBuffer` for accumulating incoming bytes.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` if bind succeeds, or an error if it fails.
    async fn do_bind(
        &self,
        reader: &mut tokio::net::tcp::OwnedReadHalf,
        buffer: &mut SmppMessageBuffer,
    ) -> std::io::Result<()> {
        let bind_message = self.create_bind_message();
        let mut buf = [0u8; 1024];
        info!(
            "Sending {} bytes bind message to SMSC at {}: {:?}",
            bind_message.len(),
            self.config.address,
            bind_message.to_hex()
        );
        if let Some(ref mut writer) = *self.writer.lock().await {
            writer.write_all(&bind_message.to_vec()).await?;
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "SMSC writer not available",
            ));
        }

        loop {
            select! {
                _ = reader.readable() => {
                    if let Ok(n) = reader.try_read(&mut buf) {
                        debug!(
                            "Received {} bytes from SMSC from {}",
                            n, self.config.address
                        );
                        buffer.write(&buf[..n]);
                        if let Some(mut message) = buffer.extract_message() {
                            match SMPPMessage::to_json(&mut message) {
                                Ok(response) => {
                                    if self.is_bind_ok(&response) {
                                        info!(
                                            "Bind at {} successful with response: {}",
                                            self.config.address,
                                            SMPPMessage::format_smpp_value(&response)
                                        );
                                        return Ok(());
                                    } else {
                                        error!(
                                            "Bind failed at {} with response: {}",
                                            self.config.address,
                                            SMPPMessage::format_smpp_value(&response)
                                        );
                                        return Err(std::io::Error::new(
                                            std::io::ErrorKind::Other,
                                            "Bind failed",
                                        ));
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to decode bind response: {} from SMSC {}",
                                        e, self.config.address
                                    );
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        "Failed to decode bind response",
                                    ));
                                }
                            }
                        }
                    }
                },
                _ = tokio::time::sleep(Duration::from_secs(self.session_init_timer as u64)) => {
                    error!("Timeout waiting for bind response from SMSC {}", self.config.address);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Timeout waiting for bind response",
                    ));
                }
            }
        }
    }

    /// Checks whether an SMPP bind response indicates a successful bind.
    ///
    /// # Parameters
    /// - `response`: A `serde_json::Value` containing the decoded bind response PDU.
    ///
    /// # Returns
    /// `bool` — `true` if the response is a bind_transmitter_resp with status 0 (success).
    fn is_bind_ok(&self, response: &Value) -> bool {
        let command_id = SMPPMessage::extract_command_id(response);
        match command_id {
            Some(CommandId::BindTransmitterResp)
            | Some(CommandId::BindReceiverResp)
            | Some(CommandId::BindTransceiverResp) => {
                let command_status = response
                    .get("command_status")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(CommandStatus::EsmeRInvBndSts as u64);
                if command_status == CommandStatus::EsmeRok as u64 {
                    info!("Bind successful with SMSC {}", self.config.address);
                    true
                } else {
                    error!(
                        "Bind failed with status: {} for SMSC {}",
                        command_status, self.config.address
                    );
                    false
                }
            }
            _ => false,
        }
    }

    /// Constructs an SMPP bind_transmitter PDU using the given configuration.
    ///
    /// # Parameters
    /// - `conn_config`: The per-connection SMPP bind configuration parameters.
    /// - `seq_allocator`: Shared sequence number allocator for the bind request.
    ///
    /// # Returns
    /// `Vec<u8>` — the serialized bind_transmitter message bytes.
    fn create_bind_message(&self) -> SMPPMessage {
        let mut buffer = BytesMut::new();

        let command_id = if self.bind_type == "transmitter" {
            CommandId::BindTransmitter
        } else if self.bind_type == "receiver" {
            CommandId::BindReceiver
        } else if self.bind_type == "transceiver" {
            CommandId::BindTransceiver
        } else {
            CommandId::BindTransceiver // default to transceiver if unknown
        };
        SMPPMessage::write_u32(0, &mut buffer); // Placeholder for command_length
        SMPPMessage::write_u32(command_id.as_u32(), &mut buffer); // command_id for bind_transmitter
        SMPPMessage::write_u32(0, &mut buffer); // command_status
        let seq_num = self.seq_allocator.next();
        SMPPMessage::write_u32(seq_num, &mut buffer); // sequence_number

        SMPPMessage::write_c_octet_str(&self.config.system_id, &mut buffer);
        SMPPMessage::write_c_octet_str(&self.config.password, &mut buffer);
        SMPPMessage::write_c_octet_str(&self.config.system_type, &mut buffer);
        SMPPMessage::write_u8(self.config.interface_version, &mut buffer);
        SMPPMessage::write_u8(self.config.addr_ton, &mut buffer); // addr_ton
        SMPPMessage::write_u8(self.config.addr_npi, &mut buffer); // addr_npi
        SMPPMessage::write_c_octet_str(&self.config.address_range, &mut buffer); // address_range

        let mut message = SMPPMessage::from_bytes_mut(buffer);
        message.update_length();
        message
    }

    /// Sends an SMPP enquire_link PDU to the SMSC to verify the connection is alive.
    ///
    /// # Parameters
    /// - `writer`: The write half of the TCP connection to the SMSC.
    /// - `sequence_number`: The sequence number to use in the enquire_link PDU.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn send_enquire_link(&self) -> std::io::Result<()> {
        let message = json! ({
            "command_id": "0x00000015",
            "command_status": 0,
            "sequence_number": self.seq_allocator.next()
        });
        let message = SMPPMessage::from_json(&message).map_or(SMPPMessage::new(vec![]), |msg| msg);
        let _ = self.send_message(&message).await?;
        Ok(())
    }

    /// Reads raw bytes from the SMSC TCP connection and extracts complete SMPP messages.
    ///
    /// # Parameters
    /// - `reader`: The read half of the TCP connection to the SMSC.
    /// - `buffer`: A reusable `SmppMessageBuffer` for accumulating partial data.
    ///
    /// # Returns
    /// `Result<Option<Vec<SmppMessageBuffer>>>` — `Ok(Some(messages))` if one or more
    /// complete messages were read, `Ok(None)` if no data was available, or an error
    /// if the connection was closed or a read failure occurred.
    async fn read_messages(
        &self,
        reader: &mut tokio::net::tcp::OwnedReadHalf,
        buffer: &mut SmppMessageBuffer,
    ) -> Result<Option<Vec<SMPPMessage>>> {
        let mut buf = [0u8; 1024];

        match reader.try_read(&mut buf) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(None);
            }
            Err(e) => {
                error!("Failed to read from SMSC: {}", e);
                return Err(e);
            }
            Ok(0) => {
                error!("SMSC connection closed");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "SMSC connection closed",
                ));
            }
            Ok(n) => {
                debug!("Received {} bytes from SMSC {}", n, self.config.address);
                buffer.write(&buf[..n]);
                let mut messages = Vec::new();
                while let Some(message) = buffer.extract_message() {
                    // Process the message
                    messages.push(message);
                }
                return Ok(Some(messages));
            }
        }
    }

    /// Processes a single incoming SMPP message: dispatches enquire_link responses,
    /// delivers inbound SMS to the message handler and stores them in `inbound_messages`,
    /// and routes responses to pending callbacks.
    ///
    /// # Parameters
    /// - `writer`: The write half of the TCP connection (used for sending responses).
    /// - `message`: The received `SmppMessageBuffer` to process.
    /// - `callbacks`: Shared map of sequence-number → oneshot senders for response correlation.
    /// - `smsc_message_handler`: The handler for processing inbound SMS (DELIVER_SM / DATA_SM).
    /// - `inbound_storage`: Shared inbound message storage backend.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn process_in_message(&self, message: SMPPMessage) -> std::io::Result<()> {
        let command_id = message.get_command_id().unwrap_or(0);
        let sequence_number = message.get_sequence_number().unwrap_or(0);
        let command_id = CommandId::from_u32(command_id).unwrap_or(CommandId::Unknown);

        match message.to_json() {
            Ok(decoded_message) => {
                debug!(
                    "Decoded message {}: {} from SMSC: {}",
                    command_id.to_str(),
                    decoded_message,
                    self.config.address
                );

                match command_id {
                    CommandId::EnquireLink => {
                        debug!(
                            "Received enquire_link from SMSC {} for {}",
                            self.config.address, sequence_number
                        );
                        self.process_enquire_link(&message).await?;
                    }

                    CommandId::EnquireLinkResp => {
                        debug!(
                            "Received enquire_link_resp from SMSC {} for {}",
                            self.config.address, sequence_number
                        );
                    }

                    CommandId::DeliverSm | CommandId::DataSm => {
                        info!(
                            "Received {} from SMSC {}",
                            decoded_message, self.config.address
                        );
                        self.handle_deliver_sm(&message, &decoded_message).await?;
                    }
                    CommandId::AlertNotification => {
                        info!(
                            "Received {} from SMSC {}",
                            decoded_message, self.config.address
                        );
                    }
                    CommandId::GenericNack => {
                        info!(
                            "Received {} from SMSC {}",
                            decoded_message, self.config.address
                        );
                    }
                    _ => {
                        info!(
                            "Received message {} from SMSC {}",
                            decoded_message, self.config.address
                        );

                        if command_id.is_response() {
                            // Notify the connection manager that a response has been received for a specific sequence number.
                            self.response_manager
                                .response_received(sequence_number, decoded_message.clone())
                                .await;
                        } else {
                            info!(
                                "Received request message with command_id {} from SMSC {} for {}",
                                command_id.to_str(),
                                self.config.address,
                                sequence_number
                            );
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to decode message: {} from SMSC {}",
                    e, self.config.address
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Failed to decode message from SMSC",
                ))
            }
        }
    }

    async fn handle_deliver_sm(
        &self,
        message: &SMPPMessage,
        decoded_message: &Value,
    ) -> std::io::Result<()> {
        info!("Received DELIVER_SM from SMSC {}", self.config.address);
        metrics::SMSC_INBOUND_MESSAGES.inc();

        let receipted_message_id = decoded_message
            .get("receipted_message_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let ems_class = decoded_message
            .get("esm_class")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let esm_class = EsmClass::from_u8(ems_class).unwrap_or_default();

        if receipted_message_id.len() > 0 {
            info!(
                "Received delivery receipt for message_id {} from SMSC {}",
                receipted_message_id, self.config.address
            );
            if let Some(receipt_url) = self
                .delivery_receipt_manager
                .get_receipt_url(&receipted_message_id, &esm_class)
                .await
            {
                info!(
                    "Found registered receipt URL {} for message_id {}",
                    receipt_url, receipted_message_id
                );
                let payload = decoded_message.clone();
                tokio::spawn(async move {
                    let client = reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(10))
                        .build()
                        .unwrap();
                    match client.post(&receipt_url).json(&payload).send().await {
                        Ok(resp) => {
                            info!(
                                "Delivery receipt for message_id {} delivered to {} — status {}",
                                receipted_message_id,
                                receipt_url,
                                resp.status()
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Delivery receipt for message_id {} delivery to {} failed: {}",
                                receipted_message_id, receipt_url, e
                            );
                        }
                    }
                });
                return Ok(());
            } else {
                info!(
                    "No registered receipt URL found for message_id {}",
                    receipted_message_id
                );
            }
        }

        let seq_num = message.get_sequence_number().unwrap_or(0);

        let command_status = match self
            .smsc_message_handler
            .handle_message(&decoded_message)
            .await
        {
            Ok(response) => response
                .get("command_status")
                .map_or(0, |v| v.as_u64().unwrap_or(0)) as u32,
            Err(e) => {
                warn!("Message handler failed: {:?}", e);
                CommandStatus::EsmeRSysErr.as_u32()
            }
        };

        info!(
            "Sending deliver_sm_resp to SMSC {} with command_status {}",
            self.config.address, command_status
        );
        // acknowledge the SMSC
        self.send_deliver_sm_resp(
            seq_num,
            message.get_command_id().unwrap_or(0),
            command_status,
        )
        .await?;

        Ok(())
    }

    /// Responds to an incoming SMPP enquire_link message by sending an enquire_link_resp
    /// with the same sequence number.
    ///
    /// # Parameters
    /// - `message`: The received enquire_link `SmppMessageBuffer`.
    /// - `writer`: The write half of the TCP connection to the SMSC.
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn process_enquire_link(&self, message: &SMPPMessage) -> std::io::Result<()> {
        debug!("Received enquire_link from SMSC, sending enquire_link_resp");
        // Send enquire_link_resp
        let seq_num = message.get_sequence_number().unwrap_or(0);
        let resp_message = json!({
            "command_id": "0x80000015",
            "command_status": 0 as u32,
            "sequence_number": seq_num
        });
        let encoded_resp_message = SMPPMessage::from_json(&resp_message)?;
        match self.send_message(&encoded_resp_message).await {
            Ok(_) => {
                debug!(
                    "Sent enquire_link_resp:{} to SMSC {}",
                    resp_message, self.config.address
                );
            }
            Err(e) => {
                error!(
                    "Failed to send enquire_link_resp:{} to SMSC {} with error: {}",
                    encoded_resp_message, self.config.address, e
                );
                return Err(e);
            }
        }
        Ok(())
    }

    /// Sends a deliver_sm_resp or data_sm_resp back to the SMSC to acknowledge
    /// receipt of an inbound message.
    ///
    /// # Parameters
    /// - `writer`: The write half of the TCP connection to the SMSC.
    /// - `sequence_number`: The sequence number from the original deliver_sm/data_sm.
    /// - `command_id`: The command ID of the original message (DELIVER_SM or DATA_SM).
    ///
    /// # Returns
    /// `std::io::Result<()>` — `Ok(())` on success, or an I/O error.
    async fn send_deliver_sm_resp(
        &self,
        sequence_number: u32,
        command_id: u32,
        command_status: u32,
    ) -> std::io::Result<()> {
        let response = SMPPMessage::create_json_response(
            command_id,
            sequence_number,
            command_status,
            vec![("message_id".to_string(), Value::String("".to_string()))],
        );

        let v = SMPPMessage::from_json(&response).map_err(|e| {
            error!("Failed to encode deliver_sm_resp: {}", e);
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to encode response")
        })?;

        self.send_message(&v).await?;

        info!("Sent response {}", response);
        Ok(())
    }

    /// Sends an SMPP message to the SMSC and optionally waits for a response if the message is a request.
    /// # Arguments
    /// * `message` - The SMPPMessage to send.
    /// # Returns
    /// `std::io::Result<Option<Value>>` - Returns `Ok(Some(response))` if a response is received, `Ok(None)` if no response is expected, or an error if sending fails.
    pub async fn send_message(&self, message: &SMPPMessage) -> std::io::Result<Option<Value>> {
        let seq_num = message.get_sequence_number().unwrap_or(0);
        let command_id = message.get_command_id().unwrap_or(0);

        let receiver = if message.is_request() && command_id != CommandId::EnquireLink.as_u32() {
            Some(self.response_manager.register_callback(seq_num).await)
        } else {
            None
        };
        let mut writer = self.writer.lock().await;
        if let Some(ref mut writer) = *writer {
            writer.write_all(&message.to_vec()).await?;
            if let Some(receiver) = receiver {
                match self.wait_response(seq_num, receiver).await {
                    Ok(response) => Ok(Some(response)),
                    Err(e) => {
                        error!(
                            "Failed to wait for response for sequence_number {}: {}",
                            seq_num, e
                        );
                        Err(e)
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "SMSC writer not available",
            ))
        }
    }

    async fn wait_response(
        &self,
        sequence_number: u32,
        receiver: tokio::sync::oneshot::Receiver<std::io::Result<Value>>,
    ) -> std::io::Result<Value> {
        match tokio::time::timeout(Duration::from_secs(self.response_timer as u64), receiver).await
        {
            Ok(Ok(response)) => match response {
                Ok(ref resp) => {
                    info!(
                        "Received response for sequence_number {}: {}",
                        sequence_number, resp
                    );
                    Ok(resp.clone())
                }
                Err(ref e) => {
                    error!(
                        "Failed to receive response for sequence_number {}: {}",
                        sequence_number, e
                    );
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Failed to receive response",
                    ))
                }
            },
            Ok(Err(e)) => {
                error!(
                    "Failed to receive response for sequence_number {}: {}",
                    sequence_number, e
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to receive response",
                ))
            }
            Err(_) => {
                error!(
                    "Timeout waiting for response for sequence_number {}",
                    sequence_number
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timeout waiting for response",
                ))
            }
        }
    }
}
