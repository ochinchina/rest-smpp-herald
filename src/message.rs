pub use crate::command_ids::*;
use crate::config::{Address, DistributionListConfig};
use crate::field_codec::{FIELD_CODECS, FieldCodec, NAMED_TLV_FIELDS, TAGGED_TLV_FIELDS};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use log::error;
use serde_json::{Number, Value};
use std::sync::LazyLock;
use std::{collections::HashMap, fmt::Display};

pub const SMPP_HEADER_LENGTH: usize = 16;

#[derive(Clone)]
pub struct DistrubutionList {
    dl: HashMap<String, Vec<Address>>,
}

impl DistrubutionList {
    pub fn new(dl: HashMap<String, Vec<Address>>) -> Self {
        DistrubutionList { dl }
    }

    pub fn create_from(distribution_list_config: &Vec<DistributionListConfig>) -> Self {
        let dl = distribution_list_config
            .iter()
            .map(|dl_config| {
                let addresses = dl_config.addresses.clone();
                (dl_config.name.clone(), addresses)
            })
            .collect::<HashMap<String, Vec<Address>>>();
        DistrubutionList { dl }
    }

    pub fn get(&self, name: &str) -> Option<&Vec<Address>> {
        self.dl.get(name)
    }
}

#[derive(Clone)]
pub struct SmppMessageBuffer {
    pub buffer: BytesMut,
}

impl SmppMessageBuffer {
    pub fn new() -> Self {
        SmppMessageBuffer {
            buffer: BytesMut::new(),
        }
    }

    pub fn from_hex(hex_str: &str) -> Result<Self, std::io::Error> {
        let bytes = hex::decode(hex_str).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Failed to decode hex string: {}", e),
            )
        })?;
        Ok(SmppMessageBuffer {
            buffer: BytesMut::from(&bytes[..]),
        })
    }
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn extract_message(&mut self) -> Option<SMPPMessage> {
        if self.buffer.len() < 16 {
            return None;
        }
        let length = BigEndian::read_u32(&self.buffer[0..4]) as usize;
        if self.buffer.len() >= length {
            let msg_bytes = self.buffer.split_to(length);
            Some(SMPPMessage::from_bytes_mut(msg_bytes))
        } else {
            None
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        SmppMessageBuffer {
            buffer: BytesMut::from(bytes),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }
}

pub struct SMPPMessage {
    message: BytesMut,
}

impl SMPPMessage {
    pub fn new(message: Vec<u8>) -> Self {
        SMPPMessage {
            message: BytesMut::from(&message[..]),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        SMPPMessage {
            message: BytesMut::from(bytes),
        }
    }

    pub fn from_hex(hex_str: &str) -> Result<Self, std::io::Error> {
        let bytes = hex::decode(hex_str).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Failed to decode hex string: {}", e),
            )
        })?;
        Ok(SMPPMessage {
            message: BytesMut::from(&bytes[..]),
        })
    }

    pub fn from_bytes_mut(bytes: BytesMut) -> Self {
        SMPPMessage { message: bytes }
    }

    pub fn len(&self) -> usize {
        self.message.len()
    }

    pub fn update_length(&mut self) {
        let length = self.message.len() as u32;
        BigEndian::write_u32(&mut self.message[0..4], length);
    }

    pub fn update_sequence_number(&mut self, seq_num: u32) {
        BigEndian::write_u32(&mut self.message[12..16], seq_num);
    }

    pub fn get_command_id(&self) -> Option<u32> {
        if self.message.len() >= 8 {
            Some(BigEndian::read_u32(&self.message[4..8]))
        } else {
            None
        }
    }

    pub fn is_request(&self) -> bool {
        if let Some(command_id) = self.get_command_id() {
            (command_id & 0x80000000) == 0
        } else {
            false
        }
    }

    pub fn get_command_status(&self) -> Option<u32> {
        if self.message.len() >= 12 {
            Some(BigEndian::read_u32(&self.message[8..12]))
        } else {
            None
        }
    }

    pub fn get_sequence_number(&self) -> Option<u32> {
        if self.message.len() >= 16 {
            Some(BigEndian::read_u32(&self.message[12..16]))
        } else {
            None
        }
    }

    pub fn to_hex(&self) -> String {
        self.message
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<String>>()
            .join("")
    }

    pub fn read_u8(buffer: &mut BytesMut) -> Option<u8> {
        buffer.try_get_u8().ok()
    }

    pub fn read_u16(buffer: &mut BytesMut) -> Option<u16> {
        buffer.try_get_u16().ok()
    }

    pub fn read_u32(buffer: &mut BytesMut) -> Option<u32> {
        buffer.try_get_u32().ok()
    }

    pub fn read(buffer: &mut BytesMut, length: usize) -> Option<Bytes> {
        if length > 0 && buffer.remaining() >= length {
            Some(buffer.copy_to_bytes(length))
        } else {
            None
        }
    }

    pub fn peek_tlv_tag(buffer: &mut BytesMut) -> Option<u16> {
        if buffer.remaining() >= 2 {
            Some(BigEndian::read_u16(&buffer[0..2]))
        } else {
            None
        }
    }

    pub fn read_tlv(buffer: &mut BytesMut) -> Option<(u16, Vec<u8>)> {
        let tag = Self::read_u16(buffer)?;
        let length = Self::read_u16(buffer)? as usize;
        let value_bytes = Self::read(buffer, length)?;
        Some((tag, value_bytes.to_vec()))
    }

    pub fn write_u8(value: u8, buffer: &mut BytesMut) {
        buffer.put_u8(value);
    }

    pub fn write_u16(value: u16, buffer: &mut BytesMut) {
        buffer.put_u16(value);
    }

    pub fn write_u32(value: u32, buffer: &mut BytesMut) {
        buffer.put_u32(value);
    }

    pub fn read_c_octet_str(buffer: &mut BytesMut) -> Option<String> {
        if let Some(pos) = buffer.as_ref().iter().position(|&b| b == 0) {
            let r = buffer.split_to(pos + 1); // Split the buffer at the null terminator
            let mut r = r.to_vec();
            r.pop(); // Remove the null terminator
            Some(String::from_utf8_lossy(&r).to_string())
        } else {
            None
        }
    }

    pub fn write_c_octet_str(s: &str, buffer: &mut BytesMut) {
        buffer.extend_from_slice(s.as_bytes());
        buffer.put_u8(0); // Null terminator
    }

    pub fn write_octet(s: &str, buffer: &mut BytesMut) {
        buffer.extend_from_slice(s.as_bytes());
    }

    pub fn write_tlv(tag: u16, value: &[u8], buffer: &mut BytesMut) {
        Self::write_u16(tag, buffer);
        Self::write_u16(value.len() as u16, buffer);
        Self::write(value, buffer);
    }

    pub fn write_c_octet_str_tlv(tag: u16, value: &str, buffer: &mut BytesMut) {
        let mut temp_buffer = BytesMut::new();
        Self::write_c_octet_str(value, &mut temp_buffer);
        Self::write_u16(tag, buffer);
        Self::write_u16(temp_buffer.len() as u16, buffer);
        buffer.extend_from_slice(&temp_buffer);
    }

    pub fn write_u8_tlv(tag: u16, value: u8, buffer: &mut BytesMut) {
        Self::write_u16(tag, buffer);
        Self::write_u16(1, buffer); // Length of u8 is 1
        Self::write_u8(value, buffer);
    }

    pub fn write_u16_tlv(tag: u16, value: u16, buffer: &mut BytesMut) {
        Self::write_u16(tag, buffer);
        Self::write_u16(2, buffer); // Length of u16 is 2
        Self::write_u16(value, buffer);
    }

    pub fn write(data: &[u8], buffer: &mut BytesMut) {
        buffer.extend_from_slice(data);
    }

    pub fn remaining(&self) -> usize {
        self.message.remaining()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.message.to_vec()
    }

    pub fn create_json_request(
        command_id: u32,
        sequence_number: u32,
        additional_fields: Vec<(String, Value)>,
    ) -> Value {
        let mut request = serde_json::Map::new();
        request.insert(
            "command_id".to_string(),
            Value::String(format!("0x{:08X}", command_id)),
        );
        request.insert("sequence_number".to_string(), Value::from(sequence_number));
        for (key, value) in additional_fields {
            request.insert(key, value);
        }
        Value::Object(request)
    }

    pub fn create_json_response(
        command_id: u32,
        sequence_number: u32,
        command_status: u32,
        additional_fields: Vec<(String, Value)>,
    ) -> Value {
        let mut response = serde_json::Map::new();

        response.insert(
            "command_id".to_string(),
            Value::String(format!("0x{:08X}", 0x80000000 | command_id)),
        );
        response.insert("command_status".to_string(), Value::from(command_status));
        response.insert("sequence_number".to_string(), Value::from(sequence_number));
        for (key, value) in additional_fields {
            response.insert(key, value);
        }
        Value::Object(response)
    }

    pub fn extract_command_id(message: &Value) -> Option<CommandId> {
        match message.get("command_id") {
            Some(Value::Number(command_id)) => command_id
                .as_u64()
                .map(|n| CommandId::from_u32(n as u32))
                .unwrap_or(None),
            Some(Value::String(command_name)) => CommandId::from_str(command_name),
            _ => match message.get("command_name") {
                Some(Value::String(command_name)) => CommandId::from_str(command_name),
                _ => None,
            },
        }
    }

    pub fn from_json(message: &Value) -> std::io::Result<Self> {
        let command_id = Self::extract_command_id(message);

        if let Some(cmd_id) = command_id {
            Self::encode_message_with_fields(cmd_id.as_u32(), message)
        } else {
            error!(
                "Missing/unknown command_id/command_name in message: {:?}",
                message
            );
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing command_id/command_name in message",
            ))
        }
    }

    fn encode_message_with_fields(command_id: u32, message: &Value) -> std::io::Result<Self> {
        let mut buffer = BytesMut::new();

        Self::write_u32(0, &mut buffer); // Placeholder for command_length
        Self::write_u32(command_id, &mut buffer);

        let number = Value::from(Number::from(0));
        message
            .get("command_status")
            .or_else(|| Some(&number))
            .and_then(|v| v.as_u64())
            .map(|n| Self::write_u32(n as u32, &mut buffer));
        message
            .get("sequence_number")
            .or_else(|| Some(&number))
            .and_then(|v| v.as_u64())
            .map(|n| Self::write_u32(n as u32, &mut buffer));

        let fields = MESSAGE_COMMAND_FIELDS.get(&command_id);
        if fields.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unknown command_id: 0x{:08X}", command_id),
            ));
        }

        if let Some(fields) = fields {
            for (field_name, default_value) in fields {
                if let Some(codec) = FIELD_CODECS.get(*field_name) {
                    if let Some(value) = message.get(*field_name).or(default_value.as_ref()) {
                        if codec.encode(value, &mut buffer).is_err() {
                            error!("Failed to encode field '{}': {:?}", field_name, value);
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("Failed to encode field: {}", field_name),
                            ));
                        }
                    } else {
                        error!("Missing field '{}'", field_name);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("Missing field: {}", field_name),
                        ));
                    }
                } else {
                    error!("Unknown field codec for '{}'", field_name);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Unknown field: {}", field_name),
                    ));
                }
            }
        } else {
            error!("No fields defined for command_id: 0x{:08X}", command_id);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("No fields defined for command_id: 0x{:08X}", command_id),
            ));
        }

        for (field_name, codec) in NAMED_TLV_FIELDS.iter() {
            if let Some(value) = message.get(*field_name) {
                if codec.encode(value, &mut buffer).is_err() {
                    error!("Failed to encode TLV field '{}': {:?}", field_name, value);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Failed to encode TLV field: {}", field_name),
                    ));
                }
            }
        }

        let mut message = SMPPMessage::from_bytes_mut(buffer);

        message.update_length();

        Ok(message)
    }

    pub fn to_json(&self) -> std::io::Result<Value> {
        let mut buffer = self.message.clone();
        let _ = Self::read_u32(&mut buffer); // command_length
        let command_id = Self::read_u32(&mut buffer);
        let command_status = Self::read_u32(&mut buffer);
        let sequence_number = Self::read_u32(&mut buffer); // sequence_number

        if sequence_number.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to read sequence_number",
            ));
        }
        let command_id = command_id.unwrap_or(0);

        let mut map: serde_json::Map<String, Value> = serde_json::Map::new();
        map.insert(
            "command_name".to_string(),
            Value::String(format!(
                "{}",
                CommandId::from_u32(command_id).map_or("Unknown", |c| c.to_str())
            )),
        );
        map.insert(
            "command_id".to_string(),
            Value::String(format!("0x{:08X}", command_id)),
        );
        map.insert(
            "command_status".to_string(),
            Value::Number(command_status.unwrap_or(0).into()),
        );
        map.insert(
            "sequence_number".to_string(),
            Value::Number(sequence_number.unwrap_or(0).into()),
        );

        if let Some(fields) = MESSAGE_COMMAND_FIELDS.get(&command_id) {
            for (field_name, _default_value) in fields {
                if let Some(codec) = FIELD_CODECS.get(*field_name) {
                    if let Ok(value) = codec.decode(&mut buffer) {
                        map.insert(field_name.to_string(), value);
                    } else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("Failed to decode field: {}", field_name),
                        ));
                    }
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Unknown field: {}", field_name),
                    ));
                }
            }
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Unknown command_id: 0x{:08X}", command_id),
            ));
        }

        loop {
            if let Some(tag) = Self::peek_tlv_tag(&mut buffer) {
                if let Some(tlv_codec) = TAGGED_TLV_FIELDS.get(&tag) {
                    let value = tlv_codec.decode(&mut buffer).unwrap_or(Value::Null);
                    map.insert(tlv_codec.name.to_string(), value);
                } else {
                    // Unknown TLV, skip or handle as needed
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Unknown TLV tag: 0x{:04X}", tag),
                    ));
                }
            } else {
                break;
            }
        }

        Ok(Value::Object(map))
    }

    /// Formats a `serde_json::Value` for display, rendering `command_id` and
    /// `command_status` fields in hex (e.g. `0x00000004`) instead of decimal.
    pub fn format_smpp_value(value: &Value) -> String {
        match value.as_object() {
            Some(map) => {
                let entries: Vec<String> = map
                    .iter()
                    .map(|(k, v)| {
                        if k == "command_id" || k == "command_status" {
                            if let Some(n) = v.as_u64() {
                                return format!("\"{}\":\"0x{:08X}\"", k, n);
                            }
                        }
                        format!("\"{}\":{}", k, v)
                    })
                    .collect();
                format!("{{{}}}", entries.join(","))
            }
            None => value.to_string(),
        }
    }

    pub fn extract_destionation_addresses(
        decoded_message: &Value,
        distribution_list: &DistrubutionList,
    ) -> std::io::Result<Vec<Address>> {
        let mut result = Vec::new();
        if let Some(command_id) = SMPPMessage::extract_command_id(decoded_message) {
            match command_id {
                CommandId::SubmitSm | CommandId::DataSm | CommandId::DeliverSm => {
                    if let (Some(ton), Some(npi), Some(dest_addr)) = (
                        decoded_message.get("dest_addr_ton"),
                        decoded_message.get("dest_addr_npi"),
                        decoded_message.get("destination_addr"),
                    ) {
                        if let (Some(ton), Some(npi), Some(address)) =
                            (ton.as_u64(), npi.as_u64(), dest_addr.as_str())
                        {
                            result.push(Address {
                                ton: ton as u8,
                                npi: npi as u8,
                                address: address.to_string(),
                            });
                        }
                    }
                }

                CommandId::SubmitMulti => {
                    if let Some(dest_addresses) = decoded_message.get("dest_addresses") {
                        if let Some(dest_addresses) = dest_addresses.as_array() {
                            for addr in dest_addresses {
                                if let (Some(ton), Some(npi), Some(dest_addr)) = (
                                    addr.get("dest_addr_ton").and_then(|v| v.as_u64()),
                                    addr.get("dest_addr_npi").and_then(|v| v.as_u64()),
                                    addr.get("destination_addr").and_then(|v| v.as_str()),
                                ) {
                                    result.push(Address {
                                        ton: ton as u8,
                                        npi: npi as u8,
                                        address: dest_addr.to_string(),
                                    });
                                } else if let Some(dl_name) =
                                    addr.get("dl_name").and_then(|v| v.as_str())
                                {
                                    if let Some(addresses) = distribution_list.get(dl_name) {
                                        result.extend(addresses.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(result)
    }

    /// Replaces the "short_message" field in the given JSON message with a predefined message
    /// if the "short_message" field is empty or missing. This function only applies to SubmitSm, DataSm, and DeliverSm commands.
    pub fn replace_message_with_predefined(
        value: &mut Value,
        predefined_message: &str,
    ) -> std::io::Result<()> {
        if let Some(command_id) = SMPPMessage::extract_command_id(value) {
            match command_id {
                CommandId::SubmitSm | CommandId::DataSm | CommandId::DeliverSm => {
                    if let Some(short_message) = value.get_mut("short_message") {
                        if short_message.as_str().is_none()
                            || short_message.as_str().unwrap().is_empty()
                        {
                            *short_message = Value::String(predefined_message.to_string());
                        }
                    } else {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Missing short_message field",
                        ));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Display for SMPPMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_json()
            .map_err(|_| std::fmt::Error)
            .and_then(|value| write!(f, "{}", Self::format_smpp_value(&value)))
    }
}

/// A static mapping of command IDs to their corresponding field names and default values.
pub static MESSAGE_COMMAND_FIELDS: LazyLock<HashMap<u32, Vec<(&'static str, Option<Value>)>>> =
    LazyLock::new(|| create_command_fields());

fn create_command_fields() -> HashMap<u32, Vec<(&'static str, Option<Value>)>> {
    let mut m: HashMap<u32, Vec<(&'static str, Option<Value>)>> = HashMap::new();
    m.insert(
        CommandId::BindTransmitter.as_u32(),
        vec![
            ("system_id", None),
            ("password", None),
            ("system_type", None),
            ("interface_version", Some(Value::Number(0x34.into()))),
            ("addr_ton", Some(Value::Number(0.into()))),
            ("addr_npi", Some(Value::Number(0.into()))),
            ("address_range", None),
        ],
    );
    m.insert(
        CommandId::BindTransmitterResp.as_u32(),
        vec![("system_id", None)],
    );
    m.insert(
        CommandId::BindReceiver.as_u32(),
        vec![
            ("system_id", None),
            ("password", None),
            ("system_type", None),
            ("interface_version", Some(Value::Number(0x34.into()))),
            ("addr_ton", Some(Value::Number(0.into()))),
            ("addr_npi", Some(Value::Number(0.into()))),
            ("address_range", None),
        ],
    );
    m.insert(
        CommandId::BindReceiverResp.as_u32(),
        vec![("system_id", None)],
    );
    m.insert(
        CommandId::BindTransceiver.as_u32(),
        vec![
            ("system_id", None),
            ("password", None),
            ("system_type", None),
            ("interface_version", Some(Value::Number(0x34.into()))),
            ("addr_ton", Some(Value::Number(0.into()))),
            ("addr_npi", Some(Value::Number(0.into()))),
            ("address_range", None),
        ],
    );
    m.insert(
        CommandId::BindTransceiverResp.as_u32(),
        vec![("system_id", None)],
    );
    m.insert(
        CommandId::Outbind.as_u32(),
        vec![("system_id", None), ("password", None)],
    );
    m.insert(CommandId::Unbind.as_u32(), vec![]);
    m.insert(CommandId::UnbindResp.as_u32(), vec![]);
    m.insert(CommandId::EnquireLink.as_u32(), vec![]);
    m.insert(CommandId::EnquireLinkResp.as_u32(), vec![]);
    m.insert(
        CommandId::AlertNotification.as_u32(),
        vec![
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", None),
            ("esme_addr_ton", Some(Value::Number(0.into()))),
            ("esme_addr_npi", Some(Value::Number(0.into()))),
            ("esme_addr", None),
        ],
    );
    m.insert(CommandId::GenericNack.as_u32(), vec![]);
    m.insert(
        CommandId::SubmitSm.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            ("dest_addr_ton", Some(Value::Number(0.into()))),
            ("dest_addr_npi", Some(Value::Number(0.into()))),
            ("destination_addr", None),
            ("esm_class", Some(Value::Number(0.into()))),
            ("protocol_id", Some(Value::Number(0.into()))),
            ("priority_flag", Some(Value::Number(0.into()))),
            (
                "schedule_delivery_time",
                Some(Value::String("".to_string())),
            ),
            ("validity_period", Some(Value::String("".to_string()))),
            ("registered_delivery", Some(Value::Number(0.into()))),
            ("replace_if_present_flag", Some(Value::Number(0.into()))),
            ("data_coding", Some(Value::Number(0.into()))),
            ("sm_default_msg_id", Some(Value::Number(0.into()))),
            ("short_message", None),
        ],
    );
    m.insert(CommandId::SubmitSmResp.as_u32(), vec![("message_id", None)]);
    m.insert(
        CommandId::DataSm.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            ("dest_addr_ton", Some(Value::Number(0.into()))),
            ("dest_addr_npi", Some(Value::Number(0.into()))),
            ("destination_addr", None),
            ("esm_class", Some(Value::Number(0.into()))),
            ("registered_delivery", Some(Value::Number(0.into()))),
            ("data_coding", Some(Value::Number(0.into()))),
        ],
    );
    m.insert(CommandId::DataSmResp.as_u32(), vec![("message_id", None)]);
    m.insert(
        CommandId::SubmitMulti.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            ("dest_addresses", None),
            ("esm_class", Some(Value::Number(0.into()))),
            ("protocol_id", Some(Value::Number(0.into()))),
            ("priority_flag", Some(Value::Number(0.into()))),
            (
                "schedule_delivery_time",
                Some(Value::String("".to_string())),
            ),
            ("validity_period", Some(Value::String("".to_string()))),
            ("registered_delivery", Some(Value::Number(0.into()))),
            ("replace_if_present_flag", Some(Value::Number(0.into()))),
            ("data_coding", Some(Value::Number(0.into()))),
            ("sm_default_msg_id", Some(Value::Number(0.into()))),
            ("short_message", None),
        ],
    );
    m.insert(
        CommandId::SubmitMultiResp.as_u32(),
        vec![("message_id", None), ("unsuccess_smes", None)],
    );
    m.insert(
        CommandId::DeliverSm.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            ("dest_addr_ton", Some(Value::Number(0.into()))),
            ("dest_addr_npi", Some(Value::Number(0.into()))),
            ("destination_addr", None),
            ("esm_class", Some(Value::Number(0.into()))),
            ("protocol_id", Some(Value::Number(0.into()))),
            ("priority_flag", Some(Value::Number(0.into()))),
            (
                "schedule_delivery_time",
                Some(Value::String("".to_string())),
            ),
            ("validity_period", Some(Value::String("".to_string()))),
            ("registered_delivery", Some(Value::Number(0.into()))),
            ("replace_if_present_flag", Some(Value::Number(0.into()))),
            ("data_coding", Some(Value::Number(0.into()))),
            ("sm_default_msg_id", Some(Value::Number(0.into()))),
            ("short_message", None),
        ],
    );
    m.insert(
        CommandId::DeliverSmResp.as_u32(),
        vec![("message_id", None)],
    );

    m.insert(
        CommandId::BroadcastSm.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            ("message_id", Some(Value::String("".to_string()))),
            ("priority_flag", Some(Value::Number(0.into()))),
            (
                "schedule_delivery_time",
                Some(Value::String("".to_string())),
            ),
            ("validity_period", Some(Value::String("".to_string()))),
            ("replace_if_present_flag", Some(Value::Number(0.into()))),
            ("data_coding", Some(Value::Number(0.into()))),
            ("sm_default_msg_id", Some(Value::Number(0.into()))),
        ],
    );

    m.insert(
        CommandId::BroadcastSmResp.as_u32(),
        vec![("message_id", None)],
    );

    m.insert(
        CommandId::CancelSm.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("message_id", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            ("dest_addr_ton", Some(Value::Number(0.into()))),
            ("dest_addr_npi", Some(Value::Number(0.into()))),
            ("destination_addr", Some(Value::String("".to_string()))),
        ],
    );

    m.insert(CommandId::CancelSmResp.as_u32(), vec![]);

    m.insert(
        CommandId::QuerySm.as_u32(),
        vec![
            ("message_id", None),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
        ],
    );

    m.insert(
        CommandId::QuerySmResp.as_u32(),
        vec![
            ("message_id", None),
            ("final_date", None),
            ("message_state", None),
            ("error_code", None),
        ],
    );

    m.insert(
        CommandId::ReplaceSm.as_u32(),
        vec![
            ("message_id", None),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
            (
                "schedule_delivery_time",
                Some(Value::String("".to_string())),
            ),
            ("validity_period", Some(Value::String("".to_string()))),
            ("registered_delivery", Some(Value::Number(0.into()))),
            ("sm_default_msg_id", Some(Value::Number(0.into()))),
            ("short_message", None),
        ],
    );

    m.insert(CommandId::ReplaceSmResp.as_u32(), vec![]);

    m.insert(
        CommandId::QueryBroadcastSm.as_u32(),
        vec![
            ("message_id", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
        ],
    );

    m.insert(
        CommandId::QueryBroadcastSmResp.as_u32(),
        vec![("message_id", None), ("message_state", None)],
    );

    m.insert(
        CommandId::CancelBroadcastSm.as_u32(),
        vec![
            ("service_type", Some(Value::String("".to_string()))),
            ("message_id", Some(Value::String("".to_string()))),
            ("source_addr_ton", Some(Value::Number(0.into()))),
            ("source_addr_npi", Some(Value::Number(0.into()))),
            ("source_addr", Some(Value::String("".to_string()))),
        ],
    );

    m.insert(CommandId::CancelBroadcastSmResp.as_u32(), vec![]);
    m
}

/// Returns true if `name` is a recognized TLV field name.
pub fn is_valid_tlv_field(name: &str) -> bool {
    NAMED_TLV_FIELDS.contains_key(name)
}
