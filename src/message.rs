use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde_json::{Number, Value};

use std::collections::HashMap;
use std::sync::LazyLock;

pub use crate::command_ids::*;
use crate::field_codec::{FIELD_CODECS, FieldCodec, NAMED_TLV_FIELDS, TAGGED_TLV_FIELDS};

pub const SMPP_HEADER_LENGTH: usize = 16;

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

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn extract_message(&mut self) -> Option<SmppMessageBuffer> {
        if self.buffer.len() < 16 {
            return None;
        }
        let length = BigEndian::read_u32(&self.buffer[0..4]) as usize;
        if self.buffer.len() >= length {
            let msg_bytes = self.buffer.split_to(length);
            Some(SmppMessageBuffer { buffer: msg_bytes })
        } else {
            None
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        SmppMessageBuffer {
            buffer: BytesMut::from(bytes),
        }
    }

    pub fn update_length(&mut self) {
        let length = self.buffer.len() as u32;
        BigEndian::write_u32(&mut self.buffer[0..4], length);
    }

    pub fn update_sequence_number(&mut self, seq_num: u32) {
        BigEndian::write_u32(&mut self.buffer[12..16], seq_num);
    }

    pub fn get_command_id(&self) -> Option<u32> {
        if self.buffer.len() >= 8 {
            Some(BigEndian::read_u32(&self.buffer[4..8]))
        } else {
            None
        }
    }

    pub fn get_command_status(&self) -> Option<u32> {
        if self.buffer.len() >= 12 {
            Some(BigEndian::read_u32(&self.buffer[8..12]))
        } else {
            None
        }
    }

    pub fn get_sequence_number(&self) -> Option<u32> {
        if self.buffer.len() >= 16 {
            Some(BigEndian::read_u32(&self.buffer[12..16]))
        } else {
            None
        }
    }

    pub fn to_hex(&self) -> String {
        self.buffer
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<String>>()
            .join(" ")
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        self.buffer.try_get_u8().ok()
    }
    pub fn read_u16(&mut self) -> Option<u16> {
        self.buffer.try_get_u16().ok()
    }
    pub fn read_u32(&mut self) -> Option<u32> {
        self.buffer.try_get_u32().ok()
    }

    pub fn read(&mut self, length: usize) -> Option<Bytes> {
        if length > 0 && self.buffer.remaining() >= length {
            Some(self.buffer.copy_to_bytes(length))
        } else {
            None
        }
    }

    pub fn peek_tlv_tag(&self) -> Option<u16> {
        if self.buffer.remaining() >= 2 {
            Some(BigEndian::read_u16(&self.buffer[0..2]))
        } else {
            None
        }
    }

    pub fn read_tlv(&mut self) -> Option<(u16, Vec<u8>)> {
        let tag = self.read_u16()?;
        let length = self.read_u16()? as usize;
        let value_bytes = self.read(length)?;
        Some((tag, value_bytes.to_vec()))
    }

    pub fn write_u8(&mut self, value: u8) {
        self.buffer.put_u8(value);
    }

    pub fn write_u16(&mut self, value: u16) {
        self.buffer.put_u16(value);
    }

    pub fn write_u32(&mut self, value: u32) {
        self.buffer.put_u32(value);
    }

    pub fn read_c_octet_str(&mut self) -> Option<String> {
        if let Some(pos) = self.buffer.as_ref().iter().position(|&b| b == 0) {
            let r = self.buffer.split_to(pos + 1); // Split the buffer at the null terminator
            let mut r = r.to_vec();
            r.pop(); // Remove the null terminator
            Some(String::from_utf8_lossy(&r).to_string())
        } else {
            None
        }
    }

    pub fn write_c_octet_str(&mut self, s: &str) {
        self.buffer.extend_from_slice(s.as_bytes());
        self.buffer.put_u8(0); // Null terminator
    }

    pub fn write_octet(&mut self, s: &str) {
        self.buffer.extend_from_slice(s.as_bytes());
    }

    pub fn write_tlv(&mut self, tag: u16, value: &[u8]) {
        self.write_u16(tag);
        self.write_u16(value.len() as u16);
        self.write(value);
    }

    pub fn write_c_octet_str_tlv(&mut self, tag: u16, value: &str) {
        let mut temp_buffer = SmppMessageBuffer::new();
        temp_buffer.write_c_octet_str(value);
        self.write_u16(tag);
        self.write_u16(temp_buffer.buffer.len() as u16);
        self.write(&temp_buffer.buffer);
    }

    pub fn write_u8_tlv(&mut self, tag: u16, value: u8) {
        self.write_u16(tag);
        self.write_u16(1); // Length of u8 is 1
        self.write_u8(value);
    }

    pub fn write_u16_tlv(&mut self, tag: u16, value: u16) {
        self.write_u16(tag);
        self.write_u16(2); // Length of u16 is 2
        self.write_u16(value);
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }
}

pub static MESSAGE_COMMAND_FIELDS: LazyLock<HashMap<u32, Vec<&'static str>>> =
    LazyLock::new(|| create_command_fields());

fn create_command_fields() -> HashMap<u32, Vec<&'static str>> {
    let mut m: HashMap<u32, Vec<&'static str>> = HashMap::new();
    m.insert(
        BIND_TRANSMITTER as u32,
        vec![
            "system_id",
            "password",
            "system_type",
            "interface_version",
            "addr_ton",
            "addr_npi",
            "address_range",
        ],
    );
    m.insert(BIND_TRANSMITTER_RESP as u32, vec!["system_id"]);
    m.insert(
        BIND_RECEIVER as u32,
        vec![
            "system_id",
            "password",
            "system_type",
            "interface_version",
            "addr_ton",
            "addr_npi",
            "address_range",
        ],
    );
    m.insert(BIND_RECEIVER_RESP as u32, vec!["system_id"]);
    m.insert(
        BIND_TRANSCEIVER as u32,
        vec![
            "system_id",
            "password",
            "system_type",
            "interface_version",
            "addr_ton",
            "addr_npi",
            "address_range",
        ],
    );
    m.insert(BIND_TRANSCEIVER_RESP as u32, vec!["system_id"]);
    m.insert(OUTBIND as u32, vec!["system_id", "password"]);
    m.insert(UNBIND as u32, vec![]);
    m.insert(UNBIND_RESP as u32, vec![]);
    m.insert(ENQUIRE_LINK as u32, vec![]);
    m.insert(ENQUIRE_LINK_RESP as u32, vec![]);
    m.insert(
        ALERT_NOTIFICATION as u32,
        vec![
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "esme_addr_ton",
            "esme_addr_npi",
            "esme_addr",
        ],
    );
    m.insert(GENERIC_NACK as u32, vec![]);
    m.insert(
        SUBMIT_SM as u32,
        vec![
            "service_type",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "dest_addr_ton",
            "dest_addr_npi",
            "destination_addr",
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
        ],
    );
    m.insert(SUBMIT_SM_RESP as u32, vec!["message_id"]);
    m.insert(
        DATA_SM as u32,
        vec![
            "service_type",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "dest_addr_ton",
            "dest_addr_npi",
            "destination_addr",
            "esm_class",
            "registered_delivery",
            "data_coding",
        ],
    );
    m.insert(DATA_SM_RESP as u32, vec!["message_id"]);
    m.insert(
        SUBMIT_MULTI as u32,
        vec![
            "service_type",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "dest_addresses",
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
        ],
    );
    m.insert(
        SUBMIT_MULTI_RESP as u32,
        vec!["message_id", "unsuccess_sme"],
    );
    m.insert(
        DELIVER_SM as u32,
        vec![
            "service_type",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "dest_addr_ton",
            "dest_addr_npi",
            "destination_addr",
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
        ],
    );
    m.insert(DELIVER_SM_RESP as u32, vec!["message_id"]);

    m.insert(
        BROADCAST_SM as u32,
        vec![
            "service_type",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "message_id",
            "priority_flag",
            "schedule_delivery_time",
            "validity_period",
            "replace_if_present_flag",
            "data_coding",
            "sm_default_msg_id",
        ],
    );

    m.insert(BROADCAST_SM_RESP as u32, vec!["message_id"]);

    m.insert(
        CANCEL_SM as u32,
        vec![
            "service_type",
            "message_id",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "dest_addr_ton",
            "dest_addr_npi",
            "destination_addr",
        ],
    );

    m.insert(CANCEL_SM_RESP as u32, vec![]);

    m.insert(
        QUERY_SM as u32,
        vec![
            "message_id",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
        ],
    );

    m.insert(
        QUERY_SM_RESP as u32,
        vec!["message_id", "final_date", "message_state", "error_code"],
    );

    m.insert(
        REPLACE_SM as u32,
        vec![
            "message_id",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
            "schedule_delivery_time",
            "validity_period",
            "registered_delivery",
            "sm_default_msg_id",
            "short_message",
        ],
    );

    m.insert(REPLACE_SM_RESP as u32, vec![]);

    m.insert(
        QUERY_BROADCAST_SM as u32,
        vec![
            "message_id",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
        ],
    );

    m.insert(
        QUERY_BROADCAST_SM_RESP as u32,
        vec!["message_id", "message_state"],
    );

    m.insert(
        CANCEL_BROADCAST_SM as u32,
        vec![
            "service_type",
            "message_id",
            "source_addr_ton",
            "source_addr_npi",
            "source_addr",
        ],
    );

    m.insert(CANCEL_BROADCAST_SM_RESP as u32, vec![]);
    m
}

pub fn update_sequence_number(message: &mut Vec<u8>, sequence_number: u32) {
    BigEndian::write_u32(&mut message[12..16], sequence_number);
}
/**
 * Encode a message represented as a serde_json::Value into a byte vector.
 * The message should contain a "command_id" field to determine the structure.
 */
pub fn encode_message(message: &Value, command_id: Option<u32>) -> std::io::Result<Vec<u8>> {
    if let Some(cmd_id) = command_id {
        encode_message_with_fields(cmd_id, message)
    } else if let Some(Value::Number(command_id)) = message.get("command_id") {
        if let Some(cmd_id) = command_id.as_u64() {
            encode_message_with_fields(cmd_id as u32, message)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid command_id",
            ))
        }
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Missing command_id",
        ))
    }
}

/**
 * Helper function to encode a message given a command_id and the message content.
 * This function looks up the fields for the given command_id and encodes them accordingly.
 *
 * Returns a byte vector representing the encoded message.
 */
fn encode_message_with_fields(command_id: u32, message: &Value) -> std::io::Result<Vec<u8>> {
    let mut buffer = SmppMessageBuffer::new();

    buffer.write_u32(0); // Placeholder for command_length
    buffer.write_u32(command_id);
    //buffer.write_u32(0); // command_status
    //buffer.write_u32(0); // sequence_number

    let number = Value::from(Number::from(0));
    message
        .get("command_status")
        .or_else(|| Some(&number))
        .and_then(|v| v.as_u64())
        .map(|n| buffer.write_u32(n as u32));
    message
        .get("sequence_number")
        .or_else(|| Some(&number))
        .and_then(|v| v.as_u64())
        .map(|n| buffer.write_u32(n as u32));

    let fields = MESSAGE_COMMAND_FIELDS.get(&command_id);
    if fields.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Unknown command_id: 0x{:08X}", command_id),
        ));
    }

    if let Some(fields) = fields {
        for field_name in fields {
            if let Some(codec) = FIELD_CODECS.get(*field_name) {
                println!("Encoding field '{}'", *field_name);
                if let Some(value) = message.get(*field_name) {
                    if codec.encode(value, &mut buffer).is_err() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("Failed to encode field: {}", field_name),
                        ));
                    }
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Missing field: {}", field_name),
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
            format!("No fields defined for command_id: 0x{:08X}", command_id),
        ));
    }

    for (field_name, codec) in NAMED_TLV_FIELDS.iter() {
        println!("Checking for TLV field '{}'", *field_name);
        if let Some(value) = message.get(*field_name) {
            println!("Encoding TLV field '{}'", *field_name);
            if codec.encode(value, &mut buffer).is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Failed to encode TLV field: {}", field_name),
                ));
            }
        }
    }

    buffer.update_length();

    Ok(buffer.buffer.to_vec())
}

/**
 * Decode a byte slice into a serde_json::Value representing the message.
 * The function reads the command_id to determine the structure of the message.
 */
pub fn decode_message(data: &[u8]) -> std::io::Result<Value> {
    let mut buffer = SmppMessageBuffer::from_bytes(data);
    let _ = buffer.read_u32(); // command_length
    let command_id = buffer.read_u32();
    let command_status = buffer.read_u32();
    let sequence_number = buffer.read_u32(); // sequence_number

    if sequence_number.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to read sequence_number",
        ));
    }
    let command_id = command_id.unwrap_or(0);

    let mut map: serde_json::Map<String, Value> = serde_json::Map::new();
    map.insert("command_id".to_string(), Value::Number(command_id.into()));
    map.insert(
        "command_status".to_string(),
        Value::Number(command_status.unwrap_or(0).into()),
    );
    map.insert(
        "sequence_number".to_string(),
        Value::Number(sequence_number.unwrap_or(0).into()),
    );

    if let Some(fields) = MESSAGE_COMMAND_FIELDS.get(&command_id) {
        for field_name in fields {
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
        if let Some(tag) = buffer.peek_tlv_tag() {
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

static COMMAND_NAME_TO_ID: LazyLock<HashMap<&'static str, u32>> =
    LazyLock::new(|| create_message_commands());
fn create_message_commands() -> HashMap<&'static str, u32> {
    let mut m: HashMap<&'static str, u32> = HashMap::new();
    m.insert("SUBMIT_SM", SUBMIT_SM);
    m
}

/// Returns true if `name` is a recognized TLV field name.
pub fn is_valid_tlv_field(name: &str) -> bool {
    NAMED_TLV_FIELDS.contains_key(name)
}

pub fn get_command_id_by_name(name: &str) -> Option<u32> {
    COMMAND_NAME_TO_ID.get(name).cloned()
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
