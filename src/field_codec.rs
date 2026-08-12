use base64::prelude::*;
use bytes::{Buf, BufMut, BytesMut};
use serde_json::Value;

use std::collections::HashMap;
use std::sync::LazyLock;

use crate::message::SMPPMessage;
use chrono::{Days, Duration, Months, prelude::*};

pub fn parse_absolute_relative_time(time_str: &str) -> Result<DateTime<Local>, String> {
    if time_str.len() != 16 {
        return Err(format!("Invalid absolute_time format: {}", time_str));
    }

    let p = &time_str[15..16];
    match p {
        "R" => {
            // Relative time
            let year = time_str[0..2].parse::<u32>().unwrap();
            let month = time_str[2..4].parse::<u32>().unwrap();
            let day = time_str[4..6].parse::<u32>().unwrap();
            let hour = time_str[6..8].parse::<u32>().unwrap();
            let minute = time_str[8..10].parse::<u32>().unwrap();
            let second = time_str[10..12].parse::<u32>().unwrap();

            return Ok(Local::now()
                + Days::new(day as u64 + year as u64 * 365)
                + Months::new(month as u32)
                + Duration::hours(hour as i64)
                + Duration::minutes(minute as i64)
                + Duration::seconds(second as i64));
        }
        "+" | "-" => {
            let s = &time_str[0..12];
            // Absolute time with timezone offset
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%y%m%d%H%M%S") {
                let nn = time_str[13..15].parse::<i64>().unwrap_or(0);
                let t = time_str[12..13].parse::<i64>().unwrap_or(0);
                let dt = match p {
                    "+" => dt - Duration::minutes(nn * 15) + Duration::milliseconds(t * 100),
                    "-" => dt + Duration::minutes(nn * 15) + Duration::milliseconds(t * 100),
                    _ => dt,
                };

                return Ok(Local.from_local_datetime(&dt).unwrap());
            } else {
                return Err(format!("Failed to parse absolute_time: {}", time_str));
            }
        }
        _ => return Err(format!("Invalid absolute_time format: {}", time_str)),
    }
}
pub trait FieldCodec {
    /**
     * Encode the given json value as binary and put data into the provided SmppMessageBuffer.
     *
     * The implementation should handle the specific encoding rules for each field type.
     */
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()>;

    /**
     * Decode binary data from the provided SmppMessageBuffer into a json value.
     *
     * The implementation should handle the specific decoding rules for each field type.
     *
     * Returns a serde_json::Value representing the decoded data.
     */
    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value>;
}

struct U8FieldCodec;
impl FieldCodec for U8FieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_u64() {
                SMPPMessage::write_u8(n as u8, buffer);
                return Ok(());
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode u8",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if buffer.len() >= 1 {
            let n = buffer.split_to(1)[0];
            Ok(Value::Number(serde_json::Number::from(n)))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to read u8 from buffer",
            ))
        }
    }
}

struct OptionalU8FieldCodec {
    default_value: u8,
}

impl OptionalU8FieldCodec {
    fn new(default_value: u8) -> Self {
        OptionalU8FieldCodec { default_value }
    }
}

impl FieldCodec for OptionalU8FieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_u64()
                && n != self.default_value as u64
            {
                SMPPMessage::write_u8(n as u8, buffer);
                return Ok(());
            }
        }
        // If the value is not a valid number, write the default value
        buffer.put_u8(self.default_value);
        Ok(())
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if buffer.len() >= 1 {
            let n = buffer.split_to(1)[0];
            Ok(Value::Number(serde_json::Number::from(n)))
        } else {
            // If we can't read a u8, return the default value
            Ok(Value::Number(serde_json::Number::from(self.default_value)))
        }
    }
}

struct VarLengthIntFieldCodec;

impl FieldCodec for VarLengthIntFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_u64() {
                let mut temp = n;
                while temp != 0 {
                    SMPPMessage::write_u8((temp & 0xff) as u8, buffer);
                    temp >>= 8;
                }
                return Ok(());
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode VarLengthInt",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        let mut result: u64 = 0;
        loop {
            if let Some(byte) = buffer.try_get_u8().ok() {
                result = result << 8;
                result |= byte as u64;
            } else {
                break;
            }
        }
        Ok(Value::Number(serde_json::Number::from(result)))
    }
}
struct COctetStrFieldCodec {
    max_length: usize,
}

impl COctetStrFieldCodec {
    fn new(max_length: usize) -> Self {
        COctetStrFieldCodec { max_length }
    }
}
impl Default for COctetStrFieldCodec {
    fn default() -> Self {
        COctetStrFieldCodec { max_length: 255 }
    }
}

impl FieldCodec for COctetStrFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::String(s) = value {
            let s = if s.len() > self.max_length {
                &s[0..self.max_length]
            } else {
                s
            };
            SMPPMessage::write_c_octet_str(s, buffer);
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode c_octet_str",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        SMPPMessage::read_c_octet_str(buffer)
            .map(Value::String)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Failed to read c_octet_str from buffer",
                )
            })
    }
}

struct OctetFieldCodec {
    length_indicator: bool,
    length_bytes: i32,
}

impl OctetFieldCodec {
    fn new(length_indicator: bool, length_bytes: i32) -> Self {
        OctetFieldCodec {
            length_indicator,
            length_bytes,
        }
    }

    fn read_length(&self, buffer: &mut BytesMut) -> Option<usize> {
        match self.length_bytes {
            1 => SMPPMessage::read_u8(buffer).map(|n| n as usize),
            2 => SMPPMessage::read_u16(buffer).map(|n| n as usize),
            4 => SMPPMessage::read_u32(buffer).map(|n| n as usize),
            _ => None,
        }
    }

    fn write_length(&self, buffer: &mut BytesMut, length: usize) {
        match self.length_bytes {
            1 => SMPPMessage::write_u8(length as u8, buffer),
            2 => SMPPMessage::write_u16(length as u16, buffer),
            4 => SMPPMessage::write_u32(length as u32, buffer),
            _ => (),
        }
    }
}

impl FieldCodec for OctetFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::String(s) = value {
            let b = match BASE64_STANDARD.decode(s) {
                Ok(decoded) => decoded,
                Err(_) => s.clone().into_bytes(),
            };
            if self.length_indicator {
                self.write_length(buffer, b.len());
            }
            SMPPMessage::write(&b, buffer);
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode OctetField",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if self.length_indicator {
            let length = self.read_length(buffer);
            if let Some(len) = length {
                if let Some(bytes) = SMPPMessage::read(buffer, len) {
                    let encoded = BASE64_STANDARD.encode(bytes);
                    return Ok(Value::String(encoded));
                }
            }
        } else {
            if let Some(bytes) = SMPPMessage::read(buffer, buffer.remaining()) {
                let encoded = BASE64_STANDARD.encode(bytes);
                return Ok(Value::String(encoded));
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to decode OctetField",
        ))
    }
}

struct U16FieldCodec;
impl FieldCodec for U16FieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_u64() {
                SMPPMessage::write_u16(n as u16, buffer);
                return Ok(());
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode u16",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if let Some(n) = SMPPMessage::read_u16(buffer) {
            Ok(Value::Number(serde_json::Number::from(n)))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to read u16 from buffer",
            ))
        }
    }
}

struct U32FieldCodec;
impl FieldCodec for U32FieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_u64() {
                SMPPMessage::write_u32(n as u32, buffer);
                return Ok(());
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode u32",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if let Some(n) = SMPPMessage::read_u32(buffer) {
            Ok(Value::Number(serde_json::Number::from(n)))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to read u32 from buffer",
            ))
        }
    }
}

struct ArrayFieldCodec {
    item_codec: Box<dyn FieldCodec + Send + Sync>,
}

impl ArrayFieldCodec {
    fn new(item_codec: Box<dyn FieldCodec + Send + Sync>) -> Self {
        ArrayFieldCodec { item_codec }
    }
}

impl FieldCodec for ArrayFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Array(arr) = value {
            if arr.len() > 255 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Array length exceeds maximum of 255",
                ));
            }
            SMPPMessage::write_u8(arr.len() as u8, buffer); // Write the array length as a single byte
            for item in arr {
                self.item_codec.encode(item, buffer)?; // Encode each item using the provided codec
            }
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Expected an array for encoding",
            ))
        }
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if let Some(length) = SMPPMessage::read_u8(buffer) {
            let mut items = Vec::new();
            for _ in 0..length {
                items.push(self.item_codec.decode(buffer)?); // Decode each item using the provided codec
            }
            Ok(Value::Array(items))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to read array length from buffer",
            ))
        }
    }
}
struct CompositeFieldCodec {
    fields: Vec<(&'static str, Box<dyn FieldCodec + Send + Sync>)>,
}

impl CompositeFieldCodec {
    fn new(fields: Vec<(&'static str, Box<dyn FieldCodec + Send + Sync>)>) -> Self {
        CompositeFieldCodec { fields }
    }
}

impl FieldCodec for CompositeFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        for (name, codec) in &self.fields {
            if let Some(field_value) = value.get(*name) {
                codec.encode(field_value, buffer)?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Missing field '{}' for encoding", name),
                ));
            }
        }
        Ok(())
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        let mut map = serde_json::Map::new();
        for (name, codec) in &self.fields {
            if let Ok(value) = codec.decode(buffer) {
                map.insert(name.to_string(), value);
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Failed to decode field '{}'", name),
                ));
            }
        }
        Ok(Value::Object(map))
    }
}

struct DestAddressFieldCodec;

impl FieldCodec for DestAddressFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        if let Value::Object(map) = value {
            let dest_addr_ton = map
                .get("dest_addr_ton")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u8;
            let dest_addr_npi = map
                .get("dest_addr_npi")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u8;
            if let Some(Value::String(addr)) = map.get("destination_addr") {
                SMPPMessage::write_u8(1, buffer); // dest_flag is 1
                SMPPMessage::write_u8(dest_addr_ton, buffer);
                SMPPMessage::write_u8(dest_addr_npi, buffer);
                SMPPMessage::write_c_octet_str(addr, buffer);
                return Ok(());
            } else if let Some(Value::String(addr)) = value.get("dl_name") {
                SMPPMessage::write_u8(2, buffer); // dest_flag is 2
                SMPPMessage::write_c_octet_str(addr, buffer);
                return Ok(());
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode dest_addr",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        if let Some(dest_flag) = SMPPMessage::read_u8(buffer) {
            match dest_flag {
                1 => {
                    if let Some(ton) = SMPPMessage::read_u8(buffer) {
                        if let Some(npi) = SMPPMessage::read_u8(buffer) {
                            if let Some(addr) = SMPPMessage::read_c_octet_str(buffer) {
                                let mut map = serde_json::Map::new();
                                map.insert(
                                    "dest_addr_ton".to_string(),
                                    Value::Number(serde_json::Number::from(ton)),
                                );
                                map.insert(
                                    "dest_addr_npi".to_string(),
                                    Value::Number(serde_json::Number::from(npi)),
                                );
                                map.insert("destination_addr".to_string(), Value::String(addr));
                                return Ok(Value::Object(map));
                            }
                        }
                    }
                }
                2 => {
                    if let Some(addr) = SMPPMessage::read_c_octet_str(buffer) {
                        let mut map = serde_json::Map::new();
                        map.insert("dl_name".to_string(), Value::String(addr));
                        return Ok(Value::Object(map));
                    }
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Unknown dest_flag value: {}", dest_flag),
                    ));
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to decode dest_addr from buffer",
        ))
    }
}

struct DestAddressesFieldCodec;

impl FieldCodec for DestAddressesFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        let dest_address_field_codec = DestAddressFieldCodec {};
        if let Value::Array(arr) = value {
            SMPPMessage::write_u8(arr.len() as u8, buffer); // number_of_dests

            for item in arr {
                dest_address_field_codec.encode(item, buffer)?;
            }
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode dest_addresses",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        let dest_address_field_codec = DestAddressFieldCodec {};
        if let Some(num_dests) = SMPPMessage::read_u8(buffer) {
            let mut dests = Vec::new();
            for _ in 0..num_dests {
                if let Ok(dest) = dest_address_field_codec.decode(buffer) {
                    dests.push(dest);
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Failed to decode one of the dest_addresses",
                    ));
                }
            }
            return Ok(Value::Array(dests));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to decode dest_addresses",
        ))
    }
}

pub struct TLVFieldCodec {
    pub tag: u16,
    pub name: &'static str,
    value_codec: Box<dyn FieldCodec + Send + Sync>,
}

impl TLVFieldCodec {
    fn new(tag: u16, name: &'static str, value_codec: Box<dyn FieldCodec + Send + Sync>) -> Self {
        TLVFieldCodec {
            tag,
            name,
            value_codec,
        }
    }
}

impl FieldCodec for TLVFieldCodec {
    fn encode(&self, value: &Value, buffer: &mut BytesMut) -> std::io::Result<()> {
        let mut temp_buffer = BytesMut::new();
        if self.value_codec.encode(value, &mut temp_buffer).is_ok() {
            SMPPMessage::write_u16(self.tag, buffer);
            SMPPMessage::write_u16(temp_buffer.len() as u16, buffer);
            buffer.extend_from_slice(&temp_buffer.to_vec());
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Failed to encode TLVField",
        ))
    }

    fn decode(&self, buffer: &mut BytesMut) -> std::io::Result<Value> {
        match SMPPMessage::read_tlv(buffer) {
            Some((tag, mvalue_bytes)) if tag == self.tag => {
                let mut temp_buffer = BytesMut::from(&mvalue_bytes[..]);
                self.value_codec.decode(&mut temp_buffer)
            }
            Some((tag, _)) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Expected TLV tag {:04X} but found {:04X}", self.tag, tag),
            )),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to read TLV from buffer",
            )),
        }
    }
}

pub static NAMED_TLV_FIELDS: LazyLock<HashMap<&'static str, Box<TLVFieldCodec>>> =
    LazyLock::new(|| create_named_tlv_fields());

fn create_tlv_fields() -> Vec<Box<TLVFieldCodec>> {
    let mut fields: Vec<Box<TLVFieldCodec>> = Vec::new();

    // Message delivery TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x001D,
        "additional_status_info_text",
        Box::new(COctetStrFieldCodec::new(256)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x130C,
        "alert_on_message_delivery",
        Box::new(OptionalU8FieldCodec::new(0)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x060B,
        "billing_identification",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0606,
        "broadcast_area_identifier",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0608,
        "broadcast_area_success",
        Box::new(U8FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0602,
        "broadcast_content_type_info",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0600,
        "broadcast_channel_indicator",
        Box::new(U8FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0601,
        "broadcast_content_type",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0609,
        "broadcast_end_time",
        Box::new(COctetStrFieldCodec::new(16)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0607,
        "broadcast_error_status",
        Box::new(U32FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0605,
        "broadcast_frequency_interval",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0603,
        "broadcast_message_class",
        Box::new(U8FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0604,
        "broadcast_rep_num",
        Box::new(U16FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x060A,
        "broadcast_service_group",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    // Callback / receipt TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x0381,
        "callback_num",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0303,
        "callback_num_atag",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0302,
        "callback_num_pres_ind",
        Box::new(U8FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0428,
        "congestion_state",
        Box::new(U8FieldCodec {}),
    )));

    // Delivery failure reason
    fields.push(Box::new(TLVFieldCodec::new(
        0x0425,
        "delivery_failure_reason",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0613,
        "dest_addr_np_country",
        Box::new(VarLengthIntFieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0612,
        "dest_addr_np_information",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0611,
        "dest_addr_np_resolution",
        Box::new(U8FieldCodec {}),
    )));

    // Destination TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x0005,
        "dest_addr_subunit",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0007,
        "dest_bearer_type",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x060E,
        "dest_network_id",
        Box::new(COctetStrFieldCodec::new(65)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0006,
        "dest_network_type",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0610,
        "dest_node_id",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0203,
        "dest_subaddress",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0008,
        "dest_telematics_id",
        Box::new(U16FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x020B,
        "dest_port",
        Box::new(U16FieldCodec {}),
    )));

    // Display / validity TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x1201,
        "display_time",
        Box::new(U8FieldCodec {}),
    )));

    // DPF (Delivery Pending Flag)
    fields.push(Box::new(TLVFieldCodec::new(
        0x0420,
        "dpf_result",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x1380,
        "its_reply_type",
        Box::new(U8FieldCodec {}),
    )));

    // ITS session info
    fields.push(Box::new(TLVFieldCodec::new(
        0x1383,
        "its_session_info",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x020D,
        "language_indicator",
        Box::new(U8FieldCodec {}),
    )));

    // Message content TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x0424,
        "message_payload",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0427,
        "message_state",
        Box::new(U8FieldCodec {}),
    )));

    // More info to send
    fields.push(Box::new(TLVFieldCodec::new(
        0x0426,
        "more_messages_to_send",
        Box::new(U8FieldCodec {}),
    )));

    // MS availability status
    fields.push(Box::new(TLVFieldCodec::new(
        0x0422,
        "ms_availability_status",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0030,
        "ms_msg_wait_facilities",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x1204,
        "ms_validity",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0423,
        "network_error_code",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    // Number of messages
    fields.push(Box::new(TLVFieldCodec::new(
        0x0304,
        "number_of_messages",
        Box::new(U8FieldCodec {}),
    )));

    // Payload type
    fields.push(Box::new(TLVFieldCodec::new(
        0x0019,
        "payload_type",
        Box::new(U8FieldCodec {}),
    )));

    // Privacy / language TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x0201,
        "privacy_indicator",
        Box::new(U8FieldCodec {}),
    )));

    // QOS time to live
    fields.push(Box::new(TLVFieldCodec::new(
        0x0017,
        "qos_time_to_live",
        Box::new(U32FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x001E,
        "receipted_message_id",
        Box::new(COctetStrFieldCodec::new(65)),
    )));

    // SAR (segmentation and reassembly) TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x020C,
        "sar_msg_ref_num",
        Box::new(U16FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x020F,
        "sar_segment_seqnum",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x020E,
        "sar_total_segments",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0210,
        "sc_interface_version",
        Box::new(U8FieldCodec {}),
    )));

    // Set DPF
    fields.push(Box::new(TLVFieldCodec::new(
        0x0421,
        "set_dpf",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x1203,
        "sms_signal",
        Box::new(U16FieldCodec {}),
    )));

    // Source TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x000D,
        "source_addr_subunit",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x000F,
        "source_bearer_type",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x060D,
        "source_network_id",
        Box::new(COctetStrFieldCodec::new(65)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x000E,
        "source_network_type",
        Box::new(U8FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x060F,
        "source_node_id",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x020A,
        "source_port",
        Box::new(U16FieldCodec {}),
    )));

    fields.push(Box::new(TLVFieldCodec::new(
        0x0202,
        "source_subaddress",
        Box::new(OctetFieldCodec::new(false, 1)),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0010,
        "source_telematics_id",
        Box::new(U8FieldCodec {}),
    )));

    // User data TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x0204,
        "user_message_reference",
        Box::new(U16FieldCodec {}),
    )));
    fields.push(Box::new(TLVFieldCodec::new(
        0x0205,
        "user_response_code",
        Box::new(U8FieldCodec {}),
    )));

    // USSD TLVs
    fields.push(Box::new(TLVFieldCodec::new(
        0x0501,
        "ussd_service_op",
        Box::new(U8FieldCodec {}),
    )));

    fields
}
fn create_named_tlv_fields() -> HashMap<&'static str, Box<TLVFieldCodec>> {
    let mut m: HashMap<&'static str, Box<TLVFieldCodec>> = HashMap::new();
    create_tlv_fields().into_iter().for_each(|codec| {
        m.insert(codec.name, codec);
    });
    m
}

pub static TAGGED_TLV_FIELDS: LazyLock<HashMap<u16, Box<TLVFieldCodec>>> =
    LazyLock::new(|| create_tagged_tlv_fields());

fn create_tagged_tlv_fields() -> HashMap<u16, Box<TLVFieldCodec>> {
    let mut m: HashMap<u16, Box<TLVFieldCodec>> = HashMap::new();
    create_tlv_fields().into_iter().for_each(|codec| {
        m.insert(codec.tag, codec);
    });
    m
}

pub static FIELD_CODECS: LazyLock<HashMap<&'static str, Box<dyn FieldCodec + Send + Sync>>> =
    LazyLock::new(|| create_field_codecs());

fn create_field_codecs() -> HashMap<&'static str, Box<dyn FieldCodec + Send + Sync>> {
    let mut m: HashMap<&'static str, Box<dyn FieldCodec + Send + Sync>> = HashMap::new();

    // Bind operations fields
    m.insert("system_id", Box::new(COctetStrFieldCodec::new(16)));
    m.insert("password", Box::new(COctetStrFieldCodec::new(9)));
    m.insert("system_type", Box::new(COctetStrFieldCodec::new(13)));
    m.insert("interface_version", Box::new(U8FieldCodec {}));
    m.insert("addr_ton", Box::new(U8FieldCodec {}));
    m.insert("addr_npi", Box::new(U8FieldCodec {}));
    m.insert("address_range", Box::new(COctetStrFieldCodec::new(41)));

    // submit_sm / deliver_sm / data_sm fields
    m.insert("service_type", Box::new(COctetStrFieldCodec::new(6)));
    m.insert("source_addr_ton", Box::new(U8FieldCodec {}));
    m.insert("source_addr_npi", Box::new(U8FieldCodec {}));
    m.insert("source_addr", Box::new(COctetStrFieldCodec::new(21)));
    m.insert("dest_addr_ton", Box::new(U8FieldCodec {}));
    m.insert("dest_addr_npi", Box::new(U8FieldCodec {}));
    m.insert("destination_addr", Box::new(COctetStrFieldCodec::new(21)));
    m.insert("esm_class", Box::new(U8FieldCodec {}));
    m.insert("protocol_id", Box::new(U8FieldCodec {}));
    m.insert("priority_flag", Box::new(U8FieldCodec {}));
    m.insert(
        "schedule_delivery_time",
        Box::new(COctetStrFieldCodec::new(17)),
    );
    m.insert("validity_period", Box::new(COctetStrFieldCodec::new(17)));
    m.insert("registered_delivery", Box::new(U8FieldCodec {}));
    m.insert("replace_if_present_flag", Box::new(U8FieldCodec {}));
    m.insert("data_coding", Box::new(U8FieldCodec {}));
    m.insert("sm_default_msg_id", Box::new(U8FieldCodec {}));
    m.insert("sm_length", Box::new(U8FieldCodec {}));
    m.insert("short_message", Box::new(OctetFieldCodec::new(true, 1)));

    // submit_sm_resp / deliver_sm_resp / query_sm fields
    m.insert("message_id", Box::new(COctetStrFieldCodec::new(65)));

    // query_sm_resp fields
    m.insert("final_date", Box::new(COctetStrFieldCodec::new(17)));
    m.insert("message_state", Box::new(U8FieldCodec {}));
    m.insert("error_code", Box::new(U8FieldCodec {}));

    // submit_multi fields
    m.insert("number_of_dests", Box::new(U8FieldCodec {}));
    m.insert("dest_flag", Box::new(U8FieldCodec {}));
    m.insert("dest_addresses", Box::new(DestAddressesFieldCodec {}));
    m.insert("dl_name", Box::new(COctetStrFieldCodec::new(21)));

    // submit_multi_resp fields
    m.insert(
        "unsuccess_smes",
        Box::new(ArrayFieldCodec::new(Box::new(CompositeFieldCodec::new(
            vec![
                ("dest_addr_ton", Box::new(U8FieldCodec {})),
                ("dest_addr_npi", Box::new(U8FieldCodec {})),
                ("destination_addr", Box::new(COctetStrFieldCodec::new(21))),
                ("error_status_code", Box::new(U32FieldCodec {})),
            ],
        )))),
    );

    // alert_notification fields
    m.insert("ms_availability_status", Box::new(U8FieldCodec {}));
    m.insert("esme_addr_ton", Box::new(U8FieldCodec {}));
    m.insert("esme_addr_npi", Box::new(U8FieldCodec {}));
    m.insert("esme_addr", Box::new(COctetStrFieldCodec::new(65)));

    m
}
