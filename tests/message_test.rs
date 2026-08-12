use base64::prelude::*;
use bytes::BytesMut;
use rest_smpp_herald::command_ids::CommandId;
use rest_smpp_herald::field_codec::{FIELD_CODECS, FieldCodec, NAMED_TLV_FIELDS};
use rest_smpp_herald::message::{MESSAGE_COMMAND_FIELDS, SMPPMessage};
use serde_json::{Value, json};

#[test]
fn test_bind_transmitter_codec() {
    let message = json!({
        "command_id": CommandId::BindTransmitter.as_u32(), // bind_transmitter
        "system_id": "test_system",
        "password": "test_pass",
        "system_type": "SMPP",
        "interface_version": 0x34,
        "addr_ton": 0,
        "addr_npi": 0,
        "address_range": "",
    });

    let mut encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = SMPPMessage::to_json(&mut encoded).unwrap();

    assert_eq!(
        decoded.get("system_id").unwrap(),
        &Value::String("test_system".to_string())
    );
    assert_eq!(
        decoded.get("password").unwrap(),
        &Value::String("test_pass".to_string())
    );
    assert_eq!(
        decoded.get("system_type").unwrap(),
        &Value::String("SMPP".to_string())
    );
    assert_eq!(
        decoded.get("interface_version").unwrap(),
        &Value::Number(0x34.into())
    );
}

#[test]
fn test_bind_transmitter_resp_codec() {
    let message = json!({
        "command_id": CommandId::BindTransmitterResp.as_u32(), // bind_transmitter_resp
        "system_id": "test_system",
    });

    let mut encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = SMPPMessage::to_json(&mut encoded).unwrap();

    assert_eq!(
        decoded.get("system_id").unwrap(),
        &Value::String("test_system".to_string())
    );
}

#[test]
fn test_outbind_codec() {
    let message = json!({
        "command_id": CommandId::Outbind.as_u32(), // outbind
        "system_id": "test_system",
        "password": "test_pass",
    });

    let mut encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = SMPPMessage::to_json(&mut encoded).unwrap();

    assert_eq!(
        decoded.get("system_id").unwrap(),
        &Value::String("test_system".to_string())
    );
    assert_eq!(
        decoded.get("password").unwrap(),
        &Value::String("test_pass".to_string())
    );
}

#[test]
fn test_alert_notification_codec() {
    let message = json!({
        "command_id": CommandId::AlertNotification.as_u32(), // alert_notification
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "esme_addr_ton": 1,
        "esme_addr_npi": 1,
        "esme_addr": "54321",
        "ms_availability_status": 0,
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
    assert_eq!(
        decoded.get("esme_addr").unwrap(),
        &Value::String("54321".to_string())
    );
    assert_eq!(
        decoded.get("esme_addr_ton").unwrap(),
        &Value::Number(1.into())
    );
    assert_eq!(
        decoded.get("esme_addr_npi").unwrap(),
        &Value::Number(1.into())
    );
}

#[test]
fn test_submit_sm_codec() {
    let message = json!({
        "command_id": CommandId::SubmitSm.as_u32(), // submit_sm
        "service_type": "test",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "54321",
        "esm_class": 0,
        "protocol_id": 0,
        "priority_flag": 0,
        "schedule_delivery_time": "",
        "validity_period": "",
        "registered_delivery": 0,
        "replace_if_present_flag": 0,
        "data_coding": 0,
        "sm_default_msg_id": 0,
        "short_message": BASE64_STANDARD.encode("Hello, World!")
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("service_type").unwrap(),
        &Value::String("test".to_string())
    );
    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
    assert_eq!(
        decoded.get("destination_addr").unwrap(),
        &Value::String("54321".to_string())
    );
    assert_eq!(
        decoded.get("short_message").unwrap(),
        &Value::String(BASE64_STANDARD.encode("Hello, World!"))
    );
}

#[test]
fn test_submit_sm_resp_codec() {
    let message = json!({
        "command_id": CommandId::SubmitSmResp.as_u32(), // submit_sm_resp
        "message_id": "msg12345",
    });
    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
}

#[test]
fn test_data_sm_codec() {
    let message = json!({
        "command_id": CommandId::DataSm.as_u32(), // data_sm
        "service_type": "test",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "54321",
        "esm_class": 0,
        "registered_delivery": 0,
        "data_coding": 0,
    });
    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("service_type").unwrap(),
        &Value::String("test".to_string())
    );
    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
    assert_eq!(
        decoded.get("destination_addr").unwrap(),
        &Value::String("54321".to_string())
    );
    assert_eq!(decoded.get("esm_class").unwrap(), &Value::Number(0.into()));
    assert_eq!(
        decoded.get("registered_delivery").unwrap(),
        &Value::Number(0.into())
    );
    assert_eq!(
        decoded.get("data_coding").unwrap(),
        &Value::Number(0.into())
    );
}

#[test]
fn test_data_sm_resp_codec() {
    let message = json!({
        "command_id": CommandId::DataSmResp.as_u32(), // data_sm_resp
        "message_id": "msg12345",
    });
    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
}
#[test]
fn test_tlv_alert_on_message_delivery() {
    match NAMED_TLV_FIELDS.get("alert_on_message_delivery") {
        Some(tlv) => {
            let mut buffer = BytesMut::new();
            SMPPMessage::write_u16(0x130C, &mut buffer); // tag
            SMPPMessage::write_u16(1, &mut buffer); // length
            SMPPMessage::write_u8(1, &mut buffer); // value

            let r = tlv.decode(&mut buffer);
            assert!(r.is_ok());
            assert_eq!(r.unwrap(), Value::Number(1.into()));
        }
        None => {
            assert!(false);
        }
    }
}

#[test]
fn test_broadcast_content_type_codec() {
    match NAMED_TLV_FIELDS.get("broadcast_content_type") {
        Some(tlv) => {
            let mut buffer = BytesMut::new();
            SMPPMessage::write_u16(0x0601, &mut buffer); // tag
            SMPPMessage::write_u16(3, &mut buffer); // length
            SMPPMessage::write_octet("123", &mut buffer); // value

            let r = tlv.decode(&mut buffer);
            print!("Decoded value: {:?}", r);
            assert!(r.is_ok());
            assert_eq!(r.unwrap(), Value::String(BASE64_STANDARD.encode("123")));
        }
        None => {
            assert!(false);
        }
    }
}

#[test]
fn test_dest_addr_np_country_codec() {
    match NAMED_TLV_FIELDS.get("dest_addr_np_country") {
        Some(tlv) => {
            let mut buffer = BytesMut::new();
            SMPPMessage::write_u16(0x0613, &mut buffer); // tag
            SMPPMessage::write_u16(3, &mut buffer); // length
            SMPPMessage::write_octet("123", &mut buffer); // value

            let r = tlv.decode(&mut buffer);
            print!("Decoded value: {:?}", r);
            assert!(r.is_ok());
            assert_eq!(r.unwrap(), Value::Number(3224115.into()));
        }
        None => {
            assert!(false);
        }
    }
}

#[test]
fn test_submit_multi_codec() {
    let message = json!({
        "command_id": CommandId::SubmitMulti.as_u32(), // submit_multi
        "service_type": "test_service",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "dest_addresses": [
            {
                "dest_addr_ton": 1,
                "dest_addr_npi": 1,
                "destination_addr": "54321"
            },
            {
                "dl_name": "dl_name-12345"
            }
        ],
        "esm_class": 0,
        "protocol_id": 0,
        "priority_flag": 0,
        "schedule_delivery_time": "",
        "validity_period": "",
        "registered_delivery": 0,
        "replace_if_present_flag": 0,
        "data_coding": 0,
        "sm_default_msg_id": 0,
        "short_message": BASE64_STANDARD.encode("Hello, World!")
    });

    let r = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
}

#[test]
fn test_submit_multi_resp_codec() {
    let message = json!({
        "command_id": CommandId::SubmitMultiResp.as_u32(), // submit_multi_resp
        "message_id": "msg12345",
        "unsuccess_smes": [
            {
                "dest_addr_ton": 1,
                "dest_addr_npi": 1,
                "destination_addr": "54321",
                "error_status_code": 0x00000000
            },
            {
                "dest_addr_ton": 2,
                "dest_addr_npi": 3,
                "destination_addr": "78901",
                "error_status_code": 0x00000001
            }
        ]
    });

    let r = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
}

#[test]
fn test_deliver_sm_codec() {
    let message = json!({
        "command_id": CommandId::DeliverSm.as_u32(), // deliver_sm
        "service_type": "test_service",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "54321",
        "esm_class": 0,
        "protocol_id": 0,
        "priority_flag": 0,
        "schedule_delivery_time": "",
        "validity_period": "",
        "registered_delivery": 0,
        "replace_if_present_flag": 0,
        "data_coding": 0,
        "sm_default_msg_id": 0,
        "short_message": BASE64_STANDARD.encode("Hello, World!")
    });
    let r = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
}

#[test]
fn test_deliver_sm_resp_codec() {
    let message = json!({
        "command_id": CommandId::DeliverSmResp.as_u32(), // deliver_sm_resp
        "message_id": "msg12345",
        "command_status": 0,
        "sequence_number": 1,
    });
    let r = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
}

#[test]
fn test_deliver_sm_resp_with_empty_message_id_codec() {
    let message = json!({
        "command_id": CommandId::DeliverSmResp.as_u32(), // deliver_sm_resp
        "command_status": 0,
        "sequence_number": 1,
        "message_id": "",
    });
    let r: Result<SMPPMessage, std::io::Error> = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
}

#[test]
fn test_broadcast_sm_codec() {
    let message = json!({
        "command_id": CommandId::BroadcastSm.as_u32(), // broadcast_sm
        "service_type": "test_service",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "message_id": "msg12345",
        "priority_flag": 0,
        "schedule_delivery_time": "",
        "validity_period": "",
        "replace_if_present_flag": 0,
        "data_coding": 0,
        "sm_default_msg_id": 0,
        "broadcast_area_identifier": BASE64_STANDARD.encode("area123"),
        "broadcast_content_type": BASE64_STANDARD.encode("text"),
        "broadcast_rep_num": 1,
        "broadcast_frequency_interval": BASE64_STANDARD.encode("123"),
    });
    let r = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
}

#[test]
fn test_cancel_sm_codec() {
    let message = json!({
        "command_id": CommandId::CancelSm.as_u32(), // cancel_sm
        "service_type": "test",
        "message_id": "msg12345",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "54321",
    });

    let r = SMPPMessage::from_json(&message);
    assert!(r.is_ok());
    let message = r.unwrap();
    println!("Encoded message: {:?}", message.to_hex());
    println!("{:?}", message.to_json().unwrap());
    assert_eq!(
        message.to_json().unwrap().get("service_type").unwrap(),
        &Value::String("test".to_string())
    );
    assert_eq!(
        message.to_json().unwrap().get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        message.to_json().unwrap().get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
    assert_eq!(
        message.to_json().unwrap().get("destination_addr").unwrap(),
        &Value::String("54321".to_string())
    );
}

#[test]
fn test_query_sm_codec() {
    let message = json!({
        "command_id": CommandId::QuerySm.as_u32(), // query_sm
        "message_id": "msg12345",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
}

#[test]
fn test_query_sm_resp_codec() {
    let message = json!({
        "command_id": CommandId::QuerySmResp.as_u32(), // query_sm_resp
        "message_id": "msg12345",
        "final_date": "",
        "message_state": 2,
        "error_code": 0,
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();
    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        decoded.get("message_state").unwrap(),
        &Value::Number(2.into())
    );
}

#[test]
fn test_replace_sm_codec() {
    let message = json!({
        "command_id": CommandId::ReplaceSm.as_u32(), // replace_sm
        "message_id": "msg12345",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
        "schedule_delivery_time": "",
        "validity_period": "",
        "registered_delivery": 0,
        "data_coding": 0,
        "sm_default_msg_id": 0,
        "short_message": BASE64_STANDARD.encode("Hello, World!")
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();
    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
    assert_eq!(
        decoded.get("short_message").unwrap(),
        &Value::String(BASE64_STANDARD.encode("Hello, World!"))
    );
}

#[test]
fn test_query_broadcast_sm_codec() {
    let message = json!({
        "command_id": CommandId::QueryBroadcastSm.as_u32(), // query_broadcast_sm
        "message_id": "msg12345",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();
    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
}

#[test]
fn test_query_broadcast_sm_resp_codec() {
    let message = json!({
        "command_id": CommandId::QueryBroadcastSmResp.as_u32(), // query_broadcast_sm_resp
        "message_id": "msg12345",
        "message_state": 2,
        "broadcast_area_identifier": BASE64_STANDARD.encode("area123"),
        "broadcast_area_success": 1,
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();
    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        decoded.get("message_state").unwrap(),
        &Value::Number(2.into())
    );
    assert_eq!(
        decoded.get("broadcast_area_identifier").unwrap(),
        &Value::String(BASE64_STANDARD.encode("area123"))
    );
    assert_eq!(
        decoded.get("broadcast_area_success").unwrap(),
        &Value::Number(1.into())
    );
}

#[test]
fn test_cancel_broadcast_sm_codec() {
    let message = json!({
        "command_id": CommandId::CancelBroadcastSm.as_u32(), // cancel_broadcast_sm
        "service_type": "test",
        "message_id": "msg12345",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "12345",
    });

    let encoded = SMPPMessage::from_json(&message).unwrap();
    let decoded = encoded.to_json().unwrap();

    assert_eq!(
        decoded.get("service_type").unwrap(),
        &Value::String("test".to_string())
    );
    assert_eq!(
        decoded.get("message_id").unwrap(),
        &Value::String("msg12345".to_string())
    );
    assert_eq!(
        decoded.get("source_addr").unwrap(),
        &Value::String("12345".to_string())
    );
}
#[test]
fn test_unknown_command_id() {
    let message = json!({
        "command_id": 0xDEADBEEF as u32, // Unknown command_id
        "system_id": "test_system",
        "password": "test_pass",
    });

    let r = SMPPMessage::from_json(&message);
    assert!(r.is_err());
}

#[test]
fn test_all_fields_are_defined() {
    for (command_id, fields) in MESSAGE_COMMAND_FIELDS.iter() {
        for (field_name, _default_value) in fields {
            assert!(
                FIELD_CODECS.contains_key(*field_name)
                    || NAMED_TLV_FIELDS.contains_key(*field_name),
                "Field '{}' for command_id 0x{:08X} does not have a codec defined",
                field_name,
                command_id
            );
        }
    }
}
