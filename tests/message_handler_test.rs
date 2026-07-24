use rest_smpp_herald::command_ids::CommandId;
use rest_smpp_herald::config::{LoadBalancingAlgorithm, RequestHandlerConfig};
use rest_smpp_herald::message::SMPPMessage;
use rest_smpp_herald::message_handler::{HttpMessageHandler, MessageHandler};
use serde_json::json;

fn make_handler(
    dest_addresses: Vec<&str>,
    dest_address_patterns: Vec<&str>,
    urls: Vec<&str>,
) -> HttpMessageHandler {
    HttpMessageHandler::new(
        dest_addresses.into_iter().map(String::from).collect(),
        dest_address_patterns
            .into_iter()
            .map(String::from)
            .collect(),
        urls.into_iter().map(String::from).collect(),
        LoadBalancingAlgorithm::RoundRobin,
    )
}

fn make_empty_distribution_list() -> rest_smpp_herald::message::DistrubutionList {
    rest_smpp_herald::message::DistrubutionList::new(std::collections::HashMap::new())
}
// --- extract_dest_addresses tests ---

#[test]
fn test_extract_dest_addresses_submit_sm() {
    let msg = json!({
        "command_id": CommandId::SubmitSm.as_u32(),
        "destination_addr": "27811234567"
    });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();

    for addr in &addrs {
        assert_eq!(addr.address, "27811234567");
    }
}

#[test]
fn test_extract_dest_addresses_deliver_sm() {
    let msg = json!({
        "command_id": CommandId::DeliverSm.as_u32(),
        "destination_addr": "44712345678"
    });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();

    for addr in &addrs {
        assert_eq!(addr.address, "44712345678");
    }
}

#[test]
fn test_extract_dest_addresses_data_sm() {
    let msg = json!({
        "command_id": CommandId::DataSm.as_u32(),
        "destination_addr": "11234567890"
    });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();

    for addr in &addrs {
        assert_eq!(addr.address, "11234567890");
    }
}

#[test]
fn test_extract_dest_addresses_submit_multi() {
    let msg = json!({
        "command_id": CommandId::SubmitMulti.as_u32(),
        "dest_addresses": [
            { "destination_addr": "27811111111" },
            { "destination_addr": "27822222222" },
            { "dl_name": "group1" }
        ]
    });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();

    for addr in &addrs {
        assert_eq!(addr.address, "27811111111");
    }
}

#[test]
fn test_extract_dest_addresses_missing_destination() {
    let msg = json!({
        "command_id": CommandId::SubmitSm.as_u32()
    });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();
    assert!(addrs.is_empty());
}

#[test]
fn test_extract_dest_addresses_unknown_command() {
    let msg = json!({
        "command_id": CommandId::BindTransmitter.as_u32(),
        "destination_addr": "27811234567"
    });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();
    assert!(addrs.is_empty());
}

#[test]
fn test_extract_dest_addresses_no_command_id() {
    let msg = json!({ "destination_addr": "27811234567" });
    let addrs = SMPPMessage::extract_destionation_addresses(&msg, &make_empty_distribution_list());
    assert!(addrs.is_ok());
    let addrs = addrs.unwrap();
    assert!(addrs.is_empty());
}

// --- match_any_address tests ---

#[test]
fn test_match_any_address_found() {
    let handler = make_handler(vec!["27811234567", "27822222222"], vec![], vec![]);
    let addr = "27811234567".to_string();
    assert!(handler.match_any_address(&addr));
}

#[test]
fn test_match_any_address_not_found() {
    let handler = make_handler(vec!["27811234567"], vec![], vec![]);
    let addr = "99999999999".to_string();
    assert!(!handler.match_any_address(&addr));
}

#[test]
fn test_match_any_address_empty_handler_addresses() {
    let handler = make_handler(vec![], vec![], vec![]);
    let addr = "27811234567".to_string();
    assert!(!handler.match_any_address(&addr));
}

#[test]
fn test_match_any_address_multiple_input_addresses() {
    let handler = make_handler(vec!["27822222222"], vec![], vec![]);
    let addrs = vec!["27811111111".to_string(), "27822222222".to_string()];
    for addr in addrs {
        if handler.match_any_address(&addr) {
            assert_eq!(addr, "27822222222");
        }
    }
}

// --- match_any_pattern tests ---

#[test]
fn test_match_any_pattern_matches() {
    let handler = make_handler(vec![], vec!["^2781.*"], vec![]);
    let addr = "27811234567".to_string();
    assert!(handler.match_any_pattern(&addr));
}

#[test]
fn test_match_any_pattern_no_match() {
    let handler = make_handler(vec![], vec!["^2781.*"], vec![]);
    let addr = "44712345678".to_string();
    assert!(!handler.match_any_pattern(&addr));
}

#[test]
fn test_match_any_pattern_multiple_patterns() {
    let handler = make_handler(vec![], vec!["^2781.*", "^4471.*"], vec![]);
    let addr = "44712345678".to_string();
    assert!(handler.match_any_pattern(&addr));
}

#[test]
fn test_match_any_pattern_invalid_regex_skipped() {
    let handler = make_handler(vec![], vec!["[invalid", "^2781.*"], vec![]);
    let addr = "27811234567".to_string();
    assert!(handler.match_any_pattern(&addr));
}

#[test]
fn test_match_any_pattern_empty_patterns() {
    let handler = make_handler(vec![], vec![], vec![]);
    let addr = "27811234567".to_string();
    assert!(!handler.match_any_pattern(&addr));
}

// --- is_my_request_address tests ---

#[test]
fn test_is_my_request_catches_all_when_no_filters() {
    let handler = make_handler(vec![], vec![], vec!["http://localhost:8080"]);
    assert!(handler.is_my_request_address("27811234567"));
}

#[test]
fn test_is_my_request_matches_by_address() {
    let handler = make_handler(vec!["27811234567"], vec![], vec!["http://localhost:8080"]);
    assert!(handler.is_my_request_address("27811234567"));
}

#[test]
fn test_is_my_request_no_match_by_address() {
    let handler = make_handler(vec!["27811234567"], vec![], vec!["http://localhost:8080"]);
    assert!(!handler.is_my_request_address("99999999999"));
}

#[test]
fn test_is_my_request_matches_by_pattern() {
    let handler = make_handler(vec![], vec!["^2781.*"], vec!["http://localhost:8080"]);
    assert!(handler.is_my_request_address("27811234567"));
}

#[test]
fn test_is_my_request_no_dest_addresses_in_message() {
    let handler = make_handler(vec!["27811234567"], vec![], vec!["http://localhost:8080"]);
    assert!(!handler.is_my_request_address(""));
}

#[test]
fn test_is_my_request_submit_multi() {
    let handler = make_handler(vec!["27822222222"], vec![], vec!["http://localhost:8080"]);
    assert!(handler.is_my_request_address("27822222222"));
}

// --- handle_message tests ---

#[tokio::test]
async fn test_handle_message_no_urls() {
    let handler = make_handler(vec![], vec![], vec![]);
    let msg = json!({ "command_id": CommandId::SubmitSm.as_u32() });
    let result = handler.handle_message(&msg).await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "No handler URLs configured"
    );
}

#[tokio::test]
async fn test_handle_message_all_urls_fail() {
    let handler = make_handler(vec![], vec![], vec!["http://127.0.0.1:1/nonexistent"]);
    let msg = json!({ "command_id": CommandId::SubmitSm.as_u32() });
    let result = handler.handle_message(&msg).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "All handler URLs failed");
}

// --- create_from tests ---

#[test]
fn test_create_from_config() {
    let config = RequestHandlerConfig {
        dest_addresses: Some(vec!["27811234567".to_string()]),
        dest_address_patterns: Some(vec!["^2782.*".to_string()]),
        urls: vec!["http://localhost:8080".to_string()],
        algorithm: Some("failover".to_string()),
    };
    let handler = HttpMessageHandler::create_from(&config);
    assert_eq!(handler.dest_addresses, vec!["27811234567"]);
    assert_eq!(handler.urls, vec!["http://localhost:8080"]);
    assert!(matches!(
        handler.algorithm,
        LoadBalancingAlgorithm::FailOver
    ));
}

#[test]
fn test_create_from_config_defaults() {
    let config = RequestHandlerConfig {
        dest_addresses: None,
        dest_address_patterns: None,
        urls: vec!["http://localhost:9090".to_string()],
        algorithm: None,
    };
    let handler = HttpMessageHandler::create_from(&config);
    assert!(handler.dest_addresses.is_empty());
    assert!(handler.dest_address_patterns.is_empty());
    assert!(matches!(
        handler.algorithm,
        LoadBalancingAlgorithm::RoundRobin
    ));
}
