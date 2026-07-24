use rest_smpp_herald::esm_class::{
    Ansi41Specific, EsmClass, GSMSpecific, MessageMode, MessageType,
};
use rest_smpp_herald::registered_delivery::{
    DeliveryReceipt, IntermediateNotification, RegisteredDelivery, SMEOriginatedAcknowledgement,
};
use rest_smpp_herald::smsc_client::delivery_receipt_manager::DeliveryReceiptManager;

#[tokio::test]
async fn register_skips_when_no_receipts_requested() {
    let manager = DeliveryReceiptManager::new();

    manager
        .register_receipt_url(
            "msg-no-receipt".to_string(),
            "http://localhost:8080/receipt".to_string(),
            RegisteredDelivery::default(),
        )
        .await;

    let url = manager
        .get_receipt_url("msg-no-receipt", &EsmClass::default())
        .await;

    assert!(url.is_none());
}

#[tokio::test]
async fn delivery_receipt_request_is_removed_after_delivery_receipt_arrives() {
    let manager = DeliveryReceiptManager::new();
    let registered_delivery = RegisteredDelivery::new(
        DeliveryReceipt::RequestedWhenDeliverySucessOrFailure,
        SMEOriginatedAcknowledgement::NoRequested,
        IntermediateNotification::NoRequested,
    );

    manager
        .register_receipt_url(
            "msg-delivery".to_string(),
            "http://localhost:8080/receipt".to_string(),
            registered_delivery,
        )
        .await;

    // A non-receipt event should not consume the registration.
    let first = manager
        .get_receipt_url("msg-delivery", &EsmClass::default())
        .await;
    assert_eq!(first.as_deref(), Some("http://localhost:8080/receipt"));

    let delivery_receipt_esm = EsmClass::new(
        MessageMode::Default,
        MessageType::DeliveryReceipt,
        Ansi41Specific::Unknown,
        GSMSpecific::NoSpecific,
    );
    let second = manager
        .get_receipt_url("msg-delivery", &delivery_receipt_esm)
        .await;
    assert_eq!(second.as_deref(), Some("http://localhost:8080/receipt"));

    // Once the requested receipt is observed, state should be removed.
    let third = manager
        .get_receipt_url("msg-delivery", &delivery_receipt_esm)
        .await;
    assert!(third.is_none());
}

#[tokio::test]
async fn remove_receipt_info_deletes_entry() {
    let manager = DeliveryReceiptManager::new();
    let registered_delivery = RegisteredDelivery::new(
        DeliveryReceipt::RequestedWhenDeliverySucessOrFailure,
        SMEOriginatedAcknowledgement::NoRequested,
        IntermediateNotification::NoRequested,
    );

    manager
        .register_receipt_url(
            "msg-remove".to_string(),
            "http://localhost:8080/receipt".to_string(),
            registered_delivery,
        )
        .await;

    manager.remove_receipt_info("msg-remove").await;

    let url = manager
        .get_receipt_url("msg-remove", &EsmClass::default())
        .await;
    assert!(url.is_none());
}

#[tokio::test]
async fn sme_ack_request_is_removed_after_ack_arrives() {
    let manager = DeliveryReceiptManager::new();
    let registered_delivery = RegisteredDelivery::new(
        DeliveryReceipt::NoRequested,
        SMEOriginatedAcknowledgement::DeliveryAckRequested,
        IntermediateNotification::NoRequested,
    );

    manager
        .register_receipt_url(
            "msg-ack".to_string(),
            "http://localhost:8080/receipt".to_string(),
            registered_delivery,
        )
        .await;

    let ack_esm = EsmClass::new(
        MessageMode::Default,
        MessageType::Default,
        Ansi41Specific::DeliveryAcknowledgement,
        GSMSpecific::NoSpecific,
    );

    // This call should return URL for current event and consume the registration.
    let first = manager.get_receipt_url("msg-ack", &ack_esm).await;
    assert_eq!(first.as_deref(), Some("http://localhost:8080/receipt"));

    let second = manager
        .get_receipt_url("msg-ack", &EsmClass::default())
        .await;
    assert!(second.is_none());
}
