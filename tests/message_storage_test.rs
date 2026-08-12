use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::{Duration, Local};
use rest_smpp_herald::message_storage::{
    DeliveryMessageInfo, DeliveryMessageInfoManager, InMemoryMessageStorage, MessageStorage,
    ScheduleDeliveryMessageHandler, SqlMessageStorage,
};
use serde_json::json;
use sqlx::Row;
use tokio::sync::Notify;

struct TestDeliveryHandler {
    calls: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

#[async_trait::async_trait]
impl ScheduleDeliveryMessageHandler for TestDeliveryHandler {
    async fn handle_schedule_delivery_message(
        &self,
        _message: serde_json::Value,
    ) -> std::io::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
        Ok(())
    }
}

// --- InMemoryMessageQueue Tests ---

#[tokio::test]
async fn in_memory_enqueue_returns_unique_id() {
    let storage = InMemoryMessageStorage::new(60);
    let msg = json!({"text": "hello", "destination_addr": "user1", "validity_period": "000000010000000R"});

    let message_id1 = "msg-1".to_string();
    let message_id2 = "msg-2".to_string();
    storage.save(&message_id1, &msg).await.unwrap();
    storage.save(&message_id2, &msg).await.unwrap();
    assert_ne!(message_id1, message_id2);
}

// --- SqliteMessageQueue Tests ---

async fn create_sqlite_storage() -> SqlMessageStorage {
    SqlMessageStorage::new("sqlite::memory:", 60).await.unwrap()
}

#[tokio::test]
async fn sqlite_enqueue_returns_unique_id() {
    let storage = create_sqlite_storage().await;
    let msg = json!({"text": "hello", "destination_addr": "user1", "validity_period": "000000010000000R"});
    let message_id1 = "msg-1".to_string();
    let message_id2 = "msg-2".to_string();
    storage.save(&message_id1, &msg).await.unwrap();
    storage.save(&message_id2, &msg).await.unwrap();
    assert_ne!(message_id1, message_id2);
}

// --- set_deliver_message_handler Tests ---

#[tokio::test]
async fn in_memory_set_deliver_message_handler_invokes_for_ready_message() {
    let storage = InMemoryMessageStorage::new(60);
    let calls = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    storage
        .set_schedule_delivery_message_handler(Box::new(TestDeliveryHandler {
            calls: Arc::clone(&calls),
            notify: Arc::clone(&notify),
        }))
        .await;

    let msg = json!({"text": "deliver-now", "destination_addr": "user1", "qos_time_to_live": "60"});
    let message_id = "msg-1".to_string();
    let _ = storage.save(&message_id, &msg).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
        .await
        .unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn in_memory_set_deliver_message_handler_waits_until_scheduled_time() {
    let storage = InMemoryMessageStorage::new(60);
    let calls = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    storage
        .set_schedule_delivery_message_handler(Box::new(TestDeliveryHandler {
            calls: Arc::clone(&calls),
            notify: Arc::clone(&notify),
        }))
        .await;

    let scheduled = (Local::now() + Duration::seconds(3))
        .format("%y%m%d%H%M%S000+")
        .to_string();
    let msg = json!({
        "text": "deliver-later",
        "destination_addr": "user1",
        "schedule_delivery_time": scheduled
    });
    let message_id = "msg-1".to_string();
    let _ = storage.save(&message_id, &msg).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    tokio::time::timeout(std::time::Duration::from_secs(6), notify.notified())
        .await
        .unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn sqlite_set_deliver_message_handler_invokes_for_ready_message() {
    let storage = create_sqlite_storage().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());

    storage
        .set_schedule_delivery_message_handler(Box::new(TestDeliveryHandler {
            calls: Arc::clone(&calls),
            notify: Arc::clone(&notify),
        }))
        .await;

    let msg = json!({"text": "deliver-now", "destination_addr": "user1", "qos_time_to_live": "60"});
    let message_id = "msg-1".to_string();
    let _ = storage.save(&message_id, &msg).await.unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
        .await
        .unwrap();
    assert!(calls.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn sqlite_cancel_message_deletes_only_matching_messages() {
    let storage = create_sqlite_storage().await;

    let msg1 = json!({
        "service_type": "svc-a",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "1111",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "2001",
        "short_message": "first",
        "qos_time_to_live": "120"
    });
    let msg2 = json!({
        "service_type": "svc-b",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "2222",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "2002",
        "short_message": "second",
        "qos_time_to_live": "120"
    });

    let message_id_1 = "cancel-target".to_string();
    let message_id_2 = "cancel-keep".to_string();
    storage.save(&message_id_1, &msg1).await.unwrap();
    storage.save(&message_id_2, &msg2).await.unwrap();

    let cancel_sm = json!({
        "message_id": "cancel-target",
        "source_addr": "1111"
    });

    let cancelled = storage.cancel_message(&cancel_sm).await.unwrap();
    assert!(cancelled);

    let row_1 = sqlx::query("SELECT COUNT(*) as c FROM message_queue WHERE message_id = $1")
        .bind("cancel-target")
        .fetch_one(storage.pool.as_ref())
        .await
        .unwrap();
    let count_1: i64 = row_1.get("c");
    assert_eq!(count_1, 0);

    let row_2 = sqlx::query("SELECT COUNT(*) as c FROM message_queue WHERE message_id = $1")
        .bind("cancel-keep")
        .fetch_one(storage.pool.as_ref())
        .await
        .unwrap();
    let count_2: i64 = row_2.get("c");
    assert_eq!(count_2, 1);
}

#[tokio::test]
async fn sqlite_replace_message_updates_stored_payload_fields() {
    let storage = create_sqlite_storage().await;

    let original = json!({
        "service_type": "svc-r",
        "source_addr_ton": 1,
        "source_addr_npi": 1,
        "source_addr": "3333",
        "dest_addr_ton": 1,
        "dest_addr_npi": 1,
        "destination_addr": "9001",
        "short_message": "before",
        "registered_delivery": 0,
        "qos_time_to_live": "120"
    });

    let message_id = "replace-target".to_string();
    storage.save(&message_id, &original).await.unwrap();

    let replace_sm = json!({
        "message_id": "replace-target",
        "source_addr": "3333",
        "short_message": "after",
        "registered_delivery": 1
    });

    let replaced = storage.replace_message(&replace_sm).await.unwrap();
    assert!(replaced);

    let row = sqlx::query("SELECT message FROM message_queue WHERE message_id = $1")
        .bind("replace-target")
        .fetch_one(storage.pool.as_ref())
        .await
        .unwrap();
    let message_json: String = row.get("message");
    let updated: serde_json::Value = serde_json::from_str(&message_json).unwrap();

    assert_eq!(updated["short_message"], json!("after"));
    assert_eq!(updated["registered_delivery"], json!(1));
}

#[test]
fn delivery_message_info_new_sets_fields() {
    let now = Local::now();
    let item = DeliveryMessageInfo::new(
        "m-1".to_string(),
        Some(now),
        Some(now + Duration::minutes(1)),
        None,
    );

    assert_eq!(item.message_id, "m-1");
    assert_eq!(item.schedule_delivery_time, Some(now));
    assert_eq!(item.validity_period, Some(now + Duration::minutes(1)));
}

#[test]
fn delivery_message_info_eq_uses_message_id() {
    let now = Local::now();
    let a = DeliveryMessageInfo::new("same-id".to_string(), Some(now), None, None);
    let b = DeliveryMessageInfo::new(
        "same-id".to_string(),
        Some(now + Duration::minutes(10)),
        Some(now + Duration::minutes(20)),
        None,
    );

    assert!(a != b);
}

#[test]
fn delivery_message_info_orders_by_schedule_delivery_time() {
    let now = Local::now();
    let earlier = DeliveryMessageInfo::new(
        "a".to_string(),
        Some(now + Duration::minutes(1)),
        Some(now + Duration::hours(1)),
        None,
    );
    let later = DeliveryMessageInfo::new(
        "b".to_string(),
        Some(now + Duration::minutes(2)),
        Some(now + Duration::hours(1)),
        None,
    );

    assert!(earlier < later);
}

#[tokio::test]
async fn delivery_message_info_manager_get_on_empty_returns_error() {
    let manager = DeliveryMessageInfoManager::new();
    let result = manager.get_delivery_message().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn delivery_message_info_manager_add_then_get_returns_message() {
    let manager = DeliveryMessageInfoManager::new();
    let item = DeliveryMessageInfo::new("m-1".to_string(), None, None, None);

    manager.add_delivery_message(item.clone()).await;
    let result = manager.get_delivery_message().await.unwrap();

    assert_eq!(result.message_id, item.message_id);
}

#[tokio::test]
async fn delivery_message_info_manager_add_then_get_returns_earliest() {
    let manager = DeliveryMessageInfoManager::new();
    let item1 = DeliveryMessageInfo::new(
        "m-1".to_string(),
        Some(Local::now() + Duration::seconds(60)),
        None,
        None,
    );
    let item2 = DeliveryMessageInfo::new("m-2".to_string(), None, None, None);

    manager.add_delivery_message(item1.clone()).await;
    manager.add_delivery_message(item2.clone()).await;
    let result = manager.get_delivery_message().await.unwrap();

    assert_eq!(result.message_id, item2.message_id);
}

#[tokio::test]
async fn delivery_message_info_manager_add_then_get_returns_earliest_with_none_schedule() {
    let manager = DeliveryMessageInfoManager::new();
    let item1 = DeliveryMessageInfo::new(
        "m-1".to_string(),
        Some(Local::now() + Duration::seconds(60)),
        None,
        None,
    );
    let item2 = DeliveryMessageInfo::new(
        "m-2".to_string(),
        Some(Local::now() + Duration::seconds(2)),
        None,
        None,
    );

    manager.add_delivery_message(item1.clone()).await;
    manager.add_delivery_message(item2.clone()).await;
    assert!(manager.get_delivery_message().await.is_err());
    tokio::time::sleep(Duration::seconds(3).to_std().unwrap()).await;
    let result = manager.get_delivery_message().await.unwrap();

    assert_eq!(result.message_id, item2.message_id);
}
