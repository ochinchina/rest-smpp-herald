use std::{ops::Add, sync::Arc};

use chrono::{DateTime, Duration, Local};

use log::{error, info};
use redis::AsyncCommands;
use serde_json::Value;
use sqlx::Row;
use tokio::{
    select,
    sync::Mutex,
    time::{Duration as TokioDuration, interval},
};

use crate::{
    config::{Address, MessageStorageConfig},
    field_codec::parse_absolute_relative_time,
};

pub fn get_schedule_delivery_time(value: &Value) -> Result<DateTime<Local>, String> {
    if let Some(schedule_delivery_time_str) =
        value.get("schedule_delivery_time").and_then(|v| v.as_str())
    {
        parse_absolute_relative_time(schedule_delivery_time_str)
    } else {
        Err("schedule_delivery_time not found in the message".to_string())
    }
}

pub fn get_validity_period(
    value: &Value,
    default_validity_period: u32,
) -> Result<DateTime<Local>, String> {
    if let Some(validity_period_str) = value.get("validity_period").and_then(|v| v.as_str()) {
        parse_absolute_relative_time(validity_period_str)
    } else if let Some(qos_time_to_live) = value.get("qos_time_to_live").and_then(|v| v.as_str()) {
        Ok(Local::now().add(Duration::seconds(
            qos_time_to_live.parse::<i64>().unwrap_or(0),
        )))
    } else {
        match get_schedule_delivery_time(value) {
            Ok(dt) => Ok(dt + Duration::seconds(default_validity_period as i64)),
            Err(_) => Ok(Local::now() + Duration::seconds(default_validity_period as i64)),
        }
    }
}

pub fn get_destination_address(value: &Value) -> Result<Address, String> {
    let destination_addr = value.get("destination_addr").and_then(|v| v.as_str());
    let dest_addr_ton = value.get("dest_addr_ton").and_then(|v| v.as_u64());
    let dest_addr_npi = value.get("dest_addr_npi").and_then(|v| v.as_u64());

    if destination_addr.is_none() || dest_addr_ton.is_none() || dest_addr_npi.is_none() {
        return Err("Missing destination address or its TON/NPI in the message".to_string());
    }
    Ok(Address {
        address: destination_addr.unwrap().to_string(),
        ton: dest_addr_ton.unwrap() as u8,
        npi: dest_addr_npi.unwrap() as u8,
    })
}

#[derive(Clone, Debug)]
pub struct DeliveryMessageInfo {
    pub message_id: String,
    pub schedule_delivery_time: Option<DateTime<Local>>,
    pub validity_period: Option<DateTime<Local>>,
    pub message: Option<Value>,
}

impl DeliveryMessageInfo {
    pub fn new(
        message_id: String,
        schedule_delivery_time: Option<DateTime<Local>>,
        validity_period: Option<DateTime<Local>>,
        message: Option<Value>,
    ) -> Self {
        DeliveryMessageInfo {
            message_id,
            schedule_delivery_time,
            validity_period,
            message,
        }
    }

    pub fn is_time_for_delivery(&self) -> bool {
        match self.schedule_delivery_time {
            Some(schedule_delivery_time) => schedule_delivery_time <= Local::now(),
            None => true,
        }
    }
}

impl PartialOrd for DeliveryMessageInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeliveryMessageInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.schedule_delivery_time.is_none() {
            if other.schedule_delivery_time.is_none() {
                match (self.validity_period, other.validity_period) {
                    (Some(self_validity), Some(other_validity)) => {
                        if self_validity < other_validity {
                            return std::cmp::Ordering::Less;
                        } else if self_validity > other_validity {
                            return std::cmp::Ordering::Greater;
                        } else {
                            return std::cmp::Ordering::Equal;
                        }
                    }
                    (Some(_), None) => return std::cmp::Ordering::Less,
                    (None, Some(_)) => return std::cmp::Ordering::Greater,
                    (None, None) => return std::cmp::Ordering::Equal,
                }
            }
        } else if other.schedule_delivery_time.is_none() {
            return std::cmp::Ordering::Greater;
        } else {
            let self_schedule = self.schedule_delivery_time.unwrap();
            let other_schedule = other.schedule_delivery_time.unwrap();
            if self_schedule < other_schedule {
                return std::cmp::Ordering::Less;
            } else if self_schedule > other_schedule {
                return std::cmp::Ordering::Greater;
            }
        }

        std::cmp::Ordering::Equal
    }
}

impl PartialEq for DeliveryMessageInfo {
    fn eq(&self, other: &Self) -> bool {
        self.message_id == other.message_id
            && self.schedule_delivery_time == other.schedule_delivery_time
            && self.validity_period == other.validity_period
            && self.message == other.message
    }
}

impl Eq for DeliveryMessageInfo {}

#[derive(Clone)]
pub struct DeliveryMessageInfoManager {
    delivery_messages: Arc<Mutex<Vec<DeliveryMessageInfo>>>,
}

impl DeliveryMessageInfoManager {
    pub fn new() -> Self {
        DeliveryMessageInfoManager {
            delivery_messages: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn add_delivery_message(&self, delivery_message: DeliveryMessageInfo) {
        let mut delivery_messages = self.delivery_messages.lock().await;

        let pos = match delivery_messages.binary_search(&delivery_message) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };

        println!(
            "Adding delivery message: {:?} at position {}",
            delivery_message.clone(),
            pos
        );

        delivery_messages.insert(pos, delivery_message);
    }

    pub async fn get_delivery_message(&self) -> Result<DeliveryMessageInfo, String> {
        let mut delivery_messages = self.delivery_messages.lock().await;

        if !delivery_messages.is_empty() {
            let delivery_message = delivery_messages[0].clone();
            if delivery_message.is_time_for_delivery() {
                delivery_messages.remove(0);
                return Ok(delivery_message);
            }
        }

        Err("No delivery messsage is available".to_string())
    }
}

#[async_trait::async_trait]
pub trait DeliveryMessageHandler: Send + Sync {
    async fn handle_delivery_message(&self, message: Value) -> std::io::Result<()>;
}

#[async_trait::async_trait]
pub trait MessageStorage: Send + Sync {
    /**
     * Saves a message to the storage and returns a unique message ID.
     */
    async fn save(&self, message_id: &String, message: &Value) -> std::io::Result<()>;

    /**
     * Sets the handler function that will be called to deliver messages. The handler should return true if the message was successfully delivered, and false otherwise.
     */
    async fn set_deliver_message_handler(
        &self,
        deliver_message_handler: Box<dyn DeliveryMessageHandler>,
    );

    /**
     * Cancels a message in the storage based on the provided cancel_sm command. Returns true if the message was successfully canceled, and false otherwise.
     */
    async fn cancel_message(&self, cancel_sm: &Value) -> std::io::Result<bool>;

    /**
     * Replaces a message in the storage based on the provided replace_sm command. Returns true if the message was successfully replaced, and false otherwise.
     */
    async fn replace_message(&self, message: &Value) -> std::io::Result<bool>;
}

#[derive(Clone)]
pub struct InMemoryMessageStorage {
    // mapping between the message id and the user, message with its schedule_delivery_time and validity_period
    messages: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<
                String,
                Vec<(
                    String,
                    Value,
                    Option<DateTime<Local>>,
                    Option<DateTime<Local>>,
                )>,
            >,
        >,
    >,
    // mapping between the user address and message id
    user_message_ids: Arc<tokio::sync::Mutex<std::collections::HashMap<String, Vec<String>>>>,

    default_validity_period: u32,
}

impl InMemoryMessageStorage {
    pub fn new(default_validity_period: u32) -> Self {
        InMemoryMessageStorage {
            messages: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            user_message_ids: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            default_validity_period,
        }
    }

    async fn remove_user_message_id(&self, user_address: &String, message_id: &String) {
        let mut user_message_ids = self.user_message_ids.lock().await;
        if let Some(ids) = user_message_ids.get_mut(user_address) {
            ids.retain(|id| id != message_id);
            if ids.is_empty() {
                user_message_ids.remove(user_address);
            }
        }
    }

    async fn remove_message_by_id(&self, message_id: &String, index: usize) {
        let mut messages = self.messages.lock().await;
        if let Some(message_list) = messages.get_mut(message_id) {
            message_list.remove(index);
            if message_list.is_empty() {
                messages.remove(message_id);
            }
        }
    }

    async fn clean_expired_messages(&self) -> std::io::Result<()> {
        let mut messages = self.messages.lock().await;
        let mut ids_to_remove = Vec::new();

        for (message_id, message_list) in messages.iter_mut() {
            let mut index: i32 = (message_list.len() - 1) as i32;
            while index >= 0 {
                let (user_address, _message, _schedule_delivery_time, validity_period) =
                    message_list[index as usize].clone();

                // If the message has a validity_period in the past, discard it and continue to the next message
                if validity_period.is_some() && validity_period.unwrap() < Local::now() {
                    message_list.remove(index as usize);
                    self.remove_user_message_id(&user_address, &message_id)
                        .await;
                }
                index -= 1;
            }

            if message_list.is_empty() {
                ids_to_remove.push(message_id.clone());
            }
        }

        for id in ids_to_remove {
            messages.remove(&id);
        }

        Ok(())
    }

    async fn get_delivery_messages(&self) -> std::io::Result<Vec<(String, i64, Value)>> {
        let mut messages = self.messages.lock().await;
        let mut delivery_messages = Vec::new();
        let mut ids_to_remove = Vec::new();

        for (message_id, message_list) in messages.iter_mut() {
            let mut index: i32 = (message_list.len() - 1) as i32;
            while index >= 0 {
                let (user_address, message, schedule_delivery_time, validity_period) =
                    message_list[index as usize].clone();

                // If the message has a schedule_delivery_time in the future, skip it and continue to the next message
                if schedule_delivery_time.is_some()
                    && schedule_delivery_time.unwrap() > Local::now()
                {
                    index -= 1;
                    continue;
                }

                // If the message has a validity_period in the past, discard it and continue to the next message
                if validity_period.is_some() && validity_period.unwrap() < Local::now() {
                    message_list.remove(index as usize);
                    self.remove_user_message_id(&user_address, &message_id)
                        .await;
                    index -= 1;
                    continue;
                }

                delivery_messages.push((message_id.clone(), index as i64, message.clone()));
                break; // Only take one message per message_id for delivery
            }

            if message_list.is_empty() {
                ids_to_remove.push(message_id.clone());
            }
        }

        for id in ids_to_remove {
            messages.remove(&id);
        }

        Ok(delivery_messages)
    }
}

#[async_trait::async_trait]
impl MessageStorage for InMemoryMessageStorage {
    async fn save(&self, message_id: &String, message: &Value) -> std::io::Result<()> {
        let dest_address = message
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if dest_address.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing destination_addr in the message",
            ));
        }

        let schedule_delivery_time = match get_schedule_delivery_time(message) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        let validity_period = match get_validity_period(message, self.default_validity_period) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_none() && validity_period.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Message has neither schedule_delivery_time nor validity_period",
            ));
        }

        if schedule_delivery_time.is_some()
            && validity_period.is_some()
            && schedule_delivery_time.unwrap() > validity_period.unwrap()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "schedule_delivery_time is later than validity_period",
            ));
        }

        if validity_period.is_some() && validity_period.unwrap() < Local::now() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Message has already expired",
            ));
        }

        let mut messages = self.messages.lock().await;
        messages
            .entry(message_id.clone())
            .or_insert_with(Vec::new)
            .push((
                dest_address.clone(),
                message.clone(),
                schedule_delivery_time,
                validity_period,
            ));

        self.user_message_ids
            .lock()
            .await
            .entry(dest_address)
            .or_insert_with(Vec::new)
            .push(message_id.clone());
        Ok(())
    }

    async fn set_deliver_message_handler(
        &self,
        deliver_message_handler: Box<dyn DeliveryMessageHandler>,
    ) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut t1 = interval(TokioDuration::from_secs(60));
            let mut t2 = interval(TokioDuration::from_secs(2));
            loop {
                select! {
                    _ = t1.tick() => {
                        if let Err(e) = self_clone.clean_expired_messages().await {
                            error!("Failed to clean expired messages: {}", e);
                        }
                    }
                    _ = t2.tick() => {
                        if let Ok(values) = self_clone.get_delivery_messages().await {
                            for (message_id, index, message) in values {
                                if deliver_message_handler.handle_delivery_message(message).await.is_ok() {
                                    error!("Successfully delivered message: {}", message_id);
                                } else {
                                    error!("Failed to deliver message: {}", message_id);
                                    self_clone.remove_message_by_id(&message_id, index as usize).await;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    async fn cancel_message(&self, cancel_sm: &Value) -> std::io::Result<bool> {
        let service_type = cancel_sm
            .get("service_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let cancel_message_id = cancel_sm
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let source_addr_ton = cancel_sm
            .get("source_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let source_addr_npi = cancel_sm
            .get("source_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let source_addr = cancel_sm
            .get("source_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let dest_addr_ton = cancel_sm
            .get("dest_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let dest_addr_npi = cancel_sm
            .get("dest_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let destination_addr = cancel_sm
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mut messages = self.messages.lock().await;
        let keys = messages.keys().cloned().collect::<Vec<String>>();
        let mut empty_message_ids = Vec::new();
        let mut removed_user_message_ids = Vec::new();

        for message_id in &keys {
            if !cancel_message_id.is_empty() && message_id != &cancel_message_id {
                continue;
            }

            let message_list = messages.get_mut(message_id).unwrap();
            let mut n = message_list.len() as i32 - 1;

            while n >= 0 {
                let (user_addr, message, _schedule_delivery_time, _validity_period) =
                    &message_list[n as usize];

                if !service_type.is_empty()
                    && service_type
                        != message
                            .get("service_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                {
                    n -= 1;
                    continue;
                }

                // Check if the message matches the cancel_sm criteria
                if !source_addr.is_empty()
                    && source_addr
                        != message
                            .get("source_addr")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                {
                    n -= 1;
                    continue;
                }
                // Check if the message matches the cancel_sm criteria
                if !destination_addr.is_empty()
                    && destination_addr
                        != message
                            .get("destination_addr")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                {
                    n -= 1;
                    continue;
                }
                // Check if the message matches the cancel_sm criteria
                if source_addr_ton != 0
                    && source_addr_ton
                        != message
                            .get("source_addr_ton")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u8
                {
                    n -= 1;
                    continue;
                }
                // Check if the message matches the cancel_sm criteria
                if source_addr_npi != 0
                    && source_addr_npi
                        != message
                            .get("source_addr_npi")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u8
                {
                    n -= 1;
                    continue;
                }

                if dest_addr_ton != 0
                    && dest_addr_ton
                        != message
                            .get("dest_addr_ton")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u8
                {
                    n -= 1;
                    continue;
                }

                if dest_addr_npi != 0
                    && dest_addr_npi
                        != message
                            .get("dest_addr_npi")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u8
                {
                    n -= 1;
                    continue;
                }
                removed_user_message_ids.push((user_addr.clone(), message_id.clone()));
                // If all criteria match, remove the message
                message_list.remove(n as usize);
                n -= 1;
            }
            if message_list.is_empty() {
                empty_message_ids.push(message_id.clone());
            }
        }

        // Remove empty message IDs from the main messages map
        for message_id in empty_message_ids {
            messages.remove(&message_id);
        }

        let n = removed_user_message_ids.len();
        for (user_addr, message_id) in removed_user_message_ids {
            self.remove_user_message_id(&user_addr, &message_id).await;
        }
        Ok(n > 0)
    }

    async fn replace_message(&self, message: &Value) -> std::io::Result<bool> {
        let message_id = message
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if message_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing message_id in the replace_sm command",
            ));
        }

        let sm_default_msg_id = message
            .get("sm_default_msg_id")
            .and_then(|v| v.as_u64())
            .unwrap_or_default() as u8;
        let short_message = message
            .get("short_message")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let message_payload = message
            .get("message_payload")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if sm_default_msg_id == 0 && short_message.is_empty() && message_payload.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing sm_default_msg_id, short_message, and message_payload in the replace_sm command",
            ));
        }

        let source_addr_ton = message
            .get("source_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let source_addr_npi = message
            .get("source_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;
        let source_addr = message
            .get("source_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let mut messages = self.messages.lock().await;

        let messages = messages.get_mut(&message_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Message with id {} not found", message_id),
            )
        })?;

        let str_schedule_delivery_time = message
            .get("schedule_delivery_time")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let parsed_schedule_delivery_time = if !str_schedule_delivery_time.is_empty() {
            Some(
                parse_absolute_relative_time(str_schedule_delivery_time).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Invalid schedule_delivery_time: {}", e),
                    )
                })?,
            )
        } else {
            None
        };
        let str_validity_period = message
            .get("validity_period")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let parsed_validity_period = if !str_validity_period.is_empty() {
            Some(
                parse_absolute_relative_time(str_validity_period).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Invalid validity_period: {}", e),
                    )
                })?,
            )
        } else {
            None
        };
        let registered_delivery = message
            .get("registered_delivery")
            .and_then(|v| v.as_u64())
            .unwrap_or_default() as u8;

        let mut n = messages.len() as i32 - 1;

        while n >= 0 {
            let (_, message, schedule_delivery_time, validity_period) = &mut messages[n as usize];
            if source_addr_ton != 0
                && source_addr_ton
                    != message
                        .get("source_addr_ton")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u8
            {
                n -= 1;
                continue;
            }
            if source_addr_npi != 0
                && source_addr_npi
                    != message
                        .get("source_addr_npi")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u8
            {
                n -= 1;
                continue;
            }
            if source_addr.is_empty() == false
                && source_addr
                    != message
                        .get("source_addr")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
            {
                n -= 1;
                continue;
            }
            // Update the message fields based on the replace_sm command
            if sm_default_msg_id != 0 {
                message["sm_default_msg_id"] = Value::from(sm_default_msg_id);
            }
            if !short_message.is_empty() {
                message["short_message"] = Value::from(short_message.clone());
            } else if !message_payload.is_empty() {
                message["short_message"] = Value::from(message_payload.clone());
            }
            if parsed_schedule_delivery_time.is_some() {
                message["schedule_delivery_time"] =
                    Value::from(str_schedule_delivery_time.to_string());
                *schedule_delivery_time = parsed_schedule_delivery_time;
            }
            if parsed_validity_period.is_some() {
                message["validity_period"] = Value::from(str_validity_period.to_string());
                *validity_period = parsed_validity_period;
            }
            message["registered_delivery"] = Value::from(registered_delivery);
            n -= 1;
        }

        messages[0].1 = messages[0].1.clone(); // Update the first message in the list with the modified message

        Ok(true)
    }
}

#[derive(Clone)]
pub struct SqlMessageStorage {
    pub pool: Arc<sqlx::Pool<sqlx::Any>>,
    pub delivery_time_messages: Arc<tokio::sync::Mutex<Vec<(i64, String)>>>,
    pub default_validity_period: u32,
}

fn create_table_ddl(database_url: &str) -> &'static str {
    if database_url.starts_with("postgres") {
        "CREATE TABLE IF NOT EXISTS message_queue (
            id BIGSERIAL PRIMARY KEY,
            service_type TEXT,
            source_addr_ton INTEGER,
            source_addr_npi INTEGER,
            source_addr TEXT,
            dest_addr_ton INTEGER,
            dest_addr_npi INTEGER,
            destination_addr TEXT NOT NULL,
            message_id TEXT NOT NULL,
            message TEXT NOT NULL,
            schedule_delivery_time TEXT,
            validity_period TEXT
        )"
    } else if database_url.starts_with("mysql") {
        "CREATE TABLE IF NOT EXISTS message_queue (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            service_type TEXT,
            source_addr_ton INT,
            source_addr_npi INT,
            source_addr TEXT,
            dest_addr_ton INT,
            dest_addr_npi INT,
            destination_addr TEXT NOT NULL,
            message_id VARCHAR(255) NOT NULL,
            message LONGTEXT NOT NULL,
            schedule_delivery_time TEXT,
            validity_period TEXT
        )"
    } else {
        "CREATE TABLE IF NOT EXISTS message_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_type TEXT,
            source_addr_ton INTEGER,
            source_addr_npi INTEGER,
            source_addr TEXT,
            dest_addr_ton INTEGER,
            dest_addr_npi INTEGER,
            destination_addr TEXT NOT NULL,
            message_id TEXT NOT NULL,
            message TEXT NOT NULL,
            schedule_delivery_time TEXT,
            validity_period TEXT
        )"
    }
}

impl SqlMessageStorage {
    pub async fn new(database_url: &str, default_validity_period: u32) -> std::io::Result<Self> {
        sqlx::any::install_default_drivers();

        let pool_opts = sqlx::pool::PoolOptions::<sqlx::Any>::new();
        // In-memory SQLite needs a single connection to share the same database
        let pool_opts = if database_url.contains(":memory:") {
            pool_opts.max_connections(1)
        } else {
            pool_opts
        };

        let pool = pool_opts
            .connect(database_url)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let pool = Arc::new(pool);
        sqlx::query(create_table_ddl(database_url))
            .execute(pool.as_ref())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(SqlMessageStorage {
            pool: Arc::clone(&pool),
            delivery_time_messages: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            default_validity_period,
        })
    }

    async fn delete_message_by_id(&self, id: i64) -> bool {
        if let Ok(_) = sqlx::query("DELETE FROM message_queue WHERE id = $1")
            .bind(id)
            .execute(self.pool.as_ref())
            .await
        {
            true
        } else {
            false
        }
    }

    /**
     * Returns a list of messages that are ready for delivery, i.e., messages that either have no schedule_delivery_time or have a schedule_delivery_time in the past. The messages are ordered by their schedule_delivery_time in ascending order. If a message has an invalid JSON format, it will be skipped and deleted from the storage.
     */
    async fn get_delivery_messages(&self) -> std::io::Result<Vec<(i64, Value)>> {
        let rows = sqlx::query(
            "SELECT id, message FROM message_queue WHERE schedule_delivery_time IS NULL OR schedule_delivery_time <= $1 ORDER BY schedule_delivery_time ASC",
        )
        .bind(Local::now().to_rfc3339())
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut messages = Vec::new();
        for row in rows {
            let id: i64 = row.get("id"); //row.get::<i64, _>("id");
            let message_json: String = row.get("message"); //row.get::<String, _>("message");
            let message = match serde_json::from_str::<Value>(&message_json) {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Failed to parse message JSON for id {}: {}", id, e);
                    if !self.delete_message_by_id(id).await {
                        error!("Failed to delete invalid message with id {}", id);
                    }
                    continue; // Skip this message and check the next one
                }
            };
            messages.push((id, message));
        }
        Ok(messages)
    }

    /**
     * Deletes all messages from the storage that have a validity_period in the past. This function is intended to be called periodically to clean up expired messages.
     */
    async fn clean_expired_messages(&self) -> std::io::Result<()> {
        sqlx::query(
            "DELETE FROM message_queue WHERE validity_period IS NOT NULL AND validity_period < $1",
        )
        .bind(Local::now().to_rfc3339())
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(())
    }

    async fn delete_messages_for_replace_if_present_flag(
        &self,
        message: &Value,
    ) -> std::io::Result<bool> {
        let service_type = message
            .get("service_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let source_addr_ton = message
            .get("source_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let source_addr_npi = message
            .get("source_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let source_addr = message
            .get("source_addr")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let dest_addr_ton = message
            .get("dest_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let dest_addr_npi = message
            .get("dest_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let destination_addr = message
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let sql = format!(
            "SELECT id, message FROM message_queue WHERE service_type = $1 AND source_addr_ton = $2 AND source_addr_npi = $3 AND source_addr = $4 AND dest_addr_ton = $5 AND dest_addr_npi = $6 AND destination_addr = $7",
        );

        let rows = sqlx::query(&sql)
            .bind(service_type)
            .bind(source_addr_ton)
            .bind(source_addr_npi)
            .bind(source_addr)
            .bind(dest_addr_ton)
            .bind(dest_addr_npi)
            .bind(destination_addr)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        for row in rows {
            let id: i64 = row.get("id");

            // Delete the existing message
            sqlx::query("DELETE FROM message_queue WHERE id = $1")
                .bind(id)
                .execute(self.pool.as_ref())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }

        Ok(true)
    }
}

#[async_trait::async_trait]
impl MessageStorage for SqlMessageStorage {
    async fn save(&self, message_id: &String, message: &Value) -> std::io::Result<()> {
        let replace_if_present_flag = message
            .get("replace_if_present_flag")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;

        if replace_if_present_flag == 1 {
            self.delete_messages_for_replace_if_present_flag(message)
                .await?;
        }

        let destination_addr = message
            .get("destination_addr")
            .and_then(|v: &Value| v.as_str())
            .unwrap_or_default()
            .to_string();

        if destination_addr.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing destination_addr in the message",
            ));
        }

        let schedule_delivery_time = match get_schedule_delivery_time(message) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_some() && schedule_delivery_time.unwrap() < Local::now() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "schedule_delivery_time is in the past",
            ));
        };

        let mut validity_period = match get_validity_period(message, self.default_validity_period) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_none() && validity_period.is_none() {
            validity_period =
                Some(Local::now() + Duration::seconds(self.default_validity_period as i64));
        }

        let schedule_delivery_time = schedule_delivery_time.map(|t| t.to_rfc3339());
        let validity_period = validity_period.map(|t| t.to_rfc3339());
        let service_type = message
            .get("service_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let source_addr_ton = message
            .get("source_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i16;
        let source_addr_npi = message
            .get("source_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i16;
        let source_addr = message
            .get("source_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let dest_addr_ton = message
            .get("dest_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i16;
        let dest_addr_npi = message
            .get("dest_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i16;

        let message_json = serde_json::to_string(message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        sqlx::query(
            "INSERT INTO message_queue (service_type, message_id, source_addr_ton, source_addr_npi, source_addr, dest_addr_ton, dest_addr_npi, destination_addr, schedule_delivery_time, validity_period, message) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(&service_type)
        .bind(&message_id)
        .bind(&source_addr_ton)
        .bind(&source_addr_npi)
        .bind(&source_addr)
        .bind(&dest_addr_ton)
        .bind(&dest_addr_npi)
        .bind(&destination_addr)
        .bind(&schedule_delivery_time)
        .bind(&validity_period)
        .bind(&message_json)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(())
    }

    async fn set_deliver_message_handler(
        &self,
        deliver_message_handler: Box<dyn DeliveryMessageHandler>,
    ) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut t1 = interval(TokioDuration::from_secs(60));
            let mut t2 = interval(TokioDuration::from_secs(2));
            loop {
                select! {
                    _ = t1.tick() => {
                        if let Err(e) = self_clone.clean_expired_messages().await {
                            error!("Failed to clean expired messages: {}", e);
                        }
                    }
                    _ = t2.tick() => {
                        if let Ok(values) = self_clone.get_delivery_messages().await {
                            for (id, message) in values {
                                if deliver_message_handler.handle_delivery_message(message).await.is_err() {
                                    error!("Failed to deliver message with id {}", id);
                                } else {
                                    info!("Successfully delivered message with id {}", id);
                                    self_clone.delete_message_by_id(id).await;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    async fn cancel_message(&self, cancel_sm: &Value) -> std::io::Result<bool> {
        let service_type = cancel_sm
            .get("service_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let message_id = cancel_sm
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let source_addr_ton = cancel_sm
            .get("source_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let source_addr_npi = cancel_sm
            .get("source_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let source_addr = cancel_sm
            .get("source_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let dest_addr_ton = cancel_sm
            .get("dest_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let dest_addr_npi = cancel_sm
            .get("dest_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let destination_addr = cancel_sm
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let mut query_fields = Vec::new();
        if !service_type.is_empty() {
            query_fields.push("service_type");
        }

        if !message_id.is_empty() {
            query_fields.push("message_id");
        }
        if source_addr_ton != 0 {
            query_fields.push("source_addr_ton");
        }
        if source_addr_npi != 0 {
            query_fields.push("source_addr_npi");
        }
        if !source_addr.is_empty() {
            query_fields.push("source_addr");
        }
        if dest_addr_ton != 0 {
            query_fields.push("dest_addr_ton");
        }
        if dest_addr_npi != 0 {
            query_fields.push("dest_addr_npi");
        }
        if !destination_addr.is_empty() {
            query_fields.push("destination_addr");
        }

        if query_fields.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "At least one query field must be provided",
            ));
        }

        let sql = format!(
            "DELETE FROM message_queue WHERE {}",
            query_fields
                .iter()
                .map(|field| format!(
                    "{} = ${}",
                    field,
                    query_fields.iter().position(|f| f == field).unwrap() + 1
                ))
                .collect::<Vec<String>>()
                .join(" AND ")
        );

        info!("Executing SQL: {}", sql);

        let mut sql_query = sqlx::query(&sql);

        if !service_type.is_empty() {
            sql_query = sql_query.bind(service_type);
        }

        if !message_id.is_empty() {
            sql_query = sql_query.bind(message_id);
        }

        if source_addr_ton != 0 {
            sql_query = sql_query.bind(source_addr_ton);
        }
        if source_addr_npi != 0 {
            sql_query = sql_query.bind(source_addr_npi);
        }
        if !source_addr.is_empty() {
            sql_query = sql_query.bind(source_addr);
        }
        if dest_addr_ton != 0 {
            sql_query = sql_query.bind(dest_addr_ton);
        }
        if dest_addr_npi != 0 {
            sql_query = sql_query.bind(dest_addr_npi);
        }
        if !destination_addr.is_empty() {
            sql_query = sql_query.bind(destination_addr);
        }

        let result = sql_query
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let n = result.rows_affected();
        info!("Deleted {} messages from the database", n);

        Ok(n > 0)
    }

    async fn replace_message(&self, message: &Value) -> std::io::Result<bool> {
        let message_id = message
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if message_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing message_id in the replace_sm command",
            ));
        }

        let sm_default_msg_id = message
            .get("sm_default_msg_id")
            .and_then(|v| v.as_u64())
            .unwrap_or_default() as u8;
        let short_message = message
            .get("short_message")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let message_payload = message
            .get("message_payload")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if sm_default_msg_id == 0 && short_message.is_empty() && message_payload.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing short_message or message_payload in the replace_sm command",
            ));
        }

        let source_addr_ton = message
            .get("source_addr_ton")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let source_addr_npi = message
            .get("source_addr_npi")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let source_addr = message
            .get("source_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let schedule_delivery_time = message
            .get("schedule_delivery_time")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let parsed_schedule_delivery_time = if !schedule_delivery_time.is_empty() {
            Some(
                parse_absolute_relative_time(&schedule_delivery_time)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            )
        } else {
            None
        };

        let validity_period = message
            .get("validity_period")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let parsed_validity_period = if !validity_period.is_empty() {
            Some(
                parse_absolute_relative_time(&validity_period)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?,
            )
        } else {
            None
        };

        let rfc3339_schedule_delivery_time = parsed_schedule_delivery_time
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();
        let rfc3339_validity_period = parsed_validity_period
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_default();

        let registered_delivery = message
            .get("registered_delivery")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;

        let mut query_fields = vec!["message_id"];
        if source_addr_ton != 0 {
            query_fields.push("source_addr_ton");
        }
        if source_addr_npi != 0 {
            query_fields.push("source_addr_npi");
        }
        if !source_addr.is_empty() {
            query_fields.push("source_addr");
        }

        let sql = format!(
            "SELECT id, message FROM message_queue WHERE {}",
            query_fields
                .iter()
                .map(|field| format!(
                    "{} = ${}",
                    field,
                    query_fields.iter().position(|f| f == field).unwrap() + 1
                ))
                .collect::<Vec<String>>()
                .join(" AND ")
        );

        info!("Executing SQL: {}", sql);
        let mut sql_query = sqlx::query(&sql).bind(&message_id);
        if source_addr_ton != 0 {
            sql_query = sql_query.bind(source_addr_ton);
        }
        if source_addr_npi != 0 {
            sql_query = sql_query.bind(source_addr_npi);
        }
        if !source_addr.is_empty() {
            sql_query = sql_query.bind(&source_addr);
        }

        let rows = sql_query
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut updated_rows = 0;
        for row in rows {
            updated_rows += 1;
            let id: i64 = row.get("id");
            let message_json: String = row.get("message");
            let message = serde_json::from_str::<Value>(&message_json).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid JSON: {}", e),
                )
            })?;

            let mut updated_message = message.clone();
            if !schedule_delivery_time.is_empty() {
                updated_message["schedule_delivery_time"] =
                    Value::String(schedule_delivery_time.clone());
            }
            if !validity_period.is_empty() {
                updated_message["validity_period"] = Value::String(validity_period.clone());
            }
            if sm_default_msg_id != 0 {
                updated_message["sm_default_msg_id"] = Value::Number(sm_default_msg_id.into());
            }
            if short_message.len() > 0 {
                updated_message["short_message"] = Value::String(short_message.clone());
            } else if message_payload.len() > 0 {
                updated_message["short_message"] = Value::String(message_payload.clone());
            }
            updated_message["registered_delivery"] = Value::Number(registered_delivery.into());

            let mut update_fields = vec!["message"];
            if !schedule_delivery_time.is_empty() {
                update_fields.push("schedule_delivery_time");
            }
            if !validity_period.is_empty() {
                update_fields.push("validity_period");
            }

            let sql = format!(
                "UPDATE message_queue SET {} WHERE id = $1",
                update_fields
                    .iter()
                    .map(|field| format!(
                        "{} = ${}",
                        field,
                        update_fields.iter().position(|f| f == field).unwrap() + 2
                    ))
                    .collect::<Vec<String>>()
                    .join(", ")
            );

            info!("Executing SQL: {}", sql);

            let mut sql_query = sqlx::query(&sql).bind(id);
            sql_query = sql_query.bind(serde_json::to_string(&updated_message).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to serialize updated message: {}", e),
                )
            })?);

            if !rfc3339_schedule_delivery_time.is_empty() {
                sql_query = sql_query.bind(&rfc3339_schedule_delivery_time);
            }
            if !rfc3339_validity_period.is_empty() {
                sql_query = sql_query.bind(&rfc3339_validity_period);
            }

            sql_query
                .execute(self.pool.as_ref())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
        Ok(updated_rows > 0)
    }
}

#[derive(Clone)]
pub struct RedisMessageStorage {
    client: redis::Client,
    prefix: String,
    default_validity_period: u32,
}

impl RedisMessageStorage {
    pub async fn new(
        url: &str,
        key_prefix: Option<String>,
        default_validity_period: u32,
    ) -> std::io::Result<Self> {
        let client = redis::Client::open(url)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Verify connectivity
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let prefix = key_prefix.unwrap_or_else(|| "mq".to_string());

        Ok(RedisMessageStorage {
            client,
            prefix,
            default_validity_period,
        })
    }

    fn entry_key(&self, entry_id: &str) -> String {
        format!("{}:entry:{}", self.prefix, entry_id)
    }

    fn msgid_key(&self, message_id: &str) -> String {
        format!("{}:msgid:{}", self.prefix, message_id)
    }

    fn user_key(&self, dest_address: &str) -> String {
        format!("{}:user:{}", self.prefix, dest_address)
    }

    fn counter_key(&self) -> String {
        format!("{}:counter", self.prefix)
    }

    fn schedule_key(&self) -> String {
        format!("{}:schedule", self.prefix)
    }

    fn expiry_key(&self) -> String {
        format!("{}:expiry", self.prefix)
    }

    async fn get_conn(&self) -> std::io::Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    async fn delete_entry(&self, entry_id: &str) -> bool {
        let mut conn = match self.get_conn().await {
            Ok(c) => c,
            Err(_) => return false,
        };

        // Get entry metadata before deleting
        let result: redis::RedisResult<(String, String, String, String)> = redis::cmd("HMGET")
            .arg(self.entry_key(entry_id))
            .arg("dest_address")
            .arg("message_id")
            .arg("schedule_delivery_time")
            .arg("validity_period")
            .query_async(&mut conn)
            .await;

        let (dest_address, message_id, _sdt, _vp) = match result {
            Ok(v) => v,
            Err(_) => return false,
        };

        let _: redis::RedisResult<()> = redis::pipe()
            .del(self.entry_key(entry_id))
            .srem(self.msgid_key(&message_id), entry_id)
            .zrem(self.schedule_key(), entry_id)
            .zrem(self.expiry_key(), entry_id)
            .query_async(&mut conn)
            .await;

        // Check if this message_id has no more entries; if so, remove from user set
        let remaining: redis::RedisResult<i64> = conn.scard(self.msgid_key(&message_id)).await;
        if remaining.unwrap_or(0) == 0 {
            let _: redis::RedisResult<()> = redis::pipe()
                .del(self.msgid_key(&message_id))
                .srem(self.user_key(&dest_address), &message_id)
                .query_async(&mut conn)
                .await;
        }

        true
    }

    async fn get_delivery_messages(&self) -> std::io::Result<Vec<(String, Value)>> {
        let mut conn = self.get_conn().await?;
        let now = Local::now().timestamp_millis() as f64;

        // Get entries whose schedule_delivery_time has passed
        let entry_ids: Vec<String> = conn
            .zrangebyscore(self.schedule_key(), f64::NEG_INFINITY, now)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Also get entries with no schedule_delivery_time (those not in the schedule set)
        // We handle this by checking all entries in msgid sets - but for efficiency,
        // we only process entries from the schedule set here. Entries without schedule_delivery_time
        // are added to the schedule set with score 0 (epoch) so they're always included.

        let mut messages = Vec::new();
        for entry_id in entry_ids {
            let result: redis::RedisResult<Vec<String>> = redis::cmd("HMGET")
                .arg(self.entry_key(&entry_id))
                .arg("message")
                .arg("validity_period")
                .query_async(&mut conn)
                .await;

            let fields = match result {
                Ok(f) if f.len() == 2 => f,
                _ => continue,
            };

            let message_json = &fields[0];
            let validity_period_str = &fields[1];

            // Skip expired messages
            if !validity_period_str.is_empty() {
                if let Ok(vp) = DateTime::parse_from_rfc3339(validity_period_str) {
                    if vp.with_timezone(&Local) < Local::now() {
                        self.delete_entry(&entry_id).await;
                        continue;
                    }
                }
            }

            match serde_json::from_str::<Value>(message_json) {
                Ok(msg) => messages.push((entry_id, msg)),
                Err(e) => {
                    error!("Failed to parse message JSON for entry {}: {}", entry_id, e);
                    self.delete_entry(&entry_id).await;
                }
            }
        }

        Ok(messages)
    }

    async fn clean_expired_messages(&self) -> std::io::Result<()> {
        let mut conn = self.get_conn().await?;
        let now = Local::now().timestamp_millis() as f64;

        let expired_ids: Vec<String> = conn
            .zrangebyscore(self.expiry_key(), f64::NEG_INFINITY, now)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        for entry_id in expired_ids {
            self.delete_entry(&entry_id).await;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageStorage for RedisMessageStorage {
    async fn save(&self, message_id: &String, message: &Value) -> std::io::Result<()> {
        let dest_address = message
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if dest_address.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing destination_addr in the message",
            ));
        }

        let schedule_delivery_time = match get_schedule_delivery_time(message) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_some() && schedule_delivery_time.unwrap() < Local::now() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "schedule_delivery_time is in the past",
            ));
        }

        let validity_period = match get_validity_period(message, self.default_validity_period) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_none() && validity_period.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Message has neither schedule_delivery_time nor validity_period",
            ));
        }

        let mut conn = self.get_conn().await?;

        let entry_id: i64 = conn
            .incr(self.counter_key(), 1i64)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let entry_id_str = entry_id.to_string();

        let sdt_str = schedule_delivery_time
            .map(|t| t.to_rfc3339())
            .unwrap_or_default();
        let vp_str = validity_period.map(|t| t.to_rfc3339()).unwrap_or_default();

        let message_json = serde_json::to_string(message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Store the entry hash
        redis::cmd("HSET")
            .arg(self.entry_key(&entry_id_str))
            .arg("dest_address")
            .arg(&dest_address)
            .arg("message_id")
            .arg(&message_id)
            .arg("message")
            .arg(&message_json)
            .arg("schedule_delivery_time")
            .arg(&sdt_str)
            .arg("validity_period")
            .arg(&vp_str)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Add entry to message_id set and user set
        let _: redis::RedisResult<()> = redis::pipe()
            .sadd(self.msgid_key(&message_id), &entry_id_str)
            .sadd(self.user_key(&dest_address), &message_id)
            .query_async(&mut conn)
            .await;

        // Add to schedule sorted set (score = timestamp in millis, or 0 if no schedule)
        let schedule_score = schedule_delivery_time
            .map(|t| t.timestamp_millis() as f64)
            .unwrap_or(0.0);
        let _: redis::RedisResult<()> = conn
            .zadd(self.schedule_key(), &entry_id_str, schedule_score)
            .await;

        // Add to expiry sorted set if there's a validity_period
        if let Some(vp) = validity_period {
            let _: redis::RedisResult<()> = conn
                .zadd(
                    self.expiry_key(),
                    &entry_id_str,
                    vp.timestamp_millis() as f64,
                )
                .await;
        }

        Ok(())
    }

    async fn set_deliver_message_handler(
        &self,
        deliver_message_handler: Box<dyn DeliveryMessageHandler>,
    ) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut t1 = interval(TokioDuration::from_secs(60));
            let mut t2 = interval(TokioDuration::from_secs(2));
            loop {
                select! {
                    _ = t1.tick() => {
                        if let Err(e) = self_clone.clean_expired_messages().await {
                            error!("Failed to clean expired messages: {}", e);
                        }
                    }
                    _ = t2.tick() => {
                        if let Ok(values) = self_clone.get_delivery_messages().await {
                            for (entry_id, message) in values {
                                if deliver_message_handler.handle_delivery_message(message).await.is_err() {
                                    error!("Failed to deliver message with entry {}", entry_id);
                                } else {
                                    info!("Successfully delivered message with entry {}", entry_id);
                                    self_clone.delete_entry(&entry_id).await;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    async fn cancel_message(&self, cancel_sm: &Value) -> std::io::Result<bool> {
        let message_id = cancel_sm
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if message_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing message_id in the cancel_sm command",
            ));
        }

        let mut conn = self.get_conn().await?;

        // Get all entry_ids for this message_id
        let entry_ids: Vec<String> = conn
            .smembers(self.msgid_key(&message_id))
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut deleted_any = false;
        for entry_id in entry_ids {
            if self.delete_entry(&entry_id).await {
                deleted_any = true;
            }
        }

        Ok(deleted_any)
    }

    async fn replace_message(&self, replace_sm_command: &Value) -> std::io::Result<bool> {
        let message_id = replace_sm_command
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if message_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing message_id in the replace_sm command",
            ));
        }

        let mut conn = self.get_conn().await?;

        // Get all entry_ids for this message_id
        let entry_ids: Vec<String> = conn
            .smembers(self.msgid_key(&message_id))
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let replaced_any = false;
        for entry_id in entry_ids {
            // Fetch the existing message
            let result: redis::RedisResult<Vec<String>> = redis::cmd("HMGET")
                .arg(self.entry_key(&entry_id))
                .arg("message")
                .query_async(&mut conn)
                .await;

            let fields = match result {
                Ok(f) if f.len() == 1 => f,
                _ => continue,
            };

            let message_json = &fields[0];
            let mut existing_message: Value = match serde_json::from_str(message_json) {
                Ok(msg) => msg,
                Err(_) => continue,
            };

            // Update fields based on replace_sm_command
            if let Some(short_message) = replace_sm_command.get("short_message") {
                existing_message["short_message"] = short_message.clone();
            } else if let Some(message_payload) = replace_sm_command.get("message_payload") {
                existing_message["short_message"] = message_payload.clone();
            }
            if let Some(schedule_delivery_time) = replace_sm_command.get("schedule_delivery_time") {
                existing_message["schedule_delivery_time"] = schedule_delivery_time.clone();
            }
            if let Some(validity_period) = replace_sm_command.get("validity_period") {
                existing_message["validity_period"] = validity_period.clone();
            }
            if let Some(registered_delivery) = replace_sm_command.get("registered_delivery") {
                existing_message["registered_delivery"] = registered_delivery.clone();
            }
        }
        Ok(replaced_any)
    }
}

#[derive(Clone)]
pub struct RabbitMqMessageStorage {
    channel: Arc<tokio::sync::Mutex<lapin::Channel>>,
    queue_name: String,
    // Local index for message lookup by message_id and dest_address
    message_index: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, Vec<(String, lapin::types::LongLongUInt)>>,
        >,
    >,
    user_index: Arc<tokio::sync::Mutex<std::collections::HashMap<String, Vec<String>>>>,
    default_validity_period: u32,
}

impl RabbitMqMessageStorage {
    pub async fn new(
        url: &str,
        queue_name: &str,
        default_validity_period: u32,
    ) -> std::io::Result<Self> {
        let conn = lapin::Connection::connect(url, lapin::ConnectionProperties::default())
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        channel
            .queue_declare(
                queue_name,
                lapin::options::QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(RabbitMqMessageStorage {
            channel: Arc::new(tokio::sync::Mutex::new(channel)),
            queue_name: queue_name.to_string(),
            message_index: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            user_index: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            default_validity_period,
        })
    }
}

#[async_trait::async_trait]
impl MessageStorage for RabbitMqMessageStorage {
    async fn save(&self, message_id: &String, message: &Value) -> std::io::Result<()> {
        let dest_address = message
            .get("destination_addr")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if dest_address.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing destination_addr in the message",
            ));
        }

        let schedule_delivery_time = match get_schedule_delivery_time(message) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_some() && schedule_delivery_time.unwrap() < Local::now() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "schedule_delivery_time is in the past",
            ));
        }

        let validity_period = match get_validity_period(message, self.default_validity_period) {
            Ok(time) => Some(time),
            Err(_) => None,
        };

        if schedule_delivery_time.is_none() && validity_period.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Message has neither schedule_delivery_time nor validity_period",
            ));
        }

        // Wrap the message with metadata for retrieval
        let envelope = serde_json::json!({
            "message_id": message_id,
            "dest_address": dest_address,
            "schedule_delivery_time": schedule_delivery_time.map(|t| t.to_rfc3339()),
            "validity_period": validity_period.map(|t| t.to_rfc3339()),
            "payload": message,
        });

        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let channel = self.channel.lock().await;
        let confirm = channel
            .basic_publish(
                "",
                &self.queue_name,
                lapin::options::BasicPublishOptions::default(),
                &payload,
                lapin::BasicProperties::default()
                    .with_delivery_mode(2) // persistent
                    .with_message_id(message_id.clone().into()),
            )
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        confirm
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Update local indexes
        let delivery_tag = 0u64; // Will be populated on consume
        self.message_index
            .lock()
            .await
            .entry(message_id.clone())
            .or_default()
            .push((dest_address.clone(), delivery_tag));

        self.user_index
            .lock()
            .await
            .entry(dest_address)
            .or_default()
            .push(message_id.clone());

        Ok(())
    }

    async fn set_deliver_message_handler(
        &self,
        deliver_message_handler: Box<dyn DeliveryMessageHandler>,
    ) {
        let channel = self.channel.lock().await.clone();
        let queue_name = self.queue_name.clone();
        let message_index = self.message_index.clone();
        let user_index = self.user_index.clone();

        tokio::spawn(async move {
            use futures::StreamExt;

            let consumer = match channel
                .basic_consume(
                    &queue_name,
                    "deliver_handler",
                    lapin::options::BasicConsumeOptions::default(),
                    lapin::types::FieldTable::default(),
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to start RabbitMQ consumer: {}", e);
                    return;
                }
            };

            tokio::pin!(consumer);

            while let Some(delivery_result) = consumer.next().await {
                let delivery = match delivery_result {
                    Ok(d) => d,
                    Err(e) => {
                        error!("RabbitMQ consumer error: {}", e);
                        continue;
                    }
                };

                let envelope: Value = match serde_json::from_slice(&delivery.data) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to parse RabbitMQ message: {}", e);
                        let _ = channel
                            .basic_ack(
                                delivery.delivery_tag,
                                lapin::options::BasicAckOptions::default(),
                            )
                            .await;
                        continue;
                    }
                };

                // Check schedule_delivery_time
                if let Some(sdt_str) = envelope
                    .get("schedule_delivery_time")
                    .and_then(|v| v.as_str())
                {
                    if let Ok(sdt) = DateTime::parse_from_rfc3339(sdt_str) {
                        if sdt.with_timezone(&Local) > Local::now() {
                            // Not ready yet, reject and requeue
                            let _ = channel
                                .basic_nack(
                                    delivery.delivery_tag,
                                    lapin::options::BasicNackOptions {
                                        requeue: true,
                                        ..Default::default()
                                    },
                                )
                                .await;
                            tokio::time::sleep(TokioDuration::from_secs(1)).await;
                            continue;
                        }
                    }
                }

                // Check validity_period
                if let Some(vp_str) = envelope.get("validity_period").and_then(|v| v.as_str()) {
                    if let Ok(vp) = DateTime::parse_from_rfc3339(vp_str) {
                        if vp.with_timezone(&Local) < Local::now() {
                            // Expired, ack and discard
                            let _ = channel
                                .basic_ack(
                                    delivery.delivery_tag,
                                    lapin::options::BasicAckOptions::default(),
                                )
                                .await;
                            // Clean up indexes
                            if let Some(msg_id) =
                                envelope.get("message_id").and_then(|v| v.as_str())
                            {
                                let mut idx = message_index.lock().await;
                                idx.remove(msg_id);
                                if let Some(dest) =
                                    envelope.get("dest_address").and_then(|v| v.as_str())
                                {
                                    let mut uidx = user_index.lock().await;
                                    if let Some(ids) = uidx.get_mut(dest) {
                                        ids.retain(|id| id != msg_id);
                                        if ids.is_empty() {
                                            uidx.remove(dest);
                                        }
                                    }
                                }
                            }
                            continue;
                        }
                    }
                }

                // Extract the payload and deliver
                let payload = match envelope.get("payload") {
                    Some(p) => p.clone(),
                    None => {
                        let _ = channel
                            .basic_ack(
                                delivery.delivery_tag,
                                lapin::options::BasicAckOptions::default(),
                            )
                            .await;
                        continue;
                    }
                };

                if deliver_message_handler
                    .handle_delivery_message(payload)
                    .await
                    .is_ok()
                {
                    // Delivery succeeded, ack
                    let _ = channel
                        .basic_ack(
                            delivery.delivery_tag,
                            lapin::options::BasicAckOptions::default(),
                        )
                        .await;
                    // Clean up indexes
                    if let Some(msg_id) = envelope.get("message_id").and_then(|v| v.as_str()) {
                        let mut idx = message_index.lock().await;
                        idx.remove(msg_id);
                        if let Some(dest) = envelope.get("dest_address").and_then(|v| v.as_str()) {
                            let mut uidx = user_index.lock().await;
                            if let Some(ids) = uidx.get_mut(dest) {
                                ids.retain(|id| id != msg_id);
                                if ids.is_empty() {
                                    uidx.remove(dest);
                                }
                            }
                        }
                    }
                } else {
                    // Delivery failed, nack and requeue
                    error!("Failed to deliver RabbitMQ message");
                    let _ = channel
                        .basic_nack(
                            delivery.delivery_tag,
                            lapin::options::BasicNackOptions {
                                requeue: true,
                                ..Default::default()
                            },
                        )
                        .await;
                }
            }
        });
    }

    async fn cancel_message(&self, cancel_sm: &Value) -> std::io::Result<bool> {
        let message_id = cancel_sm
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if message_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing message_id in the cancel_sm command",
            ));
        }

        let mut idx = self.message_index.lock().await;
        if let Some(entries) = idx.remove(&message_id) {
            let mut uidx = self.user_index.lock().await;
            for (dest_address, _delivery_tag) in entries {
                if let Some(ids) = uidx.get_mut(&dest_address) {
                    ids.retain(|id| id != &message_id);
                    if ids.is_empty() {
                        uidx.remove(&dest_address);
                    }
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn replace_message(&self, replace_sm_command: &Value) -> std::io::Result<bool> {
        let message_id = replace_sm_command
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if message_id.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing message_id in the replace_sm command",
            ));
        }

        let mut idx = self.message_index.lock().await;
        if let Some(entries) = idx.get_mut(&message_id) {
            for (dest_address, _delivery_tag) in entries.iter_mut() {
                // Here we would ideally update the message in RabbitMQ, but since RabbitMQ doesn't support direct message replacement,
                // we can only update our local index or metadata. For simplicity, we'll just log the replacement.
                info!(
                    "Replacing message with ID {} for destination {}",
                    message_id, dest_address
                );
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub async fn create_message_storage(
    mq_config: &MessageStorageConfig,
    default_validity_period: u32,
) -> Box<dyn MessageStorage> {
    match mq_config {
        MessageStorageConfig::Redis { url, key_prefix } => {
            return match RedisMessageStorage::new(url, key_prefix.clone(), default_validity_period)
                .await
            {
                Ok(storage) => Box::new(storage) as Box<dyn MessageStorage>,
                Err(e) => {
                    error!("Failed to create RedisMessageStorage: {}", e);
                    Box::new(InMemoryMessageStorage::new(default_validity_period))
                        as Box<dyn MessageStorage>
                }
            };
        }
        MessageStorageConfig::RabbitMQ { url, queue_name } => {
            return match RabbitMqMessageStorage::new(url, queue_name, default_validity_period).await
            {
                Ok(storage) => Box::new(storage) as Box<dyn MessageStorage>,
                Err(e) => {
                    error!("Failed to create RabbitMqMessageStorage: {}", e);
                    Box::new(InMemoryMessageStorage::new(default_validity_period))
                        as Box<dyn MessageStorage>
                }
            };
        }
        _ => {}
    }

    let db_path = match mq_config {
        MessageStorageConfig::Sqlite { db_path } => format!("sqlite://{}", db_path),
        MessageStorageConfig::Postgres { url } => url.clone(),
        MessageStorageConfig::MySQL { url } => url.clone(),
        MessageStorageConfig::InMemory => "sqlite::memory:".to_string(),
        MessageStorageConfig::RabbitMQ { .. } => unreachable!(),
        MessageStorageConfig::Redis { .. } => unreachable!(),
    };
    match SqlMessageStorage::new(&db_path, default_validity_period).await {
        Ok(storage) => Box::new(storage) as Box<dyn MessageStorage>,
        Err(e) => {
            error!("Failed to create SqlMessageStorage: {}", e);
            Box::new(InMemoryMessageStorage::new(default_validity_period))
                as Box<dyn MessageStorage>
        }
    }
}
