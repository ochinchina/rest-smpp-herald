#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    SCHEDULED = 0,
    ENROUTE = 1,
    DELIVERED = 2,
    EXPIRED = 3,
    DELETED = 4,
    UNDELIVERABLE = 5,
    ACCEPTED = 6,
    UNKNOWN = 7,
    REJECTED = 8,
    SKIPPED = 9,
}

impl MessageState {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => MessageState::SCHEDULED,
            1 => MessageState::ENROUTE,
            2 => MessageState::DELIVERED,
            3 => MessageState::EXPIRED,
            4 => MessageState::DELETED,
            5 => MessageState::UNDELIVERABLE,
            6 => MessageState::ACCEPTED,
            7 => MessageState::UNKNOWN,
            8 => MessageState::REJECTED,
            9 => MessageState::SKIPPED,
            _ => panic!("Invalid value for MessageState: {}", value),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            MessageState::SCHEDULED => "SCHEDULED".to_string(),
            MessageState::ENROUTE => "ENROUTE".to_string(),
            MessageState::DELIVERED => "DELIVERED".to_string(),
            MessageState::EXPIRED => "EXPIRED".to_string(),
            MessageState::DELETED => "DELETED".to_string(),
            MessageState::UNDELIVERABLE => "UNDELIVERABLE".to_string(),
            MessageState::ACCEPTED => "ACCEPTED".to_string(),
            MessageState::UNKNOWN => "UNKNOWN".to_string(),
            MessageState::REJECTED => "REJECTED".to_string(),
            MessageState::SKIPPED => "SKIPPED".to_string(),
        }
    }

    pub fn from_string(value: &str) -> Option<Self> {
        match value {
            "SCHEDULED" => Some(MessageState::SCHEDULED),
            "ENROUTE" => Some(MessageState::ENROUTE),
            "DELIVERED" => Some(MessageState::DELIVERED),
            "EXPIRED" => Some(MessageState::EXPIRED),
            "DELETED" => Some(MessageState::DELETED),
            "UNDELIVERABLE" => Some(MessageState::UNDELIVERABLE),
            "ACCEPTED" => Some(MessageState::ACCEPTED),
            "UNKNOWN" => Some(MessageState::UNKNOWN),
            "REJECTED" => Some(MessageState::REJECTED),
            "SKIPPED" => Some(MessageState::SKIPPED),
            _ => None,
        }
    }
}
