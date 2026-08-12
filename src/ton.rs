#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TON {
    Unknown = 0b00000000,
    International = 0b00000001,
    National = 0b00000010,
    NetworkSpecific = 0b00000011,
    SubscriberNumber = 0b00000100,
    Alphanumeric = 0b00000101,
    Abbreviated = 0b00000110,
}

impl TON {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0b00000000 => Some(TON::Unknown),
            0b00000001 => Some(TON::International),
            0b00000010 => Some(TON::National),
            0b00000011 => Some(TON::NetworkSpecific),
            0b00000100 => Some(TON::SubscriberNumber),
            0b00000101 => Some(TON::Alphanumeric),
            0b00000110 => Some(TON::Abbreviated),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "unknown" => Some(TON::Unknown),
            "international" => Some(TON::International),
            "national" => Some(TON::National),
            "networkspecific" => Some(TON::NetworkSpecific),
            "subscribernumber" => Some(TON::SubscriberNumber),
            "alphanumeric" => Some(TON::Alphanumeric),
            "abbreviated" => Some(TON::Abbreviated),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            TON::Unknown => "Unknown",
            TON::International => "International",
            TON::National => "National",
            TON::NetworkSpecific => "NetworkSpecific",
            TON::SubscriberNumber => "SubscriberNumber",
            TON::Alphanumeric => "Alphanumeric",
            TON::Abbreviated => "Abbreviated",
        }
    }
}
