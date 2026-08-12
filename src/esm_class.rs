#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageMode {
    Default = 0b00000000,
    Datagram = 0b00000001,
    Transaction = 0b00000010,
    StoreAndForward = 0b00000011,
}

impl MessageMode {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b00000011 {
            0b00000000 => MessageMode::Default,
            0b00000001 => MessageMode::Datagram,
            0b00000010 => MessageMode::Transaction,
            0b00000011 => MessageMode::StoreAndForward,
            _ => MessageMode::Default,
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            MessageMode::Default => "Default".to_string(),
            MessageMode::Datagram => "Datagram".to_string(),
            MessageMode::Transaction => "Transaction".to_string(),
            MessageMode::StoreAndForward => "StoreAndForward".to_string(),
        }
    }

    pub fn from_string(value: &str) -> Option<Self> {
        let value = &value.replace("_", "").replace("-", "").to_lowercase();
        match value.as_str() {
            "default" => Some(MessageMode::Default),
            "datagram" => Some(MessageMode::Datagram),
            "transaction" | "forward" => Some(MessageMode::Transaction),
            "storeandforward" | "storeforward" => Some(MessageMode::StoreAndForward),
            _ => None,
        }
    }

    pub fn is_default(&self) -> bool {
        matches!(self, MessageMode::Default)
    }

    pub fn is_store_forward(&self) -> bool {
        matches!(self, MessageMode::StoreAndForward)
    }

    pub fn is_transaction(&self) -> bool {
        matches!(self, MessageMode::Transaction)
    }

    pub fn is_datagram(&self) -> bool {
        matches!(self, MessageMode::Datagram)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Default = 0b00000000,
    DeliveryReceipt = 0b00000100,
    IntermediateDeliveryNotification = 0b00100000,
}

impl MessageType {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b00100100 {
            0b00000000 => MessageType::Default,
            0b00000100 => MessageType::DeliveryReceipt,
            0b00100000 => MessageType::IntermediateDeliveryNotification,
            _ => MessageType::Default,
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            MessageType::Default => "Default".to_string(),
            MessageType::DeliveryReceipt => "DeliveryReceipt".to_string(),
            MessageType::IntermediateDeliveryNotification => {
                "IntermediateDeliveryNotification".to_string()
            }
        }
    }

    pub fn from_string(value: &str) -> Option<Self> {
        match value {
            "Default" => Some(MessageType::Default),
            "DeliveryReceipt" => Some(MessageType::DeliveryReceipt),
            "IntermediateDeliveryNotification" => {
                Some(MessageType::IntermediateDeliveryNotification)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ansi41Specific {
    DeliveryAcknowledgement = 0b00001000,
    UserAcknowledgement = 0b00010000,
    ConversationAbort = 0b00011000,
    Unknown,
}

impl Ansi41Specific {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b00011000 {
            0b00001000 => Ansi41Specific::DeliveryAcknowledgement,
            0b00010000 => Ansi41Specific::UserAcknowledgement,
            0b00011000 => Ansi41Specific::ConversationAbort,
            _ => Ansi41Specific::Unknown,
        }
    }

    pub fn is_delivery_ack(&self) -> bool {
        matches!(self, Ansi41Specific::DeliveryAcknowledgement)
            || matches!(self, Ansi41Specific::UserAcknowledgement)
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            Ansi41Specific::DeliveryAcknowledgement => "DeliveryAcknowledgement".to_string(),
            Ansi41Specific::UserAcknowledgement => "UserAcknowledgement".to_string(),
            Ansi41Specific::ConversationAbort => "ConversationAbort".to_string(),
            Ansi41Specific::Unknown => "Unknown".to_string(),
        }
    }

    pub fn from_string(value: &str) -> Self {
        match value {
            "DeliveryAcknowledgement" => Ansi41Specific::DeliveryAcknowledgement,
            "UserAcknowledgement" => Ansi41Specific::UserAcknowledgement,
            "ConversationAbort" => Ansi41Specific::ConversationAbort,
            "Unknown" => Ansi41Specific::Unknown,
            _ => Ansi41Specific::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GSMSpecific {
    NoSpecific = 0b00000000,
    UDHIndicator = 0b01000000,
    ReplyPath = 0b10000000,
    UDHAndReplyPath = 0b11000000,
}

impl GSMSpecific {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b11000000 {
            0b00000000 => GSMSpecific::NoSpecific,
            0b01000000 => GSMSpecific::UDHIndicator,
            0b10000000 => GSMSpecific::ReplyPath,
            0b11000000 => GSMSpecific::UDHAndReplyPath,
            _ => GSMSpecific::NoSpecific,
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            GSMSpecific::NoSpecific => "NoSpecific".to_string(),
            GSMSpecific::UDHIndicator => "UDHIndicator".to_string(),
            GSMSpecific::ReplyPath => "ReplyPath".to_string(),
            GSMSpecific::UDHAndReplyPath => "UDHAndReplyPath".to_string(),
        }
    }

    pub fn from_string(value: &str) -> Self {
        match value {
            "NoSpecific" => GSMSpecific::NoSpecific,
            "UDHIndicator" => GSMSpecific::UDHIndicator,
            "ReplyPath" => GSMSpecific::ReplyPath,
            "UDHAndReplyPath" => GSMSpecific::UDHAndReplyPath,
            _ => GSMSpecific::NoSpecific,
        }
    }
}

pub struct EsmClass {
    pub message_mode: MessageMode,
    pub message_type: MessageType,
    pub ansi41_specific: Ansi41Specific,
    pub gsm_specific: GSMSpecific,
}

impl Default for EsmClass {
    fn default() -> Self {
        EsmClass {
            message_mode: MessageMode::Default,
            message_type: MessageType::Default,
            ansi41_specific: Ansi41Specific::Unknown,
            gsm_specific: GSMSpecific::NoSpecific,
        }
    }
}

impl EsmClass {
    pub fn new(
        message_mode: MessageMode,
        message_type: MessageType,
        ansi41_specific: Ansi41Specific,
        gsm_specific: GSMSpecific,
    ) -> Self {
        EsmClass {
            message_mode,
            message_type,
            ansi41_specific,
            gsm_specific,
        }
    }

    pub fn merge_from(&mut self, other: &EsmClass) {
        if other.message_mode != MessageMode::Default {
            self.message_mode = other.message_mode;
        }
        if other.message_type != MessageType::Default {
            self.message_type = other.message_type;
        }
        if other.gsm_specific != GSMSpecific::NoSpecific {
            self.gsm_specific = other.gsm_specific;
        }
        if other.ansi41_specific != Ansi41Specific::Unknown {
            self.ansi41_specific = other.ansi41_specific;
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        let message_mode = MessageMode::from_u8(value);
        let message_type = MessageType::from_u8(value);
        let ansi41_specific = Ansi41Specific::from_u8(value);
        let gsm_specific = GSMSpecific::from_u8(value);

        Some(EsmClass {
            message_mode,
            message_type,
            ansi41_specific,
            gsm_specific,
        })
    }

    pub fn as_u8(&self) -> u8 {
        let mut value = 0u8;
        value |= self.message_mode.as_u8();
        value |= self.message_type.as_u8();
        value |= self.ansi41_specific.as_u8();
        value |= self.gsm_specific.as_u8();
        value
    }

    pub fn to_string(&self) -> String {
        format!(
            "EsmClass {{ message_mode: {}, message_type: {}, ansi41_specific: {}, gsm_specific: {} }}",
            self.message_mode.to_string(),
            self.message_type.to_string(),
            self.ansi41_specific.to_string(),
            self.gsm_specific.to_string()
        )
    }
    pub fn from_string(value: &str) -> Option<Self> {
        let parts: Vec<&str> = value
            .trim_start_matches("EsmClass {")
            .trim_end_matches("}")
            .split(',')
            .map(|s| s.trim())
            .collect();

        if parts.len() != 4 {
            return None;
        }

        let message_mode =
            MessageMode::from_string(parts[0].trim_start_matches("message_mode: ").trim())?;
        let message_type =
            MessageType::from_string(parts[1].trim_start_matches("message_type: ").trim())?;
        let ansi41_specific =
            Ansi41Specific::from_string(parts[2].trim_start_matches("ansi41_specific: ").trim());
        let gsm_specific =
            GSMSpecific::from_string(parts[3].trim_start_matches("gsm_specific: ").trim());

        Some(EsmClass {
            message_mode,
            message_type,
            ansi41_specific,
            gsm_specific,
        })
    }
}
