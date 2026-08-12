#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryReceipt {
    NoRequested = 0b00000000,
    RequestedWhenDeliverySucessOrFailure = 0b00000001,
    RequestedWhenDeliveryFailure = 0b00000010,
    RequestedWhenDeliverySuccess = 0b00000011,
}

impl Default for DeliveryReceipt {
    fn default() -> Self {
        DeliveryReceipt::NoRequested
    }
}

impl DeliveryReceipt {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b00000011 {
            0b00000000 => DeliveryReceipt::NoRequested,
            0b00000001 => DeliveryReceipt::RequestedWhenDeliverySucessOrFailure,
            0b00000010 => DeliveryReceipt::RequestedWhenDeliveryFailure,
            0b00000011 => DeliveryReceipt::RequestedWhenDeliverySuccess,
            _ => unreachable!(),
        }
    }

    pub fn is_delivery_receipt_requested(&self) -> bool {
        !matches!(self, DeliveryReceipt::NoRequested)
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            DeliveryReceipt::NoRequested => "NoRequested".to_string(),
            DeliveryReceipt::RequestedWhenDeliverySucessOrFailure => {
                "RequestedWhenDeliverySucessOrFailure".to_string()
            }
            DeliveryReceipt::RequestedWhenDeliveryFailure => {
                "RequestedWhenDeliveryFailure".to_string()
            }
            DeliveryReceipt::RequestedWhenDeliverySuccess => {
                "RequestedWhenDeliverySuccess".to_string()
            }
        }
    }

    pub fn from_string(value: &str) -> Option<Self> {
        match value {
            "NoRequested" => Some(DeliveryReceipt::NoRequested),
            "RequestedWhenDeliverySucessOrFailure" => {
                Some(DeliveryReceipt::RequestedWhenDeliverySucessOrFailure)
            }
            "RequestedWhenDeliveryFailure" => Some(DeliveryReceipt::RequestedWhenDeliveryFailure),
            "RequestedWhenDeliverySuccess" => Some(DeliveryReceipt::RequestedWhenDeliverySuccess),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SMEOriginatedAcknowledgement {
    NoRequested = 0b00000000,
    DeliveryAckRequested = 0b00000100,
    ManualAckRequest = 0b00001000,
    DeliveryAckAndManualAckRequested = 0b00001100,
}

impl Default for SMEOriginatedAcknowledgement {
    fn default() -> Self {
        SMEOriginatedAcknowledgement::NoRequested
    }
}

impl SMEOriginatedAcknowledgement {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b00001100 {
            0b00000000 => SMEOriginatedAcknowledgement::NoRequested,
            0b00000100 => SMEOriginatedAcknowledgement::DeliveryAckRequested,
            0b00001000 => SMEOriginatedAcknowledgement::ManualAckRequest,
            0b00001100 => SMEOriginatedAcknowledgement::DeliveryAckAndManualAckRequested,
            _ => unreachable!(),
        }
    }

    pub fn is_sme_ack_requested(&self) -> bool {
        !matches!(self, SMEOriginatedAcknowledgement::NoRequested)
    }
    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            SMEOriginatedAcknowledgement::NoRequested => "NoRequested".to_string(),
            SMEOriginatedAcknowledgement::DeliveryAckRequested => {
                "DeliveryAckRequested".to_string()
            }
            SMEOriginatedAcknowledgement::ManualAckRequest => "ManualAckRequest".to_string(),
            SMEOriginatedAcknowledgement::DeliveryAckAndManualAckRequested => {
                "DeliveryAckAndManualAckRequested".to_string()
            }
        }
    }

    pub fn from_string(value: &str) -> Option<Self> {
        match value {
            "NoRequested" => Some(SMEOriginatedAcknowledgement::NoRequested),
            "DeliveryAckRequested" => Some(SMEOriginatedAcknowledgement::DeliveryAckRequested),
            "ManualAckRequest" => Some(SMEOriginatedAcknowledgement::ManualAckRequest),
            "DeliveryAckAndManualAckRequested" => {
                Some(SMEOriginatedAcknowledgement::DeliveryAckAndManualAckRequested)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntermediateNotification {
    NoRequested = 0b00000000,
    Requested = 0b00010000,
}

impl Default for IntermediateNotification {
    fn default() -> Self {
        IntermediateNotification::NoRequested
    }
}

impl IntermediateNotification {
    pub fn from_u8(value: u8) -> Self {
        match value & 0b00010000 {
            0b00000000 => IntermediateNotification::NoRequested,
            0b00010000 => IntermediateNotification::Requested,
            _ => unreachable!(),
        }
    }

    pub fn to_u8(&self) -> u8 {
        *self as u8
    }

    pub fn to_string(&self) -> String {
        match self {
            IntermediateNotification::NoRequested => "NoRequested".to_string(),
            IntermediateNotification::Requested => "Requested".to_string(),
        }
    }

    pub fn from_string(value: &str) -> Option<Self> {
        match value {
            "NoRequested" => Some(IntermediateNotification::NoRequested),
            "Requested" => Some(IntermediateNotification::Requested),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredDelivery {
    pub delivery_receipt: DeliveryReceipt,
    pub sme_acked: SMEOriginatedAcknowledgement,
    pub intermediate_notification: IntermediateNotification,
}

impl Default for RegisteredDelivery {
    fn default() -> Self {
        Self {
            delivery_receipt: DeliveryReceipt::default(),
            sme_acked: SMEOriginatedAcknowledgement::default(),
            intermediate_notification: IntermediateNotification::default(),
        }
    }
}

impl RegisteredDelivery {
    pub fn new(
        delivery_receipt: DeliveryReceipt,
        sme_acked: SMEOriginatedAcknowledgement,
        intermediate_notification: IntermediateNotification,
    ) -> Self {
        Self {
            delivery_receipt,
            sme_acked,
            intermediate_notification,
        }
    }

    pub fn to_u8(&self) -> u8 {
        let mut value = 0u8;
        value |= self.delivery_receipt as u8;
        value |= self.sme_acked as u8;
        value |= self.intermediate_notification as u8;
        value
    }

    pub fn from_u8(value: u8) -> Self {
        let delivery_receipt = match value & 0b00000011 {
            0b00000000 => DeliveryReceipt::NoRequested,
            0b00000001 => DeliveryReceipt::RequestedWhenDeliverySucessOrFailure,
            0b00000010 => DeliveryReceipt::RequestedWhenDeliveryFailure,
            0b00000011 => DeliveryReceipt::RequestedWhenDeliverySuccess,
            _ => unreachable!(),
        };

        let sme_acked = match value & 0b00001100 {
            0b00000000 => SMEOriginatedAcknowledgement::NoRequested,
            0b00000100 => SMEOriginatedAcknowledgement::DeliveryAckRequested,
            0b00001000 => SMEOriginatedAcknowledgement::ManualAckRequest,
            0b00001100 => SMEOriginatedAcknowledgement::DeliveryAckAndManualAckRequested,
            _ => unreachable!(),
        };

        let intermediate_notification = match value & 0b00010000 {
            0b00000000 => IntermediateNotification::NoRequested,
            0b00010000 => IntermediateNotification::Requested,
            _ => unreachable!(),
        };

        Self {
            delivery_receipt,
            sme_acked,
            intermediate_notification,
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "RegisteredDelivery {{ delivery_receipt: {}, sme_acked: {}, intermediate_notification: {} }}",
            self.delivery_receipt.to_string(),
            self.sme_acked.to_string(),
            self.intermediate_notification.to_string()
        )
    }

    pub fn from_string(value: &str) -> Option<Self> {
        let parts: Vec<&str> = value
            .trim_start_matches("RegisteredDelivery { ")
            .trim_end_matches(" }")
            .split(", ")
            .collect();

        if parts.len() != 3 {
            return None;
        }

        let delivery_receipt =
            DeliveryReceipt::from_string(parts[0].trim_start_matches("delivery_receipt: ").trim())?;

        let sme_acked = SMEOriginatedAcknowledgement::from_string(
            parts[1].trim_start_matches("sme_acked: ").trim(),
        )?;

        let intermediate_notification = IntermediateNotification::from_string(
            parts[2]
                .trim_start_matches("intermediate_notification: ")
                .trim(),
        )?;

        Some(Self {
            delivery_receipt,
            sme_acked,
            intermediate_notification,
        })
    }
}

impl Into<u8> for RegisteredDelivery {
    fn into(self) -> u8 {
        self.to_u8()
    }
}

impl From<u8> for RegisteredDelivery {
    fn from(value: u8) -> Self {
        Self::from_u8(value)
    }
}
