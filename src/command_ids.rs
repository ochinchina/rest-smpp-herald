// Bind operations

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandId {
    BindReceiver = 0x00000001,
    BindReceiverResp = 0x80000001,
    BindTransmitter = 0x00000002,
    BindTransmitterResp = 0x80000002,
    BindTransceiver = 0x00000009,
    BindTransceiverResp = 0x80000009,
    Unbind = 0x00000006,
    UnbindResp = 0x80000006,
    Outbind = 0x0000000B,

    SubmitSm = 0x00000004,
    SubmitSmResp = 0x80000004,
    SubmitMulti = 0x00000021,
    SubmitMultiResp = 0x80000021,
    DeliverSm = 0x00000005,
    DeliverSmResp = 0x80000005,
    DataSm = 0x00000103,
    DataSmResp = 0x80000103,

    QuerySm = 0x00000003,
    QuerySmResp = 0x80000003,
    CancelSm = 0x00000008,
    CancelSmResp = 0x80000008,
    ReplaceSm = 0x00000007,
    ReplaceSmResp = 0x80000007,

    EnquireLink = 0x00000015,
    EnquireLinkResp = 0x80000015,

    GenericNack = 0x80000000,

    AlertNotification = 0x00000102,

    BroadcastSm = 0x00000111,
    BroadcastSmResp = 0x80000111,

    QueryBroadcastSm = 0x00000112,
    QueryBroadcastSmResp = 0x80000112,

    CancelBroadcastSm = 0x00000113,
    CancelBroadcastSmResp = 0x80000113,

    Unknown = 0xFFFFFFFF, // Represents an unknown command ID
}

impl CommandId {
    pub fn from_u32(value: u32) -> Option<CommandId> {
        match value {
            0x00000001 => Some(CommandId::BindReceiver),
            0x80000001 => Some(CommandId::BindReceiverResp),
            0x00000002 => Some(CommandId::BindTransmitter),
            0x80000002 => Some(CommandId::BindTransmitterResp),
            0x00000009 => Some(CommandId::BindTransceiver),
            0x80000009 => Some(CommandId::BindTransceiverResp),
            0x00000006 => Some(CommandId::Unbind),
            0x80000006 => Some(CommandId::UnbindResp),
            0x0000000B => Some(CommandId::Outbind),

            0x00000004 => Some(CommandId::SubmitSm),
            0x80000004 => Some(CommandId::SubmitSmResp),
            0x00000021 => Some(CommandId::SubmitMulti),
            0x80000021 => Some(CommandId::SubmitMultiResp),
            0x00000005 => Some(CommandId::DeliverSm),
            0x80000005 => Some(CommandId::DeliverSmResp),
            0x00000103 => Some(CommandId::DataSm),
            0x80000103 => Some(CommandId::DataSmResp),

            0x00000003 => Some(CommandId::QuerySm),
            0x80000003 => Some(CommandId::QuerySmResp),
            0x00000008 => Some(CommandId::CancelSm),
            0x80000008 => Some(CommandId::CancelSmResp),
            0x00000007 => Some(CommandId::ReplaceSm),
            0x80000007 => Some(CommandId::ReplaceSmResp),

            0x00000015 => Some(CommandId::EnquireLink),
            0x80000015 => Some(CommandId::EnquireLinkResp),

            0x80000000 => Some(CommandId::GenericNack),

            0x00000102 => Some(CommandId::AlertNotification),

            0x00000111 => Some(CommandId::BroadcastSm),
            0x80000111 => Some(CommandId::BroadcastSmResp),

            0x00000112 => Some(CommandId::QueryBroadcastSm),
            0x80000112 => Some(CommandId::QueryBroadcastSmResp),
            0x00000113 => Some(CommandId::CancelBroadcastSm),
            0x80000113 => Some(CommandId::CancelBroadcastSmResp),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> u32 {
        *self as u32
    }

    pub fn from_str(name: &str) -> Option<CommandId> {
        let name = name.replace("-", "");
        let name = name.replace("_", "");
        if name.starts_with("0x") {
            if let Ok(value) = u32::from_str_radix(&name[2..], 16) {
                return CommandId::from_u32(value);
            }
        }
        match name.to_ascii_lowercase().as_str() {
            "bindreceiver" => Some(CommandId::BindReceiver),
            "bindreceiverresp" | "bindreceiverresponse" => Some(CommandId::BindReceiverResp),
            "bindtransmitter" => Some(CommandId::BindTransmitter),
            "bindtransmitterresp" | "bindtransmitterresponse" => {
                Some(CommandId::BindTransmitterResp)
            }
            "bindtransceiver" => Some(CommandId::BindTransceiver),
            "bindtransceiverresp" | "bindtransceiverresponse" => {
                Some(CommandId::BindTransceiverResp)
            }
            "unbind" => Some(CommandId::Unbind),
            "unbindresp" | "unbindresponse" => Some(CommandId::UnbindResp),
            "outbind" => Some(CommandId::Outbind),

            "submitsm" => Some(CommandId::SubmitSm),
            "submitsmresp" | "submitsmresponse" => Some(CommandId::SubmitSmResp),
            "submitmulti" => Some(CommandId::SubmitMulti),
            "submitmultiresp" | "submitmultiresponse" => Some(CommandId::SubmitMultiResp),
            "deliversm" => Some(CommandId::DeliverSm),
            "deliversmresp" | "deliversmresponse" => Some(CommandId::DeliverSmResp),
            "datasm" => Some(CommandId::DataSm),
            "datasmresp" | "datasmresponse" => Some(CommandId::DataSmResp),

            "querysm" => Some(CommandId::QuerySm),
            "querysmresp" | "querysmresponse" => Some(CommandId::QuerySmResp),
            "cancelsm" => Some(CommandId::CancelSm),
            "cancelsmresp" | "cancelsmresponse" => Some(CommandId::CancelSmResp),
            "replacesm" => Some(CommandId::ReplaceSm),
            "replacesmresp" | "replacesmresponse" => Some(CommandId::ReplaceSmResp),

            "enquirelink" => Some(CommandId::EnquireLink),
            "enquirelinkresp" | "enquirelinkresponse" => Some(CommandId::EnquireLinkResp),

            "genericnack" => Some(CommandId::GenericNack),

            "alertnotification" => Some(CommandId::AlertNotification),

            "broadcastsm" => Some(CommandId::BroadcastSm),
            "broadcastsmresp" | "broadcastsmresponse" => Some(CommandId::BroadcastSmResp),

            "querybroadcastsm" => Some(CommandId::QueryBroadcastSm),
            "querybroadcastsmresp" | "querybroadcastsmresponse" => {
                Some(CommandId::QueryBroadcastSmResp)
            }

            "cancelbroadcastsm" => Some(CommandId::CancelBroadcastSm),
            "cancelbroadcastsmresp" | "cancelbroadcastsmresponse" => {
                Some(CommandId::CancelBroadcastSmResp)
            }

            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            CommandId::BindReceiver => "BindReceiver",
            CommandId::BindReceiverResp => "BindReceiverResp",
            CommandId::BindTransmitter => "BindTransmitter",
            CommandId::BindTransmitterResp => "BindTransmitterResp",
            CommandId::BindTransceiver => "BindTransceiver",
            CommandId::BindTransceiverResp => "BindTransceiverResp",
            CommandId::Unbind => "Unbind",
            CommandId::UnbindResp => "UnbindResp",
            CommandId::Outbind => "Outbind",

            CommandId::SubmitSm => "SubmitSm",
            CommandId::SubmitSmResp => "SubmitSmResp",
            CommandId::SubmitMulti => "SubmitMulti",
            CommandId::SubmitMultiResp => "SubmitMultiResp",
            CommandId::DeliverSm => "DeliverSm",
            CommandId::DeliverSmResp => "DeliverSmResp",
            CommandId::DataSm => "DataSm",
            CommandId::DataSmResp => "DataSmResp",

            CommandId::QuerySm => "QuerySm",
            CommandId::QuerySmResp => "QuerySmResp",
            CommandId::CancelSm => "CancelSm",
            CommandId::CancelSmResp => "CancelSmResp",
            CommandId::ReplaceSm => "ReplaceSm",
            CommandId::ReplaceSmResp => "ReplaceSmResp",
            CommandId::EnquireLink => "EnquireLink",
            CommandId::EnquireLinkResp => "EnquireLinkResp",
            CommandId::GenericNack => "GenericNack",
            CommandId::AlertNotification => "AlertNotification",
            CommandId::BroadcastSm => "BroadcastSm",
            CommandId::BroadcastSmResp => "BroadcastSmResp",
            CommandId::QueryBroadcastSm => "QueryBroadcastSm",
            CommandId::QueryBroadcastSmResp => "QueryBroadcastSmResp",
            CommandId::CancelBroadcastSm => "CancelBroadcastSm",
            CommandId::CancelBroadcastSmResp => "CancelBroadcastSmResp",
            CommandId::Unknown => "Unknown",
        }
    }

    pub fn is_response(&self) -> bool {
        self.as_u32() & 0x80000000 != 0
    }

    pub fn is_request(&self) -> bool {
        !self.is_response()
    }

    pub fn is_client_message(&self) -> bool {
        match self {
            CommandId::BindReceiver
            | CommandId::BindTransmitter
            | CommandId::BindTransceiver
            | CommandId::Unbind
            | CommandId::SubmitSm
            | CommandId::SubmitMulti
            | CommandId::DeliverSmResp
            | CommandId::DataSm
            | CommandId::QuerySm
            | CommandId::CancelSm
            | CommandId::ReplaceSm
            | CommandId::EnquireLink
            | CommandId::EnquireLinkResp
            | CommandId::GenericNack
            | CommandId::BroadcastSm
            | CommandId::QueryBroadcastSm
            | CommandId::CancelBroadcastSm => true,
            _ => false,
        }
    }

    pub fn is_client_request(&self) -> bool {
        match self {
            CommandId::BindReceiver
            | CommandId::BindTransmitter
            | CommandId::BindTransceiver
            | CommandId::Unbind
            | CommandId::SubmitSm
            | CommandId::SubmitMulti
            | CommandId::DataSm
            | CommandId::QuerySm
            | CommandId::CancelSm
            | CommandId::ReplaceSm
            | CommandId::BroadcastSm
            | CommandId::QueryBroadcastSm
            | CommandId::CancelBroadcastSm => true,
            _ => false,
        }
    }
    pub fn is_smsc_message(&self) -> bool {
        match self {
            CommandId::BindReceiverResp
            | CommandId::BindTransmitterResp
            | CommandId::BindTransceiverResp
            | CommandId::Outbind
            | CommandId::UnbindResp
            | CommandId::SubmitSmResp
            | CommandId::SubmitMultiResp
            | CommandId::DeliverSm
            | CommandId::DataSmResp
            | CommandId::QuerySmResp
            | CommandId::CancelSmResp
            | CommandId::ReplaceSmResp
            | CommandId::EnquireLink
            | CommandId::EnquireLinkResp
            | CommandId::GenericNack
            | CommandId::BroadcastSmResp
            | CommandId::AlertNotification
            | CommandId::QueryBroadcastSmResp
            | CommandId::CancelBroadcastSmResp => true,
            _ => false,
        }
    }

    pub fn is_smsc_request(&self) -> bool {
        match self {
            CommandId::Outbind | CommandId::DeliverSm | CommandId::AlertNotification => true,
            _ => false,
        }
    }
}
