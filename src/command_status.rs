// SMPP v3.4 and v5.0 Command Status codes

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    /// No Error
    EsmeRok = 0x00000000,
    /// Message Length is invalid
    EsmeRInvMsgLen = 0x00000001,
    /// Command Length is invalid
    EsmeRInvCmdLen = 0x00000002,
    /// Invalid Command ID
    EsmeRInvCmdId = 0x00000003,
    /// Incorrect BIND Status for given command
    EsmeRInvBndSts = 0x00000004,
    /// Already in Bound State
    EsmeRAlyBnd = 0x00000005,
    /// Invalid Priority Flag
    EsmeRInvPrtFlg = 0x00000006,
    /// Invalid Registered Delivery Flag,
    EsmeRInvRegDlvFlg = 0x00000007,
    /// System Error
    EsmeRSysErr = 0x00000008,
    /// Invalid Source Address
    EsmeRInvSrcAdr = 0x0000000A,
    /// Invalid Destination Address
    EsmeRInvDstAdr = 0x0000000B,
    /// Invalid Message ID
    EsmeRInvMsgId = 0x0000000C,
    /// Bind Failed
    EsmeRBindFail = 0x0000000D,
    /// Invalid Password
    EsmeRInvPaswd = 0x0000000E,
    /// Invalid System ID
    EsmeRInvSysId = 0x0000000F,
    /// Cancel SM Failed
    EsmeRCancelFail = 0x00000011,
    /// Replace SM Failed
    EsmeRReplaceFail = 0x00000013,
    /// Message Queue Full
    EsmeRMsgQFul = 0x00000014,
    /// Invalid Service Type
    EsmeRInvSerTyp = 0x00000015,
    /// Invalid number of destinations
    EsmeRInvNumDests = 0x00000033,
    /// Invalid Distribution List name
    EsmeRInvDlName = 0x00000034,
    /// Destination flag is invalid (submit_multi)
    EsmeRInvDestFlag = 0x00000040,
    /// Submit w/replace functionality has been requested where replace functionality is not supported
    EsmeRInvSubRep = 0x00000042,
    /// Invalid esm_class field data
    EsmeRInvEsmClass = 0x00000043,
    /// Cannot Submit to Distribution List
    EsmeRCntSubDl = 0x00000044,
    /// submit_sm or submit_multi failed
    EsmeRSubmitFail = 0x00000045,
    /// Invalid Source address TON
    EsmeRInvSrcTon = 0x00000048,
    /// Invalid Source address NPI
    EsmeRInvSrcNpi = 0x00000049,
    /// Invalid Destination address TON
    EsmeRInvDstTon = 0x00000050,
    /// Invalid Destination address NPI
    EsmeRInvDstNpi = 0x00000051,
    /// Invalid system_type field
    EsmeRInvSysTyp = 0x00000053,
    /// Invalid replace_if_present flag
    EsmeRInvRepFlag = 0x00000054,
    /// Invalid number of messages
    EsmeRInvNumMsgs = 0x00000055,
    /// Throttling error (ESME has exceeded allowed message limits)
    EsmeRThrottled = 0x00000058,
    /// Invalid Scheduled Delivery Time
    EsmeRInvSched = 0x00000061,
    /// Invalid message validity period (Expiry time)
    EsmeRInvExpiry = 0x00000062,
    /// Predefined Message Invalid or Not Found
    EsmeRInvDftMsgId = 0x00000063,
    /// ESME Receiver Temporary App Error Code
    EsmeRxTAppn = 0x00000064,
    /// ESME Receiver Permanent App Error Code
    EsmeRxPAppn = 0x00000065,
    /// ESME Receiver Reject Message Error Code
    EsmeRxRAppn = 0x00000066,
    /// query_sm request failed
    EsmeRQueryFail = 0x00000067,
    /// Error in the optional part of the PDU Body
    EsmeRInvOptParStream = 0x000000C0,
    /// Optional Parameter not allowed
    EsmeROptParNotAllwd = 0x000000C1,
    /// Invalid Parameter Length
    EsmeRInvParLen = 0x000000C2,
    /// Expected Optional Parameter missing
    EsmeRMissingOptParam = 0x000000C3,
    /// Invalid Optional Parameter Value
    EsmeRInvOptParamVal = 0x000000C4,
    /// Delivery Failure (used for data_sm_resp)
    EsmeRDeliveryFailure = 0x000000FE,
    /// Unknown Error
    EsmeRUnknownErr = 0x000000FF,
    /// ESME Not authorised to use specified service_type
    EsmeRSerTypUnauth = 0x00000100,
    /// ESME Prohibited from using specified operation
    EsmeRProhibited = 0x00000101,
    /// Specified service_type is not available
    EsmeRSerTypUnavail = 0x00000102,
    /// Specified service_type is denied
    EsmeRSerTypDenied = 0x00000103,
    /// Invalid Data Coding Scheme
    EsmeRInvDcs = 0x00000104,
    /// Source Address Sub unit is Invalid
    EsmeRInvSrcAddrSubunit = 0x00000105,
    /// Destination Address Sub unit is Invalid
    EsmeRInvDstAddrSubunit = 0x00000106,
    /// Broadcast Frequency Interval is invalid
    EsmeRInvBcastFreqInt = 0x00000107,
    /// Broadcast Alias Name is invalid
    EsmeRInvBcastAliasName = 0x00000108,
    /// Broadcast Area Format is invalid
    EsmeRInvBcastAreaFmt = 0x00000109,
    /// Number of Broadcast Areas is invalid
    EsmeRInvNumBcastAreas = 0x0000010A,
    /// Broadcast Content Type is invalid
    EsmeRInvBcastCntType = 0x0000010B,
    /// Broadcast Message class is invalid
    EsmeRInvBcastMsgClass = 0x0000010C,
    /// broadcast_sm operation failed
    EsmeRBcastFail = 0x0000010D,
    /// query_broadcast_sm operation failed
    EsmeRBcastQueryFail = 0x0000010E,
    /// cancel_broadcast_sm operation failed
    EsmeRBcastCancelFail = 0x0000010F,
    /// Number of Repeated Broadcasts is invalid
    EsmeRInvBcastRep = 0x00000110,
    /// Broadcast Service Group is invalid
    EsmeRInvBcastSrvGrp = 0x00000111,
    /// Broadcast Channel Indicator is invalid
    EsmeRInvBcastChanInd = 0x00000112,
}

impl CommandStatus {
    pub fn from_str(value: &str) -> Option<CommandStatus> {
        match value {
            "ESME_ROK" => Some(CommandStatus::EsmeRok),
            "ESME_RINVMSGLEN" => Some(CommandStatus::EsmeRInvMsgLen),
            "ESME_RINVCMDLEN" => Some(CommandStatus::EsmeRInvCmdLen),
            "ESME_RINVCMDID" => Some(CommandStatus::EsmeRInvCmdId),
            "ESME_RINVBNDSTS" => Some(CommandStatus::EsmeRInvBndSts),
            "ESME_RALYBND" => Some(CommandStatus::EsmeRAlyBnd),
            "ESME_RINVPRTFLG" => Some(CommandStatus::EsmeRInvPrtFlg),
            "ESME_RINVREGDLVFLG" => Some(CommandStatus::EsmeRInvRegDlvFlg),
            "ESME_RSYSERR" => Some(CommandStatus::EsmeRSysErr),
            "ESME_RINVSRCADR" => Some(CommandStatus::EsmeRInvSrcAdr),
            "ESME_RINVDSTADR" => Some(CommandStatus::EsmeRInvDstAdr),
            "ESME_RINVMSGID" => Some(CommandStatus::EsmeRInvMsgId),
            "ESME_RBINDFAIL" => Some(CommandStatus::EsmeRBindFail),
            "ESME_RINVPASWD" => Some(CommandStatus::EsmeRInvPaswd),
            "ESME_RINVSYSID" => Some(CommandStatus::EsmeRInvSysId),
            "ESME_RCANCELFAIL" => Some(CommandStatus::EsmeRCancelFail),
            "ESME_RREPLACEFAIL" => Some(CommandStatus::EsmeRReplaceFail),
            "ESME_RMSGQFUL" => Some(CommandStatus::EsmeRMsgQFul),
            "ESME_RINVSERTYP" => Some(CommandStatus::EsmeRInvSerTyp),
            "ESME_RINVNUMDESTS" => Some(CommandStatus::EsmeRInvNumDests),
            "ESME_RINVDLNAME" => Some(CommandStatus::EsmeRInvDlName),
            "ESME_RINVDESTFLAG" => Some(CommandStatus::EsmeRInvDestFlag),
            "ESME_RINVSUBREP" => Some(CommandStatus::EsmeRInvSubRep),
            "ESME_RINVESMCLASS" => Some(CommandStatus::EsmeRInvEsmClass),
            "ESME_RCNTSUBDL" => Some(CommandStatus::EsmeRCntSubDl),
            "ESME_RSUBMITFAIL" => Some(CommandStatus::EsmeRSubmitFail),
            "ESME_RINVSRC_TON" => Some(CommandStatus::EsmeRInvSrcTon),
            "ESME_RINVSRC_NPI" => Some(CommandStatus::EsmeRInvSrcNpi),
            "ESME_RINVDST_TON" => Some(CommandStatus::EsmeRInvDstTon),
            "ESME_RINVDST_NPI" => Some(CommandStatus::EsmeRInvDstNpi),
            "ESME_RINVSYSTYP" => Some(CommandStatus::EsmeRInvSysTyp),
            "ESME_RINVREPLACEIFPRESENT" => Some(CommandStatus::EsmeRInvRepFlag),
            "ESME_RINVNUMMSGS" => Some(CommandStatus::EsmeRInvNumMsgs),
            "ESME_RTHROTTLED" => Some(CommandStatus::EsmeRThrottled),
            "ESME_RINVSCHED" => Some(CommandStatus::EsmeRInvSched),
            "ESME_RINVEXPIRY" => Some(CommandStatus::EsmeRInvExpiry),
            "ESME_RINVDFTMSGID" => Some(CommandStatus::EsmeRInvDftMsgId),
            "ESME_RXTAPPN" => Some(CommandStatus::EsmeRxTAppn),
            "ESME_RXPAPPN" => Some(CommandStatus::EsmeRxPAppn),
            "ESME_RXRAPPN" => Some(CommandStatus::EsmeRxRAppn),
            "ESME_RQUERYFAIL" => Some(CommandStatus::EsmeRQueryFail),
            "ESME_RINVOPTPARSTREAM" => Some(CommandStatus::EsmeRInvOptParStream),
            "ESME_ROPTPARNOTALLOWED" => Some(CommandStatus::EsmeROptParNotAllwd),
            "ESME_RINVPARLEN" => Some(CommandStatus::EsmeRInvParLen),
            "ESME_RMISSINGOPTPARAM" => Some(CommandStatus::EsmeRMissingOptParam),
            "ESME_RINVOPTPARAMVAL" => Some(CommandStatus::EsmeRInvOptParamVal),
            "ESME_RDELIVERYFAILURE" => Some(CommandStatus::EsmeRDeliveryFailure),
            "ESME_RUNKNOWNERR" => Some(CommandStatus::EsmeRUnknownErr),
            "ESME_RSERTYPUNAUTH" => Some(CommandStatus::EsmeRSerTypUnauth),
            "ESME_RPROHIBITED" => Some(CommandStatus::EsmeRProhibited),
            "ESME_RSERTYPUNAVAIL" => Some(CommandStatus::EsmeRSerTypUnavail),
            "ESME_RSERTYPDENIED" => Some(CommandStatus::EsmeRSerTypDenied),
            "ESME_RINVDCS" => Some(CommandStatus::EsmeRInvDcs),
            "ESME_RINVSRCADDRSUBUNIT" => Some(CommandStatus::EsmeRInvSrcAddrSubunit),
            "ESME_RINVDSTADDRSUBUNIT" => Some(CommandStatus::EsmeRInvDstAddrSubunit),
            "ESME_RINVBCFREQINT" => Some(CommandStatus::EsmeRInvBcastFreqInt),
            "ESME_RINVBCALIASNAME" => Some(CommandStatus::EsmeRInvBcastAliasName),
            "ESME_RINVBCAREAFMT" => Some(CommandStatus::EsmeRInvBcastAreaFmt),
            "ESME_RINVNUMBCAREAS" => Some(CommandStatus::EsmeRInvNumBcastAreas),
            "ESME_RINVBCONTTYPE" => Some(CommandStatus::EsmeRInvBcastCntType),
            "ESME_RINVBCMSGCLASS" => Some(CommandStatus::EsmeRInvBcastMsgClass),
            "ESME_RBCASTFAIL" => Some(CommandStatus::EsmeRBcastFail),
            "ESME_RBCASTQUERYFAIL" => Some(CommandStatus::EsmeRBcastQueryFail),
            "ESME_RBCASTCANCELFAIL" => Some(CommandStatus::EsmeRBcastCancelFail),
            "ESME_RINVBCREPEAT" => Some(CommandStatus::EsmeRInvBcastRep),
            "ESME_RINVBCSRVGROUP" => Some(CommandStatus::EsmeRInvBcastSrvGrp),
            "ESME_RINVBCCHANIND" => Some(CommandStatus::EsmeRInvBcastChanInd),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            CommandStatus::EsmeRok => "ESME_ROK",
            CommandStatus::EsmeRInvMsgLen => "ESME_RINVMSGLEN",
            CommandStatus::EsmeRInvCmdLen => "ESME_RINVCMDLEN",
            CommandStatus::EsmeRInvCmdId => "ESME_RINVCMDID",
            CommandStatus::EsmeRInvBndSts => "ESME_RINVBNDSTS",
            CommandStatus::EsmeRAlyBnd => "ESME_RALYBND",
            CommandStatus::EsmeRInvPrtFlg => "ESME_RINVPRTFLG",
            CommandStatus::EsmeRInvRegDlvFlg => "ESME_RINVREGDLVFLG",
            CommandStatus::EsmeRSysErr => "ESME_RSYSERR",
            CommandStatus::EsmeRInvSrcAdr => "ESME_RINVSRCADR",
            CommandStatus::EsmeRInvDstAdr => "ESME_RINVDSTADR",
            CommandStatus::EsmeRInvMsgId => "ESME_RINVMSGID",
            CommandStatus::EsmeRBindFail => "ESME_RBINDFAIL",
            CommandStatus::EsmeRInvPaswd => "ESME_RINVPASWD",
            CommandStatus::EsmeRInvSysId => "ESME_RINVSYSID",
            CommandStatus::EsmeRCancelFail => "ESME_RCANCELFAIL",
            CommandStatus::EsmeRReplaceFail => "ESME_RREPLACEFAIL",
            CommandStatus::EsmeRMsgQFul => "ESME_RMSGQFUL",
            CommandStatus::EsmeRInvSerTyp => "ESME_RINVSERTYP",
            CommandStatus::EsmeRInvNumDests => "ESME_RINVNUMDESTS",
            CommandStatus::EsmeRInvDlName => "ESME_RINVDLNAME",
            CommandStatus::EsmeRInvDestFlag => "ESME_RINVDESTFLAG",
            CommandStatus::EsmeRInvSubRep => "ESME_RINVSUBREP",
            CommandStatus::EsmeRInvEsmClass => "ESME_RINVESMCLASS",
            CommandStatus::EsmeRCntSubDl => "ESME_RCNTSUBDL",
            CommandStatus::EsmeRSubmitFail => "ESME_RSUBMITFAIL",
            CommandStatus::EsmeRInvSrcTon => "ESME_RINVSRC_TON",
            CommandStatus::EsmeRInvSrcNpi => "ESME_RINVSRC_NPI",
            CommandStatus::EsmeRInvDstTon => "ESME_RINVDST_TON",
            CommandStatus::EsmeRInvDstNpi => "ESME_RINVDST_NPI",
            CommandStatus::EsmeRInvSysTyp => "ESME_RINVSYSTYP",
            CommandStatus::EsmeRInvRepFlag => "ESME_RINVREPLACEIFPRESENT",
            CommandStatus::EsmeRInvNumMsgs => "ESME_RINVNUMMSGS",
            CommandStatus::EsmeRThrottled => "ESME_RTHROTTLED",
            CommandStatus::EsmeRInvSched => "ESME_RINVSCHED",
            CommandStatus::EsmeRInvExpiry => "ESME_RINVEXPIRY",
            CommandStatus::EsmeRInvDftMsgId => "ESME_RINVDFTMSGID",
            CommandStatus::EsmeRxTAppn => "ESME_RXTAPPN",
            CommandStatus::EsmeRxPAppn => "ESME_RXPAPPN",
            CommandStatus::EsmeRxRAppn => "ESME_RXRAPPN",
            CommandStatus::EsmeRQueryFail => "ESME_RQUERYFAIL",
            CommandStatus::EsmeRInvOptParStream => "ESME_RINVOPTPARSTREAM",
            CommandStatus::EsmeROptParNotAllwd => "ESME_ROPTPARNOTALLOWED",
            CommandStatus::EsmeRInvParLen => "ESME_RINVPARLEN",
            CommandStatus::EsmeRMissingOptParam => "ESME_RMISSINGOPTPARAM",
            CommandStatus::EsmeRInvOptParamVal => "ESME_RINVOPTPARAMVAL",
            CommandStatus::EsmeRDeliveryFailure => "ESME_RDELIVERYFAILURE",
            CommandStatus::EsmeRUnknownErr => "ESME_RUNKNOWNERR",
            CommandStatus::EsmeRSerTypUnauth => "ESME_RSERTYPUNAUTH",
            CommandStatus::EsmeRProhibited => "ESME_RPROHIBITED",
            CommandStatus::EsmeRSerTypUnavail => "ESME_RSERTYPUNAVAIL",
            CommandStatus::EsmeRSerTypDenied => "ESME_RSERTYPDENIED",
            CommandStatus::EsmeRInvDcs => "ESME_RINVDCS",
            CommandStatus::EsmeRInvSrcAddrSubunit => "ESME_RINVSRCADDRSUBUNIT",
            CommandStatus::EsmeRInvDstAddrSubunit => "ESME_RINVDSTADDRSUBUNIT",
            CommandStatus::EsmeRInvBcastFreqInt => "ESME_RINVBCFREQINT",
            CommandStatus::EsmeRInvBcastAliasName => "ESME_RINVBCALIASNAME",
            CommandStatus::EsmeRInvBcastAreaFmt => "ESME_RINVBCAREAFMT",
            CommandStatus::EsmeRInvNumBcastAreas => "ESME_RINVNUMBCAREAS",
            CommandStatus::EsmeRInvBcastCntType => "ESME_RINVBCONTTYPE",
            CommandStatus::EsmeRInvBcastMsgClass => "ESME_RINVBCMSGCLASS",
            CommandStatus::EsmeRBcastFail => "ESME_RBCASTFAIL",
            CommandStatus::EsmeRBcastQueryFail => "ESME_RBCASTQUERYFAIL",
            CommandStatus::EsmeRBcastCancelFail => "ESME_RBCASTCANCELFAIL",
            CommandStatus::EsmeRInvBcastRep => "ESME_RINVBCREPEAT",
            CommandStatus::EsmeRInvBcastSrvGrp => "ESME_RINVBCSRVGROUP",
            CommandStatus::EsmeRInvBcastChanInd => "ESME_RINVBCCHANIND",
        }
    }

    pub fn from_u32(value: u32) -> Option<CommandStatus> {
        match value {
            0x00000000 => Some(CommandStatus::EsmeRok),
            0x00000001 => Some(CommandStatus::EsmeRInvMsgLen),
            0x00000002 => Some(CommandStatus::EsmeRInvCmdLen),
            0x00000003 => Some(CommandStatus::EsmeRInvCmdId),
            0x00000004 => Some(CommandStatus::EsmeRInvBndSts),
            0x00000005 => Some(CommandStatus::EsmeRAlyBnd),
            0x00000006 => Some(CommandStatus::EsmeRInvPrtFlg),
            0x00000007 => Some(CommandStatus::EsmeRInvRegDlvFlg),
            0x00000008 => Some(CommandStatus::EsmeRSysErr),
            0x0000000A => Some(CommandStatus::EsmeRInvSrcAdr),
            0x0000000B => Some(CommandStatus::EsmeRInvDstAdr),
            0x0000000C => Some(CommandStatus::EsmeRInvMsgId),
            0x0000000D => Some(CommandStatus::EsmeRBindFail),
            0x0000000E => Some(CommandStatus::EsmeRInvPaswd),
            0x0000000F => Some(CommandStatus::EsmeRInvSysId),
            0x00000011 => Some(CommandStatus::EsmeRCancelFail),
            0x00000013 => Some(CommandStatus::EsmeRReplaceFail),
            0x00000014 => Some(CommandStatus::EsmeRMsgQFul),
            0x00000015 => Some(CommandStatus::EsmeRInvSerTyp),
            0x00000033 => Some(CommandStatus::EsmeRInvNumDests),
            0x00000034 => Some(CommandStatus::EsmeRInvDlName),
            0x00000040 => Some(CommandStatus::EsmeRInvDestFlag),
            0x00000042 => Some(CommandStatus::EsmeRInvSubRep),
            0x00000043 => Some(CommandStatus::EsmeRInvEsmClass),
            0x00000044 => Some(CommandStatus::EsmeRCntSubDl),
            0x00000045 => Some(CommandStatus::EsmeRSubmitFail),
            0x00000048 => Some(CommandStatus::EsmeRInvSrcTon),
            0x00000049 => Some(CommandStatus::EsmeRInvSrcNpi),
            0x00000050 => Some(CommandStatus::EsmeRInvDstTon),
            0x00000051 => Some(CommandStatus::EsmeRInvDstNpi),
            0x00000053 => Some(CommandStatus::EsmeRInvSysTyp),
            0x00000054 => Some(CommandStatus::EsmeRInvRepFlag),
            0x00000055 => Some(CommandStatus::EsmeRInvNumMsgs),
            0x00000058 => Some(CommandStatus::EsmeRThrottled),
            0x00000061 => Some(CommandStatus::EsmeRInvSched),
            0x00000062 => Some(CommandStatus::EsmeRInvExpiry),
            0x00000063 => Some(CommandStatus::EsmeRInvDftMsgId),
            0x00000064 => Some(CommandStatus::EsmeRxTAppn),
            0x00000065 => Some(CommandStatus::EsmeRxPAppn),
            0x00000066 => Some(CommandStatus::EsmeRxRAppn),
            0x00000067 => Some(CommandStatus::EsmeRQueryFail),
            0x000000C0 => Some(CommandStatus::EsmeRInvOptParStream),
            0x000000C1 => Some(CommandStatus::EsmeROptParNotAllwd),
            0x000000C2 => Some(CommandStatus::EsmeRInvParLen),
            0x000000C3 => Some(CommandStatus::EsmeRMissingOptParam),
            0x000000C4 => Some(CommandStatus::EsmeRInvOptParamVal),
            0x000000FE => Some(CommandStatus::EsmeRDeliveryFailure),
            0x000000FF => Some(CommandStatus::EsmeRUnknownErr),
            0x00000100 => Some(CommandStatus::EsmeRSerTypUnauth),
            0x00000101 => Some(CommandStatus::EsmeRProhibited),
            0x00000102 => Some(CommandStatus::EsmeRSerTypUnavail),
            0x00000103 => Some(CommandStatus::EsmeRSerTypDenied),
            0x00000104 => Some(CommandStatus::EsmeRInvDcs),
            0x00000105 => Some(CommandStatus::EsmeRInvSrcAddrSubunit),
            0x00000106 => Some(CommandStatus::EsmeRInvDstAddrSubunit),
            0x00000107 => Some(CommandStatus::EsmeRInvBcastFreqInt),
            0x00000108 => Some(CommandStatus::EsmeRInvBcastAliasName),
            0x00000109 => Some(CommandStatus::EsmeRInvBcastAreaFmt),
            0x0000010A => Some(CommandStatus::EsmeRInvNumBcastAreas),
            0x0000010B => Some(CommandStatus::EsmeRInvBcastCntType),
            0x0000010C => Some(CommandStatus::EsmeRInvBcastMsgClass),
            0x0000010D => Some(CommandStatus::EsmeRBcastFail),
            0x0000010E => Some(CommandStatus::EsmeRBcastQueryFail),
            0x0000010F => Some(CommandStatus::EsmeRBcastCancelFail),
            0x00000110 => Some(CommandStatus::EsmeRInvBcastRep),
            0x00000111 => Some(CommandStatus::EsmeRInvBcastSrvGrp),
            0x00000112 => Some(CommandStatus::EsmeRInvBcastChanInd),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
}
