// SMPP v3.4 and v5.0 Command Status codes

/// No Error
pub const ESME_ROK: u32 = 0x00000000;
/// Message Length is invalid
pub const ESME_RINVMSGLEN: u32 = 0x00000001;
/// Command Length is invalid
pub const ESME_RINVCMDLEN: u32 = 0x00000002;
/// Invalid Command ID
pub const ESME_RINVCMDID: u32 = 0x00000003;
/// Incorrect BIND Status for given command
pub const ESME_RINVBNDSTS: u32 = 0x00000004;
/// ESME Already in Bound State
pub const ESME_RALYBND: u32 = 0x00000005;
/// Invalid Priority Flag
pub const ESME_RINVPRTFLG: u32 = 0x00000006;
/// Invalid Registered Delivery Flag
pub const ESME_RINVREGDLVFLG: u32 = 0x00000007;
/// System error
pub const ESME_RSYSERR: u32 = 0x00000008;
/// Invalid Source Address
pub const ESME_RINVSRCADR: u32 = 0x0000000A;
/// Invalid Dest Addr
pub const ESME_RINVDSTADR: u32 = 0x0000000B;
/// Message ID is invalid
pub const ESME_RINVMSGID: u32 = 0x0000000C;
/// Bind Failed
pub const ESME_RBINDFAIL: u32 = 0x0000000D;
/// Invalid Password
pub const ESME_RINVPASWD: u32 = 0x0000000E;
/// Invalid System ID
pub const ESME_RINVSYSID: u32 = 0x0000000F;
/// Cancel SM Failed
pub const ESME_RCANCELFAIL: u32 = 0x00000011;
/// Replace SM Failed
pub const ESME_RREPLACEFAIL: u32 = 0x00000013;
/// Message Queue Full
pub const ESME_RMSGQFUL: u32 = 0x00000014;
/// Invalid Service Type
pub const ESME_RINVSERTYP: u32 = 0x00000015;
/// Invalid number of destinations
pub const ESME_RINVNUMDESTS: u32 = 0x00000033;
/// Invalid Distribution List name
pub const ESME_RINVDLNAME: u32 = 0x00000034;
/// Destination flag is invalid (submit_multi)
pub const ESME_RINVDESTFLAG: u32 = 0x00000040;
/// Submit w/replace functionality has been requested where replace functionality is not supported
pub const ESME_RINVSUBREP: u32 = 0x00000042;
/// Invalid esm_class field data
pub const ESME_RINVESMCLASS: u32 = 0x00000043;
/// Cannot Submit to Distribution List
pub const ESME_RCNTSUBDL: u32 = 0x00000044;
/// submit_sm or submit_multi failed
pub const ESME_RSUBMITFAIL: u32 = 0x00000045;
/// Invalid Source address TON
pub const ESME_RINVSRCTON: u32 = 0x00000048;
/// Invalid Source address NPI
pub const ESME_RINVSRCNPI: u32 = 0x00000049;
/// Invalid Destination address TON
pub const ESME_RINVDSTTON: u32 = 0x00000050;
/// Invalid Destination address NPI
pub const ESME_RINVDSTNPI: u32 = 0x00000051;
/// Invalid system_type field
pub const ESME_RINVSYSTYP: u32 = 0x00000053;
/// Invalid replace_if_present flag
pub const ESME_RINVREPFLAG: u32 = 0x00000054;
/// Invalid number of messages
pub const ESME_RINVNUMMSGS: u32 = 0x00000055;
/// Throttling error (ESME has exceeded allowed message limits)
pub const ESME_RTHROTTLED: u32 = 0x00000058;
/// Invalid Scheduled Delivery Time
pub const ESME_RINVSCHED: u32 = 0x00000061;
/// Invalid message validity period (Expiry time)
pub const ESME_RINVEXPIRY: u32 = 0x00000062;
/// Predefined Message Invalid or Not Found
pub const ESME_RINVDFTMSGID: u32 = 0x00000063;
/// ESME Receiver Temporary App Error Code
pub const ESME_RX_T_APPN: u32 = 0x00000064;
/// ESME Receiver Permanent App Error Code
pub const ESME_RX_P_APPN: u32 = 0x00000065;
/// ESME Receiver Reject Message Error Code
pub const ESME_RX_R_APPN: u32 = 0x00000066;
/// query_sm request failed
pub const ESME_RQUERYFAIL: u32 = 0x00000067;
/// Error in the optional part of the PDU Body
pub const ESME_RINVOPTPARSTREAM: u32 = 0x000000C0;
/// Optional Parameter not allowed
pub const ESME_ROPTPARNOTALLWD: u32 = 0x000000C1;
/// Invalid Parameter Length
pub const ESME_RINVPARLEN: u32 = 0x000000C2;
/// Expected Optional Parameter missing
pub const ESME_RMISSINGOPTPARAM: u32 = 0x000000C3;
/// Invalid Optional Parameter Value
pub const ESME_RINVOPTPARAMVAL: u32 = 0x000000C4;
/// Delivery Failure (used for data_sm_resp)
pub const ESME_RDELIVERYFAILURE: u32 = 0x000000FE;
/// Unknown Error
pub const ESME_RUNKNOWNERR: u32 = 0x000000FF;
/// ESME Not authorised to use specified service_type
pub const ESME_RSERTYPUNAUTH: u32 = 0x00000100;
/// ESME Prohibited from using specified operation
pub const ESME_RPROHIBITED: u32 = 0x00000101;
/// Specified service_type is not available
pub const ESME_RSERTYPUNAVAIL: u32 = 0x00000102;
/// Specified service_type is denied
pub const ESME_RSERTYPDENIED: u32 = 0x00000103;
/// Invalid Data Coding Scheme
pub const ESME_RINVDCS: u32 = 0x00000104;
/// Source Address Sub unit is Invalid
pub const ESME_RINVSRCADDRSUBUNIT: u32 = 0x00000105;
/// Destination Address Sub unit is Invalid
pub const ESME_RINVDSTADDRSUBUNIT: u32 = 0x00000106;
/// Broadcast Frequency Interval is invalid
pub const ESME_RINVBCASTFREQINT: u32 = 0x00000107;
/// Broadcast Alias Name is invalid
pub const ESME_RINVBCASTALIAS_NAME: u32 = 0x00000108;
/// Broadcast Area Format is invalid
pub const ESME_RINVBCASTAREAFMT: u32 = 0x00000109;
/// Number of Broadcast Areas is invalid
pub const ESME_RINVNUMBCAST_AREAS: u32 = 0x0000010A;
/// Broadcast Content Type is invalid
pub const ESME_RINVBCASTCNTTYPE: u32 = 0x0000010B;
/// Broadcast Message class is invalid
pub const ESME_RINVBCASTMSGCLASS: u32 = 0x0000010C;
/// broadcast_sm operation failed
pub const ESME_RBCASTFAIL: u32 = 0x0000010D;
/// query_broadcast_sm operation failed
pub const ESME_RBCASTQUERYFAIL: u32 = 0x0000010E;
/// cancel_broadcast_sm operation failed
pub const ESME_RBCASTCANCELFAIL: u32 = 0x0000010F;
/// Number of Repeated Broadcasts is invalid
pub const ESME_RINVBCAST_REP: u32 = 0x00000110;
/// Broadcast Service Group is invalid
pub const ESME_RINVBCASTSRVGRP: u32 = 0x00000111;
/// Broadcast Channel Indicator is invalid
pub const ESME_RINVBCASTCHANIND: u32 = 0x00000112;
