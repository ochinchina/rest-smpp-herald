pub enum ServiceType {
    NULL,
    CMT,
    CPT,
    VMN,
    VMA,
    WAP,
    USSD,
    CBS,
    GUTS,
}

impl ServiceType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "" => Some(ServiceType::NULL),
            "CMT" => Some(ServiceType::CMT),
            "CPT" => Some(ServiceType::CPT),
            "VMN" => Some(ServiceType::VMN),
            "VMA" => Some(ServiceType::VMA),
            "WAP" => Some(ServiceType::WAP),
            "USSD" => Some(ServiceType::USSD),
            "CBS" => Some(ServiceType::CBS),
            "GUTS" => Some(ServiceType::GUTS),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            ServiceType::NULL => "",
            ServiceType::CMT => "CMT",
            ServiceType::CPT => "CPT",
            ServiceType::VMN => "VMN",
            ServiceType::VMA => "VMA",
            ServiceType::WAP => "WAP",
            ServiceType::USSD => "USSD",
            ServiceType::CBS => "CBS",
            ServiceType::GUTS => "GUTS",
        }
    }
}
