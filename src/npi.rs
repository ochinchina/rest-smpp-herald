#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NPI {
    Unknown = 0b00000000,
    ISDN = 0b00000001,
    Data = 0b00000011,
    Telex = 0b00000100,
    LandMobile = 0b00000110,
    National = 0b00001000,
    Private = 0b00001001,
    ERMES = 0b00001010,
    Internet = 0b00001110,
    WAPClientId = 0b00010010,
}

impl NPI {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0b00000000 => Some(NPI::Unknown),
            0b00000001 => Some(NPI::ISDN),
            0b00000011 => Some(NPI::Data),
            0b00000100 => Some(NPI::Telex),
            0b00000110 => Some(NPI::LandMobile),
            0b00001000 => Some(NPI::National),
            0b00001001 => Some(NPI::Private),
            0b00001010 => Some(NPI::ERMES),
            0b00001110 => Some(NPI::Internet),
            0b00010010 => Some(NPI::WAPClientId),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "unknown" => Some(NPI::Unknown),
            "isdn" => Some(NPI::ISDN),
            "data" => Some(NPI::Data),
            "telex" => Some(NPI::Telex),
            "landmobile" => Some(NPI::LandMobile),
            "national" => Some(NPI::National),
            "private" => Some(NPI::Private),
            "ermes" => Some(NPI::ERMES),
            "internet" => Some(NPI::Internet),
            "wapclientid" => Some(NPI::WAPClientId),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            NPI::Unknown => "Unknown",
            NPI::ISDN => "ISDN",
            NPI::Data => "Data",
            NPI::Telex => "Telex",
            NPI::LandMobile => "LandMobile",
            NPI::National => "National",
            NPI::Private => "Private",
            NPI::ERMES => "ERMES",
            NPI::Internet => "Internet",
            NPI::WAPClientId => "WAPClientId",
        }
    }
}
