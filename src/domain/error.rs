#[derive(Debug, Clone, PartialEq)]
pub enum DomainError {
    InvalidProtocol(String),
    InvalidRequest,
    UnsupportedVersion,
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DomainError::InvalidProtocol(msg) => write!(f, "Protocol error: {}", msg),
            DomainError::InvalidRequest => write!(f, "Invalid request"),
            DomainError::UnsupportedVersion => write!(f, "Unsupported version"),
        }
    }
}

impl std::error::Error for DomainError {} 