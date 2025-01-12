#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum DomainError {
    InvalidProtocol(String),
    InvalidRequest,
    UnsupportedVersion,
    StorageError(String),
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DomainError::InvalidProtocol(msg) => write!(f, "Protocol error: {}", msg),
            DomainError::InvalidRequest => write!(f, "Invalid request"),
            DomainError::UnsupportedVersion => write!(f, "Unsupported version"),
            DomainError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for DomainError {}

