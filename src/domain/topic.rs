use std::str::FromStr;
use crate::domain::error::DomainError;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicId {
    id: [u8; 16],
}

impl TopicId {
    pub fn new(id: [u8; 16]) -> Self {
        Self { id }
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.id
    }

    pub fn to_uuid_string(&self) -> String {
        let hex = hex::encode(self.id);
        format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        )
    }

    pub fn zero() -> Self {
        Self { id: [0; 16] }
    }
}

impl FromStr for TopicId {
    type Err = DomainError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let id = hex::decode(s.replace("-", ""))
            .map_err(|_| DomainError::InvalidRequest)?;
        
        if id.len() != 16 {
            return Err(DomainError::InvalidRequest);
        }

        let mut result = [0u8; 16];
        result.copy_from_slice(&id);
        Ok(Self { id: result })
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(id: [u8; 16]) -> Self {
        Self { id }
    }
}

impl AsRef<[u8]> for TopicId {
    fn as_ref(&self) -> &[u8] {
        &self.id
    }
} 