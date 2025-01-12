#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    UnknownTopicOrPartition = 3,
    UnsupportedVersion = 35,
    InvalidRequest = 42,
    UnknownTopicId = 100,
}

impl From<ErrorCode> for i16 {
    fn from(error_code: ErrorCode) -> Self {
        error_code as i16
    }
}

impl From<i16> for ErrorCode {
    fn from(code: i16) -> Self {
        match code {
            0 => ErrorCode::None,
            3 => ErrorCode::UnknownTopicOrPartition,
            35 => ErrorCode::UnsupportedVersion,
            42 => ErrorCode::InvalidRequest,
            100 => ErrorCode::UnknownTopicId,
            _ => ErrorCode::InvalidRequest,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KafkaMessage {
    pub correlation_id: i32,
    pub payload: Vec<u8>,
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub timestamp: u64,
}

