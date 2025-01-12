use bytes::{Buf, Bytes};
use crate::application::error::ApplicationError;

/// 바이트 스트림으로부터 데이터를 파싱하는 trait
pub trait ByteParser {
    /// 남은 바이트가 충분한지 확인
    fn ensure_remaining(&self, buf: &Bytes, required: usize) -> Result<(), ApplicationError> {
        if buf.remaining() < required {
            return Err(ApplicationError::Protocol(
                format!("buffer too short: need {} bytes but has {}", required, buf.remaining())
            ));
        }
        Ok(())
    }
}

/// 특정 타입으로 역직렬화하는 trait
pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> Result<T, ApplicationError>;
}

/// 특정 타입을 바이트로 직렬화하는 trait
pub trait Serialize {
    fn serialize(&self, dst: &mut Vec<u8>) -> Result<(), ApplicationError>;
}

/// 기본 타입들의 파싱을 위한 trait
pub trait PrimitiveParser: ByteParser {
    fn parse_i8(&self, buf: &mut Bytes) -> Result<i8, ApplicationError>;
    fn parse_i16(&self, buf: &mut Bytes) -> Result<i16, ApplicationError>;
    fn parse_i32(&self, buf: &mut Bytes) -> Result<i32, ApplicationError>;
    fn parse_i64(&self, buf: &mut Bytes) -> Result<i64, ApplicationError>;
    fn parse_u8(&self, buf: &mut Bytes) -> Result<u8, ApplicationError>;
    fn parse_u16(&self, buf: &mut Bytes) -> Result<u16, ApplicationError>;
    fn parse_u32(&self, buf: &mut Bytes) -> Result<u32, ApplicationError>;
    fn parse_u64(&self, buf: &mut Bytes) -> Result<u64, ApplicationError>;
}

/// 컴팩트 문자열 타입 파싱을 위한 trait
pub trait CompactStringParser: ByteParser {
    fn parse_compact_string(&self, buf: &mut Bytes) -> Result<String, ApplicationError>;
    fn parse_compact_bytes(&self, buf: &mut Bytes) -> Result<Vec<u8>, ApplicationError>;
}

/// 컴팩트 배열 타입 파싱을 위한 trait
pub trait CompactArrayParser: ByteParser {
    fn parse_compact_array<T, F>(&self, buf: &mut Bytes, parser: F) -> Result<Vec<T>, ApplicationError>
    where
        F: Fn(&mut Bytes) -> Result<T, ApplicationError>;
}

/// 가변 정수 타입 파싱을 위한 trait
pub trait VarIntParser: ByteParser {
    fn parse_varint(&self, buf: &mut Bytes) -> Result<i64, ApplicationError>;
} 