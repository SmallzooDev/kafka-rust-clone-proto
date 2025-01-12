use bytes::{Buf, Bytes};
use crate::application::error::ApplicationError;
use super::traits::*;

/// 기본 파서 구현을 제공하는 구조체
#[derive(Debug, Default, Clone)]
pub struct BaseParser;

impl ByteParser for BaseParser {}

impl PrimitiveParser for BaseParser {
    fn parse_i8(&self, buf: &mut Bytes) -> Result<i8, ApplicationError> {
        self.ensure_remaining(buf, 1)?;
        Ok(buf.get_i8())
    }

    fn parse_i16(&self, buf: &mut Bytes) -> Result<i16, ApplicationError> {
        self.ensure_remaining(buf, 2)?;
        Ok(buf.get_i16())
    }

    fn parse_i32(&self, buf: &mut Bytes) -> Result<i32, ApplicationError> {
        self.ensure_remaining(buf, 4)?;
        Ok(buf.get_i32())
    }

    fn parse_i64(&self, buf: &mut Bytes) -> Result<i64, ApplicationError> {
        self.ensure_remaining(buf, 8)?;
        Ok(buf.get_i64())
    }

    fn parse_u8(&self, buf: &mut Bytes) -> Result<u8, ApplicationError> {
        self.ensure_remaining(buf, 1)?;
        Ok(buf.get_u8())
    }

    fn parse_u16(&self, buf: &mut Bytes) -> Result<u16, ApplicationError> {
        self.ensure_remaining(buf, 2)?;
        Ok(buf.get_u16())
    }

    fn parse_u32(&self, buf: &mut Bytes) -> Result<u32, ApplicationError> {
        self.ensure_remaining(buf, 4)?;
        Ok(buf.get_u32())
    }

    fn parse_u64(&self, buf: &mut Bytes) -> Result<u64, ApplicationError> {
        self.ensure_remaining(buf, 8)?;
        Ok(buf.get_u64())
    }
}

impl CompactStringParser for BaseParser {
    fn parse_compact_string(&self, buf: &mut Bytes) -> Result<String, ApplicationError> {
        let len = self.parse_varint(buf)?;
        let string_len = if len > 1 { len as usize - 1 } else { 0 };

        self.ensure_remaining(buf, string_len)?;
        let bytes = buf.slice(..string_len);
        buf.advance(string_len);

        String::from_utf8(bytes.to_vec())
            .map_err(|e| ApplicationError::Protocol(format!("invalid UTF-8 sequence: {}", e)))
    }

    fn parse_compact_bytes(&self, buf: &mut Bytes) -> Result<Vec<u8>, ApplicationError> {
        let len = self.parse_varint(buf)?;
        let bytes_len = if len > 1 { len as usize - 1 } else { 0 };

        self.ensure_remaining(buf, bytes_len)?;
        let bytes = buf.slice(..bytes_len);
        buf.advance(bytes_len);
        Ok(bytes.to_vec())
    }
}

impl CompactArrayParser for BaseParser {
    fn parse_compact_array<T, F>(&self, buf: &mut Bytes, parser: F) -> Result<Vec<T>, ApplicationError>
    where
        F: Fn(&mut Bytes) -> Result<T, ApplicationError>,
    {
        let len = self.parse_varint(buf)?;
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            items.push(parser(buf)?);
        }

        Ok(items)
    }
}

impl VarIntParser for BaseParser {
    fn parse_varint(&self, buf: &mut Bytes) -> Result<i64, ApplicationError> {
        const MAX_BYTES: usize = 10;
        self.ensure_remaining(buf, 1)?;

        let mut b0 = buf.get_i8() as i64;
        let mut res = b0 & 0b0111_1111;
        let mut n_bytes = 1;

        while b0 & 0b1000_0000 != 0 && n_bytes <= MAX_BYTES {
            self.ensure_remaining(buf, 1)?;
            let b1 = buf.get_i8() as i64;
            
            if buf.remaining() == 0 && b1 & 0b1000_0000 != 0 {
                return Err(ApplicationError::Protocol(
                    format!("invalid varint encoding at byte {}", n_bytes)
                ));
            }

            res += (b1 & 0b0111_1111) << 7;
            n_bytes += 1;
            b0 = b1;
        }

        Ok(res)
    }
} 