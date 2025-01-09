use bytes::{Buf, Bytes};
use crate::application::error::ApplicationError;

// === Type Definitions ===

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RecordBatch {
    pub base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: u32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Record {
    length: i64,
    attributes: i8,
    timestamp_delta: i64,
    offset_delta: i64,
    key: Vec<u8>,
    value_length: i64,
    pub value: RecordValue,
    headers: Vec<Header>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

#[derive(Debug, Clone)]
pub struct TopicValue {
    pub topic_name: String,
    pub topic_id: String,
}

#[derive(Debug, Clone)]
pub struct PartitionValue {
    pub partition_id: u32,
    pub topic_id: String,
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
    pub removing_replicas: Vec<u32>,
    pub adding_replicas: Vec<u32>,
    pub leader_id: u32,
    pub leader_epoch: u32,
    #[allow(dead_code)]
    pub partition_epoch: u32,
    #[allow(dead_code)]
    pub directories: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FeatureLevelValue {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    level: u16,
}

#[derive(Debug, Clone, Copy)]
struct Header;

pub trait Deserialize<T> {
    fn deserialize(src: &mut Bytes) -> Result<T, ApplicationError>;
}

pub struct CompactString;

pub struct CompactArray;

pub struct NullableBytes;

pub struct CompactNullableBytes;

pub struct VarInt;

pub struct Uuid;

// === Implementations ===

impl RecordBatch {
    pub fn from_bytes(src: &mut Bytes) -> Result<Self, ApplicationError> {
        if src.remaining() < 8 {
            return Err(ApplicationError::Protocol("buffer too short for base_offset".to_string()));
        }
        let base_offset = src.get_i64();

        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for batch_length".to_string()));
        }
        let batch_length = src.get_i32();

        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for partition_leader_epoch".to_string()));
        }
        let partition_leader_epoch = src.get_i32();

        if src.remaining() < 1 {
            return Err(ApplicationError::Protocol("buffer too short for magic".to_string()));
        }
        let magic = src.get_i8();

        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for crc".to_string()));
        }
        let crc = src.get_u32();

        if src.remaining() < 2 {
            return Err(ApplicationError::Protocol("buffer too short for attributes".to_string()));
        }
        let attributes = src.get_i16();

        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for last_offset_delta".to_string()));
        }
        let last_offset_delta = src.get_i32();

        if src.remaining() < 8 {
            return Err(ApplicationError::Protocol("buffer too short for base_timestamp".to_string()));
        }
        let base_timestamp = src.get_i64();

        if src.remaining() < 8 {
            return Err(ApplicationError::Protocol("buffer too short for max_timestamp".to_string()));
        }
        let max_timestamp = src.get_i64();

        if src.remaining() < 8 {
            return Err(ApplicationError::Protocol("buffer too short for producer_id".to_string()));
        }
        let producer_id = src.get_i64();

        if src.remaining() < 2 {
            return Err(ApplicationError::Protocol("buffer too short for producer_epoch".to_string()));
        }
        let producer_epoch = src.get_i16();

        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for base_sequence".to_string()));
        }
        let base_sequence = src.get_i32();

        let records = NullableBytes::deserialize::<Record, RecordBatch>(src)?;

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl Record {
    pub fn from_bytes(src: &mut Bytes) -> Result<Self, ApplicationError> {
        let length = VarInt::deserialize(src)?;
        
        if src.remaining() < 1 {
            return Err(ApplicationError::Protocol("buffer too short for attributes".to_string()));
        }
        let attributes = src.get_i8();
        
        let timestamp_delta = VarInt::deserialize(src)?;
        let offset_delta = VarInt::deserialize(src)?;
        let key = CompactNullableBytes::deserialize(src)?;
        let value_length = VarInt::deserialize(src)?;
        let value = RecordValue::from_bytes(src)?;
        let headers = CompactArray::deserialize::<Header, Record>(src)?;

        Ok(Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value_length,
            value,
            headers,
        })
    }
}

impl RecordValue {
    pub fn from_bytes(src: &mut Bytes) -> Result<Self, ApplicationError> {
        if src.remaining() < 1 {
            return Err(ApplicationError::Protocol("buffer too short for frame_version".to_string()));
        }
        let frame_version = src.get_u8();
        if frame_version != 1 {
            return Err(ApplicationError::Protocol(format!("invalid frame version: {}", frame_version)));
        }

        if src.remaining() < 1 {
            return Err(ApplicationError::Protocol("buffer too short for record_type".to_string()));
        }
        let record_type = src.get_u8();
        
        match record_type {
            2 => {
                if src.remaining() < 1 {
                    return Err(ApplicationError::Protocol("buffer too short for version".to_string()));
                }
                let version = src.get_u8();
                if version != 0 {
                    return Err(ApplicationError::Protocol(format!("invalid version for topic record: {}", version)));
                }
                
                let topic_name = CompactString::deserialize(src)?;
                let topic_id = Uuid::deserialize(src)?;
                let tagged_fields_count = VarInt::deserialize(src)?;
                if tagged_fields_count != 0 {
                    return Err(ApplicationError::Protocol(format!("unexpected tagged fields count: {}", tagged_fields_count)));
                }
                
                Ok(RecordValue::Topic(TopicValue {
                    topic_name,
                    topic_id,
                }))
            }
            3 => {
                if src.remaining() < 1 {
                    return Err(ApplicationError::Protocol("buffer too short for version".to_string()));
                }
                let version = src.get_u8();
                if version != 1 {
                    return Err(ApplicationError::Protocol(format!("invalid version for partition record: {}", version)));
                }

                if src.remaining() < 4 {
                    return Err(ApplicationError::Protocol("buffer too short for partition_id".to_string()));
                }
                let partition_id = src.get_u32();
                let topic_id = Uuid::deserialize(src)?;

                let replicas = CompactArray::deserialize::<u32, PartitionValue>(src)?;
                let in_sync_replicas = CompactArray::deserialize::<u32, PartitionValue>(src)?;
                let removing_replicas = CompactArray::deserialize::<u32, PartitionValue>(src)?;
                let adding_replicas = CompactArray::deserialize::<u32, PartitionValue>(src)?;

                if src.remaining() < 4 {
                    return Err(ApplicationError::Protocol("buffer too short for leader_id".to_string()));
                }
                let leader_id = src.get_u32();

                if src.remaining() < 4 {
                    return Err(ApplicationError::Protocol("buffer too short for leader_epoch".to_string()));
                }
                let leader_epoch = src.get_u32();

                if src.remaining() < 4 {
                    return Err(ApplicationError::Protocol("buffer too short for partition_epoch".to_string()));
                }
                let partition_epoch = src.get_u32();

                let directories = CompactArray::deserialize::<String, PartitionValue>(src)?;
                let tagged_fields_count = VarInt::deserialize(src)?;
                if tagged_fields_count != 0 {
                    return Err(ApplicationError::Protocol(format!("unexpected tagged fields count: {}", tagged_fields_count)));
                }

                Ok(RecordValue::Partition(PartitionValue {
                    partition_id,
                    topic_id,
                    replicas,
                    in_sync_replicas,
                    removing_replicas,
                    adding_replicas,
                    leader_id,
                    leader_epoch,
                    partition_epoch,
                    directories,
                }))
            }
            12 => {
                if src.remaining() < 1 {
                    return Err(ApplicationError::Protocol("buffer too short for version".to_string()));
                }
                let version = src.get_u8();
                if version != 0 {
                    return Err(ApplicationError::Protocol(format!("invalid version for feature level record: {}", version)));
                }

                let name = CompactString::deserialize(src)?;
                
                if src.remaining() < 2 {
                    return Err(ApplicationError::Protocol("buffer too short for level".to_string()));
                }
                let level = src.get_u16();
                
                let tagged_fields_count = VarInt::deserialize(src)?;
                if tagged_fields_count != 0 {
                    return Err(ApplicationError::Protocol(format!("unexpected tagged fields count: {}", tagged_fields_count)));
                }

                Ok(RecordValue::FeatureLevel(FeatureLevelValue { name, level }))
            }
            _ => Err(ApplicationError::Protocol(format!("unknown record type: {}", record_type))),
        }
    }
}

impl Deserialize<Record> for RecordBatch {
    fn deserialize(src: &mut Bytes) -> Result<Record, ApplicationError> {
        Record::from_bytes(src)
    }
}

impl Deserialize<Header> for Record {
    fn deserialize(_src: &mut Bytes) -> Result<Header, ApplicationError> {
        Ok(Header)
    }
}

impl Deserialize<String> for CompactString {
    fn deserialize(src: &mut Bytes) -> Result<String, ApplicationError> {
        Self::deserialize(src)
    }
}

impl Deserialize<u32> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> Result<u32, ApplicationError> {
        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for u32".to_string()));
        }
        Ok(src.get_u32())
    }
}

impl Deserialize<String> for PartitionValue {
    fn deserialize(src: &mut Bytes) -> Result<String, ApplicationError> {
        Uuid::deserialize(src)
    }
}

impl CompactString {
    pub fn deserialize(src: &mut Bytes) -> Result<String, ApplicationError> {
        let len = VarInt::deserialize(src)?;
        let string_len = if len > 1 { len as usize - 1 } else { 0 };
        
        if src.remaining() < string_len {
            return Err(ApplicationError::Protocol(format!("buffer too short for string of length {}", string_len)));
        }
        
        let bytes = src.slice(..string_len);
        src.advance(string_len);
        
        String::from_utf8(bytes.to_vec())
            .map_err(|e| ApplicationError::Protocol(format!("invalid UTF-8 sequence: {}", e)))
    }
}

impl CompactArray {
    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Result<Vec<T>, ApplicationError> {
        let len = VarInt::deserialize(src)?;
        let items_len = if len > 1 { len as usize - 1 } else { 0 };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            items.push(U::deserialize(src)?);
        }

        Ok(items)
    }
}

impl NullableBytes {
    pub fn deserialize<T, U: Deserialize<T>>(src: &mut Bytes) -> Result<Vec<T>, ApplicationError> {
        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol("buffer too short for length".to_string()));
        }
        let len = src.get_i32();
        let items_len = if len == -1 { 0 } else { len as usize };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            items.push(U::deserialize(src)?);
        }
        Ok(items)
    }
}

impl CompactNullableBytes {
    pub fn deserialize(src: &mut Bytes) -> Result<Vec<u8>, ApplicationError> {
        let len = VarInt::deserialize(src)?;
        let bytes_len = if len > 1 { len as usize - 1 } else { 0 };
        
        if src.remaining() < bytes_len {
            return Err(ApplicationError::Protocol(format!("buffer too short for bytes of length {}", bytes_len)));
        }
        
        let bytes = src.slice(..bytes_len);
        src.advance(bytes_len);
        Ok(bytes.to_vec())
    }
}

impl VarInt {
    pub(crate) fn deserialize<T>(buf: &mut T) -> Result<i64, ApplicationError>
    where
        T: Buf,
    {
        const MAX_BYTES: usize = 10;
        if buf.remaining() == 0 {
            return Err(ApplicationError::Protocol("buffer is empty".to_string()));
        }

        let buf_len = buf.remaining();
        let mut b0 = buf.get_i8() as i64;
        let mut res = b0 & 0b0111_1111;
        let mut n_bytes = 1;

        while b0 & 0b1000_0000 != 0 && n_bytes <= MAX_BYTES {
            if buf.remaining() == 0 {
                return Err(ApplicationError::Protocol(format!(
                    "buffer too short ({} bytes) for varint",
                    buf_len
                )));
            }

            let b1 = buf.get_i8() as i64;
            if buf.remaining() == 0 && b1 & 0b1000_0000 != 0 {
                return Err(ApplicationError::Protocol(format!(
                    "invalid varint encoding at byte {}",
                    n_bytes
                )));
            }

            res += (b1 & 0b0111_1111) << 7;
            n_bytes += 1;
            b0 = b1;
        }

        Ok(res)
    }
}

impl Uuid {
    pub fn deserialize(src: &mut Bytes) -> Result<String, ApplicationError> {
        if src.remaining() < 16 {
            return Err(ApplicationError::Protocol("buffer too short for UUID".to_string()));
        }
        
        let mut s = hex::encode(src.slice(..16));
        src.advance(16);
        s.insert(8, '-');
        s.insert(13, '-');
        s.insert(18, '-');
        s.insert(23, '-');
        Ok(s)
    }
} 

pub fn decode_varint(buf: &[u8]) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    
    for &byte in buf {
        result |= ((byte & 0x7f) as u64) << shift;
        shift += 7;
        
        if byte & 0x80 == 0 {
            break;
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint() {
        // 단순한 1바이트 varint
        assert_eq!(decode_varint(&[1]), 1);
        
        // 2바이트 varint
        assert_eq!(decode_varint(&[0x80, 0x01]), 128);
        
        // 3바이트 varint
        assert_eq!(decode_varint(&[0x80, 0x80, 0x01]), 16384);
    }

    #[test]
    fn test_compact_string_deserialize() {
        let mut bytes = Bytes::from(vec![6, b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(CompactString::deserialize(&mut bytes).unwrap(), "hello");

        // 빈 문자열
        let mut bytes = Bytes::from(vec![1]);
        assert_eq!(CompactString::deserialize(&mut bytes).unwrap(), "");

        // 잘못된 UTF-8
        let mut bytes = Bytes::from(vec![2, 0xff]);
        assert!(CompactString::deserialize(&mut bytes).is_err());
    }

    #[test]
    fn test_compact_array_deserialize() {
        // u32 배열 테스트
        let mut bytes = Bytes::from(vec![
            3,              // array length (2 + 1)
            0, 0, 0, 1,    // first element
            0, 0, 0, 2,    // second element
        ]);
        let result: Vec<u32> = CompactArray::deserialize::<u32, PartitionValue>(&mut bytes).unwrap();
        assert_eq!(result, vec![1, 2]);

        // 빈 배열
        let mut bytes = Bytes::from(vec![1]); // length = 1 means empty array
        let result: Vec<u32> = CompactArray::deserialize::<u32, PartitionValue>(&mut bytes).unwrap();
        assert_eq!(result, Vec::<u32>::new());
    }

    #[test]
    fn test_record_value_from_bytes() {
        // Topic 레코드 테스트
        let mut bytes = Bytes::from(vec![
            1,   // frame version
            2,   // record type (Topic)
            0,   // version
            5,   // topic name length (4 + 1)
            b't', b'e', b's', b't',  // topic name
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,  // topic id (UUID)
            0,   // tagged fields count
        ]);
        
        match RecordValue::from_bytes(&mut bytes).unwrap() {
            RecordValue::Topic(topic) => {
                assert_eq!(topic.topic_name, "test");
                assert_eq!(topic.topic_id, "ffffffff-ffff-ffff-ffff-ffffffffffff");
            }
            _ => panic!("Expected Topic record"),
        }

        // Partition 레코드 테스트
        let mut bytes = Bytes::from(vec![
            1,   // frame version
            3,   // record type (Partition)
            1,   // version
            0, 0, 0, 1,  // partition id
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,  // topic id (UUID)
            1,   // replicas array length (empty)
            1,   // isr array length (empty)
            1,   // removing replicas array length (empty)
            1,   // adding replicas array length (empty)
            0, 0, 0, 1,  // leader id
            0, 0, 0, 0,  // leader epoch
            0, 0, 0, 0,  // partition epoch
            1,   // directories array length (empty)
            0,   // tagged fields count
        ]);

        match RecordValue::from_bytes(&mut bytes).unwrap() {
            RecordValue::Partition(partition) => {
                assert_eq!(partition.partition_id, 1);
                assert_eq!(partition.topic_id, "ffffffff-ffff-ffff-ffff-ffffffffffff");
                assert_eq!(partition.leader_id, 1);
                assert_eq!(partition.leader_epoch, 0);
                assert!(partition.replicas.is_empty());
                assert!(partition.in_sync_replicas.is_empty());
                assert!(partition.removing_replicas.is_empty());
                assert!(partition.adding_replicas.is_empty());
            }
            _ => panic!("Expected Partition record"),
        }
    }

    #[test]
    fn test_record_batch_from_bytes() {
        let mut bytes = Bytes::from(vec![
            0, 0, 0, 0, 0, 0, 0, 1,  // base offset
            0, 0, 0, 100,            // batch length
            0, 0, 0, 0,              // partition leader epoch
            2,                       // magic
            0, 0, 0, 0,              // crc
            0, 1,                    // attributes
            0, 0, 0, 0,              // last offset delta
            0, 0, 0, 0, 0, 0, 0, 0,  // base timestamp
            0, 0, 0, 0, 0, 0, 0, 0,  // max timestamp
            0, 0, 0, 0, 0, 0, 0, 0,  // producer id
            0, 0,                    // producer epoch
            0, 0, 0, 0,              // base sequence
            0, 0, 0, 0,              // records length (empty)
        ]);

        let batch = RecordBatch::from_bytes(&mut bytes).unwrap();
        assert_eq!(batch.base_offset, 1);
        assert_eq!(batch.batch_length, 100);
        assert_eq!(batch.magic, 2);
        assert_eq!(batch.attributes, 1);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn test_record_from_bytes() {
        let mut bytes = Bytes::from(vec![
            2,                       // length
            0,                       // attributes
            2,                       // timestamp delta
            2,                       // offset delta
            1,                       // key length (empty)
            2,                       // value length
            1,                       // frame version
            12,                      // record type (FeatureLevel)
            0,                       // version
            5,                       // feature name length (4 + 1)
            b't', b'e', b's', b't',  // feature name
            0, 1,                    // level
            0,                       // tagged fields count
            1,                       // headers length (empty)
        ]);

        let record = Record::from_bytes(&mut bytes).unwrap();
        assert_eq!(record.length, 2);
        assert_eq!(record.attributes, 0);
        assert_eq!(record.timestamp_delta, 2);
        assert_eq!(record.offset_delta, 2);
        assert!(record.key.is_empty());
        
        match record.value {
            RecordValue::FeatureLevel(feature) => {
                assert_eq!(feature.name, "test");
                assert_eq!(feature.level, 1);
            }
            _ => panic!("Expected FeatureLevel record"),
        }
    }
} 