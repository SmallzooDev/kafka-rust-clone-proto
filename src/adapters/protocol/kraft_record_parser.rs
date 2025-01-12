use crate::application::error::ApplicationError;
use bytes::{Buf, Bytes};
use crate::adapters::protocol::parser::BaseParser;
use crate::adapters::protocol::parser::{PrimitiveParser, CompactStringParser, CompactArrayParser, VarIntParser};
use crate::adapters::protocol::dto::ErrorCode;
use crate::domain::message::{TopicMetadata, Partition};
use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct KraftRecordParser {
    base: BaseParser,
}

impl KraftRecordParser {
    pub fn new() -> Self {
        Self {
            base: BaseParser::default(),
        }
    }

    fn parse_compact_string(&self, src: &mut Bytes) -> Result<String, ApplicationError> {
        self.base.parse_compact_string(src)
    }

    fn parse_compact_bytes(&self, src: &mut Bytes) -> Result<Vec<u8>, ApplicationError> {
        self.base.parse_compact_bytes(src)
    }

    fn parse_varint(&self, src: &mut Bytes) -> Result<i64, ApplicationError> {
        self.base.parse_varint(src)
    }

    fn parse_compact_array<T, F>(&self, src: &mut Bytes, parser: F) -> Result<Vec<T>, ApplicationError>
    where
        F: Fn(&mut Bytes) -> Result<T, ApplicationError>,
    {
        self.base.parse_compact_array(src, parser)
    }

    fn parse_nullable_bytes<T, F>(&self, src: &mut Bytes, parser: F) -> Result<Vec<T>, ApplicationError>
    where
        F: Fn(&mut Bytes) -> Result<T, ApplicationError>,
    {
        if src.remaining() < 4 {
            return Err(ApplicationError::Protocol(
                "buffer too short for length".to_string(),
            ));
        }
        let len = src.get_i32();
        let items_len = if len == -1 { 0 } else { len as usize };

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            items.push(parser(src)?);
        }
        Ok(items)
    }

    pub fn parse_record_batch(&self, src: &mut Bytes) -> Result<RecordBatch, ApplicationError> {
        let base_offset = self.base.parse_i64(src)?;
        let batch_length = self.base.parse_i32(src)?;
        let partition_leader_epoch = self.base.parse_i32(src)?;
        let magic = self.base.parse_i8(src)?;
        let crc = self.base.parse_u32(src)?;
        let attributes = self.base.parse_i16(src)?;
        let last_offset_delta = self.base.parse_i32(src)?;
        let base_timestamp = self.base.parse_i64(src)?;
        let max_timestamp = self.base.parse_i64(src)?;
        let producer_id = self.base.parse_i64(src)?;
        let producer_epoch = self.base.parse_i16(src)?;
        let base_sequence = self.base.parse_i32(src)?;

        let records = self.parse_nullable_bytes(src, |src| self.parse_record(src))?;

        Ok(RecordBatch {
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

    pub fn parse_record(&self, src: &mut Bytes) -> Result<Record, ApplicationError> {
        let length = self.parse_varint(src)?;
        let attributes = self.base.parse_i8(src)?;
        let timestamp_delta = self.parse_varint(src)?;
        let offset_delta = self.parse_varint(src)?;
        let key = self.parse_compact_bytes(src)?;
        let value_length = self.parse_varint(src)?;
        let value = self.parse_record_value(src)?;
        let headers = self.parse_compact_array(src, |_src| Ok(Header))?;

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

    pub fn parse_record_value(&self, src: &mut Bytes) -> Result<RecordValue, ApplicationError> {
        let frame_version = self.base.parse_u8(src)?;
        if frame_version != 1 {
            return Err(ApplicationError::Protocol(format!(
                "invalid frame version: {}",
                frame_version
            )));
        }

        let record_type = self.base.parse_u8(src)?;

        match record_type {
            2 => self.parse_topic_record(src),
            3 => self.parse_partition_record(src),
            12 => self.parse_feature_level_record(src),
            _ => Err(ApplicationError::Protocol(format!(
                "unknown record type: {}",
                record_type
            ))),
        }
    }

    fn parse_topic_record(&self, src: &mut Bytes) -> Result<RecordValue, ApplicationError> {
        let version = self.base.parse_u8(src)?;
        if version != 0 {
            return Err(ApplicationError::Protocol(format!(
                "invalid version for topic record: {}",
                version
            )));
        }

        let topic_name = self.parse_compact_string(src)?;
        let topic_id = self.parse_uuid(src)?;
        let tagged_fields_count = self.parse_varint(src)?;
        if tagged_fields_count != 0 {
            return Err(ApplicationError::Protocol(format!(
                "unexpected tagged fields count: {}",
                tagged_fields_count
            )));
        }

        Ok(RecordValue::Topic(TopicValue {
            topic_name,
            topic_id,
        }))
    }

    fn parse_partition_record(&self, src: &mut Bytes) -> Result<RecordValue, ApplicationError> {
        let version = self.base.parse_u8(src)?;
        if version != 1 {
            return Err(ApplicationError::Protocol(format!(
                "invalid version for partition record: {}",
                version
            )));
        }

        let partition_id = self.base.parse_u32(src)?;
        let topic_id = self.parse_uuid(src)?;

        let replicas = self.parse_compact_array(src, |src| self.base.parse_u32(src))?;
        let in_sync_replicas = self.parse_compact_array(src, |src| self.base.parse_u32(src))?;
        let removing_replicas = self.parse_compact_array(src, |src| self.base.parse_u32(src))?;
        let adding_replicas = self.parse_compact_array(src, |src| self.base.parse_u32(src))?;

        let leader_id = self.base.parse_u32(src)?;
        let leader_epoch = self.base.parse_u32(src)?;
        let partition_epoch = self.base.parse_u32(src)?;

        let directories = self.parse_compact_array(src, |src| self.parse_compact_string(src))?;
        let tagged_fields_count = self.parse_varint(src)?;
        if tagged_fields_count != 0 {
            return Err(ApplicationError::Protocol(format!(
                "unexpected tagged fields count: {}",
                tagged_fields_count
            )));
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

    fn parse_feature_level_record(&self, src: &mut Bytes) -> Result<RecordValue, ApplicationError> {
        let version = self.base.parse_u8(src)?;
        if version != 0 {
            return Err(ApplicationError::Protocol(format!(
                "invalid version for feature level record: {}",
                version
            )));
        }

        let name = self.parse_compact_string(src)?;
        let level = self.base.parse_u16(src)?;

        let tagged_fields_count = self.parse_varint(src)?;
        if tagged_fields_count != 0 {
            return Err(ApplicationError::Protocol(format!(
                "unexpected tagged fields count: {}",
                tagged_fields_count
            )));
        }

        Ok(RecordValue::FeatureLevel(FeatureLevelValue { name, level }))
    }

    fn parse_uuid(&self, src: &mut Bytes) -> Result<String, ApplicationError> {
        if src.remaining() < 16 {
            return Err(ApplicationError::Protocol(
                "buffer too short for UUID".to_string(),
            ));
        }

        let mut s = hex::encode(src.slice(..16));
        src.advance(16);
        s.insert(8, '-');
        s.insert(13, '-');
        s.insert(18, '-');
        s.insert(23, '-');
        Ok(s)
    }

    pub fn parse_metadata(&self, data: &mut Bytes) -> Result<HashMap<String, TopicMetadata>, ApplicationError> {
        let mut topics_by_name: HashMap<String, TopicMetadata> = HashMap::new();
        let mut topics_by_id: HashMap<String, String> = HashMap::new();

        while data.remaining() > 0 {
            let record_batch = self.parse_record_batch(data)?;

            // First pass: Collect all topics
            for rec in &record_batch.records {
                if let RecordValue::Topic(topic) = &rec.value {
                    let topic_metadata = topics_by_name
                        .entry(topic.topic_name.clone())
                        .or_insert_with(|| TopicMetadata {
                            error_code: i16::from(ErrorCode::None),
                            name: topic.topic_name.clone(),
                            topic_id: topic.topic_id.clone(),
                            is_internal: false,
                            partitions: Vec::new(),
                            topic_authorized_operations: 0x0DF,
                        });
                    topics_by_id.insert(topic.topic_id.clone(), topic.topic_name.clone());
                }
            }

            // Second pass: Add partitions to corresponding topics
            for rec in &record_batch.records {
                if let RecordValue::Partition(p) = &rec.value {
                    if let Some(topic_name) = topics_by_id.get(&p.topic_id) {
                        if let Some(topic_metadata) = topics_by_name.get_mut(topic_name) {
                            topic_metadata.partitions.push(Partition::new(
                                i16::from(ErrorCode::None),
                                p.partition_id,
                                p.leader_id,
                                p.leader_epoch,
                                p.replicas.clone(),
                                p.in_sync_replicas.clone(),
                                p.adding_replicas.clone(),
                                Vec::new(),
                                p.removing_replicas.clone(),
                            ));
                        }
                    }
                }
            }
        }

        Ok(topics_by_name)
    }
}

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

#[allow(dead_code)]
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
    use bytes::Bytes;

    #[test]
    fn test_kraft_record_parser() {
        let parser = KraftRecordParser::new();
        
        // parse_metadata 테스트
        let mut data = Bytes::from(vec![
            // RecordBatch with Topic
            0, 0, 0, 0, 0, 0, 0, 1, // base offset
            0, 0, 0, 100, // batch length
            0, 0, 0, 0, // partition leader epoch
            2, // magic
            0, 0, 0, 0, // crc
            0, 1, // attributes
            0, 0, 0, 0, // last offset delta
            0, 0, 0, 0, 0, 0, 0, 0, // base timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // max timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // producer id
            0, 0, // producer epoch
            0, 0, 0, 0, // base sequence
            0, 0, 0, 1, // records length (1 record)
            // Record (Topic)
            2, // length
            0, // attributes
            2, // timestamp delta
            2, // offset delta
            1, // key length (empty)
            2, // value length
            1, // frame version
            2, // record type (Topic)
            0, // version
            5, // topic name length (4 + 1)
            b't', b'e', b's', b't', // topic name
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, // topic id
            0, // tagged fields count
            1, // headers length (empty)
        ]);

        let result = parser.parse_metadata(&mut data);
        assert!(result.is_ok());
        let topics = result.unwrap();
        assert_eq!(topics.len(), 1);
        
        let topic = topics.get("test").unwrap();
        assert_eq!(topic.name, "test");
        assert_eq!(topic.topic_id, "ffffffff-ffff-ffff-ffff-ffffffffffff");
        assert_eq!(topic.partitions.len(), 0);
    }

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
    fn test_compact_string() {
        let mut bytes = Bytes::from(vec![6, b'h', b'e', b'l', b'l', b'o']);
        let parser = KraftRecordParser::new();
        assert_eq!(parser.parse_compact_string(&mut bytes).unwrap(), "hello");

        // 빈 문자열
        let mut bytes = Bytes::from(vec![1]);
        assert_eq!(parser.parse_compact_string(&mut bytes).unwrap(), "");

        // 잘못된 UTF-8
        let mut bytes = Bytes::from(vec![2, 0xff]);
        assert!(parser.parse_compact_string(&mut bytes).is_err());
    }

    #[test]
    fn test_compact_array() {
        // u32 배열 테스트
        let mut bytes = Bytes::from(vec![
            3, // array length (2 + 1)
            0, 0, 0, 1, // first element
            0, 0, 0, 2, // second element
        ]);
        let parser = KraftRecordParser::new();
        let result = parser.parse_compact_array(&mut bytes, |src| parser.base.parse_u32(src)).unwrap();
        assert_eq!(result, vec![1, 2]);

        // 빈 배열
        let mut bytes = Bytes::from(vec![1]); // length = 1 means empty array
        let result = parser.parse_compact_array(&mut bytes, |src| parser.base.parse_u32(src)).unwrap();
        assert_eq!(result, Vec::<u32>::new());
    }

    #[test]
    fn test_record_value_from_bytes() {
        // Topic 레코드 테스트
        let mut bytes = Bytes::from(vec![
            1, // frame version
            2, // record type (Topic)
            0, // version
            5, // topic name length (4 + 1)
            b't', b'e', b's', b't', // topic name
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, // topic id (UUID)
            0,    // tagged fields count
        ]);

        let parser = KraftRecordParser::new();
        match parser.parse_record_value(&mut bytes).unwrap() {
            RecordValue::Topic(topic) => {
                assert_eq!(topic.topic_name, "test");
                assert_eq!(topic.topic_id, "ffffffff-ffff-ffff-ffff-ffffffffffff");
            }
            _ => panic!("Expected Topic record"),
        }

        // Partition 레코드 테스트
        let mut bytes = Bytes::from(vec![
            1, // frame version
            3, // record type (Partition)
            1, // version
            0, 0, 0, 1, // partition id
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, // topic id (UUID)
            1,    // replicas array length (empty)
            1,    // isr array length (empty)
            1,    // removing replicas array length (empty)
            1,    // adding replicas array length (empty)
            0, 0, 0, 1, // leader id
            0, 0, 0, 0, // leader epoch
            0, 0, 0, 0, // partition epoch
            1, // directories array length (empty)
            0, // tagged fields count
            1, // headers length (empty)
        ]);

        match parser.parse_record_value(&mut bytes).unwrap() {
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
            0, 0, 0, 0, 0, 0, 0, 1, // base offset
            0, 0, 0, 100, // batch length
            0, 0, 0, 0, // partition leader epoch
            2, // magic
            0, 0, 0, 0, // crc
            0, 1, // attributes
            0, 0, 0, 0, // last offset delta
            0, 0, 0, 0, 0, 0, 0, 0, // base timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // max timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // producer id
            0, 0, // producer epoch
            0, 0, 0, 0, // base sequence
            0, 0, 0, 0, // records length (empty)
        ]);

        let parser = KraftRecordParser::new();
        let batch = parser.parse_record_batch(&mut bytes).unwrap();
        assert_eq!(batch.base_offset, 1);
        assert_eq!(batch.batch_length, 100);
        assert_eq!(batch.magic, 2);
        assert_eq!(batch.attributes, 1);
        assert!(batch.records.is_empty());
    }

    #[test]
    fn test_record_from_bytes() {
        let mut bytes = Bytes::from(vec![
            2,  // length
            0,  // attributes
            2,  // timestamp delta
            2,  // offset delta
            1,  // key length (empty)
            2,  // value length
            1,  // frame version
            12, // record type (FeatureLevel)
            0,  // version
            5,  // feature name length (4 + 1)
            b't', b'e', b's', b't', // feature name
            0, 1, // level
            0, // tagged fields count
            1, // headers length (empty)
        ]);

        let parser = KraftRecordParser::new();
        let record = parser.parse_record(&mut bytes).unwrap();
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

    #[test]
    fn test_parse_uuid() {
        let mut bytes = Bytes::from(vec![
            0x01, 0x23, 0x45, 0x67,
            0x89, 0xab, 0xcd, 0xef,
            0xfe, 0xdc, 0xba, 0x98,
            0x76, 0x54, 0x32, 0x10,
        ]);

        let parser = KraftRecordParser::new();
        let uuid = parser.parse_uuid(&mut bytes).unwrap();
        assert_eq!(uuid, "01234567-89ab-cdef-fedc-ba9876543210");
    }

    #[test]
    fn test_parse_metadata_with_partition() {
        let parser = KraftRecordParser::new();
        
        let mut data = Bytes::from(vec![
            // First RecordBatch (Topic)
            0, 0, 0, 0, 0, 0, 0, 1, // base offset
            0, 0, 0, 100, // batch length
            0, 0, 0, 0, // partition leader epoch
            2, // magic
            0, 0, 0, 0, // crc
            0, 1, // attributes
            0, 0, 0, 0, // last offset delta
            0, 0, 0, 0, 0, 0, 0, 0, // base timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // max timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // producer id
            0, 0, // producer epoch
            0, 0, 0, 0, // base sequence
            0, 0, 0, 1, // records length (1 record)
            // Record (Topic)
            2, // length
            0, // attributes
            2, // timestamp delta
            2, // offset delta
            1, // key length (empty)
            2, // value length
            1, // frame version
            2, // record type (Topic)
            0, // version
            5, // topic name length (4 + 1)
            b't', b'e', b's', b't', // topic name
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, // topic id
            0, // tagged fields count
            1, // headers length (empty)

            // Second RecordBatch (Partition)
            0, 0, 0, 0, 0, 0, 0, 2, // base offset
            0, 0, 0, 100, // batch length
            0, 0, 0, 0, // partition leader epoch
            2, // magic
            0, 0, 0, 0, // crc
            0, 1, // attributes
            0, 0, 0, 0, // last offset delta
            0, 0, 0, 0, 0, 0, 0, 0, // base timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // max timestamp
            0, 0, 0, 0, 0, 0, 0, 0, // producer id
            0, 0, // producer epoch
            0, 0, 0, 0, // base sequence
            0, 0, 0, 1, // records length (1 record)
            // Record (Partition)
            2, // length
            0, // attributes
            2, // timestamp delta
            2, // offset delta
            1, // key length (empty)
            2, // value length
            1, // frame version
            3, // record type (Partition)
            1, // version
            0, 0, 0, 1, // partition id
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, // topic id
            1, // replicas array length (empty)
            1, // isr array length (empty)
            1, // removing replicas array length (empty)
            1, // adding replicas array length (empty)
            0, 0, 0, 1, // leader id
            0, 0, 0, 0, // leader epoch
            0, 0, 0, 0, // partition epoch
            1, // directories array length (empty)
            0, // tagged fields count
            1, // headers length (empty)
        ]);

        let result = parser.parse_metadata(&mut data).unwrap();
        assert_eq!(result.len(), 1);
        
        let topic = result.get("test").unwrap();
        assert_eq!(topic.name, "test");
        assert_eq!(topic.topic_id, "ffffffff-ffff-ffff-ffff-ffffffffffff");
        assert_eq!(topic.partitions.len(), 1);
        
        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_index, 1);
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.leader_epoch, 0);
        assert!(partition.replicas.is_empty());
        assert!(partition.in_sync_replicas.is_empty());
        assert!(partition.off_line_replicas.is_empty());
        assert!(partition.eligible_leader_replicas.is_empty());
        assert!(partition.last_known_eligible_leader_replicas.is_empty());
    }
}

