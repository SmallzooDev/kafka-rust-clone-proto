use crate::adapters::protocol::constants::{
    API_VERSIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_KEY, FETCH_KEY, PRODUCE_KEY,
};
use crate::adapters::protocol::dto::{
    DescribeTopicPartitionsRequest, FetchPartition, FetchRequest, FetchTopic, KafkaRequest,
    ProducePartitionData, ProduceRequest, ProduceTopicData, RequestHeader, RequestPayload,
    TopicRequest,
};
use crate::application::error::ApplicationError;
use bytes::{Buf, Bytes};
use super::{BaseParser, PrimitiveParser, CompactStringParser, CompactArrayParser};

#[derive(Debug, Default, Clone)]
pub struct RequestParser {
    base: BaseParser,
}

impl RequestParser {
    pub fn new() -> Self {
        Self {
            base: BaseParser::default(),
        }
    }

    pub fn parse(&self, data: &[u8]) -> Result<KafkaRequest, ApplicationError> {
        println!("[REQUEST] Raw bytes: {:02x?}", data);
        let mut buf = Bytes::copy_from_slice(data);

        let header = self.parse_header(&mut buf)?;
        let payload = self.parse_payload(&mut buf, header.api_key)?;

        Ok(KafkaRequest::new(header, payload))
    }

    fn parse_header(&self, buf: &mut Bytes) -> Result<RequestHeader, ApplicationError> {
        let api_key = self.base.parse_i16(buf)?;
        println!("[REQUEST] API Key: {}", api_key);

        let api_version = self.base.parse_i16(buf)?;
        let correlation_id = self.base.parse_i32(buf)?;
        let client_id_length = self.base.parse_i16(buf)?;

        let client_id = if client_id_length > 0 {
            let bytes = buf.copy_to_bytes(client_id_length as usize);
            Some(String::from_utf8(bytes.to_vec())
                .map_err(|_| ApplicationError::Protocol("Invalid client ID encoding".to_string()))?)
        } else {
            None
        };

        self.base.parse_u8(buf)?;

        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    fn parse_payload(&self, buf: &mut Bytes, api_key: i16) -> Result<RequestPayload, ApplicationError> {
        match api_key {
            API_VERSIONS_KEY => Ok(RequestPayload::ApiVersions),
            PRODUCE_KEY => self.parse_produce_request(buf),
            FETCH_KEY => self.parse_fetch_request(buf),
            DESCRIBE_TOPIC_PARTITIONS_KEY => self.parse_describe_topic_partitions_request(buf),
            _ => Err(ApplicationError::Protocol("Invalid API key".to_string())),
        }
    }

    fn parse_produce_request(&self, buf: &mut Bytes) -> Result<RequestPayload, ApplicationError> {
        let acks = self.base.parse_i16(buf)?;
        let timeout_ms = self.base.parse_i32(buf)?;
        
        let topic_data = self.base.parse_compact_array(buf, |buf| {
            let topic_name = self.base.parse_compact_string(buf)?;
            
            let partition_data = self.base.parse_compact_array(buf, |buf| {
                let partition = self.base.parse_i32(buf)?;
                let records_length = self.base.parse_i32(buf)? as usize;
                let records = buf.copy_to_bytes(records_length).to_vec();

                Ok(ProducePartitionData { partition, records })
            })?;

            self.base.parse_u8(buf)?; // TAG_BUFFER

            Ok(ProduceTopicData {
                name: topic_name,
                partition_data,
            })
        })?;

        self.base.parse_u8(buf)?; // TAG_BUFFER

        Ok(RequestPayload::Produce(ProduceRequest {
            acks,
            timeout_ms,
            topic_data,
        }))
    }

    fn parse_fetch_request(&self, buf: &mut Bytes) -> Result<RequestPayload, ApplicationError> {
        let max_wait_ms = self.base.parse_i32(buf)?;
        let min_bytes = self.base.parse_i32(buf)?;
        let max_bytes = self.base.parse_i32(buf)?;
        let isolation_level = self.base.parse_i8(buf)?;
        let session_id = self.base.parse_i32(buf)?;
        let session_epoch = self.base.parse_i32(buf)?;

        let topics = self.base.parse_compact_array(buf, |buf| {
            let mut topic_id = [0u8; 16];
            topic_id.copy_from_slice(&buf.copy_to_bytes(16));

            let partitions = self.base.parse_compact_array(buf, |buf| {
                let partition = self.base.parse_i32(buf)?;
                let current_leader_epoch = self.base.parse_i32(buf)?;
                let fetch_offset = self.base.parse_i64(buf)?;
                let last_fetched_epoch = self.base.parse_i32(buf)?;
                let log_start_offset = self.base.parse_i64(buf)?;
                let partition_max_bytes = self.base.parse_i32(buf)?;

                Ok(FetchPartition {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                    last_fetched_epoch,
                    log_start_offset,
                    partition_max_bytes,
                })
            })?;

            self.base.parse_u8(buf)?; // TAG_BUFFER

            Ok(FetchTopic {
                topic_id,
                partitions,
            })
        })?;

        self.base.parse_u8(buf)?; // TAG_BUFFER

        Ok(RequestPayload::Fetch(FetchRequest {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
        }))
    }

    fn parse_describe_topic_partitions_request(
        &self,
        buf: &mut Bytes,
    ) -> Result<RequestPayload, ApplicationError> {
        let topics = self.base.parse_compact_array(buf, |buf| {
            let topic_name = self.base.parse_compact_string(buf)?;
            self.base.parse_u8(buf)?; // tag buffer

            Ok(TopicRequest {
                topic_name,
                partitions: vec![],
            })
        })?;

        let response_partition_limit = self.base.parse_u32(buf)?;
        println!(
            "[REQUEST] Response partition limit: {}",
            response_partition_limit
        );
        self.base.parse_u8(buf)?; // cursor
        self.base.parse_u8(buf)?; // tag buffer

        Ok(RequestPayload::DescribeTopicPartitions(
            DescribeTopicPartitionsRequest { topics },
        ))
    }
} 