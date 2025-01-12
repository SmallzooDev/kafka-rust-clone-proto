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
use super::varint::decode_varint;

#[derive(Clone)]
pub struct RequestParser;

impl RequestParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, data: &[u8]) -> Result<KafkaRequest, ApplicationError> {
        println!("[REQUEST] Raw bytes: {:02x?}", data);
        let mut buf = Bytes::copy_from_slice(data);

        let header = self.parse_header(&mut buf)?;
        let payload = self.parse_payload(&mut buf, header.api_key)?;

        Ok(KafkaRequest::new(header, payload))
    }

    fn parse_header(&self, buf: &mut Bytes) -> Result<RequestHeader, ApplicationError> {
        // API Key (2 bytes)
        let api_key = buf.get_u16() as i16;
        println!("[REQUEST] API Key: {}", api_key);

        // API Version (2 bytes)
        let api_version = buf.get_u16() as i16;
        println!("[REQUEST] API Version: {}", api_version);

        // Correlation ID (4 bytes)
        let correlation_id = buf.get_u32() as i32;
        println!("[REQUEST] Correlation ID: {}", correlation_id);

        // Client ID
        let client_id_len = buf.get_u16() as usize;
        println!("[REQUEST] Client ID length: {}", client_id_len);
        let client_id = if client_id_len > 0 {
            let client_id_bytes = buf.copy_to_bytes(client_id_len);
            let client_id = String::from_utf8(client_id_bytes.to_vec()).map_err(|_| {
                ApplicationError::Protocol("Invalid client ID encoding".to_string())
            })?;
            Some(client_id)
        } else {
            None
        };

        buf.get_u8(); // tag buffer
        println!("[REQUEST] Remaining bytes after header: {:02x?}", buf);

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
        let acks = buf.get_i16();
        let timeout_ms = buf.get_i32();
        let topics_length = buf.get_i8() - 1;

        let mut topic_data = Vec::new();
        for _ in 0..topics_length {
            let name_length = buf.get_i8() - 1;

            let mut topic_name_buf = vec![0u8; name_length as usize];
            let bytes_to_copy = buf.copy_to_bytes(name_length as usize);
            topic_name_buf.copy_from_slice(&bytes_to_copy);
            let topic_name = String::from_utf8(topic_name_buf).map_err(|_| {
                ApplicationError::Protocol("Invalid topic name encoding".to_string())
            })?;
            let partitions_length = buf.get_i8() - 1;
            let mut partition_data = Vec::new();
            for _ in 0..partitions_length {
                let partition = buf.get_i32();
                let records_length = buf.get_i32() as usize;
                let records = buf.copy_to_bytes(records_length).to_vec();

                partition_data.push(ProducePartitionData { partition, records });
            }

            buf.get_u8(); // TAG_BUFFER

            topic_data.push(ProduceTopicData {
                name: topic_name,
                partition_data,
            });
        }

        buf.get_u8(); // TAG_BUFFER

        Ok(RequestPayload::Produce(ProduceRequest {
            acks,
            timeout_ms,
            topic_data,
        }))
    }

    fn parse_fetch_request(&self, buf: &mut Bytes) -> Result<RequestPayload, ApplicationError> {
        let max_wait_ms = buf.get_i32();
        let min_bytes = buf.get_i32();
        let max_bytes = buf.get_i32();
        let isolation_level = buf.get_i8();
        let session_id = buf.get_i32();
        let session_epoch = buf.get_i32();
        let topics_length = buf.get_i8() - 1;

        let mut topics = Vec::new();
        for _ in 0..topics_length {
            let mut topic_id = [0u8; 16];
            topic_id.copy_from_slice(&buf.copy_to_bytes(16));

            let partitions_length = buf.get_i8() - 1;
            println!("[REQUEST] Partitions Length: {}", partitions_length);

            let mut partitions = Vec::new();
            for _ in 0..partitions_length {
                let partition = buf.get_i32();
                let current_leader_epoch = buf.get_i32();
                let fetch_offset = buf.get_i64();
                let last_fetched_epoch = buf.get_i32();
                let log_start_offset = buf.get_i64();
                let partition_max_bytes = buf.get_i32();

                partitions.push(FetchPartition {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                    last_fetched_epoch,
                    log_start_offset,
                    partition_max_bytes,
                });
            }

            buf.get_u8(); // TAG_BUFFER

            topics.push(FetchTopic {
                topic_id,
                partitions,
            });
        }

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
        let mut array_length_buf = [0u8; 8];
        let mut pos = 0;

        loop {
            if pos >= buf.len() {
                return Err(ApplicationError::Protocol(
                    "Buffer too short for array length".to_string(),
                ));
            }
            let byte = buf.get_u8();
            array_length_buf[pos] = byte;
            pos += 1;
            println!("[REQUEST] Array length byte {}: {:02x}", pos, byte);

            if byte & 0x80 == 0 {
                break;
            }
        }

        let mut topics = Vec::new();
        let array_length = decode_varint(&array_length_buf[..pos]) - 1;
        println!("[REQUEST] Topics array length (decoded): {}", array_length);

        for _ in 0..array_length {
            let mut name_length_buf = [0u8; 8];
            let mut pos = 0;

            loop {
                if pos >= buf.len() {
                    return Err(ApplicationError::Protocol(
                        "Buffer too short for name length".to_string(),
                    ));
                }
                let byte = buf.get_u8();
                name_length_buf[pos] = byte;
                pos += 1;

                if byte & 0x80 == 0 {
                    break;
                }
            }

            let name_length = decode_varint(&name_length_buf[..pos]) - 1;

            let mut topic_name_buf = vec![0u8; name_length as usize];
            if name_length as usize > buf.len() {
                println!(
                    "[REQUEST] Error: name_length ({}) > remaining buffer length ({})",
                    name_length,
                    buf.len()
                );
                return Err(ApplicationError::Protocol(
                    "Buffer too short for topic name".to_string(),
                ));
            }
            let bytes_to_copy = buf.copy_to_bytes(name_length as usize);
            println!("[REQUEST] Bytes to copy: {:02x?}", bytes_to_copy);
            topic_name_buf.copy_from_slice(&bytes_to_copy);

            let topic_name = String::from_utf8(topic_name_buf).map_err(|_| {
                ApplicationError::Protocol("Invalid topic name encoding".to_string())
            })?;
            println!("[REQUEST] Topic name: {}", topic_name);

            buf.get_u8(); // tag buffer

            topics.push(TopicRequest {
                topic_name,
                partitions: vec![],
            });
        }

        let response_partition_limit = buf.get_u32();
        println!(
            "[REQUEST] Response partition limit: {}",
            response_partition_limit
        );
        buf.get_u8(); // cursor
        buf.get_u8(); // tag buffer

        Ok(RequestPayload::DescribeTopicPartitions(
            DescribeTopicPartitionsRequest { topics },
        ))
    }
} 