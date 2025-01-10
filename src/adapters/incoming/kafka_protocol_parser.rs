use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_KEY, FETCH_KEY, PRODUCE_KEY,
};
use crate::{
    adapters::incoming::protocol::messages::{
        DescribeTopicPartitionsRequest, FetchPartition, FetchRequest, FetchTopic, KafkaRequest,
        KafkaResponse, ProducePartitionData, ProduceRequest, ProduceTopicData, RequestHeader,
        RequestPayload, ResponsePayload, TopicRequest,
    },
    application::error::ApplicationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub trait PutVarint {
    fn put_uvarint(&mut self, num: i64);
}

impl PutVarint for Vec<u8> {
    fn put_uvarint(&mut self, mut num: i64) {
        while (num & !0x7F) != 0 {
            self.push(((num & 0x7F) | 0x80) as u8);
            num >>= 7;
        }
        self.push(num as u8);
    }
}

#[derive(Clone)]
pub struct KafkaProtocolParser;

impl KafkaProtocolParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_request(&self, data: &[u8]) -> Result<KafkaRequest, ApplicationError> {
        println!("[REQUEST] Raw bytes: {:02x?}", data);
        let mut buf = Bytes::copy_from_slice(data);

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
            println!("[REQUEST] Client ID: {}", client_id);
            Some(client_id)
        } else {
            println!("[REQUEST] Client ID: None");
            None
        };

        buf.get_u8(); // tag buffer
        println!("[REQUEST] Remaining bytes after header: {:02x?}", buf);

        let header = RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        };

        let payload = match api_key {
            API_VERSIONS_KEY => RequestPayload::ApiVersions,
            PRODUCE_KEY => {
                let acks = buf.get_i16();
                println!("[REQUEST] Acks: {}", acks);

                let timeout_ms = buf.get_i32();
                println!("[REQUEST] Timeout MS: {}", timeout_ms);

                let topics_length = buf.get_i8() - 1;
                println!("[REQUEST] Topics Length: {}", topics_length);

                let mut topic_data = Vec::new();
                for _ in 0..topics_length {
                    let name_length = buf.get_i8() - 1;
                    println!("[REQUEST] Topic Name Length: {}", name_length);

                    let mut topic_name_buf = vec![0u8; name_length as usize];
                    let bytes_to_copy = buf.copy_to_bytes(name_length as usize);
                    topic_name_buf.copy_from_slice(&bytes_to_copy);
                    let topic_name = String::from_utf8(topic_name_buf).map_err(|_| {
                        ApplicationError::Protocol("Invalid topic name encoding".to_string())
                    })?;
                    println!("[REQUEST] Topic Name: {}", topic_name);

                    let partitions_length = buf.get_i8() - 1;
                    println!("[REQUEST] Partitions Length: {}", partitions_length);

                    let mut partition_data = Vec::new();
                    for _ in 0..partitions_length {
                        let partition = buf.get_i32();
                        println!("[REQUEST] Partition: {}", partition);

                        let records_length = buf.get_i32() as usize;
                        println!("[REQUEST] Records Length: {}", records_length);

                        let records = buf.copy_to_bytes(records_length).to_vec();
                        println!("[REQUEST] Records: {:02x?}", records);

                        partition_data.push(ProducePartitionData { partition, records });
                    }

                    buf.get_u8(); // TAG_BUFFER

                    topic_data.push(ProduceTopicData {
                        name: topic_name,
                        partition_data,
                    });
                }

                buf.get_u8(); // TAG_BUFFER

                RequestPayload::Produce(ProduceRequest {
                    acks,
                    timeout_ms,
                    topic_data,
                })
            }
            FETCH_KEY => {
                // max_wait_ms
                let max_wait_ms = buf.get_i32();
                println!("[REQUEST] Max Wait MS: {}", max_wait_ms);

                // min_bytes
                let min_bytes = buf.get_i32();
                println!("[REQUEST] Min Bytes: {}", min_bytes);

                // max_bytes
                let max_bytes = buf.get_i32();
                println!("[REQUEST] Max Bytes: {}", max_bytes);

                // isolation_level
                let isolation_level = buf.get_i8();
                println!("[REQUEST] Isolation Level: {}", isolation_level);

                // session_id
                let session_id = buf.get_i32();
                println!("[REQUEST] Session ID: {}", session_id);

                // session_epoch
                let session_epoch = buf.get_i32();
                println!("[REQUEST] Session Epoch: {}", session_epoch);

                // topics array length (COMPACT_ARRAY)
                let topics_length = buf.get_i8() - 1;
                println!("[REQUEST] Topics Length: {}", topics_length);

                let mut topics = Vec::new();
                for _ in 0..topics_length {
                    // topic_id
                    let mut topic_id = [0u8; 16];
                    topic_id.copy_from_slice(&buf.copy_to_bytes(16));
                    println!("[REQUEST] Topic ID: {:02x?}", topic_id);

                    // partitions array length (COMPACT_ARRAY)
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

                RequestPayload::Fetch(FetchRequest {
                    max_wait_ms,
                    min_bytes,
                    max_bytes,
                    isolation_level,
                    session_id,
                    session_epoch,
                    topics,
                })
            }
            DESCRIBE_TOPIC_PARTITIONS_KEY => {
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
                        println!("[REQUEST] Name length byte {}: {:02x}", pos, byte);

                        if byte & 0x80 == 0 {
                            break;
                        }
                    }

                    let name_length = decode_varint(&name_length_buf[..pos]) - 1;
                    println!("[REQUEST] Name length (decoded): {}", name_length);

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

                RequestPayload::DescribeTopicPartitions(DescribeTopicPartitionsRequest { topics })
            }
            _ => return Err(ApplicationError::Protocol("Invalid API key".to_string())),
        };

        Ok(KafkaRequest::new(header, payload))
    }

    pub fn encode_response(&self, response: KafkaResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();

        // correlation_id
        buf.put_i32(response.correlation_id);

        match &response.payload {
            ResponsePayload::ApiVersions(api_versions) => {
                buf.put_i16(response.error_code);

                // array of api keys
                buf.put_i8((api_versions.api_versions.len() + 1) as i8);

                // Write each API version
                for version in &api_versions.api_versions {
                    buf.put_i16(version.api_key);
                    buf.put_i16(version.min_version);
                    buf.put_i16(version.max_version);
                    buf.put_i8(0); // TAG_BUFFER
                }

                // throttle time ms
                buf.put_i32(0);
                buf.put_i8(0); // TAG_BUFFER
            }
            ResponsePayload::DescribeTopicPartitions(describe_response) => {
                println!(
                    "[RESPONSE] Encoding DescribeTopicPartitions response: {:?}",
                    describe_response
                );

                // throttle time ms
                buf.put_i32(0);
                buf.put_i8(0); // TAG_BUFFER

                // topics array length (COMPACT_ARRAY)
                buf.put_i8((describe_response.topics.len() + 1) as i8);

                // Write each topic
                for topic in &describe_response.topics {
                    // topic error code
                    buf.put_i16(topic.error_code);

                    // topic name (COMPACT_STRING)
                    let topic_name_bytes = topic.topic_name.as_bytes();
                    buf.put_i8((topic_name_bytes.len() + 1) as i8);
                    buf.put_slice(topic_name_bytes);

                    // topic id (UUID)
                    buf.put_slice(&topic.topic_id);

                    // is_internal
                    buf.put_i8(topic.is_internal as i8);

                    // partitions array (COMPACT_ARRAY)
                    buf.put_i8((topic.partitions.len() + 1) as i8);
                    println!("[RESPONSE] Encoding {} partitions", topic.partitions.len());

                    // Write each partition
                    for partition in &topic.partitions {
                        println!("[RESPONSE] Encoding partition: {:?}", partition);
                        buf.put_i16(partition.error_code); // error code 먼저
                        buf.put_i32(partition.partition_id); // 그 다음 partition id
                        buf.put_i32(1); // leader id (1로 고정)
                        buf.put_i32(0); // leader epoch

                        // replica nodes array
                        buf.put_i8(2); // array length + 1 (1개의 replica이므로 2)
                        buf.put_i32(1); // replica node id (1로 고정)

                        // isr nodes array
                        buf.put_i8(2); // array length + 1 (1개의 isr이므로 2)
                        buf.put_i32(1); // isr node id (1로 고정)

                        // eligible leader replicas array
                        buf.put_i8(1); // array length + 1 (0개이므로 1)

                        // last known eligible leader replicas array
                        buf.put_i8(1); // array length + 1 (0개이므로 1)

                        // offline replicas array
                        buf.put_i8(1); // array length + 1 (0개이므로 1)

                        buf.put_i8(0); // TAG_BUFFER for partition
                    }

                    // topic authorized operations
                    buf.put_u32(0x00000df8);

                    buf.put_i8(0); // TAG_BUFFER for topic
                }

                // next_cursor (nullable)
                buf.put_u8(0xff); // null

                buf.put_i8(0); // TAG_BUFFER for entire response
            }
            ResponsePayload::Fetch(fetch_response) => {
                // TAG_BUFFER after header
                buf.put_i8(0);

                // Body
                buf.put_i32(fetch_response.throttle_time_ms);
                buf.put_i16(response.error_code);
                buf.put_i32(fetch_response.session_id);

                // responses array length (COMPACT_ARRAY)
                let mut varint_buf = Vec::new();
                varint_buf.put_uvarint((fetch_response.responses.len() as i64) + 1);
                buf.put_slice(&varint_buf);

                // Write each topic response
                for topic in &fetch_response.responses {
                    // topic_id
                    buf.put_slice(&topic.topic_id);

                    // partitions array length (COMPACT_ARRAY)
                    let mut varint_buf = Vec::new();
                    varint_buf.put_uvarint((topic.partitions.len() as i64) + 1);
                    buf.put_slice(&varint_buf);

                    // Write each partition
                    for partition in &topic.partitions {
                        // partition_index
                        buf.put_i32(partition.partition_index);

                        // error_code
                        buf.put_i16(partition.error_code);

                        // high_watermark
                        buf.put_i64(partition.high_watermark);

                        // last_stable_offset
                        buf.put_i64(0);

                        // log_start_offset
                        buf.put_i64(0);

                        // aborted_transactions array (COMPACT_ARRAY)
                        let mut varint_buf = Vec::new();
                        varint_buf.put_uvarint(1);
                        buf.put_slice(&varint_buf);

                        // preferred_read_replica
                        buf.put_i32(-1);

                        // records (COMPACT_RECORDS)
                        if let Some(records) = &partition.records {
                            let mut varint_buf = Vec::new();
                            varint_buf.put_uvarint(2);
                            buf.put_slice(&varint_buf);
                            buf.put_slice(records);
                        } else {
                            let mut varint_buf = Vec::new();
                            varint_buf.put_uvarint(1);
                            buf.put_slice(&varint_buf);
                        }

                        // TAG_BUFFER for partition
                        buf.put_i8(0);
                    }

                    // TAG_BUFFER for topic
                    buf.put_i8(0);
                }

                // TAG_BUFFER for entire response
                buf.put_i8(0);
            }
            ResponsePayload::Produce(produce_response) => {
                // array length
                buf.put_i8((produce_response.responses.len() + 1) as i8);

                // responses
                for topic_response in &produce_response.responses {
                    // topic name
                    let name_bytes = topic_response.name.as_bytes();
                    buf.put_i8((name_bytes.len() + 1) as i8);
                    buf.put_slice(name_bytes);

                    // partition responses
                    buf.put_i8((topic_response.partition_responses.len() + 1) as i8);
                    for partition_response in &topic_response.partition_responses {
                        buf.put_i32(partition_response.partition);
                        buf.put_i16(partition_response.error_code);
                        buf.put_i64(partition_response.base_offset);
                        buf.put_i64(partition_response.log_append_time);
                    }

                    buf.put_u8(0); // TAG_BUFFER
                }

                buf.put_i32(produce_response.throttle_time_ms);
                buf.put_u8(0); // TAG_BUFFER
            }
        }

        let total_size = buf.len() as i32;
        let mut final_buf = BytesMut::new();
        final_buf.put_i32(total_size);
        final_buf.put_slice(&buf);

        let result = final_buf.to_vec();
        println!("[RESPONSE] Raw bytes: {:02x?}", result);
        result
    }
}

fn decode_varint(buf: &[u8]) -> u64 {
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
    use crate::adapters::incoming::protocol::constants::{
        MAX_SUPPORTED_VERSION, UNKNOWN_TOPIC_OR_PARTITION,
    };
    use crate::adapters::incoming::protocol::messages::{
        ApiVersion, ApiVersionsResponse, DescribeTopicPartitionsResponse, PartitionInfo,
        TopicResponse,
    };

    #[test]
    fn test_parse_api_versions_request() {
        let mut data = Vec::new();

        // Message size (4 bytes)
        let message_size: u32 = 10; // API Key(2) + API Version(2) + Correlation ID(4) + Client ID length(2)
        data.extend_from_slice(&message_size.to_be_bytes());

        // Header
        data.extend_from_slice(&API_VERSIONS_KEY.to_be_bytes()); // API Key
        data.extend_from_slice(&0i16.to_be_bytes()); // API Version
        data.extend_from_slice(&123i32.to_be_bytes()); // Correlation ID
        data.extend_from_slice(&0i16.to_be_bytes()); // Client ID length (0 = null)
        data.push(0); // tag buffer

        let parser = KafkaProtocolParser::new();
        let request = parser.parse_request(&data[4..]).unwrap(); // Skip message size

        assert_eq!(request.header.api_key, API_VERSIONS_KEY);
        assert_eq!(request.header.api_version, 0);
        assert_eq!(request.header.correlation_id, 123);
        assert_eq!(request.header.client_id, None);
        assert!(matches!(request.payload, RequestPayload::ApiVersions));
    }

    #[test]
    fn test_parse_describe_topic_partitions_request() {
        let mut data = Vec::new();

        // Header
        data.extend_from_slice(&DESCRIBE_TOPIC_PARTITIONS_KEY.to_be_bytes()); // API Key
        data.extend_from_slice(&0i16.to_be_bytes()); // API Version
        data.extend_from_slice(&123i32.to_be_bytes()); // Correlation ID

        // Client ID
        let client_id = "test-client";
        data.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        data.extend_from_slice(client_id.as_bytes());

        // tag buffer after client id
        data.push(0);

        // Topics array length (COMPACT_ARRAY)
        data.push(2); // array_length + 1

        // Topic name length (COMPACT_STRING)
        let topic_name = "test-topic";
        data.push((topic_name.len() + 1) as u8);
        data.extend_from_slice(topic_name.as_bytes());

        // tag buffer after topic name
        data.push(0);

        // Response partition limit
        data.extend_from_slice(&1u32.to_be_bytes());

        // cursor
        data.push(0);

        // tag buffer after cursor
        data.push(0);

        // tag buffer at the end
        data.push(0);

        let parser = KafkaProtocolParser::new();
        let request = parser.parse_request(&data).unwrap();

        assert_eq!(request.header.api_key, DESCRIBE_TOPIC_PARTITIONS_KEY);
        assert_eq!(request.header.api_version, 0);
        assert_eq!(request.header.correlation_id, 123);
        assert_eq!(request.header.client_id, Some("test-client".to_string()));

        match request.payload {
            RequestPayload::DescribeTopicPartitions(req) => {
                assert_eq!(req.topics.len(), 1);
                assert_eq!(req.topics[0].topic_name, "test-topic");
                assert_eq!(req.topics[0].partitions, vec![]);
            }
            _ => panic!("Expected DescribeTopicPartitions payload"),
        }
    }

    #[test]
    fn test_parse_describe_topic_partitions_request_with_multiple_topics() {
        let mut data = Vec::new();

        // Header
        data.extend_from_slice(&DESCRIBE_TOPIC_PARTITIONS_KEY.to_be_bytes()); // API Key
        data.extend_from_slice(&0i16.to_be_bytes()); // API Version
        data.extend_from_slice(&123i32.to_be_bytes()); // Correlation ID
        data.extend_from_slice(&0i16.to_be_bytes()); // Client ID length (0 = null)
        data.push(0); // tag buffer

        // Topics array length (COMPACT_ARRAY)
        data.push(2); // array_length + 1

        // Topic name (COMPACT_STRING)
        let topic_name = "test-topic";
        data.push((topic_name.len() + 1) as u8);
        data.extend_from_slice(topic_name.as_bytes());

        // tag buffer after topic name
        data.push(0);

        // Response partition limit
        data.extend_from_slice(&1u32.to_be_bytes());

        // cursor
        data.push(0);

        // tag buffer after cursor
        data.push(0);

        let parser = KafkaProtocolParser::new();
        let request = parser.parse_request(&data).unwrap();

        assert_eq!(request.header.api_key, DESCRIBE_TOPIC_PARTITIONS_KEY);
        assert_eq!(request.header.api_version, 0);
        assert_eq!(request.header.correlation_id, 123);
        assert_eq!(request.header.client_id, None);

        match request.payload {
            RequestPayload::DescribeTopicPartitions(req) => {
                assert_eq!(req.topics.len(), 1);
                assert_eq!(req.topics[0].topic_name, "test-topic");
                assert_eq!(req.topics[0].partitions, vec![]);
            }
            _ => panic!("Expected DescribeTopicPartitions payload"),
        }
    }

    #[test]
    fn test_encode_api_versions_response() {
        let response = KafkaResponse::new(
            123,
            0,
            ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: MAX_SUPPORTED_VERSION,
            }])),
        );

        let parser = KafkaProtocolParser::new();
        let encoded = parser.encode_response(response);

        // Verify size
        let size = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(size > 0);

        // Verify correlation ID
        let correlation_id = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(correlation_id, 123);
    }

    #[test]
    fn test_encode_describe_topic_partitions_response() {
        let response = KafkaResponse::new(
            123,
            UNKNOWN_TOPIC_OR_PARTITION,
            ResponsePayload::DescribeTopicPartitions(DescribeTopicPartitionsResponse {
                topics: vec![TopicResponse {
                    topic_name: "test-topic".to_string(),
                    topic_id: [0; 16],
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                    is_internal: false,
                    partitions: vec![],
                }],
            }),
        );

        let parser = KafkaProtocolParser::new();
        let encoded = parser.encode_response(response);

        // Verify size
        let size = i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(size > 0);

        // Verify correlation ID
        let correlation_id = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(correlation_id, 123);

        // Verify error code
        let error_code = i16::from_be_bytes([encoded[14], encoded[15]]);
        assert_eq!(error_code, UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn test_encode_describe_topic_partitions_response_with_partitions() {
        let response = KafkaResponse::new(
            123,
            0,
            ResponsePayload::DescribeTopicPartitions(DescribeTopicPartitionsResponse {
                topics: vec![TopicResponse {
                    topic_name: "test-topic".to_string(),
                    topic_id: [1; 16],
                    error_code: 0,
                    is_internal: false,
                    partitions: vec![
                        PartitionInfo {
                            partition_id: 0,
                            error_code: 0,
                        },
                        PartitionInfo {
                            partition_id: 1,
                            error_code: 0,
                        },
                    ],
                }],
            }),
        );

        let parser = KafkaProtocolParser::new();
        let encoded = parser.encode_response(response);

        // 기본 검증
        let size = i32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
        assert!(size > 0);

        let correlation_id = i32::from_be_bytes([encoded[4], encoded[5], encoded[6], encoded[7]]);
        assert_eq!(correlation_id, 123);

        // throttle_time_ms (0)
        assert_eq!(&encoded[8..12], &[0, 0, 0, 0]);

        // topics array length (COMPACT_ARRAY) = 2 (1개의 토픽 + 1)
        assert_eq!(encoded[13], 2);

        // topic error code (0)
        assert_eq!(&encoded[14..16], &[0, 0]);

        // topic name length (COMPACT_STRING) = 11 ("test-topic" 길이 + 1)
        assert_eq!(encoded[16], 11);
        assert_eq!(&encoded[17..27], b"test-topic");

        // topic id (UUID) - 모든 바이트가 1
        assert_eq!(&encoded[27..43], &[1; 16]);

        // is_internal
        assert_eq!(encoded[43], 0);

        // partitions array length = 3 (2개의 파티션 + 1)
        assert_eq!(encoded[44], 3);
    }
}

