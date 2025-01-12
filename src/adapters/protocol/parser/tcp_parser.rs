use crate::adapters::protocol::dto::{
    KafkaRequest,
    KafkaResponse,
};
use crate::application::error::ApplicationError;

use super::request_parser::RequestParser;
use super::response_encoder::ResponseEncoder;

#[derive(Clone)]
pub struct KafkaProtocolParser {
    request_parser: RequestParser,
    response_encoder: ResponseEncoder,
}

impl KafkaProtocolParser {
    pub fn new() -> Self {
        Self {
            request_parser: RequestParser::new(),
            response_encoder: ResponseEncoder::new(),
        }
    }

    pub fn parse_request(&self, data: &[u8]) -> Result<KafkaRequest, ApplicationError> {
        self.request_parser.parse(data)
    }

    pub fn encode_response(&self, response: KafkaResponse) -> Vec<u8> {
        self.response_encoder.encode(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::protocol::constants::{API_VERSIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_KEY, MAX_SUPPORTED_VERSION, UNKNOWN_TOPIC_OR_PARTITION};
    use crate::adapters::protocol::dto::{ApiVersion, ApiVersionsResponse, DescribeTopicPartitionsResponse, PartitionInfo, RequestPayload, ResponsePayload, TopicResponse};

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

