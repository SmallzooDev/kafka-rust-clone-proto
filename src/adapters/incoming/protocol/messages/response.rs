use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY,
    DESCRIBE_TOPIC_PARTITIONS_KEY,
    FETCH_KEY,
    PRODUCE_KEY,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersionsResponse {
    pub api_versions: Vec<ApiVersion>,
}

impl ApiVersionsResponse {
    pub fn new(api_versions: Vec<ApiVersion>) -> Self {
        Self { api_versions }
    }

    pub fn default() -> Self {
        Self::new(vec![
            ApiVersion {
                api_key: API_VERSIONS_KEY,
                min_version: 0,
                max_version: 4,
            },
            ApiVersion {
                api_key: FETCH_KEY,
                min_version: 0,
                max_version: 16,
            },
            ApiVersion {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                min_version: 0,
                max_version: 0,
            },
            ApiVersion {
                api_key: PRODUCE_KEY,
                min_version: 0,
                max_version: 9,
            }
        ])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicResponse {
    pub topic_name: String,
    pub topic_id: [u8; 16],  // UUID as 16 bytes
    pub error_code: i16,     // topic level error code
    pub is_internal: bool,   // is_internal flag
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsResponse {
    pub topics: Vec<TopicResponse>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub error_code: i16,
}

impl DescribeTopicPartitionsResponse {
    pub fn new_unknown_topic(topic_name: String) -> Self {
        Self {
            topics: vec![TopicResponse {
                topic_name,
                topic_id: [0; 16],  // 00000000-0000-0000-0000-000000000000
                error_code: 3,      // UNKNOWN_TOPIC_OR_PARTITION
                is_internal: false, // external topic
                partitions: vec![],
            }],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchResponse {
    pub throttle_time_ms: i32,
    pub session_id: i32,
    pub responses: Vec<FetchableTopicResponse>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchableTopicResponse {
    pub topic_id: [u8; 16],
    pub partitions: Vec<FetchablePartitionResponse>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchablePartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub records: Option<Vec<u8>>,
}

impl FetchResponse {
    pub fn empty() -> Self {
        Self {
            throttle_time_ms: 0,
            session_id: 0,
            responses: vec![],
        }
    }

    pub fn empty_topic(topic_id: [u8; 16]) -> Self {
        Self {
            throttle_time_ms: 0,
            session_id: 0,
            responses: vec![
                FetchableTopicResponse {
                    topic_id,
                    partitions: vec![
                        FetchablePartitionResponse {
                            partition_index: 0,
                            error_code: 0,  // No Error
                            high_watermark: 0,
                            records: None,
                        }
                    ],
                }
            ],
        }
    }

    pub fn unknown_topic(topic_id: [u8; 16]) -> Self {
        Self {
            throttle_time_ms: 0,
            session_id: 0,
            responses: vec![
                FetchableTopicResponse {
                    topic_id,
                    partitions: vec![
                        FetchablePartitionResponse {
                            partition_index: 0,
                            error_code: 100, // UNKNOWN_TOPIC
                            high_watermark: 0,
                            records: None,
                        }
                    ],
                }
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProduceResponse {
    pub responses: Vec<ProduceTopicResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProduceTopicResponse {
    pub name: String,
    pub partition_responses: Vec<ProducePartitionResponse>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProducePartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponsePayload {
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
    Fetch(FetchResponse),
    Produce(ProduceResponse),
}

#[derive(Debug, Clone)]
pub struct KafkaResponse {
    pub correlation_id: i32,
    pub error_code: i16,
    pub payload: ResponsePayload,
}

impl KafkaResponse {
    pub fn new(correlation_id: i32, error_code: i16, payload: ResponsePayload) -> Self {
        Self {
            correlation_id,
            error_code,
            payload,
        }
    }
} 