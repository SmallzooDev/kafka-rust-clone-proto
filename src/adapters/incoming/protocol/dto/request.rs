use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY,
    DESCRIBE_TOPIC_PARTITIONS_KEY,
    FETCH_KEY,
    PRODUCE_KEY,
};

#[derive(Debug, Clone, PartialEq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn is_supported_version(&self) -> bool {
        match self.api_key {
            API_VERSIONS_KEY => self.api_version >= 0 && self.api_version <= 4,
            FETCH_KEY => self.api_version == 16,
            DESCRIBE_TOPIC_PARTITIONS_KEY => self.api_version == 0,
            PRODUCE_KEY => self.api_version >= 0 && self.api_version <= 9,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRequest {
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTopicPartitionsRequest {
    pub topics: Vec<TopicRequest>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopic>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchPartition {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForgottenTopic {
    pub topic_id: [u8; 16],
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProduceRequest {
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<ProduceTopicData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProduceTopicData {
    pub name: String,
    pub partition_data: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProducePartitionData {
    pub partition: i32,
    pub records: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestPayload {
    ApiVersions,
    DescribeTopicPartitions(DescribeTopicPartitionsRequest),
    Fetch(FetchRequest),
    Produce(ProduceRequest),
}

#[derive(Debug, Clone)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub payload: RequestPayload,
}

impl KafkaRequest {
    pub fn new(header: RequestHeader, payload: RequestPayload) -> Self {
        Self {
            header,
            payload,
        }
    }
} 