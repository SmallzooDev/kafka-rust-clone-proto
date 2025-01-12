#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Record {
    pub length: i64,
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i64,
    pub key: Vec<u8>,
    pub value_length: i64,
    pub value: RecordValue,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum RecordValue {
    FeatureLevel(FeatureLevelValue),
    Topic(TopicValue),
    Partition(PartitionValue),
}

#[derive(Debug, Clone)]
pub(crate) struct TopicValue {
    pub topic_name: String,
    pub topic_id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PartitionValue {
    pub partition_id: u32,
    pub topic_id: String,
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
    pub removing_replicas: Vec<u32>,
    pub adding_replicas: Vec<u32>,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub partition_epoch: u32,
    pub directories: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct FeatureLevelValue {
    pub name: String,
    pub level: u16,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Header;
