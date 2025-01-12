use crate::adapters::incoming::protocol::constants::{
    API_VERSIONS_KEY, DESCRIBE_TOPIC_PARTITIONS_KEY, FETCH_KEY, PRODUCE_KEY,
    UNKNOWN_TOPIC_OR_PARTITION, UNSUPPORTED_VERSION,
};
use crate::adapters::incoming::protocol::dto::{
    ApiVersionsResponse, DescribeTopicPartitionsResponse, ErrorCode, FetchResponse,
    FetchablePartitionResponse, FetchableTopicResponse, KafkaRequest, KafkaResponse,
    PartitionInfo, ProducePartitionResponse, ProduceResponse, ProduceTopicResponse, RequestPayload,
    ResponsePayload, TopicResponse,
};
use crate::domain::message::{KafkaMessage, TopicMetadata};
use crate::ports::incoming::message_handler::MessageHandler;
use crate::ports::outgoing::message_store::MessageStore;
use crate::ports::outgoing::metadata_store::MetadataStore;
use crate::Result;
use async_trait::async_trait;
use hex;

#[allow(dead_code)]
pub struct KafkaBroker {
    message_store: Box<dyn MessageStore>,
    metadata_store: Box<dyn MetadataStore>,
}

impl KafkaBroker {
    pub fn new(
        message_store: Box<dyn MessageStore>,
        metadata_store: Box<dyn MetadataStore>,
    ) -> Self {
        Self {
            message_store,
            metadata_store,
        }
    }

    fn convert_topic_id_to_uuid(topic_id: &[u8]) -> String {
        let topic_id_hex = hex::encode(topic_id);
        format!(
            "{}-{}-{}-{}-{}",
            &topic_id_hex[0..8],
            &topic_id_hex[8..12],
            &topic_id_hex[12..16],
            &topic_id_hex[16..20],
            &topic_id_hex[20..32]
        )
    }

    async fn handle_fetch_request(
        &self,
        request: &KafkaRequest,
        fetch_request: &RequestPayload,
    ) -> Result<KafkaResponse> {
        if let RequestPayload::Fetch(fetch_request) = fetch_request {
            match fetch_request.topics.first() {
                Some(first_topic) => {
                    let topic_id = Self::convert_topic_id_to_uuid(&first_topic.topic_id);
                    println!("[DEBUG] Looking for topic_id: {}", topic_id);
                    let topic_metadata = self
                        .metadata_store
                        .get_topic_metadata_by_ids(vec![topic_id.clone()])
                        .await?;

                    let response = match topic_metadata {
                        Some(metadata_list) => {
                            let metadata = metadata_list.first().unwrap();
                            if metadata.error_code == i16::from(ErrorCode::UnknownTopicOrPartition)
                            {
                                FetchResponse::unknown_topic(first_topic.topic_id)
                            } else {
                                if let Some(partition) = first_topic.partitions.first() {
                                    let records = self
                                        .message_store
                                        .read_messages(
                                            &metadata.name,
                                            partition.partition,
                                            partition.fetch_offset,
                                        )
                                        .await?;
                                    println!("[DEBUG] Read records: {:?}", records.is_some());

                                    FetchResponse {
                                        throttle_time_ms: 0,
                                        session_id: 0,
                                        responses: vec![FetchableTopicResponse {
                                            topic_id: first_topic.topic_id,
                                            partitions: vec![FetchablePartitionResponse {
                                                partition_index: partition.partition,
                                                error_code: 0,
                                                high_watermark: 1,
                                                records,
                                            }],
                                        }],
                                    }
                                } else {
                                    FetchResponse::empty_topic(first_topic.topic_id)
                                }
                            }
                        }
                        None => FetchResponse::unknown_topic(first_topic.topic_id),
                    };

                    Ok(KafkaResponse::new(
                        request.header.correlation_id,
                        0,
                        ResponsePayload::Fetch(response),
                    ))
                }
                None => Ok(KafkaResponse::new(
                    request.header.correlation_id,
                    0,
                    ResponsePayload::Fetch(FetchResponse::empty()),
                )),
            }
        } else {
            unreachable!()
        }
    }

    async fn handle_describe_topic_partitions(
        &self,
        request: &KafkaRequest,
        describe_request: &RequestPayload,
    ) -> Result<KafkaResponse> {
        if let RequestPayload::DescribeTopicPartitions(req) = describe_request {
            let topic_names: Vec<String> =
                req.topics.iter().map(|t| t.topic_name.clone()).collect();

            let topic_metadata = self
                .metadata_store
                .get_topic_metadata_by_names(topic_names.clone())
                .await?;

            let topics = match topic_metadata {
                Some(metadata_list) => metadata_list
                    .into_iter()
                    .map(|metadata| self.create_topic_response(metadata))
                    .collect(),
                None => topic_names
                    .into_iter()
                    .map(|topic_name| TopicResponse {
                        topic_name,
                        topic_id: [0; 16],
                        error_code: UNKNOWN_TOPIC_OR_PARTITION,
                        is_internal: false,
                        partitions: vec![],
                    })
                    .collect(),
            };

            Ok(KafkaResponse::new(
                request.header.correlation_id,
                0,
                ResponsePayload::DescribeTopicPartitions(DescribeTopicPartitionsResponse {
                    topics,
                }),
            ))
        } else {
            unreachable!()
        }
    }

    fn create_topic_response(&self, metadata: TopicMetadata) -> TopicResponse {
        let topic_id_bytes = hex::decode(metadata.topic_id.replace("-", "")).unwrap_or(vec![0; 16]);
        let mut topic_id = [0u8; 16];
        topic_id.copy_from_slice(&topic_id_bytes);

        TopicResponse {
            topic_name: metadata.name,
            topic_id,
            error_code: metadata.error_code,
            is_internal: false,
            partitions: metadata
                .partitions
                .iter()
                .map(|p| PartitionInfo {
                    partition_id: p.partition_index as i32,
                    error_code: p.error_code,
                })
                .collect(),
        }
    }

    async fn handle_produce_request(
        &self,
        request: &KafkaRequest,
        produce_request: &RequestPayload,
    ) -> Result<KafkaResponse> {
        if let RequestPayload::Produce(produce_request) = produce_request {
            let mut responses = Vec::new();
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            for topic_data in &produce_request.topic_data {
                let mut partition_responses = Vec::new();

                for partition_data in &topic_data.partition_data {
                    let message = KafkaMessage {
                        correlation_id: request.header.correlation_id,
                        topic: topic_data.name.clone(),
                        partition: partition_data.partition,
                        offset: 0, // store_message에서 할당됨
                        timestamp: current_time as u64,
                        payload: partition_data.records.clone(),
                    };

                    match self.message_store.store_message(message).await {
                        Ok(offset) => {
                            partition_responses.push(ProducePartitionResponse {
                                partition: partition_data.partition,
                                error_code: 0,
                                base_offset: offset as i64,
                                log_append_time: current_time,
                            });
                        }
                        Err(_) => {
                            partition_responses.push(ProducePartitionResponse {
                                partition: partition_data.partition,
                                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                                base_offset: -1,
                                log_append_time: -1,
                            });
                        }
                    }
                }

                responses.push(ProduceTopicResponse {
                    name: topic_data.name.clone(),
                    partition_responses,
                });
            }

            Ok(KafkaResponse::new(
                request.header.correlation_id,
                0,
                ResponsePayload::Produce(ProduceResponse {
                    responses,
                    throttle_time_ms: 0,
                }),
            ))
        } else {
            unreachable!()
        }
    }
}

#[async_trait]
impl MessageHandler for KafkaBroker {
    async fn handle_request(&self, request: KafkaRequest) -> Result<KafkaResponse> {
        if !request.header.is_supported_version() {
            return Ok(KafkaResponse::new(
                request.header.correlation_id,
                UNSUPPORTED_VERSION,
                ResponsePayload::ApiVersions(ApiVersionsResponse::default()),
            ));
        }

        match request.header.api_key {
            API_VERSIONS_KEY => Ok(KafkaResponse::new(
                request.header.correlation_id,
                0,
                ResponsePayload::ApiVersions(ApiVersionsResponse::default()),
            )),
            FETCH_KEY => self.handle_fetch_request(&request, &request.payload).await,
            DESCRIBE_TOPIC_PARTITIONS_KEY => {
                self.handle_describe_topic_partitions(&request, &request.payload)
                    .await
            }
            PRODUCE_KEY => {
                self.handle_produce_request(&request, &request.payload)
                    .await
            }
            _ => Ok(KafkaResponse::new(
                request.header.correlation_id,
                0,
                ResponsePayload::ApiVersions(ApiVersionsResponse::new(vec![])),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::adapters::incoming::protocol::dto::{
        DescribeTopicPartitionsRequest, RequestHeader, TopicRequest,
    };
    use crate::domain::message::{KafkaMessage, TopicMetadata};
    use async_trait::async_trait;

    struct MockMessageStore {
        next_offset: AtomicU64,
    }

    impl MockMessageStore {
        fn new() -> Self {
            Self {
                next_offset: AtomicU64::new(0),
            }
        }
    }

    #[async_trait]
    impl MessageStore for MockMessageStore {
        async fn store_message(&self, _message: KafkaMessage) -> Result<u64> {
            Ok(self.next_offset.fetch_add(1, Ordering::SeqCst))
        }

        async fn read_messages(
            &self,
            _topic_id: &str,
            _partition: i32,
            _offset: i64,
        ) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    struct MockMetadataStore {
        topics: Vec<TopicMetadata>,
    }

    impl MockMetadataStore {
        fn new(topics: Vec<TopicMetadata>) -> Self {
            Self { topics }
        }
    }

    #[async_trait]
    impl MetadataStore for MockMetadataStore {
        async fn get_topic_metadata_by_names(
            &self,
            topic_names: Vec<String>,
        ) -> Result<Option<Vec<TopicMetadata>>> {
            let mut result = Vec::new();
            for topic_name in topic_names {
                if let Some(topic) = self.topics.iter().find(|t| t.name == topic_name) {
                    result.push(topic.clone());
                }
            }
            if result.is_empty() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        }

        async fn get_topic_metadata_by_ids(
            &self,
            topic_ids: Vec<String>,
        ) -> Result<Option<Vec<TopicMetadata>>> {
            let mut result = Vec::new();
            for topic_id in topic_ids {
                if let Some(topic) = self.topics.iter().find(|t| t.topic_id == topic_id) {
                    result.push(topic.clone());
                }
            }
            if result.is_empty() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        }
    }

    #[tokio::test]
    async fn test_handle_describe_topic_partitions_existing_topic() -> Result<()> {
        let topic_id = "00000000-0000-0000-0000-000000000001".to_string();
        let topic_metadata = TopicMetadata {
            error_code: i16::from(ErrorCode::None),
            name: "test-topic".to_string(),
            topic_id: topic_id.clone(),
            is_internal: false,
            partitions: vec![],
            topic_authorized_operations: 0x0DF,
        };

        let broker = KafkaBroker::new(
            Box::new(MockMessageStore::new()),
            Box::new(MockMetadataStore::new(vec![topic_metadata])),
        );

        let request = KafkaRequest::new(
            RequestHeader {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                api_version: 0,
                correlation_id: 123,
                client_id: None,
            },
            RequestPayload::DescribeTopicPartitions(DescribeTopicPartitionsRequest {
                topics: vec![TopicRequest {
                    topic_name: "test-topic".to_string(),
                    partitions: vec![],
                }],
            }),
        );

        let response = broker.handle_request(request).await?;
        assert_eq!(response.correlation_id, 123);
        assert_eq!(response.error_code, 0);

        match response.payload {
            ResponsePayload::DescribeTopicPartitions(resp) => {
                assert_eq!(resp.topics.len(), 1);
                assert_eq!(resp.topics[0].topic_name, "test-topic");
                let expected_topic_id = hex::decode(topic_id.replace("-", "")).unwrap();
                assert_eq!(&resp.topics[0].topic_id[..], &expected_topic_id[..]);
                assert_eq!(resp.topics[0].error_code, 0);
                assert_eq!(resp.topics[0].partitions.len(), 0);
            }
            _ => panic!("Expected DescribeTopicPartitions response"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_handle_describe_topic_partitions_nonexistent_topic() -> Result<()> {
        let broker = KafkaBroker::new(
            Box::new(MockMessageStore::new()),
            Box::new(MockMetadataStore::new(vec![])),
        );

        let request = KafkaRequest::new(
            RequestHeader {
                api_key: DESCRIBE_TOPIC_PARTITIONS_KEY,
                api_version: 0,
                correlation_id: 123,
                client_id: None,
            },
            RequestPayload::DescribeTopicPartitions(DescribeTopicPartitionsRequest {
                topics: vec![TopicRequest {
                    topic_name: "test-topic".to_string(),
                    partitions: vec![],
                }],
            }),
        );

        let response = broker.handle_request(request).await?;
        assert_eq!(response.correlation_id, 123);
        assert_eq!(response.error_code, 0);

        match response.payload {
            ResponsePayload::DescribeTopicPartitions(resp) => {
                assert_eq!(resp.topics.len(), 1);
                assert_eq!(resp.topics[0].topic_name, "test-topic");
                assert_eq!(resp.topics[0].topic_id, [0u8; 16]);
                assert_eq!(resp.topics[0].error_code, UNKNOWN_TOPIC_OR_PARTITION);
                assert_eq!(resp.topics[0].partitions.len(), 0);
            }
            _ => panic!("Expected DescribeTopicPartitions response"),
        }

        Ok(())
    }
}

