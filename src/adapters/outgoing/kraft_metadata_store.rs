use crate::adapters::protocol::dto::ErrorCode;
use crate::adapters::protocol::parser::kraft_record_parser::{RecordBatch, RecordValue};
use crate::application::error::ApplicationError;
use crate::domain::message::Partition;
use crate::domain::message::TopicMetadata;
use crate::ports::outgoing::metadata_store::MetadataStore;
use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::read;

pub struct KraftMetadataStore {
    log_dir: PathBuf,
}

#[async_trait]
impl MetadataStore for KraftMetadataStore {
    async fn get_topic_metadata_by_names(
        &self,
        topic_names: Vec<String>,
    ) -> Result<Option<Vec<TopicMetadata>>, ApplicationError> {
        let topics_by_name = self.load_metadata().await?;

        let mut result = Vec::new();
        for name in topic_names {
            let metadata = if let Some(topic_metadata) = topics_by_name.get(&name) {
                topic_metadata.clone()
            } else {
                TopicMetadata {
                    error_code: i16::from(ErrorCode::UnknownTopicOrPartition),
                    name: name.clone(),
                    topic_id: name.clone(),
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations: 0x0DF,
                }
            };
            result.push(metadata);
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
    ) -> Result<Option<Vec<TopicMetadata>>, ApplicationError> {
        let topics_by_name = self.load_metadata().await?;

        let mut result = Vec::new();
        for id in topic_ids {
            let metadata = if let Some(topic_metadata) = topics_by_name.values().find(|m| m.topic_id == id) {
                topic_metadata.clone()
            } else {
                TopicMetadata {
                    error_code: i16::from(ErrorCode::UnknownTopicOrPartition),
                    name: id.clone(),
                    topic_id: id.clone(),
                    is_internal: false,
                    partitions: Vec::new(),
                    topic_authorized_operations: 0x0DF,
                }
            };
            result.push(metadata);
        }

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

impl KraftMetadataStore {
    pub fn new(log_dir: PathBuf) -> Self {
        Self { log_dir }
    }

    fn get_metadata_log_path(&self) -> PathBuf {
        let path = self
            .log_dir
            .join("__cluster_metadata-0")
            .join("00000000000000000000.log");
        path
    }

    async fn load_metadata(&self) -> Result<HashMap<String, TopicMetadata>, ApplicationError> {
        let path = self.get_metadata_log_path();
        let content = read(&path).await.map_err(ApplicationError::Io)?;
        let mut data = BytesMut::with_capacity(content.len());
        data.extend_from_slice(&content);
        let mut data = data.freeze();

        let mut topics_by_name: HashMap<String, TopicMetadata> = HashMap::new();
        let mut topics_by_id: HashMap<String, String> = HashMap::new(); // topic_id -> topic_name mapping

        while data.remaining() > 0 {
            let record_batch = RecordBatch::from_bytes(&mut data)?;

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

