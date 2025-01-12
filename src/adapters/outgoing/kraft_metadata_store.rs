use crate::adapters::protocol::dto::ErrorCode;
use crate::adapters::protocol::parser::kraft_record_parser::KraftRecordParser;
use crate::application::error::ApplicationError;
use crate::domain::message::TopicMetadata;
use crate::ports::outgoing::metadata_store::MetadataStore;
use async_trait::async_trait;
use bytes::BytesMut;
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

        let parser = KraftRecordParser::new();
        parser.parse_metadata(&mut data)
    }
}

