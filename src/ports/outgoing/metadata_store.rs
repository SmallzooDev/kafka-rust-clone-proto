use crate::domain::message::TopicMetadata;
use crate::Result;
use async_trait::async_trait;

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn get_topic_metadata_by_names(&self, topic_names: Vec<String>) -> Result<Option<Vec<TopicMetadata>>>;
    async fn get_topic_metadata_by_ids(&self, topic_ids: Vec<String>) -> Result<Option<Vec<TopicMetadata>>>;
} 