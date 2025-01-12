use crate::domain::message::KafkaMessage;
use crate::Result;
use async_trait::async_trait;

#[async_trait]
pub trait MessageStore: Send + Sync {
    async fn store_message(&self, message: KafkaMessage) -> Result<u64>;

    async fn read_messages(
        &self,
        topic_id: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Option<Vec<u8>>>;
}

