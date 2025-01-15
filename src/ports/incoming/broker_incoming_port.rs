use async_trait::async_trait;
use crate::Result;
use crate::adapters::protocol::dto::{KafkaRequest, KafkaResponse};

#[async_trait]
pub trait BrokerIncomingPort: Send + Sync {
    async fn handle_request(&self, request: KafkaRequest) -> Result<KafkaResponse>;
} 