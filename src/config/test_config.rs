use std::sync::Arc;
use crate::ports::incoming::message_handler::MessageHandler;
use crate::adapters::incoming::kafka_protocol_parser::KafkaProtocolParser;
use crate::adapters::incoming::protocol::messages::{KafkaRequest, KafkaResponse};
use crate::Result;
use super::app_config::{AppConfig, StoreConfig};

pub struct MockMessageHandler;

impl MockMessageHandler {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl MessageHandler for MockMessageHandler {
    async fn handle_request(&self, _request: KafkaRequest) -> Result<KafkaResponse> {
        unimplemented!("Mock implementation")
    }
}

pub fn create_test_config() -> AppConfig {
    AppConfig {
        store_config: StoreConfig::default(),
        broker: Arc::new(MockMessageHandler::new()),
        protocol_parser: KafkaProtocolParser::new(),
    }
} 