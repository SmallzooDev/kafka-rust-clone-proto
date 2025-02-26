use crate::adapters::protocol::KafkaProtocolParser;
use crate::adapters::outgoing::disk_store::DiskMessageStore;
use crate::adapters::outgoing::kraft_metadata_store::KraftMetadataStore;
use crate::application::broker_service::BrokerService;
use crate::ports::incoming::broker_incoming_port::BrokerIncomingPort;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub max_segment_size: u64,    // 세그먼트당 최대 크기
    pub max_buffer_size: usize,   // 메모리 버퍼 최대 크기
    pub flush_interval: Duration, // 버퍼 플러시 주기
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_segment_size: 1024 * 1024 * 1024,   // 1GB
            max_buffer_size: 1024 * 1024,           // 1MB
            flush_interval: Duration::from_secs(1), // 1초
        }
    }
}

#[allow(dead_code)]
pub struct AppConfig {
    pub store_config: StoreConfig,
    pub broker: Arc<dyn BrokerIncomingPort>,
    pub protocol_parser: KafkaProtocolParser,
}

impl AppConfig {
    pub fn new(_server_properties_path: &str) -> Self {
        let log_dir = PathBuf::from("/tmp/kraft-combined-logs");
        let store_config = StoreConfig::default();

        // Initialize stores
        let message_store = Box::new(DiskMessageStore::new(log_dir.clone(), store_config.clone()));
        let metadata_store = Box::new(KraftMetadataStore::new(log_dir));

        // Initialize broker with both stores
        let broker = Arc::new(BrokerService::new(message_store, metadata_store));
        let protocol_parser = KafkaProtocolParser::new();

        Self {
            store_config,
            broker,
            protocol_parser,
        }
    }
}

