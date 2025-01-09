use async_trait::async_trait;
use crate::ports::outgoing::message_store::MessageStore;
use crate::adapters::incoming::protocol::messages::KafkaMessage;
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct MemoryMessageStore {
    messages: Arc<RwLock<HashMap<(String, i32), Vec<(u64, Vec<u8>)>>>>,
    next_offset: AtomicU64,
}

impl MemoryMessageStore {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(RwLock::new(HashMap::new())),
            next_offset: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl MessageStore for MemoryMessageStore {
    async fn store_message(&self, message: KafkaMessage) -> Result<u64> {
        let mut messages = self.messages.write().await;
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);
        
        messages
            .entry((message.topic, message.partition))
            .or_insert_with(Vec::new)
            .push((offset, message.payload));
        
        Ok(offset)
    }

    async fn read_messages(&self, topic_id: &str, partition: i32, offset: i64) -> Result<Option<Vec<u8>>> {
        let messages = self.messages.read().await;
        if let Some(partition_messages) = messages.get(&(topic_id.to_string(), partition)) {
            for (msg_offset, payload) in partition_messages {
                if *msg_offset == offset as u64 {
                    return Ok(Some(payload.clone()));
                }
            }
        }
        Ok(None)
    }
} 