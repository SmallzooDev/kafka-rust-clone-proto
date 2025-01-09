use kafka_starter_rust::{
    adapters::outgoing::disk_store::DiskMessageStore,
    adapters::incoming::protocol::messages::KafkaMessage,
    config::app_config::StoreConfig,
    ports::outgoing::message_store::MessageStore,
    Result,
};
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;

#[tokio::test]
async fn test_disk_store_write_and_read() -> Result<()> {
    // 임시 디렉토리 생성
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,  // 작은 크기로 설정하여 롤링 테스트
        max_buffer_size: 5,      // 작은 버퍼로 설정하여 플러시 테스트
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 메시지 저장
    let message = KafkaMessage {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: b"Hello, Kafka!".to_vec(),
    };

    // 메시지 저장 및 오프셋 확인
    let offset = store.store_message(message.clone()).await?;
    assert_eq!(offset, 0);  // 첫 번째 메시지의 오프셋은 0이어야 함

    // 파일 생성 확인
    let topic_dir = temp_dir.path().join("test-topic-0");
    assert!(topic_dir.exists());
    
    // 세그먼트 파일 확인
    let base_offset = (offset / 1000) * 1000;
    let log_file = topic_dir.join(format!("{:020}.log", base_offset));
    let index_file = topic_dir.join(format!("{:020}.index", base_offset));
    
    assert!(log_file.exists());
    assert!(index_file.exists());

    // 메시지 읽기
    let read_result = store.read_messages(&message.topic, message.partition, offset as i64).await?;
    assert!(read_result.is_some());
    assert_eq!(read_result.unwrap(), b"Hello, Kafka!");

    // 여러 메시지 저장하여 버퍼 플러시 테스트
    for i in 1..10 {
        let msg = KafkaMessage {
            correlation_id: 1,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: format!("Message {}", i).into_bytes(),
        };
        let new_offset = store.store_message(msg).await?;
        assert_eq!(new_offset, i);
    }

    // 버퍼가 플러시되도록 잠시 대기
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 로그 파일 크기 확인
    let metadata = fs::metadata(log_file).await?;
    assert!(metadata.len() > 0);

    // 세그먼트 롤링 테스트
    let large_message = KafkaMessage {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: vec![0; 2048],  // max_segment_size보다 큰 메시지
    };

    let new_offset = store.store_message(large_message).await?;
    
    // 새로운 세그먼트 파일 생성 확인
    let new_base_offset = (new_offset / 1000) * 1000;
    let new_log_file = topic_dir.join(format!("{:020}.log", new_base_offset));
    let new_index_file = topic_dir.join(format!("{:020}.index", new_base_offset));
    
    assert!(new_log_file.exists());
    assert!(new_index_file.exists());

    Ok(())
}

#[tokio::test]
async fn test_disk_store_concurrent_access() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024 * 1024,
        max_buffer_size: 10,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);
    let store = std::sync::Arc::new(store);

    // 여러 작업을 동시에 실행
    let mut handles = vec![];
    
    for i in 0..5 {
        let store_clone = store.clone();
        let handle = tokio::spawn(async move {
            let msg = KafkaMessage {
                correlation_id: 1,
                topic: format!("topic-{}", i),
                partition: 0,
                offset: 0,
                timestamp: 0,
                payload: format!("Message from task {}", i).into_bytes(),
            };
            store_clone.store_message(msg).await
        });
        handles.push(handle);
    }

    // 모든 작업이 완료될 때까지 대기
    for handle in handles {
        let result = handle.await.expect("Task panicked")?;
        assert!(result < 5);  // 오프셋은 0부터 4까지여야 함
    }

    // 각 토픽의 디렉토리와 파일이 생성되었는지 확인
    for i in 0..5 {
        let topic_dir = temp_dir.path().join(format!("topic-{}-0", i));
        assert!(topic_dir.exists());
        
        let log_file = topic_dir.join("00000000000000000000.log");
        let index_file = topic_dir.join("00000000000000000000.index");
        
        assert!(log_file.exists());
        assert!(index_file.exists());
    }

    Ok(())
}

#[tokio::test]
async fn test_log_file_contents() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 2,      // 작은 버퍼로 설정하여 빠른 플러시 유도
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 여러 메시지 저장
    for i in 0..5 {
        let message = KafkaMessage {
            correlation_id: 1,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: format!("Message {}", i).into_bytes(),
        };
        store.store_message(message).await?;
    }

    // 버퍼가 플러시되도록 잠시 대기
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 로그 파일 확인
    let topic_dir = temp_dir.path().join("test-topic-0");
    let log_file = topic_dir.join("00000000000000000000.log");
    let index_file = topic_dir.join("00000000000000000000.index");

    println!("\n=== Log File Contents ===");
    let log_contents = fs::read(&log_file).await?;
    println!("Log file size: {} bytes", log_contents.len());
    println!("First 100 bytes: {:?}", &log_contents[..100.min(log_contents.len())]);

    println!("\n=== Index File Contents ===");
    let index_contents = fs::read(&index_file).await?;
    println!("Index file size: {} bytes", index_contents.len());
    println!("Index entries: {}", index_contents.len() / 8);  // 각 엔트리는 8바이트

    // 파일이 존재하고 내용이 있는지 확인
    assert!(log_contents.len() > 0);
    assert!(index_contents.len() > 0);
    assert_eq!(index_contents.len() % 8, 0);  // 인덱스 파일은 8바이트 단위여야 함

    Ok(())
} 