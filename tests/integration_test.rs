use kafka_starter_rust::{
    adapters::outgoing::disk_store::DiskMessageStore, config::app_config::StoreConfig,
    ports::outgoing::message_store::MessageStore, Result,
};
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;
use std::sync::Arc;
use kafka_starter_rust::domain::message::KafkaMessage;

// 테스트 결과를 출력하는 헬퍼 함수
fn print_test_header(test_name: &str) {
    println!("\n=== {} ===", test_name);
    println!("Starting test at: {:?}", std::time::SystemTime::now());
}

fn print_message_details(prefix: &str, message: &KafkaMessage) {
    println!("\n{} Details:", prefix);
    println!("Topic: {}", message.topic);
    println!("Partition: {}", message.partition);
    println!("Offset: {}", message.offset);
    println!("Correlation ID: {}", message.correlation_id);
    println!("Timestamp: {}", message.timestamp);
    println!("Payload Size: {} bytes", message.payload.len());
    if message.payload.len() <= 100 {
        println!("Payload: {:?}", String::from_utf8_lossy(&message.payload));
    } else {
        println!("Payload: <too large to display>");
    }
}

fn print_test_summary(test_name: &str, total_messages: usize, successful_messages: usize, duration: Duration) {
    println!("\n=== {} Summary ===", test_name);
    println!("Total dto processed: {}", total_messages);
    println!("Successfully processed: {}", successful_messages);
    println!("Failed dto: {}", total_messages - successful_messages);
    println!("Total duration: {:?}", duration);
}

#[allow(dead_code)]
fn print_buffer_summary(buffer_size: usize, next_offset: u64, flushed_count: usize) {
    println!("\n=== Buffer Status ===");
    println!("Current size: {} dto", buffer_size);
    println!("Next offset: {}", next_offset);
    println!("Total flushed: {} dto", flushed_count);
}

#[tokio::test]
async fn test_disk_store_write_and_read() -> Result<()> {
    print_test_header("Disk Store Write and Read Test");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    println!("Created temporary directory at: {:?}", temp_dir.path());

    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };
    println!("Store configuration: {:?}", config);

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    let message = KafkaMessage {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: b"Hello, Kafka!".to_vec(),
    };

    print_message_details("Original Message", &message);

    let offset = store.store_message(message.clone()).await?;
    println!("\nMessage stored with offset: {}", offset);

    let topic_dir = temp_dir.path().join("test-topic-0");
    println!("\nChecking directory structure:");
    println!("Topic directory exists: {}", topic_dir.exists());

    let base_offset = (offset / 1000) * 1000;
    let log_file = topic_dir.join(format!("{:020}.log", base_offset));
    let index_file = topic_dir.join(format!("{:020}.index", base_offset));

    println!("Log file exists: {}", log_file.exists());
    println!("Index file exists: {}", index_file.exists());

    let read_result = store
        .read_messages(&message.topic, message.partition, offset as i64)
        .await?;
    
    println!("\nRead Result:");
    match &read_result {
        Some(data) => println!("Retrieved message: {:?}", String::from_utf8_lossy(data)),
        None => println!("No message found"),
    }

    // 여러 메시지 저장 테스트
    println!("\nTesting multiple message storage:");
    for i in 1..10 {
        let msg = KafkaMessage {
            correlation_id: 1,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: format!("Message {}", i).into_bytes(),
        };
        let new_offset = store.store_message(msg.clone()).await?;
        println!("Stored message {} at offset {}", i, new_offset);
    }

    println!("\nWaiting for buffer flush...");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let metadata = fs::metadata(&log_file).await?;
    println!("Log file size after writes: {} bytes", metadata.len());

    // 세그먼트 롤링 테스트
    println!("\nTesting segment rolling with large message:");
    let large_message = KafkaMessage {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: vec![0; 2048],
    };

    let new_offset = store.store_message(large_message).await?;
    println!("Large message stored at offset: {}", new_offset);

    let new_base_offset = (new_offset / 1000) * 1000;
    let new_log_file = topic_dir.join(format!("{:020}.log", new_base_offset));
    let new_index_file = topic_dir.join(format!("{:020}.index", new_base_offset));

    println!("\nNew segment files:");
    println!("New log file exists: {}", new_log_file.exists());
    println!("New index file exists: {}", new_index_file.exists());

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
    let store = Arc::new(store);

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
        assert!(result < 5); // 오프셋은 0부터 4까지여야 함
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
        max_buffer_size: 2, // 작은 버퍼로 설정하여 빠른 플러시 유도
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
    println!(
        "First 100 bytes: {:?}",
        &log_contents[..100.min(log_contents.len())]
    );

    println!("\n=== Index File Contents ===");
    let index_contents = fs::read(&index_file).await?;
    println!("Index file size: {} bytes", index_contents.len());
    println!("Index entries: {}", index_contents.len() / 8); // 각 엔트리는 8바이트

    // 파일이 존재하고 내용이 있는지 확인
    assert!(log_contents.len() > 0);
    assert!(index_contents.len() > 0);
    assert_eq!(index_contents.len() % 8, 0); // 인덱스 파일은 8바이트 단위여야 함

    Ok(())
}

// 메시지 소비 테스트
#[tokio::test]
async fn test_fetch_multiple_messages() -> Result<()> {
    print_test_header("Fetch Multiple Messages Test");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    let messages = vec![
        KafkaMessage {
            correlation_id: 1,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: b"Hello CodeCrafters!".to_vec(),
        },
        KafkaMessage {
            correlation_id: 1,
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: b"Hello Kafka!".to_vec(),
        },
    ];

    println!("\nStoring dto:");
    for (i, message) in messages.iter().enumerate() {
        print_message_details(&format!("Message {}", i + 1), message);
        let offset = store.store_message(message.clone()).await?;
        println!("Stored at offset: {}", offset);
    }

    println!("\nReading dto back:");
    for (i, message) in messages.iter().enumerate() {
        let read_result = store
            .read_messages(&message.topic, message.partition, i as i64)
            .await?;
        
        match &read_result {
            Some(data) => {
                println!("\nMessage {} read successfully:", i + 1);
                println!("Expected: {:?}", String::from_utf8_lossy(&message.payload));
                println!("Actual: {:?}", String::from_utf8_lossy(data));
                assert_eq!(data, &message.payload);
            }
            None => println!("Failed to read message {}", i + 1),
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_fetch_empty_topic() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 빈 토픽에서 읽기
    let read_result = store.read_messages("empty-topic", 0, 0).await?;
    assert!(read_result.is_none());

    Ok(())
}

#[tokio::test]
async fn test_fetch_unknown_topic() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 존재하지 않는 토픽에서 읽기
    let read_result = store.read_messages("unknown-topic", 0, 0).await?;
    assert!(read_result.is_none());

    Ok(())
}

// 파티션 리스팅 테스트
#[tokio::test]
async fn test_list_partitions_multiple_topics() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 여러 토픽에 메시지 저장
    let topics = vec!["topic1", "topic2", "topic3"];
    for topic in topics.clone() {
        let message = KafkaMessage {
            correlation_id: 1,
            topic: topic.to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: b"test".to_vec(),
        };
        store.store_message(message).await?;
    }

    // 각 토픽 디렉토리 확인
    for topic in topics {
        let topic_dir = temp_dir.path().join(format!("{}-0", topic));
        assert!(topic_dir.exists());
    }

    Ok(())
}

#[tokio::test]
async fn test_list_partitions_multiple_partitions() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 여러 파티션에 메시지 저장
    let partitions = vec![0, 1];
    for partition in partitions.clone() {
        let message = KafkaMessage {
            correlation_id: 1,
            topic: "multi-partition-topic".to_string(),
            partition,
            offset: 0,
            timestamp: 0,
            payload: b"test".to_vec(),
        };
        store.store_message(message).await?;
    }

    // 각 파티션 디렉토리 확인
    for partition in partitions {
        let partition_dir = temp_dir
            .path()
            .join(format!("multi-partition-topic-{}", partition));
        assert!(partition_dir.exists());
    }

    Ok(())
}

// 동시성 테스트
#[tokio::test]
async fn test_concurrent_message_store() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = Arc::new(DiskMessageStore::new(
        temp_dir.path().to_path_buf(),
        config,
    ));

    let mut handles = vec![];

    // 동시에 여러 메시지 저장
    for i in 0..5 {
        let store_clone = store.clone();
        let handle = tokio::spawn(async move {
            let message = KafkaMessage {
                correlation_id: 1,
                topic: format!("concurrent-topic-{}", i),
                partition: 0,
                offset: 0,
                timestamp: 0,
                payload: format!("Message {}", i).into_bytes(),
            };
            store_clone.store_message(message).await
        });
        handles.push(handle);
    }

    // 모든 작업 완료 대기
    for handle in handles {
        handle.await.expect("Task failed")?;
    }

    // 각 토픽 디렉토리 확인
    for i in 0..5 {
        let topic_dir = temp_dir.path().join(format!("concurrent-topic-{}-0", i));
        assert!(topic_dir.exists());
    }

    Ok(())
}

// 기본 연결 테스트
#[tokio::test]
async fn test_disk_store_initialization() -> Result<()> {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024,
        max_buffer_size: 5,
        flush_interval: Duration::from_millis(100),
    };

    let store = DiskMessageStore::new(temp_dir.path().to_path_buf(), config);

    // 기본 메시지 저장
    let message = KafkaMessage {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: b"test".to_vec(),
    };

    let offset = store.store_message(message).await?;
    assert_eq!(offset, 0);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_heavy_load() {
    print_test_header("Concurrent Heavy Load Test");
    let start_time = std::time::Instant::now();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = StoreConfig {
        max_segment_size: 1024 * 1024,
        max_buffer_size: 1,
        flush_interval: Duration::from_millis(10),
    };

    let store = Arc::new(DiskMessageStore::new(temp_dir.path().to_path_buf(), config));
    let mut total_successful = 0;
    let client_count = 5;
    let messages_per_client = 10;
    let total_messages = client_count * messages_per_client;

    println!("\nStarting concurrent test with {} clients, {} dto each", client_count, messages_per_client);

    for i in 0..client_count {
        let topic = format!("concurrent-topic-{}", i);
        
        for j in 0..messages_per_client {
            let message = KafkaMessage {
                correlation_id: 0,
                topic: topic.clone(),
                partition: 0,
                offset: j as u64,
                timestamp: 0,
                payload: format!("Message {}-{}", i, j).into_bytes(),
            };
            
            let _offset = store.store_message(message).await.expect("Failed to store message");
            let result = store.read_messages(&topic, 0, j as i64).await.expect("Failed to read message");
            
            if result.is_some() {
                total_successful += 1;
            }

            // 10개 메시지마다 진행 상황 출력
            if total_successful % 10 == 0 {
                println!("Progress: {}/{} dto processed successfully", total_successful, total_messages);
            }
        }
    }

    let duration = start_time.elapsed();
    print_test_summary("Concurrent Heavy Load Test", total_messages, total_successful, duration);
}

#[tokio::test]
async fn test_protocol_compliance() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let store = Arc::new(DiskMessageStore::new(
        temp_dir.path().to_path_buf(),
        StoreConfig::default(),
    ));

    // 1. 메시지 크기 제한 테스트
    let large_payload = vec![0u8; 10 * 1024 * 1024]; // 10MB
    let large_message = KafkaMessage {
        correlation_id: 0,
        topic: "test-topic-large".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: large_payload.clone(),
    };
    
    let offset = store.store_message(large_message).await.expect("Failed to store large message");
    
    // 저장된 큰 메시지 검증
    let read_result = store
        .read_messages("test-topic-large", 0, offset as i64)
        .await
        .expect("Failed to read large message");
    assert!(read_result.is_some());
    assert_eq!(read_result.unwrap(), large_payload);

    // 2. 특수 문자가 포함된 토픽 이름 테스트
    let special_chars = vec!["test/topic", "test.topic", "test-topic", "test_topic"];
    for (i, topic) in special_chars.iter().enumerate() {
        let message = KafkaMessage {
            correlation_id: 0,
            topic: topic.to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            payload: format!("test message {}", i).into_bytes(),
        };
        let msg_offset = store.store_message(message.clone())
            .await
            .expect(&format!("Failed to store message for topic {}", topic));
        
        // 저장된 메시지 검증
        let read_result = store.read_messages(topic, 0, msg_offset as i64)
            .await
            .expect(&format!("Failed to read message from topic {}", topic));
        assert!(read_result.is_some());
        assert_eq!(
            String::from_utf8(read_result.unwrap()).unwrap(),
            format!("test message {}", i)
        );
    }

    // 3. 오프셋 순서 테스트
    let mut last_offset = 0u64;
    for i in 0..10 {
        let message = KafkaMessage {
            correlation_id: 0,
            topic: "test-topic-offset".to_string(),
            partition: 0,
            offset: i,
            timestamp: 0,
            payload: format!("offset test {}", i).into_bytes(),
        };
        let new_offset = store.store_message(message)
            .await
            .expect("Failed to store message for offset test");
        assert!(
            new_offset >= last_offset,
            "Offset should be monotonically increasing"
        );
        last_offset = new_offset;

        // 저장된 메시지 검증
        let read_result = store.read_messages("test-topic-offset", 0, i as i64)
            .await
            .expect("Failed to read message for offset test");
        assert!(read_result.is_some());
        assert_eq!(
            String::from_utf8(read_result.unwrap()).unwrap(),
            format!("offset test {}", i)
        );
    }

    // 4. 빈 페이로드 테스트
    let empty_message = KafkaMessage {
        correlation_id: 0,
        topic: "test-topic-empty".to_string(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        payload: vec![],
    };
    let empty_offset = store.store_message(empty_message)
        .await
        .expect("Failed to store empty message");
    
    // 빈 메시지 검증
    let read_result = store.read_messages("test-topic-empty", 0, empty_offset as i64)
        .await
        .expect("Failed to read empty message");
    assert!(read_result.is_some());
    assert_eq!(read_result.unwrap(), vec![]);
}

