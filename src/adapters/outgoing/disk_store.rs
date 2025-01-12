use crate::domain::message::KafkaMessage;
use crate::config::app_config::StoreConfig;
use crate::domain::message::TopicPartition;
use crate::ports::outgoing::message_store::MessageStore;
use crate::Result;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};

// 세그먼트 파일 관리를 위한 내부 구조체
struct SegmentFiles {
    log_file: File,
    index_file: File,
}

// 메모리 버퍼 관리를 위한 내부 구조체
#[allow(dead_code)]
struct MessageBuffer {
    messages: Vec<(u64, Bytes)>,
    last_flushed_offset: u64,
}

impl MessageBuffer {
    fn new(base_offset: u64) -> Self {
        Self {
            messages: Vec::new(),
            last_flushed_offset: base_offset,
        }
    }
}

// 세그먼트 관리를 위한 내부 구조체
struct Segment {
    base_offset: u64,
    files: SegmentFiles,
    buffer: MessageBuffer,
    next_offset: u64, // 다음에 할당할 오프셋
}

impl Segment {
    fn new(base_offset: u64, files: SegmentFiles) -> Self {
        Self {
            base_offset,
            files,
            buffer: MessageBuffer::new(base_offset),
            next_offset: base_offset, // 초기값은 base_offset
        }
    }

    fn allocate_offset(&mut self) -> u64 {
        let offset = self.next_offset;
        self.next_offset += 1;
        offset
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // 파일 핸들러는 자동으로 닫힌다
    }
}

// 세그먼트 캐시 관리를 위한 내부 구조체
struct SegmentCache {
    segments: HashMap<u64, Arc<RwLock<Segment>>>,
}

// 플러시 작업을 위한 메시지 타입
#[allow(dead_code)]
enum FlushMessage {
    Flush(TopicPartition, u64), // (topic_partition, base_offset)
    Shutdown,
}

pub struct DiskMessageStore {
    log_dir: PathBuf,
    segments: Arc<RwLock<HashMap<TopicPartition, SegmentCache>>>,
    config: StoreConfig,
    flush_sender: mpsc::Sender<FlushMessage>,
    is_running: Arc<AtomicBool>,
}

impl DiskMessageStore {
    pub fn new(log_dir: PathBuf, config: StoreConfig) -> Self {
        let (flush_sender, flush_receiver) = mpsc::channel(100);
        let is_running = Arc::new(AtomicBool::new(true));
        let segments = Arc::new(RwLock::new(HashMap::new()));

        // 플러시 작업을 처리할 백그라운드 태스크 시작
        let store = Self {
            log_dir,
            segments: segments.clone(),
            config: config.clone(),
            flush_sender,
            is_running: is_running.clone(),
        };

        store.start_flush_task(flush_receiver, config.flush_interval);

        store
    }

    fn start_flush_task(
        &self,
        mut flush_receiver: mpsc::Receiver<FlushMessage>,
        interval_duration: Duration,
    ) {
        let segments = self.segments.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = interval(interval_duration);

            while is_running.load(Ordering::SeqCst) {
                tokio::select! {
                    _ = interval.tick() => {
                        // 주기적인 플러시
                        let segments_guard = segments.read().await;
                        for (_topic_partition, cache) in segments_guard.iter() {
                            for (_base_offset, segment) in cache.segments.iter() {
                                let mut segment = segment.write().await;
                                if let Err(e) = DiskMessageStore::flush_segment(&mut segment).await {
                                    eprintln!("Error flushing segment: {:?}", e);
                                }
                            }
                        }
                    }
                    Some(msg) = flush_receiver.recv() => {
                        match msg {
                            FlushMessage::Flush(topic_partition, base_offset) => {
                                let segments_guard = segments.read().await;
                                if let Some(cache) = segments_guard.get(&topic_partition) {
                                    if let Some(segment) = cache.segments.get(&base_offset) {
                                        let mut segment = segment.write().await;
                                        if let Err(e) = DiskMessageStore::flush_segment(&mut segment).await {
                                            eprintln!("Error flushing segment: {:?}", e);
                                        }
                                    }
                                }
                            }
                            FlushMessage::Shutdown => break,
                        }
                    }
                }
            }
        });
    }

    fn get_segment_path(&self, topic_partition: &TopicPartition, base_offset: u64) -> PathBuf {
        self.log_dir
            .join(format!(
                "{}-{}",
                topic_partition.topic, topic_partition.partition
            ))
            .join(format!("{:020}.log", base_offset))
    }

    fn get_index_path(&self, topic_partition: &TopicPartition, base_offset: u64) -> PathBuf {
        self.log_dir
            .join(format!(
                "{}-{}",
                topic_partition.topic, topic_partition.partition
            ))
            .join(format!("{:020}.index", base_offset))
    }

    async fn ensure_directory_exists(&self, path: &PathBuf) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    async fn should_roll_segment(&self, segment: &Segment) -> Result<bool> {
        let current_size = segment.files.log_file.metadata().await?.len();
        Ok(current_size >= self.config.max_segment_size)
    }

    async fn roll_segment(
        &self,
        topic_partition: &TopicPartition,
        current_offset: u64,
    ) -> Result<Arc<RwLock<Segment>>> {
        let new_base_offset = current_offset + 1;
        let log_path = self.get_segment_path(topic_partition, new_base_offset);
        let index_path = self.get_index_path(topic_partition, new_base_offset);

        self.ensure_directory_exists(&log_path).await?;

        let log_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)
            .await?;

        let index_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&index_path)
            .await?;

        let new_segment = Arc::new(RwLock::new(Segment {
            base_offset: new_base_offset,
            files: SegmentFiles {
                log_file,
                index_file,
            },
            buffer: MessageBuffer::new(new_base_offset),
            next_offset: new_base_offset,
        }));

        // 새 세그먼트를 캐시에 추가
        let mut segments = self.segments.write().await;
        segments
            .entry(topic_partition.clone())
            .or_insert_with(|| SegmentCache {
                segments: HashMap::new(),
            })
            .segments
            .insert(new_base_offset, new_segment.clone());

        Ok(new_segment)
    }

    async fn get_or_create_segment(
        &self,
        topic_partition: &TopicPartition,
        offset: u64,
    ) -> Result<Arc<RwLock<Segment>>> {
        let base_offset = (offset / 1000) * 1000;
        let mut segments = self.segments.write().await;

        if let Some(cache) = segments.get(topic_partition) {
            if let Some(segment) = cache.segments.get(&base_offset) {
                // 세그먼트가 존재하면 크기 체크
                let segment_guard = segment.read().await;
                if self.should_roll_segment(&segment_guard).await? {
                    drop(segment_guard); // 락 해제
                    drop(segments); // 글로벌 락 해제
                    return self.roll_segment(topic_partition, offset).await;
                }
                return Ok(segment.clone());
            }
        }

        // 새 세그먼트 생성
        let log_path = self.get_segment_path(topic_partition, base_offset);
        let index_path = self.get_index_path(topic_partition, base_offset);

        self.ensure_directory_exists(&log_path).await?;

        let log_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)
            .await?;

        let index_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&index_path)
            .await?;

        let segment = Arc::new(RwLock::new(Segment::new(
            base_offset,
            SegmentFiles {
                log_file,
                index_file,
            },
        )));

        segments
            .entry(topic_partition.clone())
            .or_insert_with(|| SegmentCache {
                segments: HashMap::new(),
            })
            .segments
            .insert(base_offset, segment.clone());

        Ok(segment)
    }

    async fn write_message(file: &mut File, message: &KafkaMessage) -> Result<u64> {
        let position = file.metadata().await?.len();

        let mut buffer = BytesMut::new();

        // 오프셋 (8바이트)
        buffer.put_i64(message.offset as i64);

        // 메시지 크기를 위한 공간 예약 (4바이트)
        buffer.put_u32(0);

        // CRC32 예약 (4바이트)
        buffer.put_u32(0);

        // 매직 넘버 (1바이트)
        buffer.put_u8(1);

        // 속성 (1바이트)
        buffer.put_u8(0);

        // 타임스탬프 (8바이트)
        buffer.put_u64(message.timestamp);

        // 키 (현재는 빈 키)
        buffer.put_i32(0);

        // 값
        buffer.put_i32(message.payload.len() as i32);
        buffer.put_slice(&message.payload);

        // 메시지 크기 업데이트
        let message_size = buffer.len() - 12;
        buffer[8..12].copy_from_slice(&(message_size as u32).to_be_bytes());

        file.write_all(&buffer).await?;

        Ok(position)
    }

    async fn flush_segment(segment: &mut Segment) -> Result<()> {
        if segment.buffer.messages.is_empty() {
            return Ok(());
        }

        // 버퍼의 메시지들을 디스크에 쓰기
        for (offset, message_bytes) in segment.buffer.messages.drain(..) {
            let message = KafkaMessage {
                correlation_id: 0,
                topic: String::new(),
                partition: 0,
                offset,
                timestamp: 0,
                payload: message_bytes.to_vec(),
            };

            let position = Self::write_message(&mut segment.files.log_file, &message).await?;

            // 인덱스 엔트리 쓰기
            let relative_offset = offset - segment.base_offset;
            let index_entry = [
                (relative_offset as u32).to_be_bytes(),
                (position as u32).to_be_bytes(),
            ]
            .concat();
            segment.files.index_file.write_all(&index_entry).await?;
        }

        // 디스크 동기화
        segment.files.log_file.sync_data().await?;
        segment.files.index_file.sync_data().await?;

        Ok(())
    }

    #[allow(dead_code)]
    async fn cleanup(&self) -> Result<()> {
        self.is_running.store(false, Ordering::SeqCst);
        let _ = self.flush_sender.send(FlushMessage::Shutdown).await;

        // 마지막 플러시 수행
        let segments_guard = self.segments.read().await;
        for (_, cache) in segments_guard.iter() {
            for (_, segment) in cache.segments.iter() {
                let mut segment = segment.write().await;
                if let Err(e) = Self::flush_segment(&mut segment).await {
                    eprintln!("Error during final flush: {:?}", e);
                }
            }
        }

        Ok(())
    }
}

impl Drop for DiskMessageStore {
    fn drop(&mut self) {
        // 모든 세그먼트를 정리
        if let Ok(_) = tokio::runtime::Handle::try_current() {
            eprintln!("DiskMessageStore dropped");
        }
    }
}

#[async_trait]
impl MessageStore for DiskMessageStore {
    async fn store_message(&self, mut message: KafkaMessage) -> Result<u64> {
        let segment = self
            .get_or_create_segment(
                &TopicPartition {
                    topic: message.topic.clone(),
                    partition: message.partition,
                },
                message.offset,
            )
            .await?;
        let mut segment = segment.write().await;

        // 새로운 오프셋 할당
        message.offset = segment.allocate_offset();

        // 메모리 버퍼에 추가
        segment
            .buffer
            .messages
            .push((message.offset, Bytes::copy_from_slice(&message.payload)));

        // 버퍼 크기가 임계값을 넘으면 플러시
        if segment.buffer.messages.len() >= self.config.max_buffer_size {
            DiskMessageStore::flush_segment(&mut segment).await?;
        }

        Ok(message.offset)
    }

    async fn read_messages(
        &self,
        topic_id: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Option<Vec<u8>>> {
        let segment = self
            .get_or_create_segment(
                &TopicPartition {
                    topic: topic_id.to_string(),
                    partition,
                },
                offset as u64,
            )
            .await?;
        let segment = segment.read().await;

        // 먼저 메모리 버퍼에서 찾기
        for (msg_offset, message_bytes) in &segment.buffer.messages {
            if *msg_offset == offset as u64 {
                return Ok(Some(message_bytes.to_vec()));
            }
        }

        // 디스크에서 찾기
        let base_offset = (offset as u64 / 1000) * 1000;
        let relative_offset = offset as u64 - base_offset;

        // 인덱스 파일을 처음부터 읽기
        let mut index_file = segment.files.index_file.try_clone().await?;
        index_file.seek(SeekFrom::Start(0)).await?;

        // 인덱스에서 위치 찾기
        loop {
            let mut entry = [0u8; 8];
            match index_file.read_exact(&mut entry).await {
                Ok(_) => {
                    let idx_offset = u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]);
                    let pos = u32::from_be_bytes([entry[4], entry[5], entry[6], entry[7]]);

                    if idx_offset as u64 >= relative_offset {
                        // 찾은 위치에서 메시지 읽기
                        let mut log_file = segment.files.log_file.try_clone().await?;
                        log_file.seek(SeekFrom::Start(pos as u64)).await?;

                        // 오프셋 읽기 (8바이트)
                        let mut offset_buf = [0u8; 8];
                        log_file.read_exact(&mut offset_buf).await?;

                        // 메시지 크기 읽기 (4바이트)
                        let mut size_buf = [0u8; 4];
                        log_file.read_exact(&mut size_buf).await?;
                        let _message_size = u32::from_be_bytes(size_buf);

                        // CRC32 건너뛰기 (4바이트)
                        log_file.seek(SeekFrom::Current(4)).await?;

                        // 매직 넘버 (1바이트)와 속성 (1바이트) 건너뛰기
                        log_file.seek(SeekFrom::Current(2)).await?;

                        // 타임스탬프 건너뛰기 (8바이트)
                        log_file.seek(SeekFrom::Current(8)).await?;

                        // 키 길이 읽기 (4바이트)
                        let mut key_size_buf = [0u8; 4];
                        log_file.read_exact(&mut key_size_buf).await?;
                        let key_size = i32::from_be_bytes(key_size_buf);
                        
                        // 키가 있다면 건너뛰기
                        if key_size > 0 {
                            log_file.seek(SeekFrom::Current(key_size as i64)).await?;
                        }

                        // 값 길이 읽기 (4바이트)
                        let mut value_size_buf = [0u8; 4];
                        log_file.read_exact(&mut value_size_buf).await?;
                        let value_size = i32::from_be_bytes(value_size_buf);

                        // 값 읽기
                        let mut value_buf = vec![0u8; value_size as usize];
                        log_file.read_exact(&mut value_buf).await?;

                        return Ok(Some(value_buf));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

