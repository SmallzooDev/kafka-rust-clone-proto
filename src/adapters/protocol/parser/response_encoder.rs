use bytes::{BufMut, BytesMut};
use crate::adapters::protocol::dto::KafkaResponse;
use crate::adapters::protocol::dto::ResponsePayload;
use super::varint::PutVarint;

#[derive(Clone)]
pub struct ResponseEncoder;

impl ResponseEncoder {
    pub fn new() -> Self {
        Self
    }

    pub fn encode(&self, response: KafkaResponse) -> Vec<u8> {
        let mut buf = BytesMut::new();

        // correlation_id
        buf.put_i32(response.correlation_id);

        match &response.payload {
            ResponsePayload::ApiVersions(api_versions) => {
                buf.put_i16(response.error_code);

                // array of api keys
                buf.put_i8((api_versions.api_versions.len() + 1) as i8);

                // Write each API version
                for version in &api_versions.api_versions {
                    buf.put_i16(version.api_key);
                    buf.put_i16(version.min_version);
                    buf.put_i16(version.max_version);
                    buf.put_i8(0); // TAG_BUFFER
                }

                // throttle time ms
                buf.put_i32(0);
                buf.put_i8(0); // TAG_BUFFER
            }
            ResponsePayload::DescribeTopicPartitions(describe_response) => {
                println!(
                    "[RESPONSE] Encoding DescribeTopicPartitions response: {:?}",
                    describe_response
                );

                // throttle time ms
                buf.put_i32(0);
                buf.put_i8(0); // TAG_BUFFER

                // topics array length (COMPACT_ARRAY)
                buf.put_i8((describe_response.topics.len() + 1) as i8);

                // Write each topic
                for topic in &describe_response.topics {
                    // topic error code
                    buf.put_i16(topic.error_code);

                    // topic name (COMPACT_STRING)
                    let topic_name_bytes = topic.topic_name.as_bytes();
                    buf.put_i8((topic_name_bytes.len() + 1) as i8);
                    buf.put_slice(topic_name_bytes);

                    // topic id (UUID)
                    buf.put_slice(&topic.topic_id);

                    // is_internal
                    buf.put_i8(topic.is_internal as i8);

                    // partitions array (COMPACT_ARRAY)
                    buf.put_i8((topic.partitions.len() + 1) as i8);
                    println!("[RESPONSE] Encoding {} partitions", topic.partitions.len());

                    // Write each partition
                    for partition in &topic.partitions {
                        println!("[RESPONSE] Encoding partition: {:?}", partition);
                        buf.put_i16(partition.error_code); // error code 먼저
                        buf.put_i32(partition.partition_id); // 그 다음 partition id
                        buf.put_i32(1); // leader id (1로 고정)
                        buf.put_i32(0); // leader epoch

                        // replica nodes array
                        buf.put_i8(2); // array length + 1 (1개의 replica이므로 2)
                        buf.put_i32(1); // replica node id (1로 고정)

                        // isr nodes array
                        buf.put_i8(2); // array length + 1 (1개의 isr이므로 2)
                        buf.put_i32(1); // isr node id (1로 고정)

                        // eligible leader replicas array
                        buf.put_i8(1); // array length + 1 (0개이므로 1)

                        // last known eligible leader replicas array
                        buf.put_i8(1); // array length + 1 (0개이므로 1)

                        // offline replicas array
                        buf.put_i8(1); // array length + 1 (0개이므로 1)

                        buf.put_i8(0); // TAG_BUFFER for partition
                    }

                    // topic authorized operations
                    buf.put_u32(0x00000df8);

                    buf.put_i8(0); // TAG_BUFFER for topic
                }

                // next_cursor (nullable)
                buf.put_u8(0xff); // null

                buf.put_i8(0); // TAG_BUFFER for entire response
            }
            ResponsePayload::Fetch(fetch_response) => {
                // TAG_BUFFER after header
                buf.put_i8(0);

                // Body
                buf.put_i32(fetch_response.throttle_time_ms);
                buf.put_i16(response.error_code);
                buf.put_i32(fetch_response.session_id);

                // responses array length (COMPACT_ARRAY)
                let mut varint_buf = Vec::new();
                varint_buf.put_uvarint((fetch_response.responses.len() as i64) + 1);
                buf.put_slice(&varint_buf);

                // Write each topic response
                for topic in &fetch_response.responses {
                    // topic_id
                    buf.put_slice(&topic.topic_id);

                    // partitions array length (COMPACT_ARRAY)
                    let mut varint_buf = Vec::new();
                    varint_buf.put_uvarint((topic.partitions.len() as i64) + 1);
                    buf.put_slice(&varint_buf);

                    // Write each partition
                    for partition in &topic.partitions {
                        // partition_index
                        buf.put_i32(partition.partition_index);

                        // error_code
                        buf.put_i16(partition.error_code);

                        // high_watermark
                        buf.put_i64(partition.high_watermark);

                        // last_stable_offset
                        buf.put_i64(0);

                        // log_start_offset
                        buf.put_i64(0);

                        // aborted_transactions array (COMPACT_ARRAY)
                        let mut varint_buf = Vec::new();
                        varint_buf.put_uvarint(1);
                        buf.put_slice(&varint_buf);

                        // preferred_read_replica
                        buf.put_i32(-1);

                        // records (COMPACT_RECORDS)
                        if let Some(records) = &partition.records {
                            let mut varint_buf = Vec::new();
                            varint_buf.put_uvarint(2);
                            buf.put_slice(&varint_buf);
                            buf.put_slice(records);
                        } else {
                            let mut varint_buf = Vec::new();
                            varint_buf.put_uvarint(1);
                            buf.put_slice(&varint_buf);
                        }

                        // TAG_BUFFER for partition
                        buf.put_i8(0);
                    }

                    // TAG_BUFFER for topic
                    buf.put_i8(0);
                }

                // TAG_BUFFER for entire response
                buf.put_i8(0);
            }
            ResponsePayload::Produce(produce_response) => {
                // array length
                buf.put_i8((produce_response.responses.len() + 1) as i8);

                // responses
                for topic_response in &produce_response.responses {
                    // topic name
                    let name_bytes = topic_response.name.as_bytes();
                    buf.put_i8((name_bytes.len() + 1) as i8);
                    buf.put_slice(name_bytes);

                    // partition responses
                    buf.put_i8((topic_response.partition_responses.len() + 1) as i8);
                    for partition_response in &topic_response.partition_responses {
                        buf.put_i32(partition_response.partition);
                        buf.put_i16(partition_response.error_code);
                        buf.put_i64(partition_response.base_offset);
                        buf.put_i64(partition_response.log_append_time);
                    }

                    buf.put_u8(0); // TAG_BUFFER
                }

                buf.put_i32(produce_response.throttle_time_ms);
                buf.put_u8(0); // TAG_BUFFER
            }
        }

        let total_size = buf.len() as i32;
        let mut final_buf = BytesMut::new();
        final_buf.put_i32(total_size);
        final_buf.put_slice(&buf);

        let result = final_buf.to_vec();
        println!("[RESPONSE] Raw bytes: {:02x?}", result);
        result
    }
} 