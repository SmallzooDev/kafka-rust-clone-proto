/// 지원하지 않는 API 버전에 대한 에러 코드
/// Kafka 프로토콜에서 정의된 표준 에러 코드임
pub const UNSUPPORTED_VERSION: i16 = 35;

/// 현재 브로커가 지원하는 최대 API 버전
/// ApiVersions 요청에 대한 응답에서 사용됨
pub const MAX_SUPPORTED_VERSION: i16 = 4;

/// ApiVersions API의 키 값
/// Kafka 프로토콜에서 정의된 표준 API 키임
/// 클라이언트가 브로커가 지원하는 API 버전을 조회할 때 사용함
pub const API_VERSIONS_KEY: i16 = 18;
pub const FETCH_KEY: i16 = 1;
pub const PRODUCE_KEY: i16 = 0;

/// Produce API 버전 범위
pub const PRODUCE_MIN_VERSION: i16 = 0;
pub const PRODUCE_MAX_VERSION: i16 = 9;

/// DescribeTopicPartitions API의 키 값
/// Kafka 프로토콜에서 정의된 표준 API 키임
/// 클라이언트가 브로커가 지원하는 API 버전을 조회할 때 사용함
pub const DESCRIBE_TOPIC_PARTITIONS_KEY: i16 = 75;

/// DescribeTopicPartitions API는 버전 0만 지원
pub const DESCRIBE_TOPIC_PARTITIONS_MAX_VERSION: i16 = 0;
pub const DESCRIBE_TOPIC_PARTITIONS_MIN_VERSION: i16 = 0;

/// Error codes
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3; 