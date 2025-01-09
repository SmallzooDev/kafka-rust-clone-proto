# Kafka Protocol 구현

이 디렉토리는 Kafka 프로토콜의 인코딩/디코딩 구현을 포함합니다.

## 프로토콜 구조

디코딩된 응답 예시:
```rust
// 1. 클라이언트가 받는 바이트 스트림
[
    00 00             // error_code = 0 (성공)
    02                // api_versions 배열 길이 = 1 (실제 길이 + 1)
    00 12 00 00 00 04 // API_VERSIONS API (key=18, min=0, max=4)
    00 00 00 00       // throttle_time = 0ms
    01 00             // tagged_fields (길이=1, 필드 수=0)
]

// 2. 디코딩 후 Rust 구조체
ApiVersionsResponse {
    error_code: 0,
    api_versions: [
        ApiVersion {
            api_key: 18,      // API_VERSIONS
            min_version: 0,
            max_version: 4,
        }
    ]
}

// 3. 만약 여러 API를 지원한다면:
[
    00 00             // error_code = 0
    04                // api_versions 배열 길이 = 3 (실제 길이 + 1)
    00 00 00 00 00 07 // PRODUCE API (key=0, min=0, max=7)
    00 01 00 00 00 08 // FETCH API (key=1, min=0, max=8)
    00 12 00 00 00 04 // API_VERSIONS API (key=18, min=0, max=4)
    00 00 00 00       // throttle_time = 0ms
    01 00             // tagged_fields (길이=1, 필드 수=0)
]
```

### 1. UNSIGNED_VARINT 인코딩
가변 길이 정수를 인코딩하는 방식입니다. 각 바이트의 최상위 비트(MSB)는 다음 바이트의 존재 여부를 나타냅니다.

```
300을 이진수로 변환: 100101100

1단계: 첫 7비트 추출
100101100 → 0101100 (첫 7비트)
남은 비트: 1

2단계: 첫 번째 바이트 생성
- 더 비트가 남았으므로 MSB=1
- [10101100] (0xAC)

3단계: 남은 비트로 두 번째 바이트 생성
- 마지막이므로 MSB=0
- [00000010] (0x02)

결과: [0xAC, 0x02]
```


### 2. ApiVersions 응답 구조
Kafka 프로토콜의 ApiVersions 응답 형식을 구현합니다.

```rust
// 응답 구조체
struct ApiVersionsResponse {
    error_code: i16,
    api_versions: Vec<ApiVersion>,
}

struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

// 인코딩 예시 (utils.rs의 encode_api_versions_response 함수)
let response = [
    // error_code (2바이트)
    0x00, 0x00,                 // error_code = 0
    
    // api_versions array length (UNSIGNED_VARINT) : 지원하는 api 마다 양식에 맞게 써줘야함 여기는 몇개인지 알려주는 것
    0x02,                       // length = 1 (실제 길이 + 1)
    
    // ApiVersion :  항목 항상 아래의 순서 (api_key, min_version, max_version) 로 인코딩해야함
    0x00, 0x12,                // api_key = 18 (API_VERSIONS_KEY)
    0x00, 0x00,                // min_version = 0
    0x00, 0x04,                // max_version = 4
    
    // throttle_time_ms (4바이트)
    0x00, 0x00, 0x00, 0x00,    // throttle_time = 0
    
    // tagged fields section
    0x01,                      // section_length = 1
    0x00                       // field_count = 0
];
```

### 3. Tagged Fields
선택적 필드를 위한 구조입니다. 각 필드는 태그 번호와 길이를 포함합니다.

```rust
// Tagged Fields 구조
struct TaggedField {
    tag: u32,      // UNSIGNED_VARINT로 인코딩
    length: u32,   // UNSIGNED_VARINT로 인코딩
    data: Vec<u8>, // 실제 데이터
}

// 인코딩 형식
[
    field_count: UNSIGNED_VARINT,
    // 각 필드에 대해
    for each field {
        tag: UNSIGNED_VARINT,
        length: UNSIGNED_VARINT,
        data: BYTES[length]
    }
]
```


## 구현된 기능

1. **UNSIGNED_VARINT 인코딩/디코딩**
   - `encode_unsigned_varint`: 부호 없는 정수를 가변 길이로 인코딩

2. **ApiVersions 응답 인코딩**
   - `encode_api_versions_response`: ApiVersions 응답을 프로토콜 형식으로 인코딩

## 참고 사항

- 모든 다중 바이트 정수는 big-endian 형식으로 인코딩됩니다.
- Tagged Fields 섹션은 모든 메시지의 마지막에 위치합니다.
- 필드가 없는 Tagged Fields 섹션은 단일 0 바이트로 인코딩됩니다.

## 관련 문서

- [KIP-482: The Kafka Protocol should Support Optional Tagged Fields](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields) 