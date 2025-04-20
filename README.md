## 개요
- 해당 프로젝트는 [codecrafters.io](https://codecrafters.io) 사이트의 kafka 클론 코딩 프로그램을 기반으로 작성되었습니다.
- Codecrafters는 프로토콜을 기반으로 특정 프로그램을 구현할 수 있도록 단계를 나누어주고 코드베이스의 단계별 e2e 통합 테스트를 제공합니다. 
- 해당 커리큘럼이 아직 완성되지 않아, 공식 문서를 기반으로 요구사항을 추가적으로 작성했습니다. 실제 메세지를 영속화하는 부분부터는 사이트와는 무관한 임의로 추가한 요구사항이며, 통합테스트 코드로 보완하려고 노력했습니다.
- 관련한 고민은 개인 [위키](https://smallzoodev.netlify.app/_wiki/%EC%B9%B4%ED%94%84%EC%B9%B4%EB%A5%BC-%ED%97%A5%EC%82%AC%EA%B3%A0%EB%82%A0%ED%95%98%EA%B2%8C-%ED%81%B4%EB%A1%A0%EC%BD%94%EB%94%A9-%ED%95%B4%EB%B3%B4%EA%B8%B0/)에 정리되어 있습니다.

## 구현 여부
### 구현
- 어떠한 API를 지원하는지 Handshake, 통신 구현
- Kraft Log 기반의 설정, 메타데이터 구분 구현
- 토픽, 파티션 기반 producing, consuming 로직 구현
- Offset 기반으로 로그파일, 인덱스 파일로 나누어 영속화 구현 (인덱스 파일은 base-offset으로 저장, 파일 내에 메시지의 상대 offset 저장)
- 프로그램 내부에 일부 캐시 로직 구현 (페이지 캐시 아님)

### 미구현
- 메모리 캐시 교체 정책 미구현 (페이지 캐시 활용하지 않음)
- 키 - 토픽,파티션 매칭 미구현 (직접 토픽,파티션 지정하여 호출)
- 컨슈머 그룹과 리밸런싱 미구현 (직접 offset지정하여 호출은 가능)
- 레플리케이션 미구현
  
## 특이사항
- Hexagonal Architecture로 구현되어있는데, 이부분은 개인적인 공부를 위한 부분이었습니다.
- 주요 Adapter는 `Incoming : tcp`, `Outgoing : diskStore, kraftMetadataStore`등이 있습니다.
- 입출력 버퍼를 파싱하는 Protocol은 Adpater 계층에 작성되어 있습니다.

