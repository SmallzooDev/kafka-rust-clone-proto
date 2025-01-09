use crate::ports::incoming::message_handler::MessageHandler;
use crate::adapters::incoming::kafka_protocol_parser::KafkaProtocolParser;
use crate::application::ApplicationError;
use crate::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct TcpAdapter {
    listener: TcpListener,
    message_handler: Arc<dyn MessageHandler>,
    protocol_parser: KafkaProtocolParser,
}

impl TcpAdapter {
    pub async fn new(
        addr: &str, 
        message_handler: Arc<dyn MessageHandler>,
        protocol_parser: KafkaProtocolParser,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(ApplicationError::Io)?;
        Ok(Self { 
            listener,
            message_handler,
            protocol_parser,
        })
    }

    pub async fn run(&self) -> Result<()> {
        println!("Server listening on port 9092");
        
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    let message_handler = Arc::clone(&self.message_handler);
                    let protocol_parser = self.protocol_parser.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, message_handler, protocol_parser).await {
                            println!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => println!("Accept error: {}", e),
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    message_handler: Arc<dyn MessageHandler>,
    protocol_parser: KafkaProtocolParser,
) -> Result<()> {
    println!("Accepted new connection");
    
    loop {
        // 1. 요청 크기 읽기
        let mut size_bytes = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut size_bytes).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                println!("Client closed connection");
                return Ok(());
            }
            return Err(ApplicationError::Io(e));
        }
        let message_size = i32::from_be_bytes(size_bytes);
        
        // 2. 요청 데이터 읽기
        let mut request_data = vec![0; message_size as usize];
        stream.read_exact(&mut request_data).await.map_err(ApplicationError::Io)?;
        
        // 3. 프로토콜 파싱
        let request = protocol_parser.parse_request(&request_data)?;
        
        // 4. 비즈니스 로직 처리
        let response = message_handler.handle_request(request).await?;
        
        // 5. 응답 인코딩 및 전송
        let encoded = protocol_parser.encode_response(response);
        stream.write_all(&encoded).await.map_err(ApplicationError::Io)?;
    }
} 