mod adapters;
mod application;
mod config;
mod domain;
mod ports;

use crate::application::Result;
use crate::config::app_config::AppConfig;
use crate::adapters::incoming::tcp_adapter::TcpAdapter;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9092";
    let config = AppConfig::new("server.properties");

    let adapter = TcpAdapter::new(
        addr,
        config.broker,
        config.protocol_parser,
    ).await?;

    adapter.run().await?;

    Ok(())
}
