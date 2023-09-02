use std::error::Error;
use async_trait::async_trait;
use tokio::net::TcpStream;

use self::tcp_server::TCPServer;

pub mod tcp_server;

#[async_trait]
pub trait Server {
    fn new() -> Self;
    async fn init_server(&mut self) -> Result<(), Box<dyn Error>> ;
    async fn close_server(&self) -> Result<(), Box<dyn Error>>;
    async fn process_socket(stream: TcpStream);
}

pub async fn new_server(server_type: ServerType) -> Result<(), Box<dyn Error>>{

    match server_type {
        ServerType::TCPServer => 
            Ok(TCPServer::init_server(&mut TCPServer::new()).await?),
        ServerType::None => todo!(),
    }
}

pub enum ServerType {
    TCPServer,
    None,
}
