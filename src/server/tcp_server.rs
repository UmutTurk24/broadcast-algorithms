use tokio::net::{TcpListener, TcpStream};
use async_trait::async_trait;

use std::error::Error;

use super::Server;
pub struct TCPServer {
    clients: i16,
}

#[async_trait]
impl Server for TCPServer {

    fn new() -> TCPServer {
        TCPServer {
            clients: 0,
        }
    }

    async fn init_server(&mut self) -> Result<(), Box<dyn Error>> {

        let server_socket = TcpListener::bind("127.0.0.1:8080").await?;
        self.clients = 0;

        match server_socket.accept().await {
            Ok((client_socket, addr)) => {
                println!("new client: {:?}", addr);
                self.clients += 1;
                Self::process_socket(client_socket).await;
            },
            Err(e) => {
                println!("couldn't get client: {:?}", e)
            },
        }

        Ok(())
    }
    async fn close_server(&self) -> Result<(), Box<dyn Error>>{
        Ok(())
    }

    async fn process_socket(client_socket: TcpStream) {
        println!("Great job");
    }
}

