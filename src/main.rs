
use std::io;

use server::{tcp_server::TCPServer, new_server};
use tokio::net::TcpStream;
use crate::server::Server;
mod server;
#[tokio::main]
async fn main() -> io::Result<()> {

    tokio::spawn(async move {
        let _ = new_server(server::ServerType::TCPServer).await;
    });

    
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;

    // tokio::spawn(async move {
       

    // });


   
    
    Ok(())
}