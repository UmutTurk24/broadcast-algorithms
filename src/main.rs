
use std::collections::HashMap;
use std::sync::Arc;
use std::{io, vec};
use std::env;

use tokio::fs;
// use server::{tcp_server::TCPServer, new_server};
use tokio::net::{TcpStream, TcpListener};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut map =HashMap::new();
    
    let addr_list = fs::read_to_string("src/config.txt")
        .await.expect("Could not find the file");

    let wrapped_list = Arc::new(addr_list);
    
    tokio::spawn(async move {
        let lst = Arc::clone(&wrapped_list);
        for server_address in lst.lines() {
            let server_socket = TcpListener::bind(server_address.to_string()).await;
            map.insert(server_address, server_socket);
        }
    });


    

    

    // Make sure each server is aware of each other's IP? Maybe not, 
    // Connect each server with each other
    // 

    
    Ok(())
}