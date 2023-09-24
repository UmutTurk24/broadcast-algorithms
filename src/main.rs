use std::{net::SocketAddr, collections::HashMap, io::{self}};

use tokio::{sync::mpsc::{Sender, Receiver, channel}, net::{TcpListener, TcpStream, tcp::OwnedReadHalf}, io::AsyncReadExt};

#[tokio::main]
async fn main() -> Result<(),  Box<dyn std::error::Error>> {

    let (tx, rx) = channel(32);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Listening on: {:?}", listener.local_addr());

    let listener_tx = tx.clone();
    let _event_handle = event_listener(listener_tx, rx).await;
    println!("Event loop started");

    let socket_tx = tx.clone();
    let _listen_handle = connection_listener(listener, socket_tx).await;
    println!("Listening socket ready");

    loop {
        // Do join here and quit
    }

}

async fn event_listener(listener_tx: Sender<Lib>, mut rx: Receiver<Lib>) -> tokio::task::JoinHandle<()> {
    
    tokio::spawn(async move {
        let mut client_map = HashMap::new();
        loop {
            let incoming_event = rx.recv().await;
            match incoming_event {
                Some(lib_event) => {
                    match lib_event {

                        Lib::Recv(client_socket, addr) => {
                            let (client_listen, client_write) = client_socket.into_split();
                            client_map.insert(addr, client_write);
                            let client_tx = listener_tx.clone();
                            let _handle = client_handler(client_listen, client_tx);
                        },
                        Lib::Data(str) => {
                            // Handle long data
                            println!("Incoming Data: {}", str);
                        },
                        Lib::Conn(addr) => {
                            if !client_map.contains_key(&addr) {
                                let client_socket = TcpStream::connect(addr.to_string())
                                    .await.expect("Failed to connect to the given address");
                                let (client_listen, client_write) = client_socket.into_split();
                                client_map.insert(addr, client_write);
                                let client_tx = listener_tx.clone();
                                let _handle = client_handler(client_listen, client_tx);
                            }
                        }
                    }
                },
                None => {
                    rx.close();
                    break;
                },
            }
        
        
        }
    })
}

async fn client_handler(mut client_listen: OwnedReadHalf, client_tx: Sender<Lib>) -> tokio::task::JoinHandle<()> {

    tokio::spawn(async move {
        let mut buf = vec![0; 1024];
        loop {
            
            match client_listen.read(&mut buf).await {
                Ok(n) if n == 0 => {
                    // Connection closed by the server
                    break;
                }
                Ok(n) => {
                    // Data received, send it to the main thread
                    let data = String::from_utf8_lossy(&buf[..n]).to_string();
                    println!("Data is: {}", data);
                    if client_tx.send(Lib::Data(data)).await.is_err() {
                        // Channel closed, exit the loop
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from server: {}", e);
                    break;
                }
            }
        }
    })
}

async fn connection_listener(listener: TcpListener, socket_tx: Sender<Lib>) -> tokio::task::JoinHandle<()> {

    tokio::spawn(async move {

        let mut buffer = String::new();
        let stdin = io::stdin();
        loop {
            
            tokio::select! {
                Ok((socket, addr)) = listener.accept() => {
                    println!("Client connected: {}", addr);
                    socket_tx.send(Lib::Recv(socket, addr )).await.expect("Listener dropped");
                }
                Ok(res) = async {
                    stdin.read_line(&mut buffer)
                } => {
                    println!("User input: {:?}", res);
                }
            }
        }
    })
}

enum Lib {
    Recv(TcpStream, SocketAddr),
    Data(String),
    Conn(SocketAddr),
}
