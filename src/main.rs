use libp2p::PeerId;
use libp2p_server::{P2PServer, client::Event};
use async_std::io::{self, WriteExt};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    // Parse the user inputs
    let (mut client, mut event_receiver, _peer_id) = 
        P2PServer::initialize_server(Some("./src/bootstrap-nodes.txt".to_string())).await?;
    
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    
    // Client behaviour
    loop{
        let mut buffer = String::new();
        print!(">>> ");
        stdout.flush().await?;
        tokio::select! {
            
            // User inputs
            _res = stdin.read_line(&mut buffer) => {
                match buffer.trim().split_whitespace().collect::<Vec<&str>>().as_slice() {
                    ["dial", peer_id, multiaddr] => {
                        let peer_id: PeerId = peer_id.to_string().parse().unwrap();
                        client
                            .dial(peer_id, multiaddr.to_string().parse().unwrap())
                            .await
                            .expect("Dial to succeed");
                    },
                    ["send", peer_id, message] => {
                        let peer_id: PeerId = peer_id.to_string().parse().unwrap();
                        let data = message.to_string().into_bytes();
                        client.send_rr(peer_id, data).await.expect("Successfully sent data");
                    },
                    ["broadcast-dial", message] => {
                        let dialed_peers = client.get_dialed_peers().await;    
                        for peer in dialed_peers {
                            client.send_rr(peer, message.to_string().into_bytes()).await.expect("Successfully sent data");
                        }
                    },
                    ["publish", message, topic] => {
                        let _ = client.gossip_publish(topic.to_string(), message.to_string().into_bytes()).await?;
                        println!("Published message");
                    },
                    ["subscribe", topic] => {
                        client.gossip_subscribe(topic.to_string()).await;
                        println!("Subscribed to topic: {}", topic);
                    },
                    ["gossip-peers"] => {
                        let gossip_peers = client.gossip_all_peers().await;
                        println!("Gossip peers: {:?}", gossip_peers);
                    },
                    ["peers"] => {
                        let dialed_peers = client.get_dialed_peers().await;
                        println!("Dialed peers: {:?}", dialed_peers);
                    },
                    ["exit"] => {
                        println!("Exiting...");
                        return Ok(());
                    },
                    inv => {    
                        println!("Invalid command: {:?}", inv);
                    }
                }
            }

            // Handling client level events
            event = event_receiver.recv() => {
                match event {
                    Some(Event::RRRequest { request, channel }) => {
                        println!("Received request: {:?}", request);
                        client.recv_rr( channel ).await;
                    },
                    Some(Event::GossipMessage { message }) => {
                        println!("Received gossip message: {:?}", message);
                    },
                    Some(Event::KDProgressed { .. }) => todo!(),
                    None => return Ok(()),
                }
            }
        }
    }
}


