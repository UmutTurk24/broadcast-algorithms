use libp2p::PeerId;
use libp2p_server::{P2PServer, client::Event};
use async_std::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    // Parse the user inputs
    let args: Vec<String> = std::env::args().collect();
    let port = args[1].clone();

    let (mut client, mut event_receiver, _peer_id) = 
        P2PServer::initialize_server(format!("/ip4/127.0.0.1/tcp/{}", port), Some("./src/bootstrap-nodes.txt".to_string())).await?;
    
    
    let stdin = io::stdin();
    
    // Client behaviour
    loop{
        let mut buffer = String::new();
        tokio::select! {
            
            // User inputs
            _res = stdin.read_line(&mut buffer) => {
                match buffer.trim().split_whitespace().collect::<Vec<&str>>().as_slice() {
                    ["dial", peer_id, port] => {
                        let peer_id: PeerId = peer_id.to_string().parse().unwrap();
                        println!("Dialing to peer: {:?} on port: {}", peer_id, port);
                        client
                            .dial(peer_id, format!("/ip4/127.0.0.1/tcp/{}", port).parse().unwrap())
                            .await
                            .expect("Dial to succeed");
                    },
                    ["send", peer_id, message] => {
                        let peer_id: PeerId = peer_id.to_string().parse().unwrap();
                        let data = message.to_string().into_bytes();
                        client.send_rr(peer_id, data).await.expect("Successfully sent data");
                    },
                    ["broadcast-dial"] => {
                        let dialed_peers = client.get_dialed_peers().await;    
                        for peer in dialed_peers {
                            let data = "CoolData".to_string().into_bytes();
                            client.send_rr(peer, data).await.expect("Successfully sent data");
                        }
                    },
                    ["broadcast-all", message, topic] => {
                        // Get the gossip peers
                        let gossip_peers = client.gossip_all_peers().await;

                        // Get the dialed peers
                        let dialed_peers = client.get_dialed_peers().await;

                        // Get the non-dialed gossip peers
                        let non_dialed_peers = gossip_peers.iter().filter(|peer| !dialed_peers.contains(peer)).collect::<Vec<&PeerId>>();

                        // Dial the non-dialed gossip peers
                        for peer in non_dialed_peers {
                            client.dial(peer.clone(), format!("/ip4/127.0.0.1/tcp/{}", port).parse().unwrap()).await.expect("Dial to succeed");
                            println!("Dialed peer: {:?}", peer);
                        }

                        // Send the message to all the dialed peers
                        client.gossip_publish(topic.to_string(), message.to_string().into_bytes()).await;
                        println!("Published message");
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


