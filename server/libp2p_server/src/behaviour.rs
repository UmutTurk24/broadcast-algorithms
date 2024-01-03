use std::collections::{HashSet, HashMap};
use serde::{Deserialize, Serialize};
use libp2p::{PeerId, gossipsub::MessageId};
use crate::client::{Event, Client};
use async_std::io::{self, WriteExt};
use tokio::sync::mpsc::Receiver;


pub struct ReliableBroadcast;

impl ReliableBroadcast {
    
    pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        // Enum: Message, Ack, Commit
        let mut messages: HashMap<PeerId, String> = HashMap::new();
        let mut acks: HashMap<MessageId, Vec<PeerId>> = HashMap::new();
        let mut echoed_acks: HashSet<MessageId> = HashSet::new();
        let mut commits: HashMap<PeerId, PropogatedMessage> = HashMap::new();
        
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
                            let message = PropogatedMessage::Message(message.to_string());
                            let serialized_message = serde_json::to_string(&message).unwrap();

                            let _ = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await?;
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
                        ["gossip-mesh", topic] => {
                            let gossip_mesh = client.gossip_mesh(topic.to_string()).await;
                            println!("Gossip mesh: {:?}", gossip_mesh);
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
                        Some(Event::GossipMessage {message, message_id, .. }) => {
                            
                            let source = message.source;
                            let topic = message.topic;
                            
                            // Turn the message into a PropogatedMessage
                            let message: PropogatedMessage = serde_json::from_slice(&message.data).unwrap();

                            match message {
                                PropogatedMessage::Message(data) => {
                                    println!("Received gossip message: {:?}", data);

                                    // If the message has already been seen, do nothing
                                    if messages.contains_key(&source.unwrap()) {
                                        continue;
                                    }
                                    messages.insert(source.unwrap(), data);

                                    let ack = PropogatedMessage::Ack(message_id.to_string(), vec![my_peer_id.to_string()], source.unwrap().to_string());
                                    acks.insert(message_id, vec![my_peer_id]);
                                    let serialized_message = serde_json::to_string(&ack).unwrap();

                                    println!("current topic: {}", serialized_message);

                                    let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await?;
                                    println!("Published ack: {:?}", result);

                                },
                                PropogatedMessage::Ack(message_id, ack_peers, propagation_source) => {

                                    // Parse the message
                                    let message_id: MessageId = message_id.into();
                                    let ack_peers: Vec<PeerId> = ack_peers.iter().map(|peer_id| peer_id.parse().unwrap()).collect();
                                    let propagation_source: PeerId = propagation_source.parse().unwrap();

                                    // Add the acknowledged peers to the acks map
                                    if acks.contains_key(&message_id) {
                                        let acks = acks.get_mut(&message_id).unwrap();
                                        acks.extend(ack_peers);
                                    } else {
                                        acks.insert(message_id, ack_peers);
                                    }
                                    
                                    for (message_id, ack_peers) in acks.iter() {

                                        // If we have received 2/3 of the acks and a previous commit message hasn't been sent, send a commit message
                                        if (ack_peers.len() >= (2 * client.gossip_all_peers().await.len()) / 3) && !commits.contains_key(&my_peer_id) {
                                            println!("----Received 2/3 acks for message: {:?}", message_id);

                                            let ack_peers: Vec<String> = ack_peers.iter().map(|peer_id| peer_id.to_string()).collect();

                                            let commit = PropogatedMessage::Commit(message_id.to_string(), ack_peers, my_peer_id.to_string());
                                            commits.insert(my_peer_id, commit.clone());
                                            let serialized_message = serde_json::to_string(&commit).unwrap();
                                            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
                                            println!("Published commit: {:?}", result);
                                        }

                                        // Echo the ack message if 1/3 of the peers have acked
                                        if acks.len() >= (client.gossip_all_peers().await.len() / 3) && !echoed_acks.contains(&message_id) && !commits.contains_key(&my_peer_id){

                                            println!("----Received 1/3 acks for message: {:?}", message_id); 

                                            let ack_peers: Vec<String> = ack_peers.iter().map(|peer_id| peer_id.to_string()).collect();
                                            let ack = PropogatedMessage::Ack(message_id.to_string(), ack_peers, propagation_source.to_string());

                                            echoed_acks.insert(message_id.clone());
                                            let serialized_message = serde_json::to_string(&ack).unwrap();

                                            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
                                            println!("Published ack: {:?}", result);
                                        }
                                    }
                                },
                                PropogatedMessage::Commit(message_id, peers, propagation_source) => {
                                    println!("Received commit message: {:?}", message_id);
                                },
                            }
                        },
                        Some(Event::KDProgressed { .. }) => todo!(),
                        None => return Ok(()),
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum PropogatedMessage {
    Message(String), // message
    Ack(String, Vec<String>, String), // message id, acked peers, peer that sent the original message
    Commit(String, Vec<String>, String), // message id, peers, peer that sent the original message
}

