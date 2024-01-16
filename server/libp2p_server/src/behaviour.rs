use std::{collections::{HashSet, HashMap, hash_map::RandomState}, hash::Hash};

use rand_chacha::rand_core::{SeedableRng, RngCore};
use serde::{Deserialize, Serialize};
use libp2p::{PeerId, gossipsub::{MessageId, PublishError}};
use crate::client::{Event, Client};
use async_std::io::{self, WriteExt};
use tokio::sync::mpsc::Receiver;


pub struct ReliableBroadcast;

impl ReliableBroadcast {
    
    pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        // Reliable Broadcast
        let mut messages: HashMap<(PeerId, MessageId, String), HashSet<PeerId>> = HashMap::new(); // Map of messages received (pid, message id, data) -> vec of peers
        let mut reports: HashMap<PeerId, (PeerId, MessageId, String, HashSet<PeerId>)> = HashMap::new(); // Map of the reports received sender -> report
        let mut echo_tracker: HashSet<MessageId> = HashSet::new(); // Message ids that has been echoed
        
        let mut rand_chacha = rand_chacha::ChaCha20Rng::from_entropy();
        
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
                            let message = Message::PropogatedMessage(PropogatedMessage::Message(message.to_string()));
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

                            // let message: PropogatedMessage = serde_json::from_slice(&message.data).unwrap();
                            let message: Message = serde_json::from_slice(&message.data).unwrap();

                            match message {
                                Message::PropogatedMessage(propogated_message) => {
                                    match propogated_message {
                                        PropogatedMessage::Message(data) => {
                                            let result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id, topic.to_string(), &mut rand_chacha, &mut client).await;
                                        },
                                        PropogatedMessage::Echo(peer_id, amessage_id, data, _rand) => {
                                            let result = Self::handle_echo(source, peer_id, amessage_id, data, my_peer_id, topic.to_string(), &mut messages, &mut echo_tracker, &mut reports, &mut rand_chacha, &mut client).await;
                                        },
                                    }
                                },
                                _ => {},
                            }
                        },
                        Some(Event::KDProgressed { .. }) => todo!(),
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    async fn handle_message(
        messages: &mut HashMap<(PeerId, MessageId, String), HashSet<PeerId>>,
        data: String, 
        source: Option<PeerId>, 
        my_peer_id: PeerId, 
        message_id: MessageId, 
        topic: String, 
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        println!("Received gossip message: {:?}", data);

        // If the message has already been seen, do nothing
        if messages.contains_key(&(source.unwrap(), message_id.clone(), data.clone())) {
            return PublishResult::Idle;
        }

        let mut peers: HashSet<PeerId> = HashSet::new();
        peers.insert(my_peer_id);
        messages.insert((source.unwrap(), message_id.clone(), data.clone()), peers);
        
        let echo = Message::PropogatedMessage(PropogatedMessage::Echo(source.unwrap().to_string(), message_id.to_string(), data, rand_chacha.next_u64()));
        let serialized_message = serde_json::to_string(&echo).unwrap();
        let result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        println!("Published echo: {:?}", result);
        
        PublishResult::Published(result)
    }


    async fn handle_echo(
        source: Option<PeerId>,
        peer_id: String, 
        message_id: String, 
        data: String,
        my_peer_id: PeerId, 
        topic: String, 
        messages: &mut HashMap<(PeerId, MessageId, String), HashSet<PeerId>>,
        echo_tracker: &mut HashSet<MessageId>,
        reports: &mut HashMap<PeerId, (PeerId, MessageId, String, HashSet<PeerId>)>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        // Parse the message
        let peer_id: PeerId = peer_id.parse().unwrap();
        let message_id: MessageId = message_id.into();

        // Add the sender to the list of acked peers
        if messages.contains_key(&(peer_id.clone(), message_id.clone(), data.clone())) {
            let ack_peers = messages.get_mut(&(peer_id.clone(), message_id.clone(), data.clone())).unwrap();
            ack_peers.insert(source.unwrap());

            let total_peers = client.gossip_all_peers().await.len();
            
            if  1.0/3.0 > (ack_peers.len() as f64 / total_peers as f64) && !echo_tracker.contains(&message_id) {
                println!("----Received 1/3 acks for message: {:?}", message_id); 

                let echo = Message::PropogatedMessage(PropogatedMessage::Echo(peer_id.to_string(), message_id.to_string(), data, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&echo).unwrap();
                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                echo_tracker.insert(message_id);

                println!("Published ack: {:?}", result);
                return PublishResult::Published(result);
            }

            if 2.0/3.0 > (ack_peers.len() as f64/ total_peers as f64) && !reports.contains_key(&my_peer_id) {
                println!("----Received 2/3 acks for message: {:?}", message_id); 

                reports.insert(my_peer_id, (peer_id.clone(), message_id.clone(), data.clone(), ack_peers.clone()));

                let report = Message::WitnessMessage(WitnessMessage::Report(peer_id.to_string(), message_id.to_string(), data, ack_peers.iter().map(|p| p.to_string()).collect(), rand_chacha.next_u32().to_string()));
                let serialized_message = serde_json::to_string(&report).unwrap();
                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                println!("Published report: {:?}", result);
                return PublishResult::Published(result);
            }

        } else {
            let peers: HashSet<PeerId> = HashSet::new();
            messages.insert((peer_id.clone(), message_id.clone(), data.clone()), peers);
            // CHECK IF WE NEED TO ECHO HERE
        }

        return PublishResult::Idle;
    }

}
enum PublishResult {
    Idle,
    Published(Result<MessageId, PublishError>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum PropogatedMessage {
    Message(String), // message
    Echo(String, String, String, u64) // original sender of the message, message id, data 
}

pub struct WitnessBroadcast;

impl WitnessBroadcast {
   pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        // Reliable Broadcast
        let mut messages: HashMap<(PeerId, MessageId, String), Vec<PeerId>> = HashMap::new(); // Map of messages received (pid, message id, data) -> vec of peers

        // Witness Broadcast
        let mut reports: HashMap<PeerId, (PeerId, MessageId, String, Vec<PeerId>)> = HashMap::new(); // Map of the reports received sender -> report
        let mut super_reports: HashMap<PeerId, (PeerId, MessageId, String, Vec<PeerId>)> = HashMap::new(); // Map of the reports received sender -> report
        let mut report_tracker: HashMap<PeerId, HashSet<PeerId>> = HashMap::new(); // Map of the reports received sender -> report
        
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
                            let message: Message = serde_json::from_slice(&message.data).unwrap();

                            match message {
                                Message::WitnessMessage(witness_message) => {
                                    match witness_message {
                                        WitnessMessage::Report(source, message_id, data, peers, ..) => {
                                            // let result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id, topic.to_string(), &mut client).await;
                                        },
                                        WitnessMessage::SuperReport(source, message_id, data, peers) => {
                                            // let result = Self::handle_echo(&mut messages, &mut echo_tracker, source, peer_id, message_id, data, my_peer_id, topic.to_string(), &mut reports, &mut client).await;
                                        },
                                    }
                                },
                                _ => {},
                            }
                        },
                        Some(Event::KDProgressed { .. }) => todo!(),
                        None => return Ok(()),
                    }
                }
            }
        }
    }

    async fn handle_report(
        reports: &mut HashMap<(PeerId, MessageId, String), Vec<PeerId>>,
        peer_id: String,
        message_id: String,
        data: String, 
        source: Option<PeerId>, 
        my_peer_id: PeerId, 
        client: &mut Client) 
        -> PublishResult{

        println!("Received a report message: {:?}", data);

        // Check if the incoming report can be accepted
        let message_id: MessageId = message_id.into();
        let peer_id: PeerId = peer_id.parse().unwrap();
        
        PublishResult::Idle
    }


    async fn handle_echo(
        messages: &mut HashMap<(PeerId, MessageId, String), Vec<PeerId>>,
        echo_tracker: &mut HashSet<MessageId>,
        peer_id: String, 
        message_id: String, 
        data: String,
        my_peer_id: PeerId, 
        topic: String, 
        reports: &mut HashMap<PeerId, (PeerId, MessageId, String, Vec<PeerId>)>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        // Parse the message
        let message_id: MessageId = message_id.into();
        let peer_id: PeerId = peer_id.parse().unwrap();

        // Add the sender to the list of acked peers
        if messages.contains_key(&(peer_id.clone(), message_id.clone(), data.clone())) && !echo_tracker.contains(&message_id){
            let ack_peers = messages.get_mut(&(peer_id.clone(), message_id.clone(), data.clone())).unwrap();
            ack_peers.push(my_peer_id);
            
            if ack_peers.len() >= (client.gossip_all_peers().await.len() / 3) && !echo_tracker.contains(&message_id) {
                println!("----Received 1/3 acks for message: {:?}", message_id); 

                let echo = PropogatedMessage::Echo(peer_id.to_string(), message_id.to_string(), data, rand_chacha.next_u64());
                let serialized_message = serde_json::to_string(&echo).unwrap();
                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                echo_tracker.insert(message_id.clone());

                println!("Published ack: {:?}", result);
                return PublishResult::Published(result);
            }

            if ack_peers.len() >= (client.gossip_all_peers().await.len() / 3) && !reports.contains_key(&my_peer_id) {
                println!("----Received 2/3 acks for message: {:?}", message_id); 

                reports.insert(my_peer_id, (peer_id.clone(), message_id.clone(), data.clone(), ack_peers.clone()));
                return PublishResult::Idle;
            }

        } else {
            messages.insert((peer_id.clone(), message_id.clone(), data.clone()), vec![my_peer_id]);
            // CHECK IF WE NEED TO ECHO HERE
        }

        return PublishResult::Idle;
    }


}
#[derive(Serialize, Deserialize, Debug, Clone)]
// enum WitnessMessage {
//     Report(PeerId, MessageId, String, Vec<PeerId>),
//     SuperReport(PeerId, MessageId, String, Vec<PeerId>),
// }

enum WitnessMessage {
    Report(String, String, String, Vec<String>, String),
    SuperReport(String, String, String, Vec<String>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum Message {
    PropogatedMessage(PropogatedMessage),
    WitnessMessage(WitnessMessage),
}