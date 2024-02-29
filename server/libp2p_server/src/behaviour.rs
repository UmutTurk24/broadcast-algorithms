use std::{collections::{HashSet, HashMap}, hash::Hash};

use rand_chacha::rand_core::{SeedableRng, RngCore};
use serde::{Deserialize, Serialize};
use libp2p::{PeerId, gossipsub::{MessageId, PublishError}};
use crate::client::{Client, Event};
use async_std::io::{self, WriteExt};
use tokio::sync::mpsc::Receiver;


pub struct ReliableBroadcast;

impl ReliableBroadcast {
    
    pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin: io::Stdin = io::stdin();
        let mut stdout = io::stdout();

        // Reliable Broadcast
        let mut messages: HashMap<ReliableMessage, HashSet<PeerId>> = HashMap::new(); // Map of messages received (pid, message id, data) -> vec of peers
        let mut reports: HashMap<PeerId, Report> = HashMap::new(); // Map of the reports received sender -> report
        let mut echo_tracker: HashSet<String> = HashSet::new(); // Message ids that has been echoed
        
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
                        ["publish", data, topic] => {
                            let message = Message::RBMessage(RBMessage::Message(data.to_string()));
                            let serialized_message = serde_json::to_string(&message).unwrap();

                            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await?;

                            println!("Published message: {:?}", result);

                            let mut peers: HashSet<PeerId> = HashSet::new();
                            peers.insert(my_peer_id);
                            let reliable_message = ReliableMessage {
                                source: my_peer_id.to_string(),
                                message_id: result.to_string(),
                                data: data.to_string(),
                            };
                            messages.insert(reliable_message, peers);
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
                        Some(Event::GossipMessage { propagation_source, message, message_id }) => {
                            
                            let source = message.source;
                            let topic = message.topic;
                            
                            // Turn the message into a RBMessage
                            let message: Message = serde_json::from_slice(&message.data).unwrap();

                            match message {
                                Message::RBMessage(propogated_message) => {
                                    match propogated_message {
                                        RBMessage::Message(data) => {
                                            let _result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id.to_string(), topic.to_string(), &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::Echo(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_tracker, &mut reports, &mut rand_chacha, &mut client).await;
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
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        data: String, 
        source: Option<PeerId>, 
        my_peer_id: PeerId, 
        message_id: String, 
        topic: String, 
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult {

        println!("Received gossip message: {:?}", data);

        let reliable_message = ReliableMessage {
            source: source.unwrap().to_string(),
            message_id: message_id.to_string(),
            data: data,
        };

        let total_peers = client.gossip_all_peers().await.len() as f64; 

        // If the message has already been seen, do nothing
        if messages.contains_key(&reliable_message) && messages.get(&reliable_message).unwrap().len() as f64 > total_peers/3.0  {
            return PublishResult::Idle;
        }

        let mut peers: HashSet<PeerId> = HashSet::new();
        peers.insert(my_peer_id);
        peers.insert(source.unwrap());

        messages.insert(reliable_message.clone(), peers);
        
        let echo = Message::RBMessage(RBMessage::Echo(reliable_message, rand_chacha.next_u64()));
        let serialized_message = serde_json::to_string(&echo).unwrap();
        let result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        println!("Published echo: {:?}", result);

        
        PublishResult::Published(result)
    }

    async fn handle_echo(
        source: Option<PeerId>, // The peer that sent the echo
        reliable_message: ReliableMessage,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_tracker: &mut HashSet<String>,
        reports: &mut HashMap<PeerId, Report>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        // Add the sender to the list of acked peers
        if messages.contains_key(&reliable_message) {
            let ack_peers = messages.get_mut(&reliable_message).unwrap();
            ack_peers.insert(source.unwrap());

            let total_peers = client.gossip_all_peers().await.len();
            
            if  1.0/3.0 < (ack_peers.len() as f64 / total_peers as f64) && !echo_tracker.contains(&reliable_message.message_id) {
                println!("----Received 1/3 acks for message: {:?}", reliable_message.message_id); 

                // Add the original sender of the message to the messages
                ack_peers.insert(reliable_message.source.parse().unwrap());
                ack_peers.insert(my_peer_id);

                echo_tracker.insert(reliable_message.message_id.clone());
                let echo = Message::RBMessage(RBMessage::Echo(reliable_message, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&echo).unwrap();

                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;


                println!("Published ack: {:?}", result);
                return PublishResult::Published(result);
            }

            if 2.0/3.0 < (ack_peers.len() as f64/ total_peers as f64) && !reports.contains_key(&my_peer_id) {
                println!("----Received 2/3 acks for message: {:?}", reliable_message.message_id); 

                let report = Report {
                    message_owner: reliable_message.source.to_string(),
                    message_id: reliable_message.message_id.to_string(),
                    data: reliable_message.data.clone(),
                    peers: ack_peers.iter().map(|p| p.to_string()).collect(),
                };

                reports.insert(my_peer_id, report.clone());
                
                let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&report).unwrap();
                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                println!("Published report: {:?}", result);
                return PublishResult::Published(result);
            }

        } else {
            println!("----Received 0 acks for message: {:?}", reliable_message.message_id);
            let mut peers: HashSet<PeerId> = HashSet::new();
            // peers.insert(my_peer_id);
            peers.insert(source.unwrap());
            echo_tracker.insert(reliable_message.message_id.clone());
            messages.insert(reliable_message, peers);

        }

        return PublishResult::Idle;
    }

}
pub struct WitnessBroadcast;

impl WitnessBroadcast {
   pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        // Reliable Broadcast
        let mut messages: HashMap<ReliableMessage, HashSet<PeerId>> = HashMap::new(); // Map of messages received (pid, message id, data) -> vec of peers
        let mut echo_tracker: HashSet<String> = HashSet::new(); // Message ids that has been echoed

        // Witness Broadcast
        let mut waitlisted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of waitlisted reports
        let mut accepted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of accepted reports
        let mut super_report_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of super-reports that has been published

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
                        ["publish", data, topic] => {
                            let message = Message::RBMessage(RBMessage::Message(data.to_string()));
                            let serialized_message = serde_json::to_string(&message).unwrap();

                            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await?;

                            println!("Published message: {:?}", result);

                            let mut peers: HashSet<PeerId> = HashSet::new();
                            peers.insert(my_peer_id);
                            let reliable_message = ReliableMessage {
                                source: my_peer_id.to_string(),
                                message_id: result.to_string(),
                                data: data.to_string(),
                            };
                            messages.insert(reliable_message, peers);
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
                        Some(Event::GossipMessage { propagation_source, message, message_id, }) => {
                            
                            let source = message.source;
                            let topic = message.topic;
                            
                            // Turn the message
                            let message: Message = serde_json::from_slice(&message.data).unwrap();

                            match message {
                                Message::WMessage(witness_message) => {
                                    match witness_message {
                                        WMessage::Report(report, _rand) => {
                                            let _result = Self::handle_report(report, source.unwrap(), my_peer_id, topic.to_string(), &mut waitlisted_reports, &mut accepted_reports, &mut super_report_tracker, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
                                    }
                                },
                                Message::RBMessage(reliable_message) => {
                                    match reliable_message {
                                        RBMessage::Message(data) => {
                                            let _result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id.to_string(), topic.to_string(), &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::Echo(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut rand_chacha, &mut client).await;
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
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        data: String, 
        source: Option<PeerId>, 
        my_peer_id: PeerId, 
        message_id: String, 
        topic: String, 
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult {

        println!("Received gossip message: {:?}", data);

        let reliable_message = ReliableMessage {
            source: source.unwrap().to_string(),
            message_id: message_id.to_string(),
            data: data,
        };

        let total_peers = client.gossip_all_peers().await.len() as f64;

        // If the message has already been seen, do nothing
        if messages.contains_key(&reliable_message) && (messages.get(&reliable_message).unwrap().len() as f64) < total_peers/3.0  {
            return PublishResult::Idle;
        }

        let mut peers: HashSet<PeerId> = HashSet::new();
        peers.insert(my_peer_id);
        peers.insert(source.unwrap());

        messages.insert(reliable_message.clone(), peers);
        
        let echo = Message::RBMessage(RBMessage::Echo(reliable_message, rand_chacha.next_u64()));
        let serialized_message: String = serde_json::to_string(&echo).unwrap();
        let result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        println!("Published echo: {:?}", result);
        PublishResult::Published(result)
    }

    async fn handle_echo(
        source: Option<PeerId>, // The peer that sent the echo
        reliable_message: ReliableMessage,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_tracker: &mut HashSet<String>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>, // Peer who sent the report, message id -> (original sender, data, peers who acked)
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{


        // Add the sender to the list of acked peers
        if messages.contains_key(&reliable_message) {
            let ack_peers = messages.get_mut(&reliable_message).unwrap();
            ack_peers.insert(source.unwrap());

            let total_peers = client.gossip_all_peers().await.len();
            
            if  1.0/3.0 < (ack_peers.len() as f64 / total_peers as f64) && !echo_tracker.contains(&reliable_message.message_id) {
                println!("----Received 1/3 acks for message: {:?}", reliable_message.message_id); 

                // Add the original sender of the message to the messages
                ack_peers.insert(reliable_message.source.parse().unwrap());
                ack_peers.insert(my_peer_id);

                echo_tracker.insert(reliable_message.message_id.clone());
                let echo = Message::RBMessage(RBMessage::Echo(reliable_message, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&echo).unwrap();

                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;


                println!("Published ack: {:?}", result);
                return PublishResult::Published(result);
            }

            if 2.0/3.0 < (ack_peers.len() as f64/ total_peers as f64) && !accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
                println!("----Received 2/3 acks for message: {:?}", reliable_message.message_id); 
                let report = Report {
                    message_owner: reliable_message.source.to_string(),
                    message_id: reliable_message.message_id.to_string(),
                    data: reliable_message.data.clone(),
                    peers: ack_peers.iter().map(|p| p.to_string()).collect(),
                };

                accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

                let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&report).unwrap();
                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                println!("Published report: {:?}", result);
                
                // If I have already sent a report, check if any of the waitlisted reports are a subset of my report
                if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
                    println!("Handling accepted reports");

                    if !accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap().peers.contains(&source.unwrap().to_string()) {

                        // Update the peers of the accepted report
                        let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
                        report.peers.push(source.unwrap().to_string());
                    }

                    let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
                    let my_report_peers = my_report.peers.clone();
    
                    let mut to_remove = Vec::new();
    
                    // Check if any of the waitlisted reports are a subset of my report
                    for (peer_id, report) in waitlisted_reports.iter() {
                        if peer_id.1 != reliable_message.message_id {
                            continue;
                        }
                        let report_peers = report.peers.clone();
    
                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();
                        let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();
    
                        if report_peers.is_subset(&my_report_peers) {
                            accepted_reports.insert(peer_id.clone(), report.clone());
                            to_remove.push(peer_id.clone());
                        }
                    }
    
                    for peer_id in to_remove {
                        waitlisted_reports.remove(&peer_id);
                    }
    
                    // Get the number of reports for each message id
                    let mut report_count: HashMap<String, usize> = HashMap::new();
                    for (_peer_id, message_id) in accepted_reports.keys() {
                        let count = report_count.entry(message_id.clone()).or_insert(0);
                        *count += 1;
                    }
    
                    for (message_id, count) in report_count.iter() {
                        if 2.0/3.0 < (*count as f64 / total_peers as f64) && !super_report_tracker.contains(&(my_peer_id,message_id.clone())) {
    
                            // Iterate through the accepted reports and add the peers to the super report
                            let mut report_list = Vec::new();
    
                            for (peer_id, report) in accepted_reports.iter() {
                                if &report.message_id == message_id {
                                    let report = Report {
                                        message_owner: report.message_owner.to_string(),
                                        message_id: peer_id.1.to_string(),
                                        data: report.data.to_string(),
                                        peers: report.peers.iter().map(|p| p.to_string()).collect(),
                                    };
                                    report_list.push(report);
                                }
                            }
    
                            let mut report_map = HashMap::new();
                            report_map.insert(message_id.clone(), report_list);
    
                            let super_report = SuperReport(report_map);
    
                            let super_report = Message::VMessage(VMessage::SuperReport(super_report, rand_chacha.next_u64()));
                            let serialized_message = serde_json::to_string(&super_report).unwrap();
                            let result = client.gossip_publish(topic, serialized_message.into_bytes()).await;
    
                            super_report_tracker.insert((my_peer_id, message_id.clone()));
    
                            println!("Published super report: {:?}", result);
                            
                        }
                        return PublishResult::Published(result);
                    }
                }
            }

            

        } else {
            println!("----Received 0 acks for message: {:?}", reliable_message.message_id);
            let mut peers: HashSet<PeerId> = HashSet::new();
            // peers.insert(my_peer_id);
            peers.insert(source.unwrap());
            echo_tracker.insert(reliable_message.message_id.clone());
            messages.insert(reliable_message, peers);

        }

        return PublishResult::Idle;
    }

    async fn handle_report(
        report: Report,
        peer_id: PeerId,
        my_peer_id: PeerId, 
        topic: String,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        let message_id = report.message_id.clone();
        let peers = report.peers.clone();

        println!("Received a report message: {:?}", message_id);

        // Parse the message;
        let incoming_report_peers: HashSet<PeerId> = peers.iter().map(|p: &String| p.parse().unwrap()).collect();
        
        // Get my report
        let my_report = accepted_reports.get(&(my_peer_id, message_id.clone()));
        
        // Currently I haven't created my own report
        if my_report.is_none() {
            // Add the report to the waitlisted reports
            waitlisted_reports.insert((peer_id, message_id.clone()), report);
            return PublishResult::Idle;
        }

        let my_report = my_report.unwrap();
        let my_report_peers: HashSet<PeerId> = my_report.peers.iter().map(|p: &String| p.parse().unwrap()).collect();

        // Check if the incoming report was heard before
        if accepted_reports.contains_key(&(peer_id, message_id.clone())) || waitlisted_reports.contains_key(&(peer_id, message_id.clone())){
            return PublishResult::Idle;
        }

        // Check if the the incoming report should be accepted
        let my_report_peers: HashSet<_> = my_report_peers.iter().cloned().collect();

        if incoming_report_peers.is_subset(&my_report_peers) {                
            accepted_reports.insert((peer_id, message_id.clone()), report);
        } else {
            waitlisted_reports.insert((peer_id, message_id.clone()), report);
        }

        // Check if a certain message has been accepted by 2/3 of the peers
        let total_peers = client.gossip_all_peers().await.len();

        // Get the number of reports for each message id
        let mut report_count: HashMap<String, usize> = HashMap::new();
        for (_peer_id, message_id) in accepted_reports.keys() {
            let count = report_count.entry(message_id.clone()).or_insert(0);
            *count += 1;
        }

        for (message_id, count) in report_count.iter() {
            if 2.0/3.0 < (*count as f64 / total_peers as f64) && !super_report_tracker.contains(&(my_peer_id,message_id.clone())) {

                // Iterate through the accepted reports and add the peers to the super report
                let mut report_list = Vec::new();
                
                for (peer_id, report) in accepted_reports.iter() {
                    if &report.message_id == message_id {
                        let report = Report {
                            message_owner: report.message_owner.to_string(),
                            message_id: peer_id.1.to_string(),
                            data: report.data.to_string(),
                            peers: report.peers.iter().map(|p| p.to_string()).collect(),
                        };
                        report_list.push(report);
                    }
                }
                
                let mut report_map = HashMap::new();
                report_map.insert(message_id.clone(), report_list);

                let super_report = SuperReport(report_map);
                
                let super_report = Message::VMessage(VMessage::SuperReport(super_report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&super_report).unwrap();
                let result = client.gossip_publish(topic, serialized_message.into_bytes()).await;
                
                super_report_tracker.insert((my_peer_id, message_id.clone()));

                println!("Published super report: {:?}", result);
                return PublishResult::Published(result);
            }
        }
        PublishResult::Idle
    }

}

pub struct VabaBroadcast;

impl VabaBroadcast {
   pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        // Reliable Broadcast
        let mut messages: HashMap<ReliableMessage, HashSet<PeerId>> = HashMap::new(); // Map of messages received (pid, message id, data) -> vec of peers
        let mut echo_tracker: HashSet<String> = HashSet::new(); // Message ids that has been echoed

        // Reliable Witness Broadcast
        let mut waitlisted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of waitlisted reports, (ReportOwner, MessageId) -> (OriginalSender, Data, Peers)
        let mut accepted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of accepted reports
        let mut report_echoes: HashMap<(PeerId,String), HashSet<PeerId>> = HashMap::new(); // Original Message Sender ID, Message ID -> Set of peers who echoed the message
        let mut report_echo_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of super-echoes that has been published
        let mut super_report_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of super-reports that has been published
        
        // VABA Broadcast
        let mut super_reports: HashMap<(PeerId, String), SuperReport> = HashMap::new(); // Map of accepted super reports
        let mut ultra_reports: HashMap<(PeerId, String), UltraReport> = HashMap::new(); // Map of accepted super reports

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
                        ["publish", data, topic] => {
                            let message = Message::RBMessage(RBMessage::Message(data.to_string()));
                            let serialized_message = serde_json::to_string(&message).unwrap();

                            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await?;

                            println!("Published message: {:?}", result);

                            let mut peers: HashSet<PeerId> = HashSet::new();
                            peers.insert(my_peer_id);
                            let reliable_message = ReliableMessage {
                                source: my_peer_id.to_string(),
                                message_id: result.to_string(),
                                data: data.to_string(),
                            };
                            messages.insert(reliable_message, peers);
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
                        Some(Event::GossipMessage { propagation_source, message, message_id, }) => {
                            
                            let source = message.source;
                            let topic = message.topic;
                            
                            // Turn the message
                            let message: Message = serde_json::from_slice(&message.data).unwrap();

                            match message {
                                Message::WMessage(witness_message) => {
                                    match witness_message {
                                        WMessage::Report(report, _rand) => {
                                            let _result = Self::handle_report(report, source.unwrap(), my_peer_id, topic.to_string(), &mut messages, &mut report_echo_tracker, &mut waitlisted_reports, &mut accepted_reports, &mut super_report_tracker, &mut report_echoes, &mut super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        WMessage::ReportEcho(report_owner, report, _rand) => {
                                            let _result = Self::handle_echo_report(report_owner.parse().unwrap(), report, source.unwrap(), my_peer_id, topic.to_string(), &mut waitlisted_reports, &mut accepted_reports, &mut super_report_tracker, &mut report_echoes, &mut report_echo_tracker, &mut super_reports, &mut rand_chacha, &mut client).await;
                                        }
                                    }
                                },
                                Message::RBMessage(reliable_message) => {
                                    match reliable_message {
                                        RBMessage::Message(data) => {
                                            let _result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id.to_string(), topic.to_string(), &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::Echo(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                    }
                                },
                                Message::VMessage(vaba_message) => {
                                    match vaba_message {
                                        VMessage::SuperReport(super_report, _rand) => {
                                            let _result = Self::handle_super_report(super_report, source.unwrap(), my_peer_id, topic.to_string(), &mut super_reports, &mut ultra_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        VMessage::UltraReport(ultra_report, _rand) => {
                                            let _result = Self::handle_ultra_report(ultra_report, source.unwrap(), &mut ultra_reports).await;
                                        },
                                    }
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

    async fn handle_message(
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        data: String, 
        source: Option<PeerId>, 
        my_peer_id: PeerId, 
        message_id: String, 
        topic: String, 
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        println!("Received gossip message: {:?}", data);

        let reliable_message = ReliableMessage {
            source: source.unwrap().to_string(),
            message_id: message_id.to_string(),
            data: data,
        };

        let total_peers = client.gossip_all_peers().await.len() as f64;

        // If the message has already been seen, do nothing
        if messages.contains_key(&reliable_message) && (messages.get(&reliable_message).unwrap().len() as f64)< total_peers/3.0  {
            return PublishResult::Idle;
        }

        let mut peers: HashSet<PeerId> = HashSet::new();
        peers.insert(my_peer_id);
        peers.insert(source.unwrap());

        messages.insert(reliable_message.clone(), peers);
        
        let echo = Message::RBMessage(RBMessage::Echo(reliable_message, rand_chacha.next_u64()));
        let serialized_message = serde_json::to_string(&echo).unwrap();
        let result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        println!("Published echo: {:?}", result);
        PublishResult::Published(result)
    }

    async fn handle_echo(
        source: Option<PeerId>, // The peer that sent the echo
        reliable_message: ReliableMessage,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_tracker: &mut HashSet<String>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>, // Peer who sent the report, message id -> (original sender, data, peers who acked)
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        // Add the sender to the list of acked peers
        if messages.contains_key(&reliable_message) {
            let ack_peers = messages.get_mut(&reliable_message).unwrap();
            ack_peers.insert(source.unwrap());

            let total_peers = client.gossip_all_peers().await.len();
            
            if  1.0/3.0 < (ack_peers.len() as f64 / total_peers as f64) && !echo_tracker.contains(&reliable_message.message_id) {
                println!("----Received 1/3 acks for message: {:?}", reliable_message.message_id); 

                // Add the original sender of the message to the messages
                ack_peers.insert(reliable_message.source.parse().unwrap());
                ack_peers.insert(my_peer_id);

                echo_tracker.insert(reliable_message.message_id.clone());
                let echo = Message::RBMessage(RBMessage::Echo(reliable_message, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&echo).unwrap();

                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                println!("Published ack: {:?}", result);
                return PublishResult::Published(result);
            }

            if 2.0/3.0 < (ack_peers.len() as f64/ total_peers as f64) && !accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
                println!("----Received 2/3 acks for message: {:?}", reliable_message.message_id); 
                let report = Report {
                    message_owner: reliable_message.source.to_string(),
                    message_id: reliable_message.message_id.to_string(),
                    data: reliable_message.data.clone(),
                    peers: ack_peers.iter().map(|p| p.to_string()).collect(),
                };

                accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

                let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&report).unwrap();
                let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

                println!("Published report: {:?}", result);
                
                // If I have already sent a report, check if any of the waitlisted reports are a subset of my report
                if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
                    println!("Handling accepted reports");

                    if !accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap().peers.contains(&source.unwrap().to_string()) {

                        // Update the peers of the accepted report
                        let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
                        report.peers.push(source.unwrap().to_string());
                    }

                    let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
                    let my_report_peers = my_report.peers.clone();
    
                    let mut to_remove = Vec::new();
    
                    // Check if any of the waitlisted reports are a subset of my report
                    for (peer_id, report) in waitlisted_reports.iter() {
                        if peer_id.1 != reliable_message.message_id {
                            continue;
                        }
                        let report_peers = report.peers.clone();
    
                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();
                        let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();
    
                        if report_peers.is_subset(&my_report_peers) {
                            accepted_reports.insert(peer_id.clone(), report.clone());
                            to_remove.push(peer_id.clone());
                        }
                    }
    
                    for peer_id in to_remove {
                        waitlisted_reports.remove(&peer_id);
                    }
    
                    // Get the number of reports for each message id
                    let mut report_count: HashMap<String, usize> = HashMap::new();
                    for (_peer_id, message_id) in accepted_reports.keys() {
                        let count = report_count.entry(message_id.clone()).or_insert(0);
                        *count += 1;
                    }
    
                    for (message_id, count) in report_count.iter() {
                        if 2.0/3.0 < (*count as f64 / total_peers as f64) && !super_report_tracker.contains(&(my_peer_id,message_id.clone())) {
    
                            // Iterate through the accepted reports and add the peers to the super report
                            let mut report_list = Vec::new();
    
                            for (peer_id, report) in accepted_reports.iter() {
                                if &report.message_id == message_id {
                                    let report = Report {
                                        message_owner: report.message_owner.to_string(),
                                        message_id: peer_id.1.to_string(),
                                        data: report.data.to_string(),
                                        peers: report.peers.iter().map(|p| p.to_string()).collect(),
                                    };
                                    report_list.push(report);
                                }
                            }
    
                            let mut report_map = HashMap::new();
                            report_map.insert(message_id.clone(), report_list);
    
                            let super_report = SuperReport(report_map);
                            super_reports.insert((my_peer_id, message_id.clone()), super_report.clone());
    
                            let super_report = Message::VMessage(VMessage::SuperReport(super_report, rand_chacha.next_u64()));
                            let serialized_message = serde_json::to_string(&super_report).unwrap();
                            let result = client.gossip_publish(topic, serialized_message.into_bytes()).await;
    
                            super_report_tracker.insert((my_peer_id, message_id.clone()));
    
                            println!("Published super report: {:?}", result);
                            
                        }
                        return PublishResult::Published(result);
                    }
                }
            }


            
            
        } else {
            println!("----Received 0 acks for message: {:?}", reliable_message.message_id);
            let mut peers: HashSet<PeerId> = HashSet::new();
            // peers.insert(my_peer_id);
            peers.insert(source.unwrap());
            echo_tracker.insert(reliable_message.message_id.clone());
            messages.insert(reliable_message, peers);
        }

        return PublishResult::Idle;
    }

    async fn handle_report(
        report: Report,
        peer_id: PeerId,
        my_peer_id: PeerId, 
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        report_echo_tracker: &mut HashSet<(PeerId, String)>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        report_echoes: &mut HashMap<(PeerId,String), HashSet<PeerId>>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client
    ) -> PublishResult{
        
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // If I haven't seen this report, add the incoming report 
        if !report_echoes.contains_key(&(peer_id, report.message_id.clone())) {

            let mut peers = HashSet::new();
            peers.insert(peer_id);
            report_echoes.insert((peer_id, report.message_id.clone()), peers);

            if !report_echo_tracker.contains(&(peer_id, report.message_id.clone())) {
                let report_echo = Message::WMessage(WMessage::ReportEcho(peer_id.to_string(), report.clone(), rand_chacha.next_u64())); 
                let serialized_message = serde_json::to_string(&report_echo).unwrap();
                let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;
            }
        }
        else {            
            // Add this report to the report_echoes
            report_echoes.get_mut(&(peer_id, report.message_id.clone())).unwrap().insert(peer_id);

            // Trust the report if it has been echoed by 1/3 of the peers
            if (report_echoes.get(&(peer_id, report.message_id.clone())).unwrap().len() as f64 / total_peers) > 1.0/3.0 {

                // Get my own report
                let my_report = accepted_reports.get(&(my_peer_id, report.message_id.clone()));

                // Currently I haven't created my own report
                if my_report.is_none() {

                    // Update my OG tracker with the new report
                    let peers = messages.get(&ReliableMessage { source: report.message_owner.clone(), message_id: report.message_id.clone(), data: report.data.clone() });
                    if peers.is_none() {

                        // Create the peers with incoming report's peers
                        let reliable_message = ReliableMessage {
                            source: report.message_owner.to_string(),
                            message_id: report.message_id.to_string(),
                            data: report.data.to_string(),
                        };
                        
                        // Insert the reliable message
                        messages.insert(reliable_message, report.peers.iter().map(|p| p.parse().unwrap()).collect());
                        
                        // Update accepted reports
                        accepted_reports.insert((my_peer_id, report.message_id.clone()), report.clone());

                    } else {
                        // Get the incoming report's peers
                        let incoming_report_peers: HashSet<PeerId> = report.peers.iter().map(|p| p.parse().unwrap()).collect();
                        
                        // Insert every peer into the my own peer list
                        for peer in incoming_report_peers {
                            messages.get_mut(&ReliableMessage { source: report.message_owner.clone(), message_id: report.message_id.clone(), data: report.data.clone() }).unwrap().insert(peer);
                        }
                    }

                    // Publish my report
                    let report = Message::WMessage(WMessage::Report(report.clone(), rand_chacha.next_u64()));
                    let serialized_message = serde_json::to_string(&report).unwrap();
                    let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;
                } else {
                    // Add the reports peers to my own report
                    let incoming_report_peers: HashSet<PeerId> = report.peers.iter().map(|p| p.parse().unwrap()).collect();
                    
                    // Insert every peer into the my own peer list
                    for peer in incoming_report_peers {
                        messages.get_mut(&ReliableMessage { source: report.message_owner.clone(), message_id: report.message_id.clone(), data: report.data.clone() }).unwrap().insert(peer);
                        if !accepted_reports.get(&(my_peer_id, report.message_id.clone())).unwrap().peers.contains(&peer.to_string()) {
        
                            let report = accepted_reports.get_mut(&(my_peer_id, report.message_id.clone())).unwrap();
                            // Update the peers of the accepted report
                            report.peers.push(peer.to_string());
                        }
                    }
                }
            }
        }

        let incoming_report_peers: HashSet<PeerId> = report.peers.iter().map(|p| p.parse().unwrap()).collect();
        println!("Received a report message: {:?}", report.message_id);

        // Get my report
        let my_report = accepted_reports.get(&(my_peer_id, report.message_id.clone()));
        
        // Currently I haven't created my own report
        if my_report.is_none() {
            // Add the report to the waitlisted reports
            waitlisted_reports.insert((peer_id, report.message_id.clone()), report);
            return PublishResult::Idle;
        }

        let my_report = my_report.unwrap();

        // Check if the incoming report was heard before
        if accepted_reports.contains_key(&(peer_id, report.message_id.clone())) || waitlisted_reports.contains_key(&(peer_id, my_report.message_id.clone())){
            return PublishResult::Idle;
        }

        let my_report_peers: HashSet<PeerId> = my_report.peers.iter().map(|p: &String| p.parse().unwrap()).collect();

        // Check if the the incoming report should be accepted
        if incoming_report_peers.is_subset(&my_report_peers) {  
            accepted_reports.insert((peer_id, my_report.message_id.clone()), report);
        } else {
            waitlisted_reports.insert((peer_id, my_report.message_id.clone()), report);
        }

        // Check if a certain message has been accepted by 2/3 of the peers
        let total_peers = client.gossip_all_peers().await.len();

        // Get the number of reports for each message id
        let mut report_count: HashMap<String, usize> = HashMap::new();
        for (_peer_id, message_id) in accepted_reports.keys() {
            let count = report_count.entry(message_id.clone()).or_insert(0);
            *count += 1;
        }

        for (message_id, count) in report_count.iter() {
            if 2.0/3.0 < (*count as f64 / total_peers as f64) && !super_report_tracker.contains(&(my_peer_id,message_id.clone())) {

                // Iterate through the accepted reports and add the peers to the super report
                let mut report_list = Vec::new();
                
                for (peer_id, report) in accepted_reports.iter() {
                    if &report.message_id == message_id {
                        let report = Report {
                            message_owner: report.message_owner.to_string(),
                            message_id: peer_id.1.to_string(),
                            data: report.data.to_string(),
                            peers: report.peers.iter().map(|p| p.to_string()).collect(),
                        };
                        report_list.push(report);
                    }
                }
                
                let mut report_map = HashMap::new();
                report_map.insert(message_id.clone(), report_list);

                let super_report = SuperReport(report_map);
                super_reports.insert((my_peer_id, message_id.clone()), super_report.clone());
                
                let super_report = Message::VMessage(VMessage::SuperReport(super_report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&super_report).unwrap();
                let result = client.gossip_publish(topic, serialized_message.into_bytes()).await;
                
                super_report_tracker.insert((my_peer_id, message_id.clone()));

                println!("Published super report: {:?}", result);
                return PublishResult::Published(result);
            }
        }
        PublishResult::Idle
    }

    async fn handle_echo_report(
        report_owner: PeerId,
        echoed_report: Report,
        source: PeerId,
        my_peer_id: PeerId,
        topic: String,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        report_echoes: &mut HashMap<(PeerId, String), HashSet<PeerId>>,
        report_echo_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client,
    ) -> PublishResult {

        let total_peers = client.gossip_all_peers().await.len() as f64;

        // If I haven't seen the report, add it to the echoed reports
        if !report_echoes.contains_key(&(report_owner, echoed_report.message_id.clone())) {
            let mut peers = HashSet::new();
            peers.insert(source);
            report_echoes.insert((report_owner, echoed_report.message_id.clone()), peers);

            return PublishResult::Idle;
        }

        // Add the source to the echoed reports
        report_echoes.get_mut(&(report_owner, echoed_report.message_id.clone())).unwrap().insert(source);

        if (report_echoes.get(&(report_owner, echoed_report.message_id.clone())).unwrap().len() as f64 / total_peers) > 1.0/3.0 && 
            !report_echo_tracker.contains(&(report_owner, echoed_report.message_id.clone())) { 
            
            // Check if the report is already accepted or waitlisted, if not add it to the waitlisted reports
            if !accepted_reports.contains_key(&(report_owner, echoed_report.message_id.clone())) && 
                !waitlisted_reports.contains_key(&(report_owner, echoed_report.message_id.clone())) {
                
                let my_report = accepted_reports.get(&(my_peer_id, echoed_report.message_id.clone()));

                // Currently I haven't created my own report
                if my_report.is_none() {

                    // Add the report to the waitlisted reports
                    waitlisted_reports.insert(
                        (report_owner, echoed_report.message_id.clone()), 
                        echoed_report
                    );

                    return PublishResult::Idle;
                } 

                let my_report = my_report.unwrap();
                let my_report_peers: HashSet<PeerId> = my_report.peers.iter().map(|p| p.parse().unwrap()).collect();

                // Check if the echoed report should be accepted or waitlisted
                if report_echoes.get(&(report_owner, echoed_report.message_id.clone())).unwrap().is_subset(&my_report_peers) {
                    accepted_reports.insert(
                        (report_owner, echoed_report.message_id.clone()), 
                        echoed_report.clone()
                    );
                } else {
                    waitlisted_reports.insert(
                        (report_owner, echoed_report.message_id.clone()), 
                        echoed_report.clone()
                    );   
                }
            }

            report_echo_tracker.insert((report_owner, echoed_report.message_id.clone()));

            // Publish the echoed report
            let report = Message::WMessage(WMessage::ReportEcho(report_owner.to_string(), echoed_report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;


            println!("Published echoed report: {:?}", result);
            return PublishResult::Published(result);
        }
        if (report_echoes.get(&(report_owner, echoed_report.message_id.clone())).unwrap().len() as f64 / total_peers) > 2.0/3.0 &&
            !super_report_tracker.contains(&(my_peer_id, echoed_report.message_id.clone())) {
            
            // Create a Super Report
            let mut report_list = Vec::new();
            for (peer_id, report) in accepted_reports.iter() {
                if &report.message_id == &echoed_report.message_id {
                    let report = Report {
                        message_owner: report.message_owner.to_string(),
                        message_id: peer_id.1.to_string(),
                        data: report.data.to_string(),
                        peers: report.peers.iter().map(|p| p.to_string()).collect(),
                    };
                    report_list.push(report);
                }
            }

            let mut report_map = HashMap::new();
            report_map.insert(echoed_report.message_id.clone(), report_list);

            let super_report = SuperReport(report_map);
            super_reports.insert((my_peer_id, echoed_report.message_id.clone()), super_report.clone());

            let super_report = Message::VMessage(VMessage::SuperReport(super_report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&super_report).unwrap();
            let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;
            
            println!("Published super report: {:?}", result);
            return PublishResult::Published(result);
        }

        PublishResult::Idle
    }

    async fn handle_super_report(
        super_report: SuperReport,
        source: PeerId,
        my_peer_id: PeerId,
        topic: String,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        ultra_reports: &mut HashMap<(PeerId, String), UltraReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client,
    ) -> PublishResult {

        println!("Received super report");
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Get the message_id of the super report
        let message_id = super_report.0.keys().next().unwrap().clone();
        // If I haven't seen the super report, add it to the super reports
        if !super_reports.contains_key(&(source, super_report.0.keys().next().unwrap().clone())) {
            super_reports.insert((source, super_report.0.keys().next().unwrap().clone()), super_report);
        }

        // Check if there is a message in super_reports that has been accepted by 2/3 of the peers
        let mut report_count: HashMap<String, usize> = HashMap::new();
        for (peer_id, _cur_super_report) in super_reports.iter() {
            if peer_id.1 != message_id {
                continue;
            }
            let count = report_count.entry(message_id.clone()).or_insert(0);
            *count += 1;
        }

        for (message_id, count) in report_count.iter() {
            if 2.0/3.0 < (*count as f64 / total_peers) && !ultra_reports.contains_key(&(my_peer_id, message_id.clone())) {
                println!("----Received 2/3 super reports for message: {:?}", message_id);

                // Iterate through the super reports and add the peers to the ultra report
                let mut super_report_list = Vec::new();
                for (_peer_id, super_report) in super_reports.iter() {
                    if super_report.0.contains_key(&message_id.to_string()) {
                        super_report_list.push(super_report.clone());
                    }
                }

                let mut ultra_report = HashMap::new();
                ultra_report.insert(message_id.clone(), super_report_list);
                let ultra_report = UltraReport(ultra_report);
                ultra_reports.insert((my_peer_id, message_id.clone()), ultra_report.clone());
                
                let ultra_report = Message::VMessage(VMessage::UltraReport(ultra_report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&ultra_report).unwrap();
                let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;


                println!("Published ultra report: {:?}", result);
                return PublishResult::Published(result);
            }
        }
        

        PublishResult::Idle
    }

    async fn handle_ultra_report(
        ultra_report: UltraReport,
        source: PeerId,
        ultra_reports: &mut HashMap<(PeerId, String), UltraReport>,
    ) -> PublishResult {

        println!("Received ultra report");

        // If I haven't seen the ultra report, add it to the ultra reports
        if !ultra_reports.contains_key(&(source, ultra_report.0.keys().next().unwrap().clone())) {
            ultra_reports.insert((source, ultra_report.0.keys().next().unwrap().clone()), ultra_report);
        }

        PublishResult::Idle
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
enum Message {
    RBMessage(RBMessage),
    WMessage(WMessage),
    VMessage(VMessage),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum RBMessage {
    Message(String), // message
    Echo(ReliableMessage, u64) // original sender of the message, message id, data 
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum WMessage {
    Report(Report, u64), // I received a report
    ReportEcho(String, Report, u64), // Echo of a report (Sender of the report, report, random number)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum VMessage {
    SuperReport(SuperReport, u64), // This report says that I received 2/3 # of reports for a certain message
    UltraReport(UltraReport, u64) // original sender of the message, message id, data 
}

enum PublishResult {
    Idle,
    Published(Result<MessageId, PublishError>),
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct ReliableMessage {
    source: String,
    message_id: String,
    data: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Report {
    message_owner: String,
    message_id: String,
    data: String,
    peers: Vec<String>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct SuperReport(HashMap<String, Vec<Report>>); // MessageId to Reports (SuperReport for this original Message ID)

#[derive(Serialize, Deserialize, Debug, Clone)]
struct UltraReport(HashMap<String, Vec<SuperReport>>); // MessageId to SuperReports (UltraReport for this original Message ID)