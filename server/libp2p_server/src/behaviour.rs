use std::{collections::{HashSet, HashMap}, hash::Hash};

use rand_chacha::rand_core::{SeedableRng, RngCore};
use serde::{Deserialize, Serialize};
use libp2p::{PeerId, gossipsub::{MessageId, PublishError}};
use crate::client::{Client, Event};
use async_std::{fs::OpenOptions, io::{self, WriteExt}};
use tokio::{sync::mpsc::Receiver, time::Instant};


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
                                        RBMessage::MessageEcho(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_tracker, &mut reports, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
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
        
        let echo = Message::RBMessage(RBMessage::MessageEcho(reliable_message, rand_chacha.next_u64()));
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
                let echo = Message::RBMessage(RBMessage::MessageEcho(reliable_message, rand_chacha.next_u64()));
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
                                        RBMessage::MessageEcho(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
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
        
        let echo = Message::RBMessage(RBMessage::MessageEcho(reliable_message, rand_chacha.next_u64()));
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
                let echo = Message::RBMessage(RBMessage::MessageEcho(reliable_message, rand_chacha.next_u64()));
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
        let mut echo_tracker: HashSet<String> = HashSet::new(); // RB message-ids that has been echoed

        // Reliable Witness Broadcast
        let mut accepted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of accepted reports
        let mut waitlisted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of waitlisted reports, (ReportOwner, MessageId) -> (OriginalSender, Data, Peers)
        let mut ack_tracker: HashSet<(ReliableMessage, PeerId)> = HashSet::new(); // (Peer,MessageId), Set of acks that has been published
        let mut report_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of reports that has been published
        let mut echo_messages: HashMap<(ReliableMessage, PeerId), HashSet<PeerId>> = HashMap::new(); // Original Message Sender ID, Message ID -> Set of peers who echoed the message
        let mut ack_echo_tracker: HashSet<(ReliableMessage, PeerId)> = HashSet::new(); // (Peer,MessageId), Set of acks that has been echoed
        
        // VABA Broadcast
        let mut super_reports: HashMap<(PeerId, String), SuperReport> = HashMap::new(); // Map of accepted super reports
        let mut waitlisted_super_reports: HashMap<(PeerId, String), SuperReport> = HashMap::new(); // Map of waitlisted super reports
        let mut super_report_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of super-reports that has been published
        let mut ultra_reports: HashMap<(PeerId, String), UltraReport> = HashMap::new(); // Map of accepted super reports

        let mut rand_chacha = rand_chacha::ChaCha20Rng::from_entropy();

        let mut before = Instant::now();

        // Client behaviour
        loop{            
            let mut buffer = String::new();
            print!(">>> ");
            stdout.flush().await?;
            // Internal state of messages
            // println!("Messages: {:?}", messages);
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
                            before = Instant::now();
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
                            println!("Gossip peers: {:?}", gossip_peers.len());
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
                                Message::RBMessage(reliable_message) => {
                                    match reliable_message {
                                        RBMessage::Message(data) => {
                                            let _result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id.to_string(), topic.to_string(), &mut echo_tracker, &mut report_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::MessageEcho(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_messages, &mut ack_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::Ack(reliable_message, echo_owner, _rand) => {
                                            let _result = Self::handle_acks(reliable_message, source, echo_owner.parse().unwrap(), my_peer_id, topic.to_string(), &mut messages, &mut echo_messages, &mut ack_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
                                    }
                                },
                                Message::WMessage(witness_message) => {
                                    match witness_message {
                                        WMessage::Report(report, _rand) => {
                                            let _result = Self::handle_report(report, source.unwrap(), my_peer_id, topic.to_string(), &mut waitlisted_reports, &mut accepted_reports, &mut super_report_tracker, &mut super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
                                    }
                                },
                                Message::VMessage(vaba_message) => {
                                    match vaba_message {
                                        VMessage::SuperReport(super_report, _rand) => {
                                            let _result = Self::handle_super_report(super_report, source.unwrap(), my_peer_id, topic.to_string(), &mut accepted_reports, &mut super_reports, &mut waitlisted_super_reports, &mut ultra_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        VMessage::UltraReport(ultra_report, _rand) => {
                                            
                                            stdout.flush().await?;
                                            println!("Elapsed time: {:.2?}", before.elapsed());
                                            stdout.flush().await?;

                                            let _result = Self::handle_ultra_report(ultra_report, source.unwrap(), &mut ultra_reports).await;
                                        },
                                    }
                                },
                                Message::EvilMessage => {
                                    let mut counter = 0;
                                    for _ in 0..100000 {
                                        counter += 1;
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
        echo_tracker: &mut HashSet<String>,
        report_tracker: &mut HashSet<(PeerId, String)>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        println!("Received gossip message: {:?}", data);
        
        let reliable_message = ReliableMessage {
            source: source.unwrap().to_string(),
            message_id: message_id.to_string(),
            data: data,
        };

        // Add the RB into the messages, and add myself and the message owner to the list
        if messages.contains_key(&reliable_message) {
            if !messages.get_mut(&reliable_message).unwrap().contains(&my_peer_id) {
                messages.get_mut(&reliable_message).unwrap().insert(my_peer_id);
            }

            if !messages.get_mut(&reliable_message).unwrap().contains(&source.unwrap()) {
                messages.get_mut(&reliable_message).unwrap().insert(source.unwrap());
            }
        } else {
            let new_peers = vec![my_peer_id, source.unwrap()];
            messages.insert(reliable_message.clone(), new_peers.into_iter().collect());
        }
        
        // Echo it if I haven't echoed it before
        if !echo_tracker.contains(&reliable_message.message_id) {
            echo_tracker.insert(reliable_message.message_id.clone());
            let echo = Message::RBMessage(RBMessage::MessageEcho(reliable_message.clone(), rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&echo).unwrap();
            let _result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
            print!("Published echo for the first time hearing it: {:?}", _result);
        }
        
        
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Check if RB has reached 2t/3, if yes publish a report, and check to see if report published before
        if total_peers * 2.0/3.0 < messages.get(&reliable_message).unwrap().len() as f64 && !report_tracker.contains(&(my_peer_id, reliable_message.message_id.clone())){
            // Publish a report
            let report = Report {
                message_owner: reliable_message.source.to_string(),
                message_id: reliable_message.message_id.to_string(),
                data: reliable_message.data.clone(),
                peers: messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect(),
            };

            // Accept my own report
            accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

            let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

            println!("Published report after getting a message: {:?}", result);
            report_tracker.insert((my_peer_id, reliable_message.message_id.to_string()));
        }

        // Update my report if I have it
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            report.peers = messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect();
        }

        // Update the Accepted/Waitlisted Reports
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let mut to_remove = Vec::new();

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
        }            


        // Check if AcceptedReports reached 2t/3,  
            // if yes publish a super report
            // update the super report tracker
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

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
                    let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                    println!("Published super report: {:?}", result);
                }
            }
        }

        // If I have a report for this super report
            // Check if any waitlisted super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) &&
            super_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut to_remove: Vec<(PeerId, String)> = Vec::new();

            for (peer_id, super_report) in waitlisted_super_reports.iter() {
                if peer_id.1 != reliable_message.message_id {
                    continue;
                }

                let mut is_big_subset = false;
                
                for (_message_id, reports) in super_report.0.iter() {

                    for report in reports {
                        let report_peers = report.peers.clone();

                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                        if !report_peers.is_subset(&my_report_peers) {
                            is_big_subset = false;
                        }
                    }
                }

                if is_big_subset {
                    super_reports.insert(peer_id.clone(), super_report.clone());
                    to_remove.push(peer_id.clone());
                }
            }

            for peer_id in to_remove {
                waitlisted_super_reports.remove(&peer_id);
            }
        } 

        PublishResult::Idle
    }

    async fn handle_echo(
        source: Option<PeerId>, // The peer that sent the echo
        reliable_message: ReliableMessage,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_messages: &mut HashMap<(ReliableMessage, PeerId), HashSet<PeerId>>,
        ack_tracker: &mut HashSet<(ReliableMessage, PeerId)>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{
        
        // println!("Received echo for message: {:?}", reliable_message.message_id);

        // I am A, and B sent RB to everyone. C sent this echo to me
        // A is my_peer_id, B is pid in RB, C is source
        let source = source.unwrap();
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Add the echo to the echo_list (RB,C -> Peers)
            // Add C to the echo_list
        if echo_messages.contains_key(&(reliable_message.clone(), source)) { 
            echo_messages.get_mut(&(reliable_message.clone(), source)).unwrap().insert(my_peer_id);
            echo_messages.get_mut(&(reliable_message.clone(), source)).unwrap().insert(source);
        } else {
            let mut peers: HashSet<PeerId> = HashSet::new();
            peers.insert(source);
            peers.insert(my_peer_id);
            echo_messages.insert((reliable_message.clone(), source), peers);    
        }
        
        // Check if I have ack this echo from C before
            // If not, echo it: AckEcho(RB, C)
        if !ack_tracker.contains(&(reliable_message.clone(), source)) {

            ack_tracker.insert((reliable_message.clone(), source));

            let ack = Message::RBMessage(RBMessage::Ack(reliable_message.clone(), source.to_string(), rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&ack).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
            // println!("Published ack for incoming echo: {:?}", result);
        }

        // If this echo has been confirmed by 2t/3 other nodes, then add the message owner (B) to the list
        if echo_messages.get(&(reliable_message.clone(), source)).unwrap().len() as f64 > total_peers * 2.0/3.0 {
            if messages.contains_key(&reliable_message) {
                messages.get_mut(&reliable_message).unwrap().insert(source);
            } else {
                let mut peers: HashSet<PeerId> = HashSet::new();
                peers.insert(source);
                messages.insert(reliable_message.clone(), peers);
            }
        }

        // If messages has t/3 peers, then add the message owner (B) to the list
        if messages.contains_key(&reliable_message) && messages.get(&reliable_message).unwrap().len() as f64 > total_peers * 1.0/3.0 {
            messages.get_mut(&reliable_message).unwrap().insert(reliable_message.source.parse().unwrap());
        }

        
        // Check if I have received 2t/3 echoes for this message
        // Check if the RB owner sent this message as well
            // If yes, publish a report
        if !accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) && 
            messages.contains_key(&reliable_message) {
            if messages.get(&reliable_message).unwrap().contains(&reliable_message.source.parse().unwrap()) &&
            messages.get(&reliable_message).unwrap().len() as f64 > total_peers * 2.0/3.0 {


            let report = Report {
                message_owner: reliable_message.source.to_string(),
                message_id: reliable_message.message_id.to_string(),
                data: reliable_message.data.clone(),
                peers: messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect(),
            };

            // Accept my own report
            accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

            let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

            println!("Published report: {:?}", result);
            // return PublishResult::Published(result);
        }}

        // Update my report if I have it
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            report.peers = messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect();
        }

        // Update the Accepted/Waitlisted Reports
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let mut to_remove = Vec::new();

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
        }            


        // Check if AcceptedReports reached 2t/3,  
            // if yes publish a super report
            // update the super report tracker
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

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
                    let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                    println!("Published super report: {:?}", _result);
                }
            }
        }

        
        // If I have a report for this super report
            // Check if any waitlisted super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) &&
            super_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut to_remove: Vec<(PeerId, String)> = Vec::new();

            for (peer_id, super_report) in waitlisted_super_reports.iter() {
                if peer_id.1 != reliable_message.message_id {
                    continue;
                }

                let mut is_big_subset = false;
                
                for (_message_id, reports) in super_report.0.iter() {

                    for report in reports {
                        let report_peers = report.peers.clone();

                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                        if !report_peers.is_subset(&my_report_peers) {
                            is_big_subset = false;
                        }
                    }
                }

                if is_big_subset {
                    super_reports.insert(peer_id.clone(), super_report.clone());
                    to_remove.push(peer_id.clone());
                }
            }

            for peer_id in to_remove {
                waitlisted_super_reports.remove(&peer_id);
            }
        } 
        PublishResult::Idle
    }

    async fn handle_acks(
        reliable_message: ReliableMessage,
        source: Option<PeerId>,
        echo_owner: PeerId,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_messages: &mut HashMap<(ReliableMessage, PeerId), HashSet<PeerId>>,
        ack_tracker: &mut HashSet<(ReliableMessage, PeerId)>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        // println!("Received ack for echo: {:?}", reliable_message);
        
        // I am A, and B sent RB to everyone. C sent this ack to me about D
        // A is my_peer_id, B is pid in RB, C is source, D is echo_owner

        let source = source.unwrap();
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Add the echo to the echo_list (RB,C -> Peers)
            // Add C to the echo_list
        if echo_messages.contains_key(&(reliable_message.clone(), echo_owner)) {
            echo_messages.get_mut(&(reliable_message.clone(), echo_owner)).unwrap().insert(source);
        } else {
            let mut peers: HashSet<PeerId> = HashSet::new();
            peers.insert(source);
            echo_messages.insert((reliable_message.clone(), echo_owner), peers);
        }

        // If this list has t+1 peers in it, add the echo owner (D) to the list 
        if echo_messages.get(&(reliable_message.clone(), echo_owner)).unwrap().len() as f64 > total_peers * 1.0/3.0 {
            echo_messages.get_mut(&(reliable_message.clone(), echo_owner)).unwrap().insert(echo_owner);
        }

        // If this list has 2t+1 peers in it, add the echo owner (D) to the messages list 
        if echo_messages.get(&(reliable_message.clone(), echo_owner)).unwrap().len() as f64 > total_peers * 2.0/3.0 {
            if messages.contains_key(&reliable_message) {
                messages.get_mut(&reliable_message).unwrap().insert(echo_owner);
            } else {
                let mut peers: HashSet<PeerId> = HashSet::new();
                peers.insert(echo_owner);
                messages.insert(reliable_message.clone(), peers);
            }
        }

        // If this list has t+1 peers in it, and I haven't send an ack before, sent it
        if echo_messages.get(&(reliable_message.clone(), echo_owner)).unwrap().len() as f64 > total_peers * 1.0/3.0 &&
            !ack_tracker.contains(&(reliable_message.clone(), source)) {
            
            ack_tracker.insert((reliable_message.clone(), source));

            let ack = Message::RBMessage(RBMessage::Ack(reliable_message.clone(), source.to_string(), rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&ack).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
            println!("Published ack for incoming echo: {:?}", result);
        }


        // Check if I have received 2t/3 echoes for this message
        // Check if the RB owner sent this message as well
            // If yes, publish a report
        if !accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) && 
            messages.contains_key(&reliable_message) {
            
            
            if messages.get(&reliable_message).unwrap().contains(&reliable_message.source.parse().unwrap()) &&
            messages.get(&reliable_message).unwrap().len() as f64 > total_peers * 2.0/3.0 {

            let report = Report {
                message_owner: reliable_message.source.to_string(),
                message_id: reliable_message.message_id.to_string(),
                data: reliable_message.data.clone(),
                peers: messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect(),
            };

            // Accept my own report
            accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

            let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

            println!("Published report: {:?}", result);
            // return PublishResult::Published(result);
        }}


        // Update my report if I have it
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            report.peers = messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect();
        }


        // Update the Accepted/Waitlisted Reports
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let mut to_remove = Vec::new();

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
        }            


        // Check if AcceptedReports reached 2t/3,  
            // if yes publish a super report
            // update the super report tracker
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

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
                    let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                    println!("Published super report: {:?}", result);
                }
            }
        }

        // If I have a report for this super report
            // Check if any waitlisted super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) &&
            super_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut to_remove: Vec<(PeerId, String)> = Vec::new();

            for (peer_id, super_report) in waitlisted_super_reports.iter() {
                if peer_id.1 != reliable_message.message_id {
                    continue;
                }

                let mut is_big_subset = false;
                
                for (_message_id, reports) in super_report.0.iter() {

                    for report in reports {
                        let report_peers = report.peers.clone();

                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                        if !report_peers.is_subset(&my_report_peers) {
                            is_big_subset = false;
                        }
                    }
                }

                if is_big_subset {
                    super_reports.insert(peer_id.clone(), super_report.clone());
                    to_remove.push(peer_id.clone());
                }
            }

            for peer_id in to_remove {
                waitlisted_super_reports.remove(&peer_id);
            }
        } 

        
        PublishResult::Idle
    }

// source, echo_owner.parse().unwrap(), my_peer_id, topic.to_string(), &mut messages, &mut echo_messages, &mut ack_echo_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
    // async fn handle_ack_echo(
    //     ack_echo: 

    // ) -> PublishResult {
    //     PublishResult::Idle
    // }

    async fn handle_report(
        report: Report,
        peer_id: PeerId,
        my_peer_id: PeerId, 
        topic: String,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client
    ) -> PublishResult{

        println!("Received report for message: {:?}", report.message_id);

        // If I don't have a report for this message, add it to the waitlist
        if !accepted_reports.contains_key(&(peer_id, report.message_id.clone())) {
            waitlisted_reports.insert((peer_id, report.message_id.clone()), report.clone());
        }
        // If I have a report for this message
            // Check if the incoming report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, report.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, report.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let report_peers = report.peers.clone();

            let report_peers: HashSet<String> = report_peers.iter().cloned().collect();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            if report_peers.is_subset(&my_report_peers) {
                accepted_reports.insert((peer_id, report.message_id.clone()), report.clone());
            } else {
                waitlisted_reports.insert((peer_id, report.message_id.clone()), report.clone());
            }
        }

        // Check if I have received 2t/3 reports for this message
            // If yes, publish a super report
            // update the super report tracker
        let total_peers = client.gossip_all_peers().await.len();
        // let total_peers = 4 as f64;

        let reliable_message = ReliableMessage {
            source: report.message_owner.to_string(),
            message_id: report.message_id.to_string(),
            data: report.data.to_string(),
        };
            
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

            let mut report_count: HashMap<String, usize> = HashMap::new();
            for (_peer_id, message_id) in accepted_reports.keys() {
                let count: &mut usize = report_count.entry(message_id.clone()).or_insert(0);
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
                    let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                    println!("Published super report: {:?}", _result);
                }
            }
        }

        PublishResult::Idle
        
        
    }

    async fn handle_super_report(
        super_report: SuperReport,
        source: PeerId,
        my_peer_id: PeerId,
        topic: String,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        ultra_reports: &mut HashMap<(PeerId, String), UltraReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client,
    ) -> PublishResult {

        println!("Received super report: {:?}", super_report.0.keys().next().unwrap());

        // If I don't have a report for this super report, add it to the waitlist
        if !super_reports.contains_key(&(my_peer_id, super_report.0.keys().next().unwrap().to_string())) {
            waitlisted_super_reports.insert((source, super_report.0.keys().next().unwrap().to_string()), super_report.clone());
        }

        // If I have a report for this super report
            // Check if the incoming super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, super_report.0.keys().next().unwrap().to_string())) {

            // Check if my report is a superset of every report in the incoming super report
            let my_report = accepted_reports.get_mut(&(my_peer_id, super_report.0.keys().next().unwrap().to_string())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut is_big_subset = true;

            for (_peer_id, report_list) in super_report.0.iter() {
                for report in report_list {
                    let report_peers = report.peers.clone();

                    let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                    if !report_peers.is_subset(&my_report_peers) {
                        waitlisted_super_reports.insert((source, super_report.0.keys().next().unwrap().to_string()), super_report.clone());
                        is_big_subset = false;
                    }
                }
            }
            if is_big_subset {
                super_reports.insert((source, super_report.0.keys().next().unwrap().to_string()), super_report.clone());
            }
        } 

        // Check if I have received 2t/3 super reports for this message
            // If yes, publish a ultra report
            // update the ultra report tracker
        let total_peers = client.gossip_all_peers().await.len();

        let mut report_count: HashMap<String, usize> = HashMap::new();

        for (_peer_id, message_id) in super_reports.keys() {
            let count = report_count.entry(message_id.clone()).or_insert(0);
            *count += 1;
        }

        for (message_id, count) in report_count.iter() {
            if 2.0/3.0 < (*count as f64 / total_peers as f64) && !ultra_reports.contains_key(&(my_peer_id, message_id.clone())) {

                // Iterate through the super reports and add the peers to the ultra report
                let mut super_report_list = Vec::new();

                for (_peer_id, super_report) in super_reports.iter() {
                    if &super_report.0.keys().next().unwrap() == &message_id {
                        super_report_list.push(super_report.clone());
                    }
                }

                let mut super_report_map = HashMap::new();
                super_report_map.insert(message_id.clone(), super_report_list);

                let ultra_report = UltraReport(super_report_map);

                ultra_reports.insert((my_peer_id, message_id.clone()), ultra_report.clone());

                let ultra_report = Message::VMessage(VMessage::UltraReport(ultra_report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&ultra_report).unwrap();
                let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                println!("Published ultra report: {:?}", _result);
            }
        }

        PublishResult::Idle
    }

    async fn handle_ultra_report(
        ultra_report: UltraReport,
        source: PeerId,
        ultra_reports: &mut HashMap<(PeerId, String), UltraReport>,
    ) -> PublishResult {

        println!("YAY Received ultra report: {:?}", ultra_report.0.keys().next().unwrap());

        // Add the ultra report to the list
        ultra_reports.insert((source, ultra_report.0.keys().next().unwrap().to_string()), ultra_report.clone());

        PublishResult::Idle
    }
}



pub struct EvilVabaBroadcast;

impl EvilVabaBroadcast {
   pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {

        // Reliable Broadcast

        let mut before = Instant::now();

        // Send a message every 500ms 

        loop {
            let elapsed = before.elapsed();
            if elapsed.as_millis() > 500 {
                let message: Message = Message::RBMessage(RBMessage::Message("Hello".to_string()));
                let serialized_message = serde_json::to_string(&message).unwrap();
                let result = client.gossip_publish("test".to_string(), serialized_message.into_bytes()).await;
                println!("Published message: {:?}", result);
                before = Instant::now();
            }
        }
    
    
    }

}

pub struct ChillVabaBroadcast;

impl ChillVabaBroadcast {
   pub async fn run(mut client: Client, mut event_receiver: Receiver<Event>, my_peer_id: PeerId) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = io::stdin();
        let mut stdout = io::stdout();

        // Reliable Broadcast
        let mut messages: HashMap<ReliableMessage, HashSet<PeerId>> = HashMap::new(); // Map of messages received (pid, message id, data) -> vec of peers
        let mut echo_tracker: HashSet<String> = HashSet::new(); // RB message-ids that has been echoed

        // Reliable Witness Broadcast
        let mut accepted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of accepted reports
        let mut waitlisted_reports: HashMap<(PeerId, String), Report> = HashMap::new(); // Map of waitlisted reports, (ReportOwner, MessageId) -> (OriginalSender, Data, Peers)
        let mut ack_tracker: HashSet<(ReliableMessage, PeerId)> = HashSet::new(); // (Peer,MessageId), Set of acks that has been published
        let mut report_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of reports that has been published
        let mut echo_messages: HashMap<(ReliableMessage, PeerId), HashSet<PeerId>> = HashMap::new(); // Original Message Sender ID, Message ID -> Set of peers who echoed the message
        let mut ack_echo_tracker: HashSet<(ReliableMessage, PeerId)> = HashSet::new(); // (Peer,MessageId), Set of acks that has been echoed
        
        // VABA Broadcast
        let mut super_reports: HashMap<(PeerId, String), SuperReport> = HashMap::new(); // Map of accepted super reports
        let mut waitlisted_super_reports: HashMap<(PeerId, String), SuperReport> = HashMap::new(); // Map of waitlisted super reports
        let mut super_report_tracker: HashSet<(PeerId, String)> = HashSet::new(); // (Peer,MessageId), Set of super-reports that has been published
        let mut ultra_reports: HashMap<(PeerId, String), UltraReport> = HashMap::new(); // Map of accepted super reports

        let mut rand_chacha = rand_chacha::ChaCha20Rng::from_entropy();

        let mut before = Instant::now();

        // Client behaviour
        loop{            

            tokio::select! {

               

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
                                Message::RBMessage(reliable_message) => {
                                    match reliable_message {
                                        RBMessage::Message(data) => {
                                            let _result = Self::handle_message(&mut messages, data, source, my_peer_id, message_id.to_string(), topic.to_string(), &mut echo_tracker, &mut report_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::MessageEcho(reliable_message, _rand) => {
                                            let _result = Self::handle_echo(source, reliable_message, my_peer_id, topic.to_string(), &mut messages, &mut echo_messages, &mut ack_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        RBMessage::Ack(reliable_message, echo_owner, _rand) => {
                                            let _result = Self::handle_acks(reliable_message, source, echo_owner.parse().unwrap(), my_peer_id, topic.to_string(), &mut messages, &mut echo_messages, &mut ack_tracker, &mut accepted_reports, &mut waitlisted_reports, &mut super_report_tracker, &mut super_reports, &mut waitlisted_super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
                                    }
                                },
                                Message::WMessage(witness_message) => {
                                    match witness_message {
                                        WMessage::Report(report, _rand) => {
                                            let _result = Self::handle_report(report, source.unwrap(), my_peer_id, topic.to_string(), &mut waitlisted_reports, &mut accepted_reports, &mut super_report_tracker, &mut super_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        _ => {},
                                    }
                                },
                                Message::VMessage(vaba_message) => {
                                    match vaba_message {
                                        VMessage::SuperReport(super_report, _rand) => {
                                            let _result = Self::handle_super_report(super_report, source.unwrap(), my_peer_id, topic.to_string(), &mut accepted_reports, &mut super_reports, &mut waitlisted_super_reports, &mut ultra_reports, &mut rand_chacha, &mut client).await;
                                        },
                                        VMessage::UltraReport(ultra_report, _rand) => {
                                            
                                            stdout.flush().await?;
                                            println!("Elapsed time: {:.2?}", before.elapsed());
                                            stdout.flush().await?;

                                            let _result = Self::handle_ultra_report(ultra_report, source.unwrap(), &mut ultra_reports).await;
                                        },
                                    }
                                },
                                Message::EvilMessage => {
                                    let mut counter = 0;
                                    for _ in 0..100000 {
                                        counter += 1;
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
        echo_tracker: &mut HashSet<String>,
        report_tracker: &mut HashSet<(PeerId, String)>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{
        
        let reliable_message = ReliableMessage {
            source: source.unwrap().to_string(),
            message_id: message_id.to_string(),
            data: data,
        };

        // Add the RB into the messages, and add myself and the message owner to the list
        if messages.contains_key(&reliable_message) {
            if !messages.get_mut(&reliable_message).unwrap().contains(&my_peer_id) {
                messages.get_mut(&reliable_message).unwrap().insert(my_peer_id);
            }

            if !messages.get_mut(&reliable_message).unwrap().contains(&source.unwrap()) {
                messages.get_mut(&reliable_message).unwrap().insert(source.unwrap());
            }
        } else {
            let new_peers = vec![my_peer_id, source.unwrap()];
            messages.insert(reliable_message.clone(), new_peers.into_iter().collect());
        }
        
        // Echo it if I haven't echoed it before
        if !echo_tracker.contains(&reliable_message.message_id) {
            echo_tracker.insert(reliable_message.message_id.clone());
            let echo = Message::RBMessage(RBMessage::MessageEcho(reliable_message.clone(), rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&echo).unwrap();
            let _result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        }
        
        
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Check if RB has reached 2t/3, if yes publish a report, and check to see if report published before
        if total_peers * 2.0/3.0 < messages.get(&reliable_message).unwrap().len() as f64 && !report_tracker.contains(&(my_peer_id, reliable_message.message_id.clone())){
            // Publish a report
            let report = Report {
                message_owner: reliable_message.source.to_string(),
                message_id: reliable_message.message_id.to_string(),
                data: reliable_message.data.clone(),
                peers: messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect(),
            };

            // Accept my own report
            accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

            let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

            report_tracker.insert((my_peer_id, reliable_message.message_id.to_string()));
        }

        // Update my report if I have it
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            report.peers = messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect();
        }

        // Update the Accepted/Waitlisted Reports
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let mut to_remove = Vec::new();

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
        }            


        // Check if AcceptedReports reached 2t/3,  
            // if yes publish a super report
            // update the super report tracker
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

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
                    let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                }
            }
        }

        // If I have a report for this super report
            // Check if any waitlisted super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) &&
            super_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut to_remove: Vec<(PeerId, String)> = Vec::new();

            for (peer_id, super_report) in waitlisted_super_reports.iter() {
                if peer_id.1 != reliable_message.message_id {
                    continue;
                }

                let mut is_big_subset = false;
                
                for (_message_id, reports) in super_report.0.iter() {

                    for report in reports {
                        let report_peers = report.peers.clone();

                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                        if !report_peers.is_subset(&my_report_peers) {
                            is_big_subset = false;
                        }
                    }
                }

                if is_big_subset {
                    super_reports.insert(peer_id.clone(), super_report.clone());
                    to_remove.push(peer_id.clone());
                }
            }

            for peer_id in to_remove {
                waitlisted_super_reports.remove(&peer_id);
            }
        } 

        PublishResult::Idle
    }

    async fn handle_echo(
        source: Option<PeerId>, // The peer that sent the echo
        reliable_message: ReliableMessage,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_messages: &mut HashMap<(ReliableMessage, PeerId), HashSet<PeerId>>,
        ack_tracker: &mut HashSet<(ReliableMessage, PeerId)>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{
        

        // I am A, and B sent RB to everyone. C sent this echo to me
        // A is my_peer_id, B is pid in RB, C is source
        let source = source.unwrap();
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Add the echo to the echo_list (RB,C -> Peers)
            // Add C to the echo_list
        if echo_messages.contains_key(&(reliable_message.clone(), source)) { 
            echo_messages.get_mut(&(reliable_message.clone(), source)).unwrap().insert(my_peer_id);
            echo_messages.get_mut(&(reliable_message.clone(), source)).unwrap().insert(source);
        } else {
            let mut peers: HashSet<PeerId> = HashSet::new();
            peers.insert(source);
            peers.insert(my_peer_id);
            echo_messages.insert((reliable_message.clone(), source), peers);    
        }
        
        // Check if I have ack this echo from C before
            // If not, echo it: AckEcho(RB, C)
        if !ack_tracker.contains(&(reliable_message.clone(), source)) {

            ack_tracker.insert((reliable_message.clone(), source));

            let ack = Message::RBMessage(RBMessage::Ack(reliable_message.clone(), source.to_string(), rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&ack).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        }

        // If this echo has been confirmed by 2t/3 other nodes, then add the message owner (B) to the list
        if echo_messages.get(&(reliable_message.clone(), source)).unwrap().len() as f64 > total_peers * 2.0/3.0 {
            if messages.contains_key(&reliable_message) {
                messages.get_mut(&reliable_message).unwrap().insert(source);
            } else {
                let mut peers: HashSet<PeerId> = HashSet::new();
                peers.insert(source);
                messages.insert(reliable_message.clone(), peers);
            }
        }

        // If messages has t/3 peers, then add the message owner (B) to the list
        if messages.contains_key(&reliable_message) && messages.get(&reliable_message).unwrap().len() as f64 > total_peers * 1.0/3.0 {
            messages.get_mut(&reliable_message).unwrap().insert(reliable_message.source.parse().unwrap());
        }

        
        // Check if I have received 2t/3 echoes for this message
        // Check if the RB owner sent this message as well
            // If yes, publish a report
        if !accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) && 
            messages.contains_key(&reliable_message) {
            if messages.get(&reliable_message).unwrap().contains(&reliable_message.source.parse().unwrap()) &&
            messages.get(&reliable_message).unwrap().len() as f64 > total_peers * 2.0/3.0 {


            let report = Report {
                message_owner: reliable_message.source.to_string(),
                message_id: reliable_message.message_id.to_string(),
                data: reliable_message.data.clone(),
                peers: messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect(),
            };

            // Accept my own report
            accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

            let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;

        }}

        // Update my report if I have it
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            report.peers = messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect();
        }

        // Update the Accepted/Waitlisted Reports
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let mut to_remove = Vec::new();

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
        }            


        // Check if AcceptedReports reached 2t/3,  
            // if yes publish a super report
            // update the super report tracker
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

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
                    let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                }
            }
        }

        
        // If I have a report for this super report
            // Check if any waitlisted super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) &&
            super_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut to_remove: Vec<(PeerId, String)> = Vec::new();

            for (peer_id, super_report) in waitlisted_super_reports.iter() {
                if peer_id.1 != reliable_message.message_id {
                    continue;
                }

                let mut is_big_subset = false;
                
                for (_message_id, reports) in super_report.0.iter() {

                    for report in reports {
                        let report_peers = report.peers.clone();

                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                        if !report_peers.is_subset(&my_report_peers) {
                            is_big_subset = false;
                        }
                    }
                }

                if is_big_subset {
                    super_reports.insert(peer_id.clone(), super_report.clone());
                    to_remove.push(peer_id.clone());
                }
            }

            for peer_id in to_remove {
                waitlisted_super_reports.remove(&peer_id);
            }
        } 
        PublishResult::Idle
    }

    async fn handle_acks(
        reliable_message: ReliableMessage,
        source: Option<PeerId>,
        echo_owner: PeerId,
        my_peer_id: PeerId,
        topic: String,
        messages: &mut HashMap<ReliableMessage, HashSet<PeerId>>,
        echo_messages: &mut HashMap<(ReliableMessage, PeerId), HashSet<PeerId>>,
        ack_tracker: &mut HashSet<(ReliableMessage, PeerId)>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client) 
        -> PublishResult{

        
        // I am A, and B sent RB to everyone. C sent this ack to me about D
        // A is my_peer_id, B is pid in RB, C is source, D is echo_owner

        let source = source.unwrap();
        let total_peers = client.gossip_all_peers().await.len() as f64;

        // Add the echo to the echo_list (RB,C -> Peers)
            // Add C to the echo_list
        if echo_messages.contains_key(&(reliable_message.clone(), echo_owner)) {
            echo_messages.get_mut(&(reliable_message.clone(), echo_owner)).unwrap().insert(source);
        } else {
            let mut peers: HashSet<PeerId> = HashSet::new();
            peers.insert(source);
            echo_messages.insert((reliable_message.clone(), echo_owner), peers);
        }

        // If this list has t+1 peers in it, add the echo owner (D) to the list 
        if echo_messages.get(&(reliable_message.clone(), echo_owner)).unwrap().len() as f64 > total_peers * 1.0/3.0 {
            echo_messages.get_mut(&(reliable_message.clone(), echo_owner)).unwrap().insert(echo_owner);
        }

        // If this list has 2t+1 peers in it, add the echo owner (D) to the messages list 
        if echo_messages.get(&(reliable_message.clone(), echo_owner)).unwrap().len() as f64 > total_peers * 2.0/3.0 {
            if messages.contains_key(&reliable_message) {
                messages.get_mut(&reliable_message).unwrap().insert(echo_owner);
            } else {
                let mut peers: HashSet<PeerId> = HashSet::new();
                peers.insert(echo_owner);
                messages.insert(reliable_message.clone(), peers);
            }
        }

        // If this list has t+1 peers in it, and I haven't send an ack before, sent it
        if echo_messages.get(&(reliable_message.clone(), echo_owner)).unwrap().len() as f64 > total_peers * 1.0/3.0 &&
            !ack_tracker.contains(&(reliable_message.clone(), source)) {
            
            ack_tracker.insert((reliable_message.clone(), source));

            let ack = Message::RBMessage(RBMessage::Ack(reliable_message.clone(), source.to_string(), rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&ack).unwrap();
            let result: Result<MessageId, PublishError> = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        }


        // Check if I have received 2t/3 echoes for this message
        // Check if the RB owner sent this message as well
            // If yes, publish a report
        if !accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) && 
            messages.contains_key(&reliable_message) {
            
            
            if messages.get(&reliable_message).unwrap().contains(&reliable_message.source.parse().unwrap()) &&
            messages.get(&reliable_message).unwrap().len() as f64 > total_peers * 2.0/3.0 {

            let report = Report {
                message_owner: reliable_message.source.to_string(),
                message_id: reliable_message.message_id.to_string(),
                data: reliable_message.data.clone(),
                peers: messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect(),
            };

            // Accept my own report
            accepted_reports.insert((my_peer_id, reliable_message.message_id.clone()), report.clone());

            let report = Message::WMessage(WMessage::Report(report, rand_chacha.next_u64()));
            let serialized_message = serde_json::to_string(&report).unwrap();
            let result = client.gossip_publish(topic.to_string(), serialized_message.into_bytes()).await;
        }}


        // Update my report if I have it
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let report = accepted_reports.get_mut(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            report.peers = messages.get(&reliable_message).unwrap().iter().map(|p| p.to_string()).collect();
        }


        // Update the Accepted/Waitlisted Reports
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let mut to_remove = Vec::new();

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
        }            


        // Check if AcceptedReports reached 2t/3,  
            // if yes publish a super report
            // update the super report tracker
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

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
                    let result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));

                }
            }
        }

        // If I have a report for this super report
            // Check if any waitlisted super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) &&
            super_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, reliable_message.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut to_remove: Vec<(PeerId, String)> = Vec::new();

            for (peer_id, super_report) in waitlisted_super_reports.iter() {
                if peer_id.1 != reliable_message.message_id {
                    continue;
                }

                let mut is_big_subset = false;
                
                for (_message_id, reports) in super_report.0.iter() {

                    for report in reports {
                        let report_peers = report.peers.clone();

                        let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                        if !report_peers.is_subset(&my_report_peers) {
                            is_big_subset = false;
                        }
                    }
                }

                if is_big_subset {
                    super_reports.insert(peer_id.clone(), super_report.clone());
                    to_remove.push(peer_id.clone());
                }
            }

            for peer_id in to_remove {
                waitlisted_super_reports.remove(&peer_id);
            }
        } 

        
        PublishResult::Idle
    }

    async fn handle_report(
        report: Report,
        peer_id: PeerId,
        my_peer_id: PeerId, 
        topic: String,
        waitlisted_reports: &mut HashMap<(PeerId, String), Report>,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_report_tracker: &mut HashSet<(PeerId, String)>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client
    ) -> PublishResult{


        // If I don't have a report for this message, add it to the waitlist
        if !accepted_reports.contains_key(&(peer_id, report.message_id.clone())) {
            waitlisted_reports.insert((peer_id, report.message_id.clone()), report.clone());
        }
        // If I have a report for this message
            // Check if the incoming report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, report.message_id.clone())) {
            let my_report = accepted_reports.get(&(my_peer_id, report.message_id.clone())).unwrap();
            let my_report_peers = my_report.peers.clone();

            let report_peers = report.peers.clone();

            let report_peers: HashSet<String> = report_peers.iter().cloned().collect();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            if report_peers.is_subset(&my_report_peers) {
                accepted_reports.insert((peer_id, report.message_id.clone()), report.clone());
            } else {
                waitlisted_reports.insert((peer_id, report.message_id.clone()), report.clone());
            }
        }

        // Check if I have received 2t/3 reports for this message
            // If yes, publish a super report
            // update the super report tracker
        let total_peers = client.gossip_all_peers().await.len();

        let reliable_message = ReliableMessage {
            source: report.message_owner.to_string(),
            message_id: report.message_id.to_string(),
            data: report.data.to_string(),
        };
            
        if accepted_reports.contains_key(&(my_peer_id, reliable_message.message_id.clone())) {

            let mut report_count: HashMap<String, usize> = HashMap::new();
            for (_peer_id, message_id) in accepted_reports.keys() {
                let count: &mut usize = report_count.entry(message_id.clone()).or_insert(0);
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
                    let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

                    super_report_tracker.insert((my_peer_id, message_id.clone()));
                }
            }
        }

        PublishResult::Idle
        
        
    }

    async fn handle_super_report(
        super_report: SuperReport,
        source: PeerId,
        my_peer_id: PeerId,
        topic: String,
        accepted_reports: &mut HashMap<(PeerId, String), Report>,
        super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        waitlisted_super_reports: &mut HashMap<(PeerId, String), SuperReport>,
        ultra_reports: &mut HashMap<(PeerId, String), UltraReport>,
        rand_chacha: &mut rand_chacha::ChaCha20Rng,
        client: &mut Client,
    ) -> PublishResult {


        // If I don't have a report for this super report, add it to the waitlist
        if !super_reports.contains_key(&(my_peer_id, super_report.0.keys().next().unwrap().to_string())) {
            waitlisted_super_reports.insert((source, super_report.0.keys().next().unwrap().to_string()), super_report.clone());
        }

        // If I have a report for this super report
            // Check if the incoming super report needs to be accepted
        if accepted_reports.contains_key(&(my_peer_id, super_report.0.keys().next().unwrap().to_string())) {

            // Check if my report is a superset of every report in the incoming super report
            let my_report = accepted_reports.get_mut(&(my_peer_id, super_report.0.keys().next().unwrap().to_string())).unwrap();
            let my_report_peers = my_report.peers.clone();
            let my_report_peers: HashSet<String> = my_report_peers.iter().cloned().collect();

            let mut is_big_subset = true;

            for (_peer_id, report_list) in super_report.0.iter() {
                for report in report_list {
                    let report_peers = report.peers.clone();

                    let report_peers: HashSet<String> = report_peers.iter().cloned().collect();

                    if !report_peers.is_subset(&my_report_peers) {
                        waitlisted_super_reports.insert((source, super_report.0.keys().next().unwrap().to_string()), super_report.clone());
                        is_big_subset = false;
                    }
                }
            }
            if is_big_subset {
                super_reports.insert((source, super_report.0.keys().next().unwrap().to_string()), super_report.clone());
            }
        } 

        // Check if I have received 2t/3 super reports for this message
            // If yes, publish a ultra report
            // update the ultra report tracker
        let total_peers = client.gossip_all_peers().await.len();

        let mut report_count: HashMap<String, usize> = HashMap::new();

        for (_peer_id, message_id) in super_reports.keys() {
            let count = report_count.entry(message_id.clone()).or_insert(0);
            *count += 1;
        }

        for (message_id, count) in report_count.iter() {
            if 2.0/3.0 < (*count as f64 / total_peers as f64) && !ultra_reports.contains_key(&(my_peer_id, message_id.clone())) {

                // Iterate through the super reports and add the peers to the ultra report
                let mut super_report_list = Vec::new();

                for (_peer_id, super_report) in super_reports.iter() {
                    if &super_report.0.keys().next().unwrap() == &message_id {
                        super_report_list.push(super_report.clone());
                    }
                }

                let mut super_report_map = HashMap::new();
                super_report_map.insert(message_id.clone(), super_report_list);

                let ultra_report = UltraReport(super_report_map);

                ultra_reports.insert((my_peer_id, message_id.clone()), ultra_report.clone());

                let ultra_report = Message::VMessage(VMessage::UltraReport(ultra_report, rand_chacha.next_u64()));
                let serialized_message = serde_json::to_string(&ultra_report).unwrap();
                let _result = client.gossip_publish(topic.clone(), serialized_message.into_bytes()).await;

            }
        }

        PublishResult::Idle
    }

    async fn handle_ultra_report(
        ultra_report: UltraReport,
        source: PeerId,
        ultra_reports: &mut HashMap<(PeerId, String), UltraReport>,
    ) -> PublishResult {

        // Add the ultra report to the list
        ultra_reports.insert((source, ultra_report.0.keys().next().unwrap().to_string()), ultra_report.clone());

        PublishResult::Idle
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
enum Message {
    RBMessage(RBMessage),
    WMessage(WMessage),
    VMessage(VMessage),
    EvilMessage,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct EvilMessage;

#[derive(Serialize, Deserialize, Debug, Clone)]
enum RBMessage {
    Message(String), // message
    MessageEcho(ReliableMessage, u64), // RB 
    Ack(ReliableMessage, String, u64), // C said B acked A (so B), RB
    AckEcho(AckEcho, u64), // D (me) received ack from C about B's Echo on A's Message. RB is on A, First String is on B, Second String is on C. 
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct AckEcho{
    reliable_message: ReliableMessage,
    echo_owner: String, // Echo Owner
    ack_owner: String, // Ack Owner
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