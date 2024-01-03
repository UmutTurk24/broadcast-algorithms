use libp2p::kad::{GetClosestPeersError,Kademlia,KademliaEvent,QueryId, QueryResult, QueryStats, ProgressStep, Quorum, Record};
use libp2p::kad::store::MemoryStore;

use tokio_stream::StreamExt;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::macros::support::{Future, Pin};

use libp2p::multiaddr::Protocol;
use libp2p::{PeerId, Multiaddr, Swarm, mdns, gossipsub};
use libp2p::swarm::{NetworkBehaviour, Executor, SwarmEvent};
use libp2p::request_response::{self, RequestId, ResponseChannel};
use serde::{Deserialize, Serialize};

use void::Void;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;

#[derive(Clone)]
pub struct Client {
    pub sender: mpsc::Sender<Command>,
}
#[allow(unused)]
impl Client {
    /// Listen for incoming connections on the given address.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Client` to start listening with.
    /// * `addr` - The `Multiaddr` to listen on.
    pub async fn start_listening(
        &mut self,
        addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, one_sender })
            .await
            .expect("command receiver dropped");
        one_receiver.await.expect("start listening failed")
    }

    /// Dial the given peer at the given address.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Client` to dial with.
    /// * `peer_id` - The `PeerId` to dial.
    /// * `peer_addr` - The `Multiaddr` to dial.
    pub async fn dial(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial {peer_id, peer_addr, one_sender })
                .await
                .expect("Failed to send dial command");
        one_receiver.await.expect("Receiver has not responded")
    }

    /// Send data to the given peer.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Client` to send data with.
    /// * `peer_id` - The `PeerId` to send data to.
    /// * `data` - The data to send.
    ///
    /// # Returns
    ///
    /// The response from the peer as a `Vec<u8>`.
    pub async fn send_rr(
        &mut self,
        peer_id: PeerId,
        data: Vec<u8>,
    ) -> Result <Vec<u8>, Box<dyn Error + Send>> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::SendRR { data, peer_id, one_sender })
            .await
            .expect("Failed to send data");
        one_receiver.await.expect("Receiver has not responded to SendRR command")
    }

    /// Confirm a request.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Client` to confirm the request with.
    /// * `channel` - The `ResponseChannel` to confirm the request for.
    pub async fn recv_rr(
        &mut self,
        channel: ResponseChannel<AuthenticatedResponse>,
    ) {
        self.sender
            .send(Command::RecvRR { channel: channel })
            .await
            .expect("Could not respond");
    }
    
    /// Get the list of peers.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Client` to get the list of peers from.
    ///
    /// # Returns
    ///
    /// A `HashSet` of `PeerId`s representing the list of peers.
    pub async fn get_dialed_peers(&mut self) -> HashSet<PeerId> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::GetDialedPeers { one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to GetDialedPeers command")
    }

    pub async fn kd_add_address(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::KdAddAddress {peer_id, peer_addr, one_sender })
                .await
                .expect("Failed to send dial command");
        one_receiver.await.expect("Receiver has not responded")
    }

    // https://docs.rs/libp2p-kad/latest/libp2p_kad/struct.Behaviour.html

    /*
        Each Kademlia Event Request gives out a request id. 
        Depending on the request that is being made, it is client's responsibility to match
        the request id with the response id.
     */

    pub async fn kd_get_closest_peers(&mut self, key: Vec<u8>) -> QueryId {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::KDGetClosestPeers { key, one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to KDGetClosestPeers command")
    }

    pub async fn kd_start_providing(&mut self, key: Vec<u8>) -> QueryId {
        let (one_sender, one_receiver) = oneshot::channel();

        self.sender
            .send(Command::KDStartProviding { key, one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to KDStartProviding command")
    }

    pub async fn kd_get_providers(&mut self, key: Vec<u8>) -> QueryId {
        let (one_sender, one_receiver) = oneshot::channel();

        self.sender
            .send(Command::KDGetProviders { key, one_sender })
            .await
            .expect("Command receiver not to be dropped.");

        one_receiver.await.expect("Receiver has not responded to KDGetProviders command")
    }

    pub async fn kd_get_record(&mut self, key: Vec<u8>) -> QueryId {
        let (one_sender, one_receiver) = oneshot::channel();

        self.sender
            .send(Command::KDGetRecord { key, one_sender })
            .await
            .expect("Command receiver not to be dropped.");

        one_receiver.await.expect("Receiver has not responded to KDGetRecord command")

    }

    pub async fn kd_put_record(&mut self, record: Record, quorum: Option<Quorum>) -> QueryId {
        let (one_sender, one_receiver) = oneshot::channel();
        
        if (quorum.is_none()) { 
            let default_quorum = Quorum::One;
            self.sender
                .send(Command::KDPutRecord { record, quorum: Some(default_quorum), one_sender })
                .await
                .expect("Command receiver not to be dropped.");
        } else {
            self.sender
                .send(Command::KDPutRecord { record, quorum, one_sender })
                .await
                .expect("Command receiver not to be dropped.");
        }
        one_receiver.await.expect("Receiver has not responded to KDPutRecord command")
    }

    // https://docs.rs/libp2p-gossipsub/latest/libp2p_gossipsub/struct.Behaviour.html

    /*
        Similar to Kademlia events, Gossipsub publish event also give out a request id
    */

    pub async fn gossip_publish(&mut self, topic: String, message: Vec<u8>) -> Result<gossipsub::MessageId, gossipsub::PublishError> {
        let (one_sender, one_receiver) = oneshot::channel();

        self.sender
            .send(Command::GossipPublish { topic, message, one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to GossipPublish command")
    }

    pub async fn gossip_subscribe(&mut self, topic: String) {
        self.sender
            .send(Command::GossipSubscribe { topic })
            .await
            .expect("Command receiver not to be dropped.");
    }

    pub async fn gossip_all_peers(&mut self) -> HashSet<(PeerId)> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::GossipAllPeers{ one_sender})
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to GossipAllPeers command")
    }

    pub async fn gossip_mesh(&mut self, topic: String) -> HashSet<PeerId> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::GossipMesh{ topic, one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to GossipAllPeers command")
    }

}

/// A struct representing the event loop for a libp2p client.
pub struct EventLoop {
    swarm: Swarm<ComposedBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_requests: HashMap<RequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
    pending_behaviour_requests: HashMap<QueryId, oneshot::Sender<HashSet<PeerId>>>,
}
#[allow(unused)]
impl EventLoop {

    /// Creates a new `EventLoop` with the given `Swarm`, `mpsc::Receiver<Command>`, and `mpsc::Sender<Event>`.
    ///
    /// # Arguments
    ///
    /// * `swarm` - The `Swarm` to use for the event loop.
    /// * `command_receiver` - The `mpsc::Receiver<Command>` to use for receiving commands.
    /// * `event_sender` - The `mpsc::Sender<Event>` to use for sending events.

    pub fn new(
        swarm: Swarm<ComposedBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm, 
            command_receiver,
            event_sender,
            pending_requests: Default::default(),
            pending_behaviour_requests: Default::default(),
        }
    }

    /// Runs the event loop for the client.
    ///
    /// # Arguments
    ///
    /// * `self` - The `EventLoop` to run.
    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                event = self.swarm.next() => self.handle_event(event.expect("Swarm stream to be infinite.")).await,
                command = self.command_receiver.recv() => match command {
                    Some(c) => self.handle_command(c).await,
                    None => return,
                },
            }
        }
    }

    /// Handles a libp2p event.
    ///
    /// # Arguments
    ///
    /// * `self` - The `EventLoop` to handle the event for.
    /// * `event` - The `SwarmEvent` to handle.
    async fn handle_event(
        &mut self,
        event: SwarmEvent<ComposedEvent, either::Either<either::Either<either::Either<Void, std::io::Error>, Void>, Void>>,
    ) {
        match event {
            /// Handling Composed Event Behaviours
            /// 
            /// 
            /// ComposedEvent::RequestResponse
            /// Message::Response
            /// Message::Request
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                request_response::Event::Message {message, peer})) => { 
                    match message {
                        request_response::Message::Response { request_id, response } => {
                            // Send the response to the pending request
                            let _ = self
                                .pending_requests
                                .remove(&request_id)
                                .expect("Request to still be pending.")
                                .send(Ok(response.0));
                        }
                        request_response::Message::Request { request, channel, .. } => {
                            // Send the request to the event sender
                            self.event_sender
                                .send({ Event::RRRequest { request: request.0, channel }})
                                .await
                                .expect("Failed to make a request");
                        }
                    }
            },
            /// Kademlia Events: https://docs.rs/libp2p-kad/latest/libp2p_kad/enum.QueryResult.html
            /// All of the come in the form of: OutboundQueryProgressed
            /// KDGetClosestPeers
            /// 
            // SwarmEvent::Behaviour(ComposedEvent::Kademlia(
            //     KademliaEvent::OutboundQueryProgressed { id, result: QueryResult::KDGetClosestPeers(result), stats, step })) => {
            //         println!("Outbound query progressed: {:?}, {:?}, {:?}, {:?}", id, result, stats, step);
            // },
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(KademliaEvent::OutboundQueryProgressed {
            result: QueryResult::GetClosestPeers(result),
            ..
            })) => {
               
                match result {
                    Ok(ok) => {
                        if !ok.peers.is_empty() {
                            println!("Query finished with closest peers: {:#?}", ok.peers)
                        } else {
                            // The example is considered failed as there
                            // should always be at least 1 reachable peer.
                            println!("Query finished with no closest peers.")
                        }
                    }
                    Err(GetClosestPeersError::Timeout { peers, .. }) => {
                        if !peers.is_empty() {
                            println!("Query timed out with closest peers: {peers:#?}")
                        } else {
                            // The example is considered failed as there
                            // should always be at least 1 reachable peer.
                            println!("Query timed out with no closest peers.");
                        }
                    }
                }
            },

            SwarmEvent::Behaviour(ComposedEvent::Kademlia(KademliaEvent::OutboundQueryProgressed { id, result, stats, step })) => {
                self.event_sender
                    .send({ Event::KDProgressed { id, result, stats, step }})
                    .await
                    .expect("Failed to make a request");
            }
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(event)) => {println!("Kademlia event: {:?}", event);},
            SwarmEvent::Behaviour(ComposedEvent::Gossipsub(event)) => {

                println!("Gossipsub event: {:?}", event);
                match event {
                    gossipsub::Event::Message {propagation_source, message_id, message } => {
                        println!("Received message: {:?} from {:?}", String::from_utf8_lossy(&message.data), propagation_source);
                        self.event_sender
                            .send({ Event::GossipMessage { propagation_source, message_id, message }})
                            .await
                            .expect("Failed to make a request");
                    }
                    gossipsub::Event::Subscribed { peer_id, topic } => {
                        println!("Subscribed to {:?} from {:?}", topic, peer_id);
                    }
                    gossipsub::Event::Unsubscribed { peer_id, topic } => {
                        println!("Unsubscribed from {:?} from {:?}", topic, peer_id);
                    }
                    _ => {}
                }
            },
            SwarmEvent::Behaviour(ComposedEvent::Mdns(event)) => {
                println!("Mdns event: {:?}", event);
                match event {
                    mdns::Event::Discovered(list) => {
                        
                        for (peer_id, multiaddr) in list {
                            println!("Discovered {:?} with address {}", peer_id, multiaddr);

                            // Dial the discovered peer if not dialed before
                            let dial_result = self.swarm.dial(multiaddr.clone().with(Protocol::P2p(peer_id)));

                            if dial_result.is_err() {
                                println!("Failed to dial {:?} \n", multiaddr);
                                continue;
                            }
                            
                            // Add the discovered peers to the kademlia routing table
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, multiaddr.clone());

                            // Add the discovered peers to the gossipsub routing table
                            self.swarm
                                .behaviour_mut()
                                .gossipsub
                                .add_explicit_peer(&peer_id);
                        }
                    }
                    mdns::Event::Expired(expired) => {
                        for (peer_id, multiaddr) in expired {
                            println!("Expired {:?} with address {}", peer_id, multiaddr);
                            self.swarm
                                .behaviour_mut()
                                .kademlia
                                .remove_address(&peer_id, &multiaddr);

                            self.swarm
                                .behaviour_mut()
                                .gossipsub
                                .remove_explicit_peer(&peer_id);
                        }
                    }
                }
            },
            SwarmEvent::Behaviour(event) => {println!("General event: {:?}", event);},
            SwarmEvent::Dialing { peer_id: Some(peer_id), .. } => { },
            SwarmEvent::IncomingConnection { .. } => {},
            SwarmEvent::IncomingConnectionError { .. } => {},
            SwarmEvent::ConnectionClosed { .. } => {},
            SwarmEvent::ConnectionEstablished { .. } => {},
            SwarmEvent::OutgoingConnectionError { .. } => {},
            SwarmEvent::NewListenAddr { .. } => {},
            SwarmEvent::ExpiredListenAddr { .. } => {},
            SwarmEvent::ListenerClosed { .. } => {},
            SwarmEvent::ListenerError { .. } => {},
            _ => {}
        }

    }

    /// Handles a command received from the command receiver.
    ///
    /// # Arguments
    ///
    /// * `self` - The `EventLoop` to handle the command for.
    /// * `command` - The `Command` to handle.
    async fn handle_command(&mut self, command: Command) {
        match command {
            
            Command::StartListening { addr, one_sender } => {
                let result = self.swarm.listen_on(addr.clone()).expect("Failed to listen on address {addr}");
                // println!("Listening on {:?}", addr);
                one_sender.send(Ok(()));
            }
            Command::Dial { peer_id, peer_addr, one_sender } => {  
                // Dial the peer              
                println!("Res 2: {:?}", peer_addr.clone().with(Protocol::P2p(peer_id)));
                let dial_result = self.swarm.dial(peer_addr.clone().with(Protocol::P2p(peer_id)));
                if dial_result.is_err() {
                    println!("Failed to dial {:?}", peer_addr);
                }

                // Add the dialed peer to the kademlia routing table
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, peer_addr.clone());

                one_sender.send(Ok(()));
            }
            Command::KdAddAddress { peer_id, peer_addr, one_sender } => {  
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, peer_addr.clone());
                
                one_sender.send(Ok(()));
            }
            Command::SendRR { data, peer_id, one_sender } => {
                let request_id = self.swarm.behaviour_mut().request_response.send_request(&peer_id, AuthenticatedRequest(data));
                self.pending_requests.insert(request_id, one_sender);
            }
            Command::RecvRR { channel } => {
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, AuthenticatedResponse("Success".to_string().into_bytes()))
                    .expect("Responding to the request");
            }
            Command::GetDialedPeers { one_sender } => {
                let peers = self.swarm.connected_peers();
                let mut peer_list: HashSet<PeerId> = HashSet::new();
                for peer in peers {
                    let peer_id = peer.clone();
                    peer_list.insert(peer_id);
                }
                one_sender.send(peer_list);
            }
            Command::KDGetClosestPeers { key, one_sender } => {
                let request_id = self.swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(key);
                one_sender.send(request_id);
            },
            Command::KDStartProviding { key, one_sender } => {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(key.into());
            },
            Command::KDGetProviders { key , one_sender} => {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .get_providers(key.into());
            },
            Command::KDGetRecord { key , one_sender} => {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .get_record(key.into());
            },
            Command::KDPutRecord { record, quorum, one_sender } => {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .put_record(record, quorum.unwrap());
            },
            Command::GossipPublish { topic, message, one_sender } => {
                let message_id = self.swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(gossipsub::IdentTopic::new(topic), message);
                one_sender.send(message_id);
            },
            Command::GossipSubscribe { topic } => {
                self.swarm
                    .behaviour_mut()
                    .gossipsub
                    .subscribe(&gossipsub::IdentTopic::new(topic));
            },
            Command::GossipAllPeers { one_sender } => {
                let peers = self.swarm.behaviour_mut().gossipsub.all_peers();
                let mut peer_list = HashSet::new();
                // Return all of the peer ids, without their associated topics
                for peer in peers {
                    let peer_id = peer.0.clone();
                    peer_list.insert(peer_id);
                }
                one_sender.send(peer_list);
            },
            Command::GossipMesh { topic, one_sender } => {
                let peers = 
                    self.swarm
                    .behaviour_mut()
                    .gossipsub
                    .mesh_peers(&gossipsub::TopicHash::from_raw(topic));

                let mut peer_list = HashSet::new();

                for peer in peers {
                    peer_list.insert(peer.clone());
                }
                one_sender.send(peer_list);
            }

        }
    }
}
#[allow(unused)]
#[derive(Debug)]
pub enum Command {
    StartListening {
        addr: Multiaddr,
        one_sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        one_sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    KdAddAddress {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        one_sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    SendRR {
        data: Vec<u8>,
        peer_id: PeerId,
        one_sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    RecvRR {
        channel: ResponseChannel<AuthenticatedResponse>,
    },
    GetDialedPeers {
        one_sender: oneshot::Sender<HashSet<PeerId>>,
    },
    KDGetClosestPeers {
        key: Vec<u8>,
        one_sender: oneshot::Sender<QueryId>,
    },
    KDStartProviding {
        key: Vec<u8>,
        one_sender: oneshot::Sender<QueryId>,
    },
    KDGetProviders {
        key: Vec<u8>,
        one_sender: oneshot::Sender<QueryId>,
    },
    KDGetRecord {
        key: Vec<u8>,
        one_sender: oneshot::Sender<QueryId>,
    },
    KDPutRecord {
        record: Record,
        quorum: Option<Quorum>,
        one_sender: oneshot::Sender<QueryId>,
    },
    GossipPublish {
        topic: String,
        message: Vec<u8>,
        one_sender: oneshot::Sender<Result<gossipsub::MessageId, gossipsub::PublishError>>,
    },
    GossipSubscribe {
        topic: String,
    },
    GossipAllPeers {
        one_sender: oneshot::Sender<HashSet<(PeerId)>>,
    },
    GossipMesh {
        topic: String,
        one_sender: oneshot::Sender<HashSet<PeerId>>,
    },

}
#[allow(unused)]
#[derive(Debug)]
pub enum Event {
    RRRequest {
        request: Vec<u8>,
        channel: ResponseChannel<AuthenticatedResponse>,
    },
    KDProgressed {
        id: QueryId,
        result: QueryResult,
        stats: QueryStats,
        step: ProgressStep,
    },
    GossipMessage {
        propagation_source: PeerId, 
        message_id: gossipsub::MessageId,
        message: gossipsub::Message,
    },
}

pub async fn new_executor() -> TokioExecutor {
    let executor = TokioExecutor;
    executor
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "ComposedEvent")]
pub struct ComposedBehaviour {
    pub request_response: request_response::cbor::Behaviour<AuthenticatedRequest, AuthenticatedResponse>,
    pub kademlia: Kademlia<MemoryStore>,
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}


#[derive(Debug)]
pub enum ComposedEvent {
    RequestResponse(request_response::Event<AuthenticatedRequest, AuthenticatedResponse>),
    Kademlia(KademliaEvent),
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<request_response::Event<AuthenticatedRequest, AuthenticatedResponse>> for ComposedEvent {
    fn from(event: request_response::Event<AuthenticatedRequest, AuthenticatedResponse>) -> Self {
        ComposedEvent::RequestResponse(event)
    }
}

impl From<KademliaEvent> for ComposedEvent {
    fn from(event: KademliaEvent) -> Self {
        ComposedEvent::Kademlia(event)
    }
}

impl From<gossipsub::Event> for ComposedEvent {
    fn from(event: gossipsub::Event) -> Self {
        ComposedEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for ComposedEvent {
    fn from(event: mdns::Event) -> Self {
        ComposedEvent::Mdns(event)
    }
}

pub struct TokioExecutor;

impl Executor for TokioExecutor {
    fn exec(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }
}

// Simple data exchange protocol
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticatedRequest(Vec<u8>);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticatedResponse(Vec<u8>);





