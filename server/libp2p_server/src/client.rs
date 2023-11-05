
use libp2p::kad::GetClosestPeersError;
use libp2p::kad::Kademlia;
use libp2p::kad::KademliaEvent;
use libp2p::kad::QueryId;
use libp2p::kad::QueryResult;
use libp2p::kad::store::MemoryStore;
use tokio_stream::StreamExt;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::macros::support::Future;
use tokio::macros::support::Pin;

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

    pub async fn bootstrap_to(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::BootstrapTo {peer_id, peer_addr, one_sender })
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
    pub async fn send_data(
        &mut self,
        peer_id: PeerId,
        data: Vec<u8>,
    ) -> Result <Vec<u8>, Box<dyn Error + Send>> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::SendData { data, peer_id, one_sender })
            .await
            .expect("Failed to send data");
        one_receiver.await.expect("Receiver has not responded to SendData command")

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

    /// Confirm a request.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Client` to confirm the request with.
    /// * `channel` - The `ResponseChannel` to confirm the request for.
    pub async fn confirm_request(
        &mut self,
        channel: ResponseChannel<AuthenticatedResponse>,
    ) {
        self.sender
            .send(Command::ConfirmRequest { channel: channel })
            .await
            .expect("Could not respond");
    }


    /// Get the list of peers closest to the given key.
    /// 
    /// # Arguments
    /// 
    /// * `self` - The `Client` to get the list of peers from.
    /// * `key` - The key to get the list of peers closest to.

    pub async fn get_closest_peers(&mut self, key: Vec<u8>) -> HashSet<PeerId> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::GetClosestPeers { key, one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to GetClosestPeers command")
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
                                .send({ Event::InboundRequest { request: request.0, channel }})
                                .await
                                .expect("Failed to make a request");
                        }
                    }
            },
            /// Kademlia Events: https://docs.rs/libp2p-kad/latest/libp2p_kad/enum.QueryResult.html
            /// All of the come in the form of: OutboundQueryProgressed
            /// GetClosestPeers
            /// 
            // SwarmEvent::Behaviour(ComposedEvent::Kademlia(
            //     KademliaEvent::OutboundQueryProgressed { id, result: QueryResult::GetClosestPeers(result), stats, step })) => {
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
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(event)) => {println!("Kademlia event: {:?}", event);},
            SwarmEvent::Behaviour(ComposedEvent::Gossipsub(event)) => {println!("Gossipsub event: {:?}", event);},
            SwarmEvent::Behaviour(ComposedEvent::Mdns(event)) => {println!("Mdns event: {:?}", event);},
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
            Command::BootstrapTo { peer_id, peer_addr, one_sender } => {  
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, peer_addr.clone());
                
                one_sender.send(Ok(()));
            }
            Command::SendData { data, peer_id, one_sender } => {
                let request_id = self.swarm.behaviour_mut().request_response.send_request(&peer_id, AuthenticatedRequest(data));
                self.pending_requests.insert(request_id, one_sender);
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
            Command::ConfirmRequest { channel } => {
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, AuthenticatedResponse("Success".to_string().into_bytes()))
                    .expect("Responding to the request");
            }
            Command::GetClosestPeers { key, one_sender } => {
                let request_id = self.swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(key);
                self.pending_behaviour_requests.insert(request_id, one_sender);
            },
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
    BootstrapTo {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        one_sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    SendData {
        data: Vec<u8>,
        peer_id: PeerId,
        one_sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    ConfirmRequest {
        channel: ResponseChannel<AuthenticatedResponse>,
    },
    GetDialedPeers {
        one_sender: oneshot::Sender<HashSet<PeerId>>,
    },
    GetClosestPeers {
        key: Vec<u8>,
        one_sender: oneshot::Sender<HashSet<PeerId>>,
    },
}
#[allow(unused)]
#[derive(Debug)]
pub enum Event {
    InboundRequest {
        request: Vec<u8>,
        channel: ResponseChannel<AuthenticatedResponse>,
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





