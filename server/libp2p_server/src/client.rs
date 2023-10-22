
use tokio_stream::StreamExt;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::macros::support::Future;
use tokio::macros::support::Pin;

use libp2p::multiaddr::Protocol;
use libp2p::{PeerId, Multiaddr, Swarm};
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
        let res = one_receiver.await.expect("Receiver has not responded to SendData command");
        res
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
    pub async fn get_peers(&mut self) -> HashSet<PeerId> {
        let (one_sender, one_receiver) = oneshot::channel();
        self.sender
            .send(Command::GetPeers { one_sender })
            .await
            .expect("Command receiver not to be dropped.");
        one_receiver.await.expect("Receiver has not responded to GetPeers command")
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

}

/// A struct representing the event loop for a libp2p client.
pub struct EventLoop {
    swarm: Swarm<ComposedBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_data_requests: HashMap<RequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
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
            pending_data_requests: Default::default(),
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
        event: SwarmEvent<ComposedEvent, Void>,
    ) {
        match event {
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                request_response::Event::Message {message, peer})) => { 
                    match message {
                        request_response::Message::Response { request_id, response } => {

                            let _ = self
                                .pending_data_requests
                                .remove(&request_id)
                                .expect("Request to still be pending.")
                                .send(Ok(response.0));
                            println!("I just responded");
                        
                        }
                        request_response::Message::Request { request, channel, .. } => {
                            self.event_sender
                                .send({ Event::InboundRequest { request: request.0, channel }})
                                .await
                                .expect("Oops, failed to make a request");
                            println!("I made a request!");
                        }
                    }
            },
            SwarmEvent::Behaviour(event) => { 
                println!("Unknown behaviour event{:?} ", event);
            },
            SwarmEvent::Dialing { peer_id: Some(peer_id), .. } => 
            {
                println!("Successful dial to {}", peer_id)
            },
            SwarmEvent::IncomingConnection { .. } => {
                // println!("Incoming connection!")
            },
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
                let dial_result = self.swarm.dial(peer_addr.clone().with(Protocol::P2p(peer_id)));
                dial_result.expect("Failed to dial");
                one_sender.send(Ok(()));
            }
            Command::SendData { data, peer_id, one_sender } => {

                let request_id = self.swarm.behaviour_mut().request_response.send_request(&peer_id, AuthenticatedRequest(data));
                self.pending_data_requests.insert(request_id, one_sender);
                
            }
            Command::GetPeers { one_sender } => {
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
    SendData {
        data: Vec<u8>,
        peer_id: PeerId,
        one_sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    ConfirmRequest {
        channel: ResponseChannel<AuthenticatedResponse>,
    },
    GetPeers {
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
}


#[derive(Debug)]
pub enum ComposedEvent {
    RequestResponse(request_response::Event<AuthenticatedRequest, AuthenticatedResponse>),
}

impl From<request_response::Event<AuthenticatedRequest, AuthenticatedResponse>> for ComposedEvent {
    fn from(event: request_response::Event<AuthenticatedRequest, AuthenticatedResponse>) -> Self {
        ComposedEvent::RequestResponse(event)
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





