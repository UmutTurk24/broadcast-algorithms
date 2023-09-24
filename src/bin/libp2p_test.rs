use async_std::io;
use either::Either;


use clap::Parser;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::macros::support::Future;
use tokio::macros::support::Pin;
use tokio::*;
use tokio::sync::mpsc::Receiver;

use libp2p::core::upgrade::Version;
use libp2p::multiaddr::Protocol;
use libp2p::{identity, PeerId, tcp, Transport, StreamProtocol, yamux, noise, Multiaddr, Swarm};
use libp2p::swarm::{SwarmBuilder, behaviour, NetworkBehaviour, Executor, SwarmEvent};
use libp2p::request_response::{self, ProtocolSupport, RequestId, ResponseChannel};
use serde::{Deserialize, Serialize};
use tokio::runtime::{self, Runtime};
use void::Void;
use std::collections::HashMap;
use std::error::Error;
use std::collections::{hash_map, HashSet};

#[allow(unused)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 
    let opt = Opt::parse();

    let (mut client, mut events, event_loop) = new_server(opt.secret_key_seed).await?;
    tokio::spawn( event_loop.run());

    client
        .start_listening(opt.listen_address)
        .await
        .expect("Listening not to fail.");
    
    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();

        tokio::select! {
            res = stdin.read_line(&mut buffer) => {
                match buffer.trim() {
                    "dial" => {
                        let peer_id = match opt.peer.iter().last() {
                            Some(Protocol::P2p(peer_id)) => peer_id,
                            _ => return Err("Expect peer multiaddr to contain peer ID.".into()),
                        };
                        client
                            .dial(peer_id, opt.peer.clone())
                            .await
                            .expect("Dial to succeed");
                    },
                    "send" => {
                        let peer_id = match opt.peer.iter().last() {
                            Some(Protocol::P2p(peer_id)) => peer_id,
                            _ => return Err("Expect peer multiaddr to contain peer ID.".into()),
                        };
                        let data = "CoolData".to_string().into_bytes();
                        client.send_data(peer_id, data).await.expect("Successfully sent data");
                    },
                    "broadcast" => {
                        todo!();
                    },
                    _ => println!("Unknown command"),
                }
            }
            event = events.recv() => {
                match event {
                    Some(Event::InboundRequest { request, channel }) => {
                        println!("Received request: {:?}", request);
                        client.confirm_request( channel ).await;
                    }
                    None => return Ok(()),
                }
            }
        }
        

        
        
        


        
        
    }

    Ok(())
}
#[derive(Parser, Debug)]
#[clap(name = "libp2p_test")]
struct Opt {
    #[clap(long)]
    secret_key_seed: u8,

    #[clap(long)]
    listen_address: Multiaddr,
    
    #[clap(long)]
    peer: Multiaddr,
}


#[allow(unused)]

async fn new_server(secret_key_seed: u8) -> Result<(Client, Receiver<Event>, EventLoop), Box<dyn Error>> {

    let mut bytes = [0u8; 32];
    bytes[0] = secret_key_seed;
    let local_key = identity::Keypair::ed25519_from_bytes(bytes).unwrap();
    let peer_id = PeerId::from(local_key.public());

    let executor = new_executor().await;
    println!("This Peer id: {}", peer_id);
    
    let self_behaviour = ComposedBehaviour {
        request_response: request_response::cbor::Behaviour::new(
            [(
                StreamProtocol::new("/message-exchange/1"),
                ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        ),
    };

    let transport = tcp::tokio::Transport::new(Default::default())
        .upgrade(Version::V1Lazy)
        .authenticate(noise::Config::new(&local_key)?)
        .multiplex(yamux::Config::default())
        .boxed();

    let mut swarm = 
        SwarmBuilder::with_executor(transport, self_behaviour, peer_id, executor).build();

    let (command_sender, command_receiver) = mpsc::channel(32);
    let (event_sender, event_receiver) = mpsc::channel(32);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}


#[derive(Clone)]
pub(crate) struct Client {
    sender: mpsc::Sender<Command>,
}
#[allow(unused)]
impl Client {
    /// Listen for incoming connections on the given address.
    pub(crate) async fn start_listening(
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
    pub(crate) async fn dial(
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
    pub(crate) async fn send_data(
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

    pub(crate) async fn broadcast_data(){
        todo!()
    }

    pub(crate) async fn confirm_request(
        &mut self,
        channel: ResponseChannel<AuthenticatedResponse>,
    ) {
        self.sender
            .send(Command::ConfirmRequest { channel: channel })
            .await
            .expect("Could not respond");
    }

}

pub(crate) struct EventLoop {
    swarm: Swarm<ComposedBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_data_requests: HashMap<RequestId, oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>>,
}
#[allow(unused)]
impl EventLoop {
    fn new(
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
                                .send(Ok("Noice".to_string().into_bytes()));
                            println!("I just responded");
                        
                        }
                        request_response::Message::Request { request, channel, .. } => {
                            self.event_sender
                                .send({ Event::InboundRequest { request: request.0, channel }})
                                .await
                                .expect("GJOB");
                            println!("I made a request!");
                        }
                    }
            },
            SwarmEvent::Behaviour(_) => { 
                println!("Unknown behaviour event");
            },
            SwarmEvent::Dialing { peer_id: Some(peer_id), .. } => 
                println!("Successful dial to {}", peer_id),
            SwarmEvent::IncomingConnection { .. } => {
                println!("Incoming connection!")
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

    async fn handle_command(&mut self, command: Command) {
        match command {
            
            Command::StartListening { addr, one_sender } => {
                let result = self.swarm.listen_on(addr.clone()).expect("Failed to listen on address {addr}");
                println!("Listening on {:?}", addr);
                one_sender.send(Ok(()));
            }
            Command::Dial { peer_id, peer_addr, one_sender } => {               
                let dial_result = self.swarm.dial(peer_addr.clone().with(Protocol::P2p(peer_id)));
                println!("Dialing ... {}", peer_addr.to_string());
                dial_result.expect("Failed to dial");
                one_sender.send(Ok(()));
            }
            Command::SendData { data, peer_id, one_sender } => {

                let request_id = self.swarm.behaviour_mut().request_response.send_request(&peer_id, AuthenticatedRequest(data));
                self.pending_data_requests.insert(request_id, one_sender);
                // one_sender.send(Ok(()));
                // let result = self.swarm.send_data(data, peer).map_err(|e| e.into());
                // let _ = sender.send(result);
            }
            Command::BroadcastData { data, peer_ids, one_sender } => {
                // let result = self.swarm.broadcast_data(data, channel).map_err(|e| e.into());
                // let _ = sender.send(result);
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
enum Command {
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
    BroadcastData {
        data: Vec<u8>,
        peer_ids: Vec<PeerId>,
        one_sender: oneshot::Sender<Result<Vec<u8>, Box<dyn Error + Send>>>,
    },
    ConfirmRequest {
        channel: ResponseChannel<AuthenticatedResponse>,
    },
}
#[allow(unused)]
#[derive(Debug)]
pub(crate) enum Event {
    InboundRequest {
        request: Vec<u8>,
        channel: ResponseChannel<AuthenticatedResponse>,
    },
}

async fn new_executor() -> TokioExecutor {
    let executor = TokioExecutor;
    executor
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "ComposedEvent")]
struct ComposedBehaviour {
    request_response: request_response::cbor::Behaviour<AuthenticatedRequest, AuthenticatedResponse>,
}


#[derive(Debug)]
enum ComposedEvent {
    RequestResponse(request_response::Event<AuthenticatedRequest, AuthenticatedResponse>),
}

impl From<request_response::Event<AuthenticatedRequest, AuthenticatedResponse>> for ComposedEvent {
    fn from(event: request_response::Event<AuthenticatedRequest, AuthenticatedResponse>) -> Self {
        ComposedEvent::RequestResponse(event)
    }
}

struct TokioExecutor;

impl Executor for TokioExecutor {
    fn exec(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }
}

// Simple data exchange protocol
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuthenticatedRequest(Vec<u8>);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuthenticatedResponse(Vec<u8>);
