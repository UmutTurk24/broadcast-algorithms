use client::ComposedBehaviour;
use client::EventLoop;
use client::new_executor;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

use libp2p::core::upgrade::Version;
use libp2p::{identity, PeerId, tcp, Transport, StreamProtocol, yamux, noise, Multiaddr};
use libp2p::swarm::SwarmBuilder;
use libp2p::request_response::{self, ProtocolSupport};

use std::collections::HashMap;
use std::error::Error;

mod client;
use client::{Client, Event};

pub struct Servers {
    pub peer_list: HashMap<u32, (PeerId, Multiaddr)>,
    pub sender: HashMap<u32, tokio::sync::mpsc::Sender<UserEvent>>,
}

impl Servers {
    /// Creates a new `Servers` instance.
    pub fn new() -> Self {
        Servers {
            peer_list: HashMap::new(),
            sender: HashMap::new(),
        }
    }

    /// Initializes the servers and creates channels for communication.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Servers` instance to initialize.
    /// * `number` - The number of servers to initialize.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the servers were successfully initialized, or an error if initialization failed.
    pub async fn init_servers(&mut self, number: u32) -> Result<(), Box<dyn std::error::Error>> {

        for i in 0..number {
            let listening_address = create_listen_address(i as u32);
            let (client, event_receiver, peer_id) = 
                P2PServer::initialize_server(i as u8,listening_address.clone()).await?;

            // Create the channel for the user to send events to the server
            let (user_sender, user_receiver) = tokio::sync::mpsc::channel::<UserEvent>(100);
            self.sender.insert(i,user_sender);
            client_receiver_behaviour(client, event_receiver, user_receiver).await;

            let peer_addr = define_peer_addr(peer_id, listening_address.clone());
            self.peer_list.insert(i, (peer_id.clone(), peer_addr));
        }
        Ok(())
    }

    /// Connects the servers to each other.
    ///
    /// # Arguments
    ///
    /// * `self` - The `Servers` instance to connect.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the servers were successfully connected, or an error if connection failed.
    pub async fn connect_servers(&mut self) -> Result<(), Box<dyn std::error::Error>> {

        let number_servers = self.peer_list.len();

        // Grab each server and dial the other servers
        for i in 0..number_servers {
            let current_sender = self.sender.get(&(i as u32)).unwrap();

            for j in 0..number_servers {
                if i != j {
                    let (peer_id, peer_addr) = self.peer_list.get(&(j as u32)).unwrap();
                    current_sender.send(UserEvent::DialPeer(peer_id.clone(), peer_addr.clone()))
                        .await.expect("Could not dial the peer");
                }
            }
        }
        Ok(())
    }

}
fn create_listen_address(number: u32) -> String {
    return format!("/ip4/127.0.0.1/tcp/{}", 40820 + number);
}

/// Define the peer address for the given `PeerId` and `Multiaddr`.
///
/// # Arguments
///
/// * `peer_id` - The `PeerId` to include in the address.
/// * `peer_addr` - The `Multiaddr` to include in the address.
///
/// # Returns
///
/// The `Multiaddr` representing the peer address.
fn define_peer_addr(peer_id: PeerId, peer_addr: String) -> Multiaddr {
    let peer_addr: Multiaddr = format!("{}/p2p/{}", peer_addr, peer_id.to_base58()).parse::<Multiaddr>().unwrap();
    return peer_addr;
}

/// The behavior of the client receiver.
///
/// # Arguments
///
/// * `client` - The `Client` to use for communication.
/// * `event_receiver` - The `Receiver` for network events.
/// * `receiver` - The `Receiver` for user events.
async fn client_receiver_behaviour(mut client: Client, mut event_receiver: Receiver<Event>, mut receiver: Receiver<UserEvent>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                user_event = receiver.recv() => {
                    match user_event {
                        Some(UserEvent::DialPeer(peer_id, peer_addr)) => {
                            client.dial(peer_id, peer_addr).await.expect("Could not dial the peer");
                        },
                        Some(UserEvent::SendData(data, peer_id)) => {
                            println!("Sending data {:?}", data.clone());
                            client.send_data(peer_id, data).await.expect("Successfully sent data");
                        },
                        Some(UserEvent::Broadcast(data)) => {
                            let dialed_peers = client.get_peers().await; 
                            for peer in dialed_peers {
                                client.send_data(peer, data.clone()).await.expect("Could not send data");
                            }
                            println!("Broadcasting data");
                        },
                        None => {},
                    }
                }
                network_event = event_receiver.recv() => {
                    match network_event {
                        Some(Event::InboundRequest { request, channel }) => {
                            println!("Received request: {:?}", request);
                            client.confirm_request( channel ).await;
                        }
                        None => {},
                    }
                }
            }
        }
    });
}

#[derive(Debug)]
pub enum UserEvent {
    DialPeer(PeerId, Multiaddr),
    SendData(Vec<u8>, PeerId),
    Broadcast(Vec<u8>),
}

pub struct P2PServer;

/// Initialize a P2P server with the given secret key seed and listen address.
///
/// # Arguments
///
/// * `secret_key_seed` - The seed for the secret key used to identify the server.
/// * `listen_address` - The address to listen on for incoming connections.
///
/// # Returns
///
/// A tuple containing a `Client` for sending commands to the server, a `Receiver` for receiving events from the server, and the `PeerId` of the server.
///
/// # Errors
///
/// Returns an error if there was a problem initializing the server.
impl P2PServer {
    
    pub async fn initialize_server(secret_key_seed: u8, listen_address: String) -> Result<(Client, Receiver<Event>, PeerId), Box<dyn Error>> {
        // Generate a local keypair and peer ID from the secret key seed
        let mut bytes = [0u8; 32];
        bytes[0] = secret_key_seed;
        let local_key = identity::Keypair::ed25519_from_bytes(bytes).unwrap();
        let peer_id = PeerId::from(local_key.public());
        
        // Create a new executor for the server
        let executor = new_executor().await;

        // Create the behavior for the server
        let self_behaviour = ComposedBehaviour {
            request_response: request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/message-exchange/1"),
                    ProtocolSupport::Full,
                )],
                request_response::Config::default(),
            ),
        };

        // Create the transport for the server
        let transport = tcp::tokio::Transport::new(Default::default())
            .upgrade(Version::V1Lazy)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(yamux::Config::default())
            .boxed();

        // Build the swarm for the server
        let swarm = 
            SwarmBuilder::with_executor(transport, self_behaviour, peer_id.clone(), executor).build();
    
        // Create channels for sending commands and receiving events
        let (command_sender, command_receiver) = mpsc::channel(32);
        let (event_sender, event_receiver) = mpsc::channel(32);
        
        // Create an event loop for the server and spawn it in a new task
        let event_loop = EventLoop::new(swarm, command_receiver, event_sender);
        tokio::spawn( event_loop.run());

        // Create a client for sending commands to the server
        let mut client = Client {sender: command_sender};

        // Start listening on the given address
        let addr: Multiaddr = listen_address.parse().expect("Failed to parse address");
        client.start_listening(addr).await.expect("Failed to start listening");
        
        // Return the client, event receiver, and peer ID
        Ok((client, event_receiver, peer_id))
    }
    
}
