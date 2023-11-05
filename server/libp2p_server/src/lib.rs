use client::ComposedBehaviour;
use client::EventLoop;
use client::new_executor;

use libp2p::gossipsub;
use libp2p::identity::Keypair;
use libp2p::kad::store::MemoryStore;
use tokio::fs::File;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::io::AsyncWriteExt;
use tokio::time::Duration;

use libp2p::core::upgrade::Version;
use libp2p::{identity, PeerId, tcp, Transport, StreamProtocol, yamux, noise, Multiaddr, mdns};
use libp2p::swarm::SwarmBuilder;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::kad::{Kademlia, KademliaConfig};

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Cursor;


mod client;
use client::{Client, Event};

pub const BOOT_STARTER: u32 = 1000;

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
                P2PServer::initialize_server(listening_address.clone(), None).await?;

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


pub async fn generate_keys(num: u32) -> Vec<Keypair> {

    let mut file = File::create("boot_nodes.txt").await.unwrap();
    let mut keys = Vec::new();
    
    for i in BOOT_STARTER..(BOOT_STARTER + num) {
        let bytes = transform_u32_to_array_of_u8(i);

        let local_key = identity::Keypair::ed25519_from_bytes(bytes).unwrap();
        let peer_id = PeerId::from(local_key.public()).to_string();
        let mut buffer = Cursor::new(peer_id + "\n") ;

        file.write_all_buf(&mut buffer).await.expect("Failed to write generated peer_id to file");
        keys.push(local_key);
    }

    keys
}

pub fn transform_u32_to_array_of_u8(x:u32) -> [u8;32] {
    let b1 : u8 = ((x >> 24) & 0xff) as u8;
    let b2 : u8 = ((x >> 16) & 0xff) as u8;
    let b3 : u8 = ((x >> 8) & 0xff) as u8;
    let b4 : u8 = (x & 0xff) as u8;

    let mut bytes = [0u8; 32];
    bytes[0] = b1;
    bytes[1] = b2;
    bytes[2] = b3;
    bytes[3] = b4;
    return bytes
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
                            let dialed_peers = client.get_dialed_peers().await; 
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
    
    pub async fn initialize_server(listen_address: String, bootstrap_nodes: Option<String>) -> Result<(Client, Receiver<Event>, PeerId), Box<dyn Error>> {

        // ==================================================
        // =============     KEY GENERATIONS    =============
        // ==================================================
        // Generate a local keypair and peer ID 
        let local_key = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());
        
        // Create a new executor for the server
        let executor = new_executor().await;

        // ==================================================
        // =============  BEHAVIOUR DEFINITIONS =============
        // ==================================================

        let request_response_behaviour = 
            request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/message-exchange/1"),
                    ProtocolSupport::Full,
                )],
                request_response::Config::default(),
        );

        let kademlia_behaviour = 
            Kademlia::with_config(
                peer_id, 
                MemoryStore::new(peer_id), 
                KademliaConfig::default()
        );
        // 

        // To content-address message, we can take the hash of message and use it as an ID.
        // WARNING: USING A STD HASH FUNCTION IS NOT SECURE!
        let message_id_fn = |message: &gossipsub::Message| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            gossipsub::MessageId::from(s.finish().to_string())
        };
        
        // Set a custom gossipsub configuration
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
            .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
            .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
            .build()
            .expect("Valid config");

        // Build a gossipsub network behaviour
        let mut gossipsub_behaviour = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )
            .expect("Correct configuration");

        // Create a Gossipsub topic
        let topic = gossipsub::IdentTopic::new("test-net");
        // Subscribe to a topic
        gossipsub_behaviour.subscribe(&topic)?;

        // Build mdns network behaviour
        let mdns_behaviour = mdns::tokio::Behaviour::new(
            mdns::Config::default(), peer_id
            )
            .expect("Incorrect configuration");

        // Create the behavior for the server
        let self_behaviour = ComposedBehaviour {
            request_response: request_response_behaviour,
            kademlia: kademlia_behaviour,
            gossipsub: gossipsub_behaviour,
            mdns: mdns_behaviour,
        };

        // Create the transport for the server
        let transport = tcp::tokio::Transport::new(Default::default())
            .upgrade(Version::V1Lazy)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(yamux::Config::default())
            .boxed();

        // ===========================================
        // =============  BUILDING SWARM =============
        // ===========================================

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

        // If bootstrap nodes were provided, connect to them
        if let Some(bootstrap_nodes) = bootstrap_nodes {
            let file = tokio::fs::read_to_string(bootstrap_nodes).await.expect("Failed to read bootstrap nodes file");
            let lines = file.lines();
            for line in lines {
                let parts: Vec<&str> = line.split(" ").collect();
                let (peer_addr, peer_id) = (parts[0], parts[1]);
                
                client.bootstrap_to(peer_id.parse()?, peer_addr.parse()?).await.expect("Failed to bootstrap to peer");
            }
        }

        let peers = client.get_closest_peers(PeerId::random().to_bytes()).await;
        println!("Closest peers: {:?}", peers);
        
        // Return the client, event receiver, and peer ID
        Ok((client, event_receiver, peer_id))
    }
    
}
