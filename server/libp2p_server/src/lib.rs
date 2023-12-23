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

use libp2p::{identity, PeerId, tcp, Transport, StreamProtocol, yamux, noise, Multiaddr, mdns};
use libp2p::swarm::SwarmBuilder;
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::kad::{Kademlia, KademliaConfig};
use libp2p::tokio_development_transport;
use libp2p::kad::Record;

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hash;
use std::hash::Hasher;


pub mod client;
use client::{Client, Event};



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
                        Some(UserEvent::SendRR(data, peer_id)) => {
                            println!("Sending data {:?}", data.clone());
                            client.send_rr(peer_id, data).await.expect("Successfully sent data");
                        },
                        Some(UserEvent::Broadcast(data)) => {
                            let dialed_peers = client.get_dialed_peers().await; 
                            for peer in dialed_peers {
                                client.send_rr(peer, data.clone()).await.expect("Could not send data");
                            }
                            println!("Broadcasting data");
                        },
                        None => {},
                    }
                }
                network_event = event_receiver.recv() => {
                    match network_event {
                        Some(Event::RRRequest { request, channel }) => {
                            println!("Received request: {:?}", request);
                            client.recv_rr( channel ).await;
                        },
                        Some(Event::GossipMessage { message }) => {
                            println!("Received gossip message: {:?}", message);
                        },
                        Some(client::Event::KDProgressed { .. }) => todo!(),
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
    SendRR(Vec<u8>, PeerId),
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

        println!("Local peer id: {:?}", peer_id);
        
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
        let transport = tokio_development_transport(local_key)?;

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
                
                client.kd_add_address(peer_id.parse()?, peer_addr.parse()?).await.expect("Failed to bootstrap to peer");
            }
        }
        
        // Return the client, event receiver, and peer ID
        Ok((client, event_receiver, peer_id))
    }
    
    pub fn create_record(key: Vec<u8>, value: Vec<u8>, publisher: Option<PeerId>) -> Record {
        Record {
            key: key.into(),
            value,
            publisher,
            expires: None,
        }
    }
}
