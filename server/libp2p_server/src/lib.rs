use client::ComposedBehaviour;
use client::EventLoop;

use futures::future::Either;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::OrTransport;
use libp2p::core::upgrade;
use libp2p::gossipsub;
use libp2p::kad::store::MemoryStore;
use libp2p::quic;
use tokio::sync::mpsc;
use tokio::time::Duration;

use libp2p::{identity, PeerId, tcp, Transport, StreamProtocol, yamux, noise, Multiaddr, mdns};
use libp2p::request_response::{self, ProtocolSupport};
use libp2p::swarm::SwarmBuilder;
use libp2p::kad::{Kademlia, KademliaConfig};

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hash;
use std::hash::Hasher;

pub mod client;
use client::Client;
pub mod behaviour;
pub mod poly_commit;

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
    
    pub async fn initialize_server(bootstrap_nodes: Option<String>, client_behaviour: ClientBehaviour) -> Result<(), Box<dyn Error>> {

        // ==================================================
        // =============     KEY GENERATIONS    =============
        // ==================================================

        // Generate a local keypair and peer ID 
        let local_key = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());

        println!("Local peer id: {:?}", peer_id);

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
        println!("Subscribed to test-net successfully {}", gossipsub_behaviour.subscribe(&topic)?);

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

        // Create the transport layers (tcp and quic) for the server
        let tcp_transport = libp2p::tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key).expect("signing libp2p-noise static keypair"))
            .multiplex(yamux::Config::default())
            .timeout(std::time::Duration::from_secs(20))
            .boxed();
    
        let quic_transport = quic::tokio::Transport::new(quic::Config::new(&local_key));
    
        let transport = OrTransport::new(quic_transport, tcp_transport)
            .map(|either_output, _| match either_output {
                Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            })
            .boxed();

        // ===========================================
        // =============  BUILDING SWARM =============
        // ===========================================

        // Build the swarm for the server
        let swarm = SwarmBuilder::with_tokio_executor(transport, self_behaviour, peer_id).build();
    
        // Create channels for sending commands and receiving events
        let (command_sender, command_receiver) = mpsc::channel(32);
        let (event_sender, event_receiver) = mpsc::channel(32);
        
        // Create an event loop for the server and spawn it in a new task
        let event_loop = EventLoop::new(swarm, command_receiver, event_sender);
        tokio::spawn( event_loop.run());

        // Create a client for sending commands to the server
        let mut client = Client {sender: command_sender};

        // Start listening on a randomly available port (assigned by the OS)
        client.start_listening("/ip4/0.0.0.0/tcp/0".parse()?).await.expect("Failed to start listening");
        client.start_listening("/ip4/0.0.0.0/udp/0/quic-v1".parse()?).await.expect("Failed to start listening");

        // If bootstrap nodes were provided, connect to them
        // if bootstrap_nodes.is_some() {
        //     let file = tokio::fs::read_to_string(bootstrap_nodes.unwrap()).await.expect("Failed to read bootstrap nodes file");
        //     let lines = file.lines();
        //     for line in lines {
        //         let parts: Vec<&str> = line.split(" ").collect();
        //         let (peer_addr, peer_id) = (parts[0], parts[1]);
                
        //         client.kd_add_address(peer_id.parse()?, peer_addr.parse()?).await.expect("Failed to bootstrap to peer");
        //     }
        // }
        // if let Some(bootstrap_nodes) = bootstrap_nodes {
        //     let file = tokio::fs::read_to_string(bootstrap_nodes).await.expect("Failed to read bootstrap nodes file");
        //     let lines = file.lines();
        //     for line in lines {
        //         let parts: Vec<&str> = line.split(" ").collect();
        //         let (peer_addr, peer_id) = (parts[0], parts[1]);
                
        //         client.kd_add_address(peer_id.parse()?, peer_addr.parse()?).await.expect("Failed to bootstrap to peer");
        //     }
        // }

        // ==================================================
        // =============  BEHAVIOUR SELECTION   =============
        // ==================================================

        // Match the behaviour function
        match client_behaviour {
            ClientBehaviour::ReliableBroadcast => {
                behaviour::ReliableBroadcast::run(client.clone(), event_receiver, peer_id).await?;
            },
            ClientBehaviour::WitnessBroadcast => {
                behaviour::WitnessBroadcast::run(client.clone(), event_receiver, peer_id).await?;
            },
            ClientBehaviour::VabaBroadcast => {
                behaviour::VabaBroadcast::run(client.clone(), event_receiver, peer_id).await?;
            }
        }

        Ok(())

    }
    
    
}

pub enum ClientBehaviour {
    ReliableBroadcast,
    WitnessBroadcast,
    VabaBroadcast,
}