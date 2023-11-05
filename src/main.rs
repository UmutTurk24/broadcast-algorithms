
use futures::prelude::*;
use libp2p::core::upgrade::Version;
use libp2p::{
    identity, noise, ping,
    swarm::{SwarmBuilder, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Transport,
};

use libp2p_server::{P2PServer};
use std::error::Error;
use std::time::Duration;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    let (client, event_receiver, peer_id) = 
        P2PServer::initialize_server("/ip4/127.0.0.1/tcp/40820".to_string(), Some("./src/bootstrap-nodes.txt".to_string())).await?;
    
    loop{

    }

}

