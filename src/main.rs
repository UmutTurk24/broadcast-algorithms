#![allow(non_camel_case_types)]
use libp2p_server::{P2PServer, ClientBehaviour};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    // Run the server
    P2PServer::initialize_server(Some("./src/bootstrap-nodes.txt".to_string()), ClientBehaviour::VabaBroadcast).await?;

    Ok(())
}
