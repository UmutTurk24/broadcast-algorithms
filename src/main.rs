#![allow(non_camel_case_types)]
use libp2p_server::{P2PServer, ClientBehaviour};
use tokio::{sync::mpsc::Receiver, time::Instant};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    // Run the server
    P2PServer::initialize_server(Some("./src/bootstrap-nodes.txt".to_string()), ClientBehaviour::VabaBroadcast).await?;

    // let mut before = Instant::now();
    // let mut counter = 0;
    // for _ in 0..10000000 {
    //     counter += 1;
    // }

    // println!("Elapsed time: {:.2?}", before.elapsed());

    Ok(())
}
