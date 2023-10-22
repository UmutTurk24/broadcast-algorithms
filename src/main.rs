use libp2p_server::{Servers, UserEvent};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 
    // Initialize the servers
    let mut servers = Servers::new();

    
    // Initialize and Connect the servers
    servers.init_servers(5).await?;
    servers.connect_servers().await?;
    

    // Test sending data to a peer
    let sender = servers.sender.get(&1).unwrap();

    loop {
        // Sleep 2 seconds
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        sender.send(UserEvent::Broadcast(vec![1,2,3])).await.expect("Could not send data");
    }
}
