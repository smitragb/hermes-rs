use tonic::transport::Channel;
use tracing::{info, warn};

mod hermes_client;
use hermes_client::hermes::hermes_service_client::HermesServiceClient;
use hermes_client::hermes::{ReadRequest, WriteRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client_id = format!("Client-{}", 1);
    info!("Starting client with ID: {}", client_id);

    // Connect to the server
    let channel = Channel::from_static("http://[::1]:50052")
        .connect()
        .await?;
    
    let mut client = HermesServiceClient::new(channel);

    // Write request
    let mut key = "KEY1".to_string();
    let value = "VAL1".to_string();
    
    info!("Submitting Write request: ({}, {})", key, value);
    let write_req = tonic::Request::new(WriteRequest {
        key: key.clone(),
        value,
        client_id: client_id.clone(),
    });

    client.write(write_req).await?;

    // Send a read_req
    info!("Sending read request...");
    let mut read_req = tonic::Request::new(ReadRequest {
        key: key.clone(),
        client_id: client_id.clone(),
    });

    match client.read(read_req).await {
        Ok(resp) => info!("Server responded for key ({}): '{}'", key.clone(), resp.into_inner().value),
        Err(e) if e.code() == tonic::Code::NotFound => warn!("Key '{}' not found", key.clone()),
        Err(e) => warn!("Server error: {:?}", e),
    }

    key = "KEY2".to_string();

    read_req = tonic::Request::new(ReadRequest {
        key: key.clone(),
        client_id: client_id.clone(),
    });
    
    match client.read(read_req).await {
        Ok(resp) => info!("Server responded for key ({}): '{}'", key, resp.into_inner().value),
        Err(e) if e.code() == tonic::Code::NotFound => warn!("Key '{}' not found", key),
        Err(e) => warn!("Server error: {:?}", e),
    }
    
    Ok(())
}
