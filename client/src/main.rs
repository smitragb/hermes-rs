use tonic::transport::Channel;
use tracing::info;

mod hermes_client;
use hermes_client::hermes::hermes_service_client::HermesServiceClient;
use hermes_client::hermes::{ReadRequest, WriteRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client_id = format!("Client-{}", rand::random::<u8>());
    info!("Starting client with ID: {}", client_id);

    // Connect to the server
    let channel = Channel::from_static("http://[::1]:50052")
        .connect()
        .await?;
    
    let mut client = HermesServiceClient::new(channel);

    // Send a read_req
    let key = "KEY1".to_string();
    info!("Sending read request...");
    let read_req = tonic::Request::new(ReadRequest {
        key: key.clone(),
        client_id: client_id.clone(),
    });

    let read_response = client.read(read_req).await?;
    let read_resp = read_response.into_inner();
    
    info!(
        "Server responded for key ({}): '{}'",
         key, read_resp.value
    );

    // Write request
    let value = "VAL1".to_string();
    info!("Submitting Write request: ({}, {})", key, value);
    let write_req = tonic::Request::new(WriteRequest {
        key,
        value,
        client_id: client_id.clone(),
    });

    client.write(write_req).await?;
    
    Ok(())
}
