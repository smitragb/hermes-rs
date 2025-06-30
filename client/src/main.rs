use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;
use tracing::info;

// Include the generated protobuf code
pub mod ping {
    tonic::include_proto!("ping");
}

use ping::{
    ping_service_client::PingServiceClient,
    PingRequest, RandomRequest,
};

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client_id = format!("client-{}", rand::random::<u32>());
    info!("Starting client with ID: {}", client_id);

    // Connect to the server
    let channel = Channel::from_static("http://[::1]:50051")
        .connect()
        .await?;
    
    let mut client = PingServiceClient::new(channel);

    // Send a ping
    info!("Sending ping...");
    let ping_request = tonic::Request::new(PingRequest {
        message: "Hello from client!".into(),
        timestamp: current_timestamp(),
        client_id: client_id.clone(),
    });

    let ping_response = client.ping(ping_request).await?;
    let ping_resp = ping_response.into_inner();
    
    info!(
        "Server responded: '{}' (timestamp: {}, server_id: {})",
        ping_resp.message, ping_resp.server_timestamp, ping_resp.server_id
    );

    // Request a random number
    info!("Requesting random number...");
    let request_id = format!("req-{}", rand::random::<u32>());
    let random_request = tonic::Request::new(RandomRequest {
        min: 1,
        max: 100,
        request_id: request_id.clone(),
    });

    let random_response = client.get_random_number(random_request).await?;
    let random_resp = random_response.into_inner();
    
    info!(
        "Server generated random number: {} (timestamp: {}, request_id: {})",
        random_resp.number, random_resp.timestamp, random_resp.request_id
    );

    // Demo: Multiple rapid requests (useful for testing replication later)
    info!("Sending multiple rapid requests...");
    for i in 0..5 {
        let request_id = format!("batch-req-{}", i);
        let request = tonic::Request::new(RandomRequest {
            min: i * 10,
            max: (i + 1) * 10,
            request_id: request_id.clone(),
        });
        
        let response = client.get_random_number(request).await?;
        let resp = response.into_inner();
        
        info!("Request {}: got number {} (id: {})", i, resp.number, resp.request_id);
    }

    Ok(())
}
