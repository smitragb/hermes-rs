use tonic::transport::Server;
use tracing::info;

mod hermes_server;
use hermes_server::hermes::hermes_service_server::HermesServiceServer;
use hermes_server::HermesServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let addr = "[::1]:50052".parse()?;
    let hermes_server = HermesServer::new(1);

    info!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(HermesServiceServer::new(hermes_server))
        .serve(addr)
        .await?;

    Ok(())
}
