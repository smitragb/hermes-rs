use std::env::args;
use repl::replication::replication_service_server::ReplicationServiceServer;
use server::Config;
use tonic::transport::Server;
use tracing::info;

mod hermes_server;
use hermes_server::hermes::hermes_service_server::HermesServiceServer;
use hermes_server::HermesServer;

mod storage;
mod repl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args: Vec<String> = args().collect(); 
    let config = Config::build(&args)?;

    let addr_fmt = format!("[::1]:{}", config.port_no);
    let addr: std::net::SocketAddr = addr_fmt.parse()?;
    let hermes_server = HermesServer::new(config.server_id, addr_fmt, config.peer_ports);

    info!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(HermesServiceServer::new(hermes_server.clone()))
        .add_service(ReplicationServiceServer::new(hermes_server))
        .serve(addr)
        .await?;

    Ok(())
}
