use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};

// Include the generated protobuf code
pub mod ping {
    tonic::include_proto!("ping");
}

use ping::{
    ping_service_server::{PingService, PingServiceServer},
    PingRequest, PingResponse, RandomRequest, RandomResponse,
};

#[derive(Debug, Default)]
pub struct PingServer {
    server_id: String,
}

impl PingServer {
    pub fn new() -> Self {
        Self {
            server_id: format!("server-{}", rand::random::<u32>()),
        }
    }

    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

#[tonic::async_trait]
impl PingService for PingServer {
    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        
        info!(
            "Received ping from client '{}': '{}' at timestamp {}",
            req.client_id, req.message, req.timestamp
        );

        let response = PingResponse {
            message: format!("Pong! Server received: '{}'", req.message),
            server_timestamp: Self::current_timestamp(),
            server_id: self.server_id.clone(),
        };

        Ok(Response::new(response))
    }

    async fn get_random_number(
        &self,
        request: Request<RandomRequest>,
    ) -> Result<Response<RandomResponse>, Status> {
        let req = request.into_inner();
        
        if req.min >= req.max {
            warn!("Invalid range: min ({}) >= max ({})", req.min, req.max);
            return Err(Status::invalid_argument(
                "min must be less than max"
            ));
        }

        let mut rng = rand::thread_rng();
        let number = rng.gen_range(req.min..req.max);
        
        info!(
            "Generated random number {} in range [{}, {}) for request '{}'",
            number, req.min, req.max, req.request_id
        );

        let response = RandomResponse {
            number,
            timestamp: Self::current_timestamp(),
            request_id: req.request_id,
        };

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let addr = "[::1]:50051".parse()?;
    let ping_server = PingServer::new();

    info!("Starting gRPC server {} on {}", ping_server.server_id, addr);

    Server::builder()
        .add_service(PingServiceServer::new(ping_server))
        .serve(addr)
        .await?;

    Ok(())
}
