use tonic::{Request, Response, Status};
use tracing::info;

pub mod hermes {
    tonic::include_proto!("hermes");
}

use hermes::{
    hermes_service_server::HermesService,
    ReadRequest, ReadResponse, WriteRequest, WriteResponse 
};

#[derive(Default, Debug)]
pub struct HermesServer {
    server_id: String,
}

impl HermesServer {
    pub fn new(id: i32) -> Self {
        Self {
            server_id: format!("server-{}", id),
        }
    }
}

#[tonic::async_trait]
impl HermesService for HermesServer {
    async fn read (
        &self, 
        request: Request<ReadRequest> 
    ) -> Result<Response<ReadResponse>, Status> {
        let req = request.into_inner();

        info! (
            "Server-{}: Read request received from {} for key-{}",
            self.server_id, req.client_id, req.key
        );
        
        let value = "Hello World".to_string();

        let resp = ReadResponse { value };

        Ok(Response::new(resp))
    }

    async fn write (
        &self, 
        request: Request<WriteRequest>
    ) -> Result <Response<WriteResponse>, Status> {
        let  req = request.into_inner();

        info! (
            "Server-{}: Write request received from client-{}: (Key: {}, Value: {})",
             self.server_id, req.client_id, req.key, req.value
        );

        Ok(Response::new(WriteResponse{}))
    }
}

