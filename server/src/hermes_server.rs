use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;
use crate::storage::KVStore;

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
    store: KVStore,
}

impl HermesServer {
    pub fn new(id: i32) -> Self {
        Self {
            server_id: format!("Server-{}", id),
            store: Arc::new(RwLock::new(HashMap::new())),
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
            "{}: Read request received from {} for key-{}",
            self.server_id, req.client_id, req.key
        );

        let map = self.store.read().await;
        match map.get(&req.key) {
            Some(val) => Ok(Response::new(ReadResponse {
                value: val.clone(),
            })),
            None => Err(Status::not_found(format!("Key '{}' not found", req.key))),
        }
    }

    async fn write (
        &self, 
        request: Request<WriteRequest>
    ) -> Result <Response<WriteResponse>, Status> {
        let  req = request.into_inner();

        info! (
            "{}: Write request received from {}: (Key: {}, Value: {})",
             self.server_id, req.client_id, req.key, req.value
        );

        self.store.write().await.insert(req.key, req.value);

        Ok(Response::new(WriteResponse{}))
    }
}

