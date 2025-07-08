use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;
use crate::storage::{HermesValue, KVStore};

pub mod hermes {
    tonic::include_proto!("hermes");
}

use hermes::{
    hermes_service_server::HermesService,
    ReadRequest, ReadResponse, WriteRequest, WriteResponse 
};

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct HermesServer {
    server_id: String,
    node_id: u16,
    self_addr: String,
    store: Arc<RwLock<KVStore>>,
    replay_timeout: u8
}

impl HermesServer {
    pub fn new(server_id: u16, self_addr: String) -> Self {
        Self {
            server_id: format!("Server-{}", server_id),
            node_id: server_id,
            self_addr,
            store: Arc::new(RwLock::new(HashMap::new())),
            replay_timeout: 1,
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
            Some(shared_val) => {
                info! ("Found Key: '{}'", req.key);
                let stale = !HermesValue::wait_till_valid_or_timeout(
                    shared_val, self.replay_timeout as u64
                ).await;
                let value = shared_val.read().await.get_value();
                Ok (Response::new (ReadResponse {
                    value,
                    stale
                }))
            },
            None => Err(Status::not_found(format!("Key '{}' not found", req.key))),
        }

    }

    async fn write (
        &self, 
        request: Request<WriteRequest>
    ) -> Result <Response<WriteResponse>, Status> {
        let req = request.into_inner();

        info! (
            "{}: Write request received from {}: (Key: {}, Value: {})",
             self.server_id, req.client_id, req.key, req.value
        );

        //self.store.write().await.insert(req.key, req.value);
        if let Some(shared_val) = {
            let read_lock = self.store.read().await;
            read_lock.get(&req.key).cloned()
        } {
            let _ = HermesValue::wait_till_valid_or_timeout(
                &shared_val, self.replay_timeout as u64
            ).await;

            {
                let mut guard = shared_val.write().await;
                guard.coord_valid_to_write_transition(req.value, self.node_id);
                // broadcast_invalidate
                // receive acks
                // broadcast_validate
                guard.coord_write_to_valid_transition();
            }

        } else {
            let new_val = Arc::new(RwLock::new(HermesValue::new(req.key.clone(), req.value, self.node_id)));
            let mut write_lock = self.store.write().await;
            write_lock.insert(req.key, new_val);
        }
        
        Ok(Response::new(WriteResponse{}))
    }
}

