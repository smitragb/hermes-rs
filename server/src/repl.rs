use std::sync::Arc;

use crate::{hermes_server::HermesServer, storage::{HermesValue, TimeStamp}};

pub mod replication {
    tonic::include_proto!("replication");
}

use replication::replication_service_server::ReplicationService;
use replication::{
    InvalidateRequest, InvalidateResponse, ValidateRequest, Ack
};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;

#[tonic::async_trait]
#[allow(unused_variables)]
impl ReplicationService for HermesServer {
    async fn validate (
        &self,
        request: Request<ValidateRequest>
    ) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        let curr_ts = TimeStamp::new(req.local_ts, req.node_id);
        let node_id = self.get_node();
       
        info!("[n{}] Received VAL request for key: {}", node_id, key);
        let map = self.store.read().await;
        if let Some(shared_val) = map.get(&key) {
            let val_ts = {
                let guard = shared_val.read().await;
                guard.get_timestamp()
            };

            if curr_ts != val_ts {
                info!("[n{}] Rejecting write since timestamps don't match!", node_id);
            } else {
                {
                    let mut guard = shared_val.write().await;
                    guard.fol_invalid_to_valid_transition();
                }
            }
        }
        Ok(Response::new(Ack {}))
    }

    async fn invalidate (
        &self,
        request: Request<InvalidateRequest>
    ) -> Result<Response<InvalidateResponse>, Status> {
        let req = request.into_inner();
        let key = req.key.clone();
        let value = req.value.clone();
        let node_id = self.get_node();
        let curr_ts = TimeStamp::new(req.local_ts, req.node_id);
        let mut resp = Response::new(InvalidateResponse {
            accept: true,
            responder: node_id as u32,
        });

        info!("[n{}] Received Invalidate RPC from {} for key {}", node_id, req.node_id, req.key.clone());
        let shared_val = {
            let map = self.store.read().await;
            map.get(&key).cloned()
        };
        
        match shared_val {
            Some(val) => {
                let val_ts = {
                    let guard = val.read().await;
                    guard.get_timestamp()
                };
                info!("[n{}] got ts: {:?}",node_id, val_ts);
                if curr_ts < val_ts {
                    info!(
                        "[n{}] Rejecting write for key: {} since current timestamp is lower",
                        node_id, req.key
                    );
                    resp.get_mut().accept = false;
                } else {
                    {
                        let mut guard = val.write().await;
                        guard.fol_invalidate(value, curr_ts);
                    }
                    info!("[n{}] accepting write for key: {}", node_id, req.key);
                }

            },
            None => {
                let new_val = Arc::new(RwLock::new(HermesValue::new(key.clone(), value.clone(), node_id)));
                {
                    let mut store_guard = self.store.write().await;
                    info!("[n{}] Got write lock for DB", node_id);
                    store_guard.insert(key.clone(), new_val.clone());
                }
                
                {
                    let mut val_guard = new_val.write().await;
                    info!("[n{}] Got write lock for key {}", node_id, key.clone());
                    val_guard.fol_invalidate(value, curr_ts);
                }

            },
        }
        
        Ok (resp)
    }
}
