use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::future::join_all;
use tokio::{sync::RwLock, time::timeout};
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::{
    repl::replication::{replication_service_client::ReplicationServiceClient, InvalidateRequest, ValidateRequest}, 
    storage::{HermesValue, KVStore}
};

pub mod hermes {
    tonic::include_proto!("hermes");
}

use hermes::{
    hermes_service_server::HermesService,
    ReadRequest, ReadResponse, WriteRequest, WriteResponse 
};

#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
pub struct HermesServer {
    node_id: u16,
    self_addr: String,
    pub store: Arc<RwLock<KVStore>>,
    pub replay_timeout: u8,
    pub peers: Vec<String>,
}

impl HermesServer {
    pub fn new(node_id: u16, self_addr: String, peers: Vec<String>) -> Self {
        Self {
            node_id,
            self_addr,
            store: Arc::new(RwLock::new(HashMap::new())),
            replay_timeout: 1,
            peers,
        }
    }

    pub fn get_node(&self) -> u16 {
        self.node_id
    }

    async fn broadcast_validate(
        &self,
        key: &String,
        logical_time: u32,
        task_timeout: Duration
    ) {
        let mut tasks = vec![];

        for peer in &self.peers {
            let peer_addr = format!("http://{}", peer);
            let key = key.clone();
            let node_id = self.node_id.clone();
            info!("[n{}] Sending VAL to peer: {}",node_id, peer.clone());
            tasks.push(tokio::spawn(async move {
                match timeout(task_timeout, async move {
                    match ReplicationServiceClient::connect(peer_addr).await {
                        Ok(mut client) => {
                            match client.validate(Request::new(ValidateRequest {
                                key,
                                local_ts: logical_time,
                                node_id: node_id as u32,
                            })).await {
                                Ok(_) => info!("Successfully sent VAL"),
                                Err(e) => error!("Validate RPC failed to {}", e),
                            }
                        },
                        Err(_) => error!("Couldn't connect to peer"),
                    }
                }).await {
                    Ok(_) => {},
                    Err(e) => error!("Timeout! {}", e),
                }
            })); 
        }
        join_all(tasks).await;
    }

    async fn broadcast_invalidate (
        &self,
        key: &String,
        value: &String,
        logical_time: u32,
        task_timeout: Duration
    ) {
        let mut tasks = vec![];
        for peer in &self.peers {
            let peer_addr = format!("http://{}", peer);
            let key = key.clone();
            let value = value.clone();
            let node_id = self.node_id.clone();
            info!("[n{}] Sending INV to peer: {}", node_id, peer.clone());
            tasks.push(tokio::spawn(async move {
                match timeout(task_timeout, async move {
                    match ReplicationServiceClient::connect(peer_addr).await {
                        Ok(mut client) => {
                            match client.invalidate(Request::new(InvalidateRequest {
                                key,
                                value,
                                local_ts: logical_time,
                                node_id: node_id as u32,
                            })).await {
                                Ok(_) => info!("Successfully sent INV"),
                                Err(e) => error!("Invalidate RPC failed for {}", e),
                            }
                        },
                        Err(_) => error!("Couldn't connect to peer"),
                    }
                }).await {
                    Ok(_) => {},
                    Err(e) => error!("Timeout! {}", e),
                }
            })); 
        }
        join_all(tasks).await;
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
            "[n{}]: Read request received from {} for key-{}",
            self.node_id, req.client_id, req.key
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
        let key = req.key;
        let value = req.value;
        let task_timeout = Duration::from_secs(10);

        info! (
            "[n{}]: Write request received from {}: (Key: {}, Value: {})",
             self.node_id, req.client_id, key.clone(), value.clone()
        );

        let shared_val = {
            let read_lock = self.store.read().await;
            read_lock.get(&key).cloned()
        };

        let hermes_val = match shared_val {
            Some(val) => val,
            None => {
                let mut write_lock = self.store.write().await;
                write_lock.entry(key.clone())
                    .or_insert(
                        Arc::new(RwLock::new(HermesValue::new(key.clone(), value.clone(), self.node_id)))
                    )
                    .clone()
            }
        };
        
        {
            let _ = HermesValue::wait_till_valid_or_timeout(
                &hermes_val, self.replay_timeout as u64
            ).await;
            
            let mut guard = hermes_val.write().await;
            let logical_time = guard.get_timestamp().get_logical_time();
            info!("[n{}] Transitioning {} to WRITE state", self.node_id, value.clone());
            guard.coord_valid_to_write_transition(value.clone(), self.node_id);
            
            // Broadcast Invalidate
            self.broadcast_invalidate(&key, &value, logical_time, task_timeout).await;       
            
            // Broadcast Validate
            self.broadcast_validate(&key, logical_time, task_timeout).await;

            info!("[n{}] Transitioning {} back to VALID", self.node_id, value.clone());
            guard.coord_write_to_valid_transition();
        }
 
        Ok(Response::new(WriteResponse{}))
    }
}
