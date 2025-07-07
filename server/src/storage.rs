use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type KVStore = Arc<RwLock<HashMap<String, String>>>;
