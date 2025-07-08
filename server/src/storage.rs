use std::time::Duration;
use std::{cmp::Ordering, sync::atomic::AtomicU8};
use std::collections::HashMap;
use tokio::sync::{Notify, RwLock};
use tokio::time::timeout;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(PartialEq)]
enum State {
    VALID, 
    INVALID, 
    WRITE, 
    REPLAY, 
    TRANSIENT,
}

#[allow(dead_code)]
impl State {
    fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(State::VALID),
            1 => Some(State::INVALID),
            2 => Some(State::WRITE),
            3 => Some(State::REPLAY),
            4 => Some(State::TRANSIENT),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct TimeStamp {
    logical_time: u16,
    node_id: u16,
}

impl PartialEq for TimeStamp {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.logical_time == other.logical_time
    }

    fn ne(&self, other: &Self) -> bool {
        self.node_id != other.node_id || self.logical_time != other.logical_time
    }
}

impl Eq for TimeStamp {}

impl PartialOrd for TimeStamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeStamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.logical_time.cmp(&other.logical_time) {
            Ordering::Equal => self.node_id.cmp(&other.node_id),
            ord => ord,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct HermesValue {
    key: String,
    value: String, 
    stall_notify: Arc<Notify>,
    timestamp: TimeStamp, 
    state: AtomicU8
}

#[allow(dead_code)]
impl HermesValue {
    pub fn new(key: String, value: String, node_id: u16) -> Self {
        Self {
            key: key.clone(),
            value,
            stall_notify: Arc::new(Notify::new()),
            timestamp: TimeStamp {
                logical_time: 0,
                node_id,
            },
            state: AtomicU8::new(State::VALID as u8),
        }
    }
    
    pub async fn wait_till_valid(shared: &Arc<RwLock<Self>>) {
        let notify = {
            let guard = shared.read().await;
            guard.stall_notify.clone()
        };

        loop {
            notify.notified().await;
            if shared.read().await.is_valid() {
                break;
            }
        }
    }

    pub async fn wait_till_valid_or_timeout(
        shared: &Arc<RwLock<Self>>, 
        transition_timeout: u64
    ) -> bool {

        let notify = {
            let guard = shared.read().await;
            guard.stall_notify.clone()
        };

        let result = timeout(Duration::from_millis(transition_timeout), async {
            loop {
                notify.notified().await;
                if shared.read().await.is_valid() {
                    break;
                }
            }
        }).await;

        result.is_ok() && shared.read().await.is_valid()
    }
    
    pub fn state_to_string(&self) -> String {
        match State::from_u8(self.state.load(std::sync::atomic::Ordering::Acquire)) {
            Some(State::VALID) => "VALID".to_string(),
            Some(State::INVALID) => "INVALID".to_string(),
            Some(State::WRITE) => "WRITE".to_string(),
            Some(State::REPLAY) => "REPLAY".to_string(),
            Some(State::TRANSIENT) => "TRANSIENT".to_string(),
            _ => String::new(),
        }
    }

    #[inline(always)]
    pub fn coord_valid_to_write_transition(&mut self, new_value: String, node_id: u16) {
        let expected_state = State::VALID as u8;
        if let Ok(_)  = self.state.compare_exchange(
            expected_state, 
            State::WRITE as u8, 
            std::sync::atomic::Ordering::SeqCst, 
            std::sync::atomic::Ordering::SeqCst
        ) {
            self.timestamp.logical_time += 1;
            self.timestamp.node_id = node_id;
            self.value = new_value;
        }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        match State::from_u8(self.state.load(std::sync::atomic::Ordering::Acquire)) {
            Some(State::VALID) => true,
            _ => false,
        }
    }

    #[inline(always)]
    pub fn is_write(&self) -> bool {
        match State::from_u8(self.state.load(std::sync::atomic::Ordering::Acquire)) {
            Some(State::WRITE) => true,
            _ => false,
        }
    }
   
    #[inline(always)] 
    pub fn coord_write_to_valid_transition(&mut self) {
        let expected_state = State::WRITE as u8;
        if let Ok(_) = self.state.compare_exchange(
            expected_state, 
            State::VALID as u8, 
            std::sync::atomic::Ordering::SeqCst, 
            std::sync::atomic::Ordering::SeqCst
        ) {
            self.stall_notify.notify_one();
        }
    }

    #[inline(always)]
    pub fn coord_write_to_invalid_transition(&mut self) {
        let expected_state = State::WRITE as u8;
        let _ = self.state.compare_exchange(
            expected_state, 
            State::INVALID as u8, 
            std::sync::atomic::Ordering::SeqCst, 
            std::sync::atomic::Ordering::SeqCst
        );
    } 

    #[inline(always)]
    pub fn fol_invalid_to_replay_transition(&mut self) {
        let expected_state = State::INVALID as u8;
        let _ = self.state.compare_exchange(
            expected_state, 
            State::REPLAY as u8, 
            std::sync::atomic::Ordering::SeqCst, 
            std::sync::atomic::Ordering::SeqCst
        );
    }

    #[inline(always)]
    pub fn fol_replay_to_write_transition(&mut self) {
        let expected_state = State::REPLAY as u8;
        let _ = self.state.compare_exchange(
            expected_state, 
            State::WRITE as u8, 
            std::sync::atomic::Ordering::SeqCst, 
            std::sync::atomic::Ordering::SeqCst
        );
    }

    #[inline(always)]
    pub fn fol_invalidate(&mut self, value: String, ts: TimeStamp) {
        self.state.store(State::INVALID as u8, std::sync::atomic::Ordering::Release);
        self.value = value;
        self.timestamp = ts;
    }

    #[inline(always)]
    pub fn fol_invalid_to_valid_transition(&mut self) {
        let expected_state = State::INVALID as u8;
        let _ = self.state.compare_exchange(
            expected_state, 
            State::VALID as u8, 
            std::sync::atomic::Ordering::SeqCst, 
            std::sync::atomic::Ordering::SeqCst
        );
        self.stall_notify.notify_one();
    }

    #[inline(always)] 
    pub fn get_timestamp(&self) -> TimeStamp {
        self.timestamp.clone()
    }

    #[inline(always)]
    pub fn get_value(&self) -> String {
        self.value.clone()
    }
}

pub type SharedHermesValue = Arc<RwLock<HermesValue>>;
pub type KVStore = HashMap<String, SharedHermesValue>;
