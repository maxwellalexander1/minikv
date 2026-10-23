use std::collections::HashMap;

/// One stored value. `expires_at_ms` is an absolute unix timestamp in
/// millis (0 / None = no expiry). M1 stores it but M3 will enforce it.
pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at_ms: Option<u64>,
}

/// Contents of one shard, always accessed under that shard's Mutex.
pub struct ShardInner {
    pub map: HashMap<String, Entry>,
}

impl ShardInner {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}
