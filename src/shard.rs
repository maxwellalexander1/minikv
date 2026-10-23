use std::collections::HashMap;

pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at_ms: Option<u64>,
}

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
