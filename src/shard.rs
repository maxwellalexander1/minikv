use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;

pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at_ms: Option<u64>,
}

pub struct ShardInner {
    pub map: HashMap<String, Entry>,
    pub wal: BufWriter<File>,
}

impl ShardInner {
    pub fn new(wal: BufWriter<File>) -> Self {
        Self {
            map: HashMap::new(),
            wal,
        }
    }
}
