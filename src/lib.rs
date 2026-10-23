pub mod hash;
pub mod shard;

use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hash::fnv1a64;
use shard::{Entry, ShardInner};

/// How aggressively we push WAL bytes to disk (M2 wires this up).
/// M1 ignores it but keeps it in Config so the API stays stable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    Always,
    EverySecond,
    Never,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: PathBuf,
    pub shards: usize,
    pub sync: SyncPolicy,
}

/// Current wall-clock time in millis since unix epoch.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

/// Sharded in-memory map. Each shard has its own lock so threads
/// working on different shards never block each other.
///
/// `Store` is `Send + Sync` (via `Vec<Mutex<...>>`) so it can be
/// shared with `Arc<Store>`.
pub struct Store {
    shards: Vec<Mutex<ShardInner>>,
}

impl Store {
    pub fn open(cfg: Config) -> io::Result<Store> {
        assert!(cfg.shards > 0, "shards must be >= 1");
        // M2 will also: create dir, write/read meta file, replay WALs.
        // M1: just empty shards.
        let mut shards = Vec::with_capacity(cfg.shards);
        for _ in 0..cfg.shards {
            shards.push(Mutex::new(ShardInner::new()));
        }
        Ok(Store { shards })
    }

    fn pick(&self, key: &str) -> usize {
        (fnv1a64(key) % self.shards.len() as u64) as usize
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let idx = self.pick(key);
        let guard = self.shards[idx].lock().unwrap();
        guard.map.get(key).map(|e| e.value.clone())
    }

    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> io::Result<()> {
        let idx = self.pick(key);
        let mut guard = self.shards[idx].lock().unwrap();
        // Absolute timestamp so a future WAL replay (M2) stays deterministic.
        // M3 will enforce expiry on get + sweeper. M1 stores it only.
        let expires_at_ms = ttl.map(|d| now_ms().saturating_add(d.as_millis() as u64));
        guard.map.insert(
            key.to_string(),
            Entry {
                value: value.to_vec(),
                expires_at_ms,
            },
        );
        Ok(())
    }

    pub fn delete(&self, key: &str) -> io::Result<bool> {
        let idx = self.pick(key);
        let mut guard = self.shards[idx].lock().unwrap();
        Ok(guard.map.remove(key).is_some())
    }

    /// Total keys across all shards. Locks one shard at a time, never two.
    pub fn len(&self) -> usize {
        let mut total = 0;
        for s in &self.shards {
            total += s.lock().unwrap().map.len();
        }
        total
    }
}
