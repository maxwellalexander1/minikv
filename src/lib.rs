pub mod hash;
pub mod shard;
pub mod wal;

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hash::fnv1a64;
use shard::{Entry, ShardInner};
use wal::{encode_del, encode_set, read_valid, Record};

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

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn wal_path(dir: &std::path::Path, i: usize) -> PathBuf {
    dir.join(format!("shard-{i}.wal"))
}

pub struct Store {
    shards: Vec<Mutex<ShardInner>>,
    sync: SyncPolicy,
}

impl Store {
    pub fn open(cfg: Config) -> io::Result<Store> {
        assert!(cfg.shards > 0, "shards must be >= 1");
        fs::create_dir_all(&cfg.dir)?;

        // meta file remembers shard count so keys map to the same shard after restart
        let meta = cfg.dir.join("meta");
        if meta.exists() {
            let s = fs::read_to_string(&meta)?;
            let n: usize = s.trim().parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad meta file")
            })?;
            if n != cfg.shards {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("shard count changed: was {n}, now {}", cfg.shards),
                ));
            }
        } else {
            fs::write(&meta, cfg.shards.to_string())?;
        }

        let mut shards = Vec::with_capacity(cfg.shards);
        for i in 0..cfg.shards {
            let path = wal_path(&cfg.dir, i);
            let mut map = std::collections::HashMap::new();

            if path.exists() {
                let bytes = fs::read(&path)?;
                let (recs, valid_len) = read_valid(&bytes);
                // cut off torn tail
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(valid_len as u64)?;
                let now = now_ms();
                for r in recs {
                    match r {
                        Record::Set { key, value, expires_at_ms } => {
                            if let Some(t) = expires_at_ms {
                                if t <= now {
                                    continue;
                                }
                            }
                            map.insert(key, Entry { value, expires_at_ms });
                        }
                        Record::Del { key } => {
                            map.remove(&key);
                        }
                    }
                }
            }

            let f = OpenOptions::new().create(true).append(true).open(&path)?;
            let wal = std::io::BufWriter::new(f);
            shards.push(Mutex::new(ShardInner { map, wal }));
        }
        Ok(Store { shards, sync: cfg.sync })
    }

    fn pick(&self, key: &str) -> usize {
        (fnv1a64(key) % self.shards.len() as u64) as usize
    }

    fn sync_one(wal: &mut std::io::BufWriter<std::fs::File>, policy: SyncPolicy) -> io::Result<()> {
        wal.flush()?;
        if policy == SyncPolicy::Always {
            wal.get_ref().sync_data()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let idx = self.pick(key);
        let guard = self.shards[idx].lock().unwrap();
        guard.map.get(key).map(|e| e.value.clone())
    }

    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> io::Result<()> {
        let idx = self.pick(key);
        let mut guard = self.shards[idx].lock().unwrap();
        let expires_at_ms = ttl.map(|d| now_ms().saturating_add(d.as_millis() as u64));
        let bytes = encode_set(key, value, expires_at_ms);
        // log first, if this fails we don't touch the map
        guard.wal.write_all(&bytes)?;
        Self::sync_one(&mut guard.wal, self.sync)?;
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
        let bytes = encode_del(key);
        guard.wal.write_all(&bytes)?;
        Self::sync_one(&mut guard.wal, self.sync)?;
        Ok(guard.map.remove(key).is_some())
    }

    pub fn len(&self) -> usize {
        let mut total = 0;
        for s in &self.shards {
            total += s.lock().unwrap().map.len();
        }
        total
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        for s in &self.shards {
            if let Ok(mut g) = s.lock() {
                let _ = g.wal.flush();
            }
        }
    }
}
