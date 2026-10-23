pub mod hash;
pub mod shard;
pub mod wal;

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
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

fn expired(e: &Entry, now: u64) -> bool {
    match e.expires_at_ms {
        Some(t) => t <= now,
        None => false,
    }
}

pub struct Store {
    shards: Arc<Vec<Mutex<ShardInner>>>,
    sync: SyncPolicy,
    stop: Arc<AtomicBool>,
    sweeper: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Store {
    pub fn open(cfg: Config) -> io::Result<Store> {
        assert!(cfg.shards > 0, "shards must be >= 1");
        fs::create_dir_all(&cfg.dir)?;

        let meta = cfg.dir.join("meta");
        if meta.exists() {
            let s = fs::read_to_string(&meta)?;
            let n: usize = s
                .trim()
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad meta file"))?;
            if n != cfg.shards {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("shard count changed: was {n}, now {}", cfg.shards),
                ));
            }
        } else {
            fs::write(&meta, cfg.shards.to_string())?;
        }

        let mut inner = Vec::with_capacity(cfg.shards);
        for i in 0..cfg.shards {
            let path = wal_path(&cfg.dir, i);
            let mut map = std::collections::HashMap::new();

            if path.exists() {
                let bytes = fs::read(&path)?;
                let (recs, valid_len) = read_valid(&bytes);
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
            inner.push(Mutex::new(ShardInner { map, wal }));
        }

        let shards = Arc::new(inner);
        let stop = Arc::new(AtomicBool::new(false));
        let sync = cfg.sync;

        // background sweeper: every 200ms clean one shard at a time
        let t_shards = Arc::clone(&shards);
        let t_stop = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut ticks = 0;
            while !t_stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(200));
                if t_stop.load(Ordering::Relaxed) {
                    break;
                }
                let now = now_ms();
                for s in t_shards.iter() {
                    if let Ok(mut g) = s.lock() {
                        g.map.retain(|_, e| !expired(e, now));
                    }
                }
                // EverySecond mode syncs about once a second
                ticks += 1;
                if sync == SyncPolicy::EverySecond && ticks % 5 == 0 {
                    for s in t_shards.iter() {
                        if let Ok(mut g) = s.lock() {
                            let _ = g.wal.flush();
                            let _ = g.wal.get_ref().sync_data();
                        }
                    }
                }
            }
        });

        Ok(Store {
            shards,
            sync,
            stop,
            sweeper: Mutex::new(Some(handle)),
        })
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
        let mut guard = self.shards[idx].lock().unwrap();
        let e = guard.map.get(key)?;
        if expired(e, now_ms()) {
            guard.map.remove(key);
            return None;
        }
        Some(e.value.clone())
    }

    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> io::Result<()> {
        let idx = self.pick(key);
        let mut guard = self.shards[idx].lock().unwrap();
        let expires_at_ms = ttl.map(|d| now_ms().saturating_add(d.as_millis() as u64));
        let bytes = encode_set(key, value, expires_at_ms);
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
        for s in self.shards.iter() {
            total += s.lock().unwrap().map.len();
        }
        total
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Ok(mut h) = self.sweeper.lock() {
            if let Some(j) = h.take() {
                let _ = j.join();
            }
        }
        for s in self.shards.iter() {
            if let Ok(mut g) = s.lock() {
                let _ = g.wal.flush();
            }
        }
    }
}
