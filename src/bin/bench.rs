use minikv::{Config, Store, SyncPolicy};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn make_store(sync: SyncPolicy) -> (tempfile::TempDir, Store) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = Config {
        dir: tmp.path().to_path_buf(),
        shards: 16,
        sync,
    };
    let s = Store::open(cfg).unwrap();
    (tmp, s)
}

fn bench_reads(threads: usize, hot: bool) -> f64 {
    let (_tmp, store) = make_store(SyncPolicy::Never);
    for i in 0..10_000 {
        store.set(&format!("k{i}"), b"value12345", None).unwrap();
    }
    let store = Arc::new(store);
    let per = 50_000;
    let start = Instant::now();
    let mut hs = vec![];
    for _ in 0..threads {
        let s = Arc::clone(&store);
        hs.push(std::thread::spawn(move || {
            for i in 0..per {
                let k = if hot {
                    "k1".to_string()
                } else {
                    format!("k{}", i % 10_000)
                };
                let _ = s.get(&k);
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    (threads * per) as f64 / secs
}

fn bench_writes(threads: usize, sync: SyncPolicy) -> f64 {
    let (_tmp, store) = make_store(sync);
    let store = Arc::new(store);
    let per = 5_000;
    let start = Instant::now();
    let mut hs = vec![];
    for t in 0..threads {
        let s = Arc::clone(&store);
        hs.push(std::thread::spawn(move || {
            for i in 0..per {
                s.set(&format!("t{t}-{i}"), b"value12345", None).unwrap();
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    (threads * per) as f64 / secs
}

fn main() {
    println!("threads: 1, 4, 8 | 16 shards");
    for t in [1, 4, 8] {
        let r = bench_reads(t, false);
        println!("read spread {t}t: {r:.0} ops/sec");
    }
    for t in [1, 4, 8] {
        let r = bench_reads(t, true);
        println!("read hot {t}t: {r:.0} ops/sec");
    }
    for (name, p) in [
        ("Never", SyncPolicy::Never),
        ("EverySecond", SyncPolicy::EverySecond),
        ("Always", SyncPolicy::Always),
    ] {
        for t in [1, 4, 8] {
            let r = bench_writes(t, p);
            println!("write {name} {t}t: {r:.0} ops/sec");
        }
    }
    // keep unused import happy on some builds
    let _ = Duration::from_millis(1);
}
