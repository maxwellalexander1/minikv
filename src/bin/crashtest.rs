use minikv::{Config, Store, SyncPolicy};
use std::env;
use std::io::Write;

fn main() {
    let dir = env::args().nth(1).expect("usage: crashtest <dir>");
    let cfg = Config {
        dir: dir.into(),
        shards: 4,
        sync: SyncPolicy::Always,
    };
    let store = Store::open(cfg).unwrap();
    let out = std::io::stdout();
    let mut lock = out.lock();
    let mut i = 0;
    loop {
        let k = format!("k{i}");
        store.set(&k, k.as_bytes(), None).unwrap();
        writeln!(lock, "{k}").unwrap();
        i += 1;
    }
}
