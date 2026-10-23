use minikv::{Config, Store, SyncPolicy};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn test_config(shards: usize) -> Config {
    Config {
        dir: PathBuf::from("/tmp/minikv-m1-unused"),
        shards,
        sync: SyncPolicy::Never,
    }
}

#[test]
fn basic_set_get_delete_len() {
    let store = Store::open(test_config(16)).unwrap();
    assert_eq!(store.len(), 0);
    assert_eq!(store.get("color"), None);

    store.set("color", b"blue", None).unwrap();
    assert_eq!(store.get("color"), Some(b"blue".to_vec()));
    assert_eq!(store.len(), 1);

    // Overwrite
    store.set("color", b"red", None).unwrap();
    assert_eq!(store.get("color"), Some(b"red".to_vec()));
    assert_eq!(store.len(), 1);

    // Delete
    assert_eq!(store.delete("color").unwrap(), true);
    assert_eq!(store.get("color"), None);
    assert_eq!(store.delete("color").unwrap(), false);
    assert_eq!(store.len(), 0);
}

#[test]
fn empty_value_and_binary() {
    let store = Store::open(test_config(4)).unwrap();
    store.set("empty", b"", None).unwrap();
    assert_eq!(store.get("empty"), Some(vec![]));

    let blob: Vec<u8> = (0..=255u8).collect();
    store.set("blob", &blob, Some(Duration::from_secs(60))).unwrap();
    assert_eq!(store.get("blob"), Some(blob));
}

#[test]
fn eight_threads_distinct_ranges() {
    let store = Arc::new(Store::open(test_config(16)).unwrap());
    let mut handles = vec![];

    for t in 0..8 {
        let s = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            // Each thread owns 1000 distinct keys: no overlap between threads.
            for i in 0..1000 {
                let k = format!("t{t}-key-{i}");
                s.set(&k, format!("v-{t}-{i}").as_bytes(), None).unwrap();
                // Read-your-own-write
                assert_eq!(s.get(&k), Some(format!("v-{t}-{i}").into_bytes()));
            }
            // Mixed ops: delete even keys
            for i in (0..1000).step_by(2) {
                let k = format!("t{t}-key-{i}");
                assert!(s.delete(&k).unwrap());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // 8 threads * 500 surviving (odd) keys each = 4000
    assert_eq!(store.len(), 4000);

    // Spot-check final state matches expectations
    assert_eq!(store.get("t0-key-1"), Some(b"v-0-1".to_vec()));
    assert_eq!(store.get("t0-key-0"), None); // deleted
    assert_eq!(store.get("t7-key-999"), Some(b"v-7-999".to_vec()));
}
