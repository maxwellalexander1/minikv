use minikv::{Config, Store, SyncPolicy};
use std::time::Duration;

fn cfg(dir: &std::path::Path) -> Config {
    Config {
        dir: dir.to_path_buf(),
        shards: 4,
        sync: SyncPolicy::Never,
    }
}

#[test]
fn expires_on_get() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(cfg(tmp.path())).unwrap();
    s.set("short", b"v", Some(Duration::from_millis(100))).unwrap();
    assert_eq!(s.get("short"), Some(b"v".to_vec()));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(s.get("short"), None);
}

#[test]
fn sweeper_cleans_without_get() {
    let tmp = tempfile::tempdir().unwrap();
    let s = Store::open(cfg(tmp.path())).unwrap();
    s.set("a", b"1", Some(Duration::from_millis(100))).unwrap();
    s.set("b", b"2", None).unwrap();
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(s.get("b"), Some(b"2".to_vec()));
    assert_eq!(s.len(), 1);
}

#[test]
fn expired_not_restored() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = Store::open(cfg(tmp.path())).unwrap();
        s.set("gone", b"x", Some(Duration::from_millis(100))).unwrap();
        s.set("stay", b"y", None).unwrap();
        std::thread::sleep(Duration::from_millis(200));
    }
    let s2 = Store::open(cfg(tmp.path())).unwrap();
    assert_eq!(s2.get("gone"), None);
    assert_eq!(s2.get("stay"), Some(b"y".to_vec()));
}
