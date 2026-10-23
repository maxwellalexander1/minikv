use minikv::{hash::fnv1a64, Config, Store, SyncPolicy};
use std::fs::OpenOptions;
use std::io::Write;

fn cfg(dir: &std::path::Path, shards: usize) -> Config {
    Config {
        dir: dir.to_path_buf(),
        shards,
        sync: SyncPolicy::Never,
    }
}

fn shard_file(dir: &std::path::Path, key: &str, shards: usize) -> std::path::PathBuf {
    let i = (fnv1a64(key) % shards as u64) as usize;
    dir.join(format!("shard-{i}.wal"))
}

#[test]
fn reopen_keeps_data() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = Store::open(cfg(tmp.path(), 4)).unwrap();
        s.set("a", b"1", None).unwrap();
        s.set("b", b"2", None).unwrap();
        s.delete("a").unwrap();
    }
    let s2 = Store::open(cfg(tmp.path(), 4)).unwrap();
    assert_eq!(s2.get("a"), None);
    assert_eq!(s2.get("b"), Some(b"2".to_vec()));
}

#[test]
fn torn_tail_is_cut() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = Store::open(cfg(tmp.path(), 2)).unwrap();
        s.set("k1", b"v1", None).unwrap();
        s.set("k2", b"v2", None).unwrap();
    }
    // glue half a record on the end of k2's shard file
    let p = shard_file(tmp.path(), "k2", 2);
    let mut f = OpenOptions::new().append(true).open(&p).unwrap();
    f.write_all(&[9, 0, 0, 0, 1, 2, 3]).unwrap();
    f.flush().unwrap();

    let s2 = Store::open(cfg(tmp.path(), 2)).unwrap();
    assert_eq!(s2.get("k1"), Some(b"v1".to_vec()));
    assert_eq!(s2.get("k2"), Some(b"v2".to_vec()));
}

#[test]
fn flipped_byte_drops_tail() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = Store::open(cfg(tmp.path(), 2)).unwrap();
        s.set("keep", b"yes", None).unwrap();
    }
    let p = shard_file(tmp.path(), "keep", 2);
    let mut bytes = std::fs::read(&p).unwrap();
    let n = bytes.len() - 1;
    bytes[n] ^= 0xff;
    std::fs::write(&p, &bytes).unwrap();

    let s2 = Store::open(cfg(tmp.path(), 2)).unwrap();
    assert_eq!(s2.get("keep"), None);
}

#[test]
fn delete_survives_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let s = Store::open(cfg(tmp.path(), 4)).unwrap();
        s.set("x", b"1", None).unwrap();
        s.delete("x").unwrap();
    }
    let s2 = Store::open(cfg(tmp.path(), 4)).unwrap();
    assert_eq!(s2.get("x"), None);
}

#[test]
fn shard_count_change_fails() {
    let tmp = tempfile::tempdir().unwrap();
    drop(Store::open(cfg(tmp.path(), 4)).unwrap());
    match Store::open(cfg(tmp.path(), 8)) {
        Ok(_) => panic!("should fail"),
        Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
    }
}
