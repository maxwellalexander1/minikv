use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn kill_test() {
    for _ in 0..5 {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_str().unwrap().to_string();
        let bin = env!("CARGO_BIN_EXE_crashtest");

        let mut child = Command::new(bin)
            .arg(&dir)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let mut keys = vec![];
        let mut line = String::new();
        // let it write ~200 keys then kill it
        for _ in 0..200 {
            line.clear();
            let n = reader.read_line(&mut line).unwrap();
            if n == 0 {
                break;
            }
            keys.push(line.trim().to_string());
        }
        child.kill().unwrap();
        let _ = child.wait();

        let cfg = minikv::Config {
            dir: tmp.path().to_path_buf(),
            shards: 4,
            sync: minikv::SyncPolicy::Always,
        };
        let s = minikv::Store::open(cfg).unwrap();
        for k in &keys {
            assert_eq!(s.get(k), Some(k.as_bytes().to_vec()), "missing {k}");
        }
    }
}
