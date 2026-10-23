# MiniKV: Design Document

*A small sharded key-value store in Rust with TTL expiry and a write-ahead log (WAL) with crash recovery. Written to be built in 3 to 5 hours, either solo or pair-programmed with an AI.*

## 1. Goal and scope

Build a library (plus a tiny test harness) that stores byte values under string keys, expires keys after a TTL, and survives a process crash by replaying an append-only log on startup.

**In scope:** sharded in-memory map, TTL (lazy expiry plus a background sweeper), per-shard WAL, recovery by replay, configurable fsync policy, crash-recovery tests, a benchmark.

**Out of scope (do not build):** networking, log compaction or snapshots, replication, transactions, range scans, any encoding beyond the record format below.

**Why it exists:** it reuses the sharding and sweeper ideas from ShardGate and adds what makes a system database-like: durability and recovery.

## 2. Architecture

```
              Store
   +------------------------+
   | shards: Vec<Shard>     |    shard index = fnv1a64(key) % N
   +------------------------+
     |         |          |
  Shard 0   Shard 1 ... Shard N-1
  Mutex<ShardInner>
    map: HashMap<String, Entry>
    wal: BufWriter<File>      (file: shard-<i>.wal)

  Sweeper thread: every ~200 ms, lock ONE shard at a time,
  remove expired entries, and (EverySecond policy) sync the WAL.
```

`Entry { value: Vec<u8>, expires_at_ms: Option<u64> }` where expires\_at\_ms is an absolute unix timestamp in milliseconds.

**Key decisions and the reasoning behind them**

- **Per-shard WAL files.** Each shard owns its own log, so writes to different shards never contend on a shared file, and there is no cross-shard ordering problem. The cost is more files; that is fine.
- **Absolute expiry timestamps.** A relative TTL stored in the log would restart its clock on every replay. Storing expires\_at makes replay deterministic.
- **Stable hash, written by hand.** Use FNV-1a 64 (about 10 lines). Do not use the standard library's default hasher for routing: its output is not guaranteed stable across Rust versions, and a key must map to the same shard after a restart. Store the shard count in a `meta` file and refuse to open if it differs.
- **Log under the shard lock.** The WAL append and the map update both happen while holding the shard lock, so log order equals apply order for any given key.
- **Log, then apply.** If the WAL write fails, return the error and do not touch the map.
- **Sweeper does not log.** Expiry is derived from timestamps, so replay can skip expired entries itself.

## 3. Public API

```rust
pub struct Config { pub dir: PathBuf, pub shards: usize, pub sync: SyncPolicy }
pub enum SyncPolicy { Always, EverySecond, Never }

impl Store {
    pub fn open(cfg: Config) -> io::Result<Store>;
    pub fn get(&self, key: &str) -> Option<Vec<u8>>;
    pub fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> io::Result<()>;
    pub fn delete(&self, key: &str) -> io::Result<bool>;
    pub fn len(&self) -> usize;
}
```

Store must be `Send + Sync` so it can be shared via `Arc`. On drop: stop the sweeper thread and flush every WAL.

## 4. WAL format and recovery

Record layout (all integers little-endian):

```
[ len: u32 ][ crc32: u32 ][ payload: len bytes ]

payload = op: u8 | key_len: u32 | key bytes
          | (SET only) expires_at_ms: u64 (0 = none) | val_len: u32 | value bytes

op: 1 = SET, 2 = DEL
```

The CRC covers the payload only (use the `crc32fast` crate).

**Write path for set:** lock shard, encode record, write to the BufWriter, apply the sync policy, insert into the map, unlock.

**Sync policies**

- `Always`: flush, then `sync_data` after every record. Slow, and the only mode that protects against power loss.
- `EverySecond`: flush after every record; the sweeper thread calls `sync_data` about once per second.
- `Never`: flush into the OS only; the OS decides when it reaches disk.

Note: `BufWriter::flush` pushes bytes to the OS. It does not guarantee they are on disk. Only `sync_data` or `sync_all` does that.

**Recovery (on open), per shard file**

1. Read records sequentially.
2. For each valid record: SET inserts into the map (skip it if expires\_at is already in the past), DEL removes the key.
3. At the first short read or CRC mismatch, stop. Truncate the file to the end of the last good record (this removes a torn tail), then continue.
4. Reopen the file in append mode.

## 5. TTL

- `set` with a ttl computes `expires_at = now_ms + ttl`.
- `get` checks the entry: if it has expired, remove it from the map (memory only) and return None.
- The sweeper loop: sleep, then for each shard in turn, lock it, collect and remove expired keys, unlock. Never hold two shard locks at once.
- Use wall-clock time (`SystemTime`) because timestamps must survive restarts. Clock jumps are a known limitation; say so in the README.
- The WAL keeps old records for expired or overwritten keys because there is no compaction. State this as a limitation, not an oversight.

## 6. Concurrency rules

1. A thread holds at most one shard lock at a time.
2. The WAL append happens inside the shard lock.
3. The sweeper keeps its critical sections short: collect the expired keys, remove them, release.
4. No lock is held while sleeping or doing I/O that is not part of the write path.

## 7. Project layout

```
minikv/
  Cargo.toml           deps: crc32fast; dev-deps: tempfile (optional: criterion)
  src/lib.rs           Store, Config, SyncPolicy
  src/shard.rs         ShardInner, apply and replay
  src/wal.rs           encode_record, read_records (iterator), truncate helper
  src/hash.rs          fnv1a64
  src/bin/crashtest.rs writer process used by the kill test
  src/bin/bench.rs     benchmark
  tests/               integration tests
  README.md
```

## 8. Build order

| Milestone | Time | Done when |
| --- | --- | --- |
| M1: hash, in-memory sharded map, get/set/delete | 45 min | Unit tests pass; an 8-thread test passes; no WAL yet |
| M2: WAL encode/decode, wired into set/delete, replay on open, torn-tail handling | 60 to 90 min | Write, drop, reopen returns the data; a test that appends half a record still opens and keeps earlier data |
| M3: TTL (lazy plus sweeper) | 30 min | Expired keys vanish on get and by sweep, and are not restored after reopen |
| M4: kill test and benchmark | 30 min | Kill test passes repeatedly; ops/sec table recorded |
| M5: README and resume bullets | 15 min | Repo pushed with README and build instructions |

If you are short on time, finish M1 and M2 and skip nothing in M2. The WAL and recovery are the point of the project.

## 9. Test plan

- **Unit:** record encode/decode round trip; CRC mismatch is detected; hash is stable (hard-code expected values for a few keys so a future change is caught).
- **Recovery:** (a) clean reopen; (b) truncated final record; (c) a flipped byte in the final record; (d) expired key not restored after reopen; (e) a deleted key stays deleted after reopen.
- **Concurrency:** 8 threads each own a distinct key range and run many mixed operations; afterwards assert the final state matches what each thread expects.
- **Kill test:** the `crashtest` binary writes keys in sequence and prints each key after `set` returns Ok. The parent process reads that output, sends SIGKILL at a random moment, reopens the store, and asserts every printed key is present. Repeat many times.

**Honest caveat about the kill test:** SIGKILL stops the process, but the OS still holds the data it was given. The test proves your replay logic and torn-tail handling. It does not prove power-loss durability. Only `fsync` (SyncPolicy::Always) protects against that, and it is hard to test, so do not claim more than you tested.

## 10. Benchmark plan

- Release build; record the CPU and core count.
- Threads: 1, 4, 8.
- Workloads: spread keys (about 10k distinct keys) and one hot key.
- Reads: get-only throughput.
- Writes: set-heavy throughput under each sync policy (Never, EverySecond, Always).
- Report a small table of ops/sec. Expect Always to be far slower on writes. That is the point: be ready to explain fsync cost and note that group commit is the standard fix (which you did not build).

## 11. Resume bullets

Replace the NCF entry with something like this, filling in numbers only from your own runs:

```
MiniKV -- Sharded Key-Value Store | Rust                      Oct. 2026
- Built a sharded in-memory key-value store (16 shards) with TTL expiry and a per-shard
  write-ahead log of CRC-checked records; recovers by log replay and truncates torn tails.
- Verified recovery with a SIGKILL test (X of X acknowledged writes recovered over N runs);
  benchmarked X M ops/sec reads and Y K ops/sec writes (fsync per write) vs Z K buffered, 8 threads.
```

**Rules:** claim fsync durability only if you ran and measured SyncPolicy::Always. Do not write ACID, distributed, or database. Keep the GitHub link and a README that states the limitations (no compaction, no replication, wall-clock TTL).

## 12. Questions to be ready for in an interview

- Why log before applying? What happens if the process dies between the two?
- What is a torn write, and how do you detect it? Why a CRC?
- What does fsync guarantee that BufWriter::flush does not?
- Why per-shard WALs? What would change with one global log (ordering, contention, group commit)?
- Why absolute expiry timestamps in the log?
- What breaks if the system clock changes?
- How would you add compaction? (snapshot, then rotate or truncate the log)
- Why is the hot-key case slower than spread keys?
- What does Always cost, and how does group commit reduce that cost?

## 13. Prompt for an AI pair programmer

Paste this along with the document:

```
I'm building MiniKV, a small Rust key-value store, following the attached design doc.
I want to learn as I go, so act as a pair programmer, not a code generator.
- Work one milestone at a time, starting with M1.
- For each step, explain the concept in a few sentences, then have me write the function.
- Give hints before solutions. Only show full code if I ask, or if I'm still stuck after two attempts.
- Review my code for bugs, lock-ordering mistakes, and unsafe assumptions.
- Do not add features outside the scope in section 1.
- At the end of each milestone, ask me to explain my design decisions back to you,
  as interview practice.
```
