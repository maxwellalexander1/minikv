# minikv

Small sharded key-value store in Rust with TTL and a write-ahead log for crash recovery.

## layout

- `src/hash.rs` - FNV-1a hash to pick shards
- `src/shard.rs` - per-shard map + WAL file
- `src/wal.rs` - record encode/decode `[len|crc|payload]`
- `src/lib.rs` - `Store` with `get/set/delete`
- `src/bin/crashtest.rs` - writer used by the kill test
- `src/bin/bench.rs` - quick benchmark

## run

```sh
cargo test
cargo run --release --bin bench
```

## how it works

Keys are hashed to a shard (`fnv1a64(key) % N`). Each shard has its own lock and its own `shard-i.wal` file so threads don't block each other much.

`set` does: lock shard, write record to WAL, flush (and `sync_data` if `Always`), then update the map. If the WAL write fails it doesn't touch the map. On open it replays each WAL and cuts off any half-written tail (bad CRC or short read).

TTL is stored as an absolute time. `get` drops expired keys, and a background thread sweeps every 200ms.

Sync policies:
- `Always` - flush + sync every write. Safe, slow on real disks.
- `EverySecond` - flush every write, sync once a second in the sweeper.
- `Never` - just flush, OS decides.

## bench

Release build, 16 shards, 10k keys:

```
read spread 1t: 6126549 ops/sec
read spread 4t: 10761045 ops/sec
read spread 8t: 7287786 ops/sec
read hot 1t: 10929320 ops/sec
read hot 4t: 5194137 ops/sec
read hot 8t: 3140877 ops/sec
write Never 1t: 325861 ops/sec
write Never 4t: 1143413 ops/sec
write Never 8t: 1082167 ops/sec
write EverySecond 1t: 576084 ops/sec
write EverySecond 4t: 883530 ops/sec
write EverySecond 8t: 925237 ops/sec
write Always 1t: 1488756 ops/sec
write Always 4t: 945231 ops/sec
write Always 8t: 2069131 ops/sec
```

Note: this was on /tmp (tmpfs) so `sync_data` is cheap here. On a real disk `Always` would be a lot slower. Hot keys are slower with more threads because they all fight for one shard lock.

Kill test: writer prints each key after `set` returns, parent kills it after ~200 keys and checks all printed keys survive reopen. Passes.

## limits

No compaction (WAL keeps old records), no replication, no transactions, TTL uses wall clock so clock jumps can expire things early/late.
