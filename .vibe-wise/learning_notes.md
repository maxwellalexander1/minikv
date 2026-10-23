# MiniKV Learning Notes

## Design (from Design Doc)
- Store = 16 whiteboards (shards). Key picks board via fnv1a64(key) % N.
- Notebook (WAL) per board coming in M2. Whiteboard = fast but lost on crash.
- Rule: notebook first, then whiteboard. If notebook write fails, don't touch whiteboard.
- Language: Rust (best to impress MongoDB — shows Send/Sync, locks, fsync vs flush).

## M1 — what changed and why (2026-10-23)
- `Cargo.toml`: new crate minikv. Added crc32fast now so M2 needs no dep change.
- `src/hash.rs`: hand-made fnv1a64. Stable across restarts, unlike DefaultHasher.
- `src/shard.rs`: ShardInner { map }. Entry stores value + absolute expires_at (M3 will enforce).
- `src/lib.rs`: Store { shards: Vec<Mutex<ShardInner>> }. One lock per shard = threads on different shards don't block. API: open/get/set/delete/len. set stores absolute expiry timestamp for future replay determinism.
- `tests/m1_test.rs`: basic CRUD + 8-thread distinct-range test.
- Tests: `cargo test` — 1 unit + 3 integration, all pass.

## Interview line
- "Whiteboard lost on crash, notebook isn't — so log first, then apply, replay repaints after restart."
