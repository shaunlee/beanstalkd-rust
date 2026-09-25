# beanstalkd-rust

A Rust reimplementation of [beanstalkd](https://github.com/beanstalkd/beanstalkd), the simple work queue, that is byte-for-byte compatible with the original protocol so existing clients work unmodified.

Status: **P0 complete**, an in-memory server compatible with the reference. Persistence (P1), TLS/auth/metrics (P2) and Raft replication (P3) are planned; see `docs/PLAN.md`.

## Build and run

```sh
cargo build --release -p bstk-server
./target/release/beanstalkd-rs -l 127.0.0.1 -p 11300
```

Flags: `-l ADDR`, `-p PORT`, `-z MAX_JOB_SIZE`, `-V` (verbose), `-v` (version). `SIGUSR1` enters drain mode.

## Test

The differential tests compare against the reference C beanstalkd, which must be built first:

```sh
scripts/build-ref.sh      # builds the pinned reference into .ref/
scripts/check.sh          # fmt, clippy, build, all tests incl. differential
clients/run-smoke.sh      # real-client smoke tests (needs python3 and go)
```

Benchmarks: see `docs/BENCH.md` (`scripts/build-ref.sh --optimized`, then `bench/run-matrix.sh`).

## Documentation

- `docs/DESIGN.md`: architecture and design
- `docs/PLAN.md`: development, test and acceptance plan
- `docs/COMPAT.md`: behavior quirks of the reference we mirror, and known differences
- `docs/BENCH.md`: performance results
