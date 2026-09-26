# beanstalkd-rust

A Rust reimplementation of [beanstalkd](https://github.com/beanstalkd/beanstalkd), the simple work queue, that is byte-for-byte compatible with the original protocol so existing clients work unmodified.

Status: **P3 complete**: a server compatible with the reference, with an optional write-ahead log (`-b`), TLS / mTLS, optional token authentication, Prometheus metrics, and an optional Raft cluster mode (3 or 5 nodes) that keeps serving through the loss of a minority of nodes. Performance work (P4) is next; see `docs/PLAN.md`.

## Build and run

```sh
cargo build --release -p bstk-server
./target/release/beanstalkd-rs -l 127.0.0.1 -p 11300
```

Flags: `-l ADDR`, `-p PORT`, `-z MAX_JOB_SIZE`, `-V` (verbose), `-v` (version). `SIGUSR1` enters drain mode.

Persistence, as in the reference:

- `-b DIR`: write-ahead log directory
- `-f MS`: fsync at most once every MS milliseconds (default 50); `-f0` fsyncs every write before replying
- `-F`: never fsync
- `-s BYTES`: binlog file size (default 10 MiB)

Behavior across restarts and the intentional differences from the reference are listed in `docs/COMPAT.md` (section "Binlog" and the D list).

## Configuration, TLS and monitoring

```sh
beanstalkd-rs --config /etc/beanstalkd-rs.toml
beanstalkd-rs --config /etc/beanstalkd-rs.toml --check-config
```

The TOML file (fully commented example: `docs/beanstalkd-rs.example.toml`) can define several listeners, each plaintext or TLS with `auth = "none"`, `"token"` or `"mtls"`, plus binlog, logging and an HTTP listener serving `/healthz`, `/readyz`, `/metrics` (Prometheus) and `/admin` (JSON). Everything is off by default; without a config file the server behaves exactly like the reference. Token authentication is a beanstalkd-rs extension (`auth <token>`), so it needs client support; mTLS works with any client that can use TLS.

## Cluster mode

With a `[cluster]` section (see the example file and `docs/DESIGN.md` §8), 3 or 5 nodes replicate every job and connection through Raft; clients connect to any node with the same protocol.

- **Bootstrap** a new cluster by starting every initial node with `--cluster-init`, all with the same `[[cluster.peer]]` list. It is refused if the node's `data_dir` already holds state; if a reachable peer already belongs to a running cluster, the node does not bootstrap but joins as below.
- **Restart** a node without `--cluster-init`.
- **Rejoin**: a node started with an empty `data_dir` (a new disk, or after losing its data) and without `--cluster-init` rejoins its cluster. Until it has caught up with a leader it does not vote, does not stand for election and serves no clients (`/readyz` is 503); a `rejoin` marker in `data_dir` keeps it in this mode across crashes. It needs a running leader, so at most a minority of nodes may be rejoining at once. Never restore an old copy of a `data_dir`: wipe it instead.
- Cluster traffic uses mTLS (`[cluster.tls]`). `insecure_plaintext = true` is for tests and is accepted only on loopback addresses unless `insecure_plaintext_allow_remote = true` is also set.

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

## License

MIT, see `LICENSE`. beanstalkd itself is also MIT-licensed.
