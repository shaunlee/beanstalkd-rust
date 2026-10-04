# beanstalkd-rust

[![CI](https://github.com/shaunlee/beanstalkd-rust/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shaunlee/beanstalkd-rust/actions/workflows/ci.yml)

`beanstalkd-rs` is a Rust reimplementation of
[beanstalkd](https://github.com/beanstalkd/beanstalkd), the simple work
queue. It speaks the beanstalkd protocol byte for byte, so existing
clients work unmodified, and adds TLS, monitoring and an optional
replicated cluster mode.

**Status**: phases P0 to P5 are done (protocol compatibility, write-ahead
log, TLS and operability, Raft cluster mode, performance, production
readiness: Linux validation, CI, packaging, operations guide, hardening).
Next: dynamic cluster membership (P6). 0.5.0 is the first release. See [CHANGELOG.md](CHANGELOG.md)
and the plan in [docs/PLAN.md](docs/PLAN.md).

## Features

- Every command, reply and edge case of the beanstalkd protocol, checked
  against the reference server by differential tests and real clients;
  the reference's command line (`-l -p -z -b -f -F -s -V -v`).
- Write-ahead log (`-b`) with the reference's fsync policies; jobs survive
  restarts and crashes, and no reply is sent before its change is logged.
- Graceful shutdown on SIGTERM, drain mode on SIGUSR1.
- TOML configuration (`--config`, `--check-config`): several listeners,
  each plaintext or TLS, with mutual TLS or token authentication.
- HTTP endpoints: `/healthz`, `/readyz`, `/metrics` (Prometheus) and
  `/admin` (JSON).
- Cluster mode: 3 or 5 nodes replicate every job and connection through
  Raft over mutual TLS and keep serving through the loss of a minority of
  nodes; clients connect to any node.
- More operations per CPU second than the reference in standalone
  plaintext mode ([docs/BENCH.md](docs/BENCH.md)).

The few intentional differences from the reference are listed in
[docs/COMPAT.md](docs/COMPAT.md).

## Quick start

From source (Rust 1.98 or newer):

```sh
cargo build --release --locked -p bstk-server
./target/release/beanstalkd-rs -l 127.0.0.1 -p 11300 -b ./binlog
```

With Docker (the image is built locally; none is published):

```sh
docker build -t beanstalkd-rs .
docker run -d -p 127.0.0.1:11300:11300 -v bstk-data:/data beanstalkd-rs -l 0.0.0.0 -p 11300 -b /data
```

From a release archive (Linux x86_64 / aarch64, glibc or static musl;
macOS aarch64), on the
[releases page](https://github.com/shaunlee/beanstalkd-rust/releases):

```sh
sha256sum -c --ignore-missing SHA256SUMS
tar -xzf beanstalkd-rs-<version>-<target>.tar.gz
```

Each archive holds the binary, a systemd unit, example configurations,
the cluster certificate script and the operations guide. Installation
with systemd, configuration, clusters, backups, upgrades and monitoring:
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Documentation

- [docs/OPERATIONS.md](docs/OPERATIONS.md): operations guide
- [docs/beanstalkd-rs.example.toml](docs/beanstalkd-rs.example.toml): every configuration key
- [docs/DESIGN.md](docs/DESIGN.md): architecture and design
- [docs/COMPAT.md](docs/COMPAT.md): differences from the reference beanstalkd
- [docs/BENCH.md](docs/BENCH.md): performance results
- [docs/PLAN.md](docs/PLAN.md): development, test and acceptance plan
- [CHANGELOG.md](CHANGELOG.md): changes per release

## Development and CI

The differential tests compare against the reference C beanstalkd, built
first:

```sh
scripts/build-ref.sh      # builds the pinned reference into .ref/
scripts/check.sh          # fmt, clippy, build, all tests incl. differential
clients/run-smoke.sh      # real-client smoke tests (needs python3 and go)
```

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | Runner | What |
|---|---|---|
| `check` | `ubuntu-latest` (x86_64), `ubuntu-24.04-arm` (aarch64), `macos-latest` | builds the reference, then `scripts/check.sh --no-fail-fast` (fmt, clippy, build, all tests including the differential and stunnel suites) |
| `smoke` | `ubuntu-latest` | `clients/run-smoke.sh` in plain, binlog, restart, TLS, mTLS, cluster and cluster leader-kill modes |
| `chaos` | `ubuntu-latest` | 200 in-process seeds and 5 multi-process runs |
| `docker` | `ubuntu-latest` | builds the image and runs `scripts/docker-smoke.sh` |
| `deny` | `ubuntu-latest` | `cargo deny check` (`deny.toml`) |

`.github/workflows/release.yml` builds, smoke-tests and publishes the
release archives on a `v*` tag equal to `v` + the workspace version (a
manual run is a dry run). `.github/workflows/weekly.yml` runs 1,000
in-process chaos seeds and 50 multi-process runs.

The same locally:

```sh
BSTK_REQUIRE_STUNNEL=1 scripts/check.sh --no-fail-fast   # needs stunnel (stunnel4 on Debian/Ubuntu)
SMOKE_CLUSTER_KILL=1 clients/run-smoke.sh                 # one smoke mode; see clients/README.md
cargo build -p bstk-server
BSTK_CHAOS_SEEDS=200 cargo test --release -p bstk-chaos --test inprocess full -- --ignored --nocapture
BSTK_CHAOS_MP_RUNS=5 cargo test --release -p bstk-chaos --test multiprocess full -- --ignored --nocapture
cargo deny check
```

On macOS, stunnel must be 5.82 or newer for the stunnel suites (older
versions close a half-closed connection before forwarding the reply).
Benchmarks: [docs/BENCH.md](docs/BENCH.md) (`scripts/build-ref.sh
--optimized`, then `bench/run-matrix.sh`).

## License

MIT, see [LICENSE](LICENSE). beanstalkd itself is also MIT-licensed.
