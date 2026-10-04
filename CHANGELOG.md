# Changelog

All notable user-facing changes to beanstalkd-rs are recorded here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project uses [Semantic Versioning](https://semver.org/) (before 1.0, a
minor version may change behavior; such changes are called out here).

Release tags are `v<version>` and match the workspace version in
`Cargo.toml`. 0.5.0 is the first tagged release; the `vN` labels in
`docs/DESIGN.md` §10 are revisions of the design document, not releases.

## [Unreleased]

### Added

- **Operations guide** ([docs/OPERATIONS.md](https://github.com/shaunlee/beanstalkd-rust/blob/main/docs/OPERATIONS.md)):
  install, configuration, persistence, security, cluster bootstrap, node
  loss and rejoin, backup and restore, upgrades, monitoring and alerts,
  troubleshooting. Shipped in the release archives with
  `scripts/mkcluster-certs.sh`, which creates a cluster CA and node
  certificates.

### Changed

- **Cluster protocol version 4** (incompatible): nodes of 0.5.x cannot join
  a cluster of later versions, or the reverse. Upgrade a 0.5.x cluster by
  stopping every node, upgrading all of them, then starting them again; a
  rolling upgrade from 0.5.x is not possible. Version 4 adds an extended
  status probe and an authenticated admin channel on the cluster port (for
  membership changes in a later release);
  `scripts/mkcluster-certs.sh DIR admin` issues its client certificate
  (SAN `bstk-admin`).
- Log lines carry ANSI color codes only when stderr is a terminal, so
  journald, `docker logs` and log files get plain text.

### Fixed

- When every file descriptor is in use, the accept loops pause 50 ms after
  each failed `accept` instead of retrying at once, which on Linux spun a
  core and logged hundreds of thousands of warnings per second.
- A binlog with a segment number or job id at the top of the 64-bit range
  (only possible in a damaged or hand-made file) is reported as corrupt at
  startup instead of crashing or reusing a job id. The same applies to
  snapshot file names and to snapshot payloads in cluster mode.
- Cluster `/readyz` (and `beanstalkd_cluster_ready`) now reports 503 while
  the node is isolated from the cluster and closing client connections;
  before, a cut-off leader kept answering 200.

## [0.5.0]

First release. A Rust reimplementation of
[beanstalkd](https://github.com/beanstalkd/beanstalkd) 1.13 that existing
clients can use unmodified.

### Added

- **Protocol compatibility**: every command, reply, error code and edge case
  of the beanstalkd text protocol, checked byte for byte against the
  reference server by differential tests and real clients (Python
  greenstalk, Go go-beanstalk). The few intentional differences are listed
  in [docs/COMPAT.md](https://github.com/shaunlee/beanstalkd-rust/blob/main/docs/COMPAT.md).
- **Reference command line**: `-l`, `-p`, `-z`, `-b`, `-f`, `-F`, `-s`,
  `-V`, `-v` with the reference's defaults; `--version` as an alias of `-v`.
  `-u` is rejected: run the server as the desired user (for example with the
  systemd unit in `packaging/systemd/`).
- **Write-ahead log** (`-b DIR`, `-f MS`, `-F`, `-s BYTES`): jobs survive
  restarts and crashes, and no reply is sent before its change reaches the
  log. The on-disk format is not the reference's: a reference binlog
  directory cannot be reused.
- **Graceful shutdown** on SIGTERM / SIGINT (syncs the log; the reference
  exits abruptly), and drain mode on SIGUSR1 as in the reference.
- **Configuration file** (`--config FILE`, TOML; `--check-config` validates
  it): several listeners, binlog, logging, HTTP and cluster settings. See
  [docs/beanstalkd-rs.example.toml](https://github.com/shaunlee/beanstalkd-rust/blob/main/docs/beanstalkd-rs.example.toml) and
  the ready-made files in [packaging/examples/](https://github.com/shaunlee/beanstalkd-rust/tree/main/packaging/examples).
- **TLS and mutual TLS** listeners, and an optional token authentication
  extension (`auth <token>`, needs client support) on TLS listeners, with
  limits on unauthenticated connections.
- **HTTP monitoring** (off by default): `/healthz`, `/readyz`, `/metrics`
  (Prometheus) and `/admin` (read-only JSON).
- **Cluster mode** (`[cluster]`): 3 or 5 nodes replicate every job and
  connection through Raft over mutual TLS, keep serving through the loss of
  a minority of nodes, and look like one beanstalkd server to clients
  connected to any node. Bootstrap with `--cluster-init`; a node with an
  empty data directory rejoins safely. See [docs/DESIGN.md](https://github.com/shaunlee/beanstalkd-rust/blob/main/docs/DESIGN.md)
  §8 and [docs/OPERATIONS.md](https://github.com/shaunlee/beanstalkd-rust/blob/main/docs/OPERATIONS.md).
- **Performance**: in standalone plaintext mode, more operations per CPU
  second than the reference (1.04x to 1.29x on the benchmark matrix);
  worker threads are tunable with `--threads` / `server.threads`. See
  [docs/BENCH.md](https://github.com/shaunlee/beanstalkd-rust/blob/main/docs/BENCH.md).
- **Packaging**: release archives for Linux (x86_64, aarch64; glibc 2.34
  or newer, or fully static with musl) and macOS (aarch64) with SHA-256
  checksums, a container image (`Dockerfile`), a
  hardened systemd unit and example configurations
  ([packaging/](https://github.com/shaunlee/beanstalkd-rust/tree/main/packaging)).

[Unreleased]: https://github.com/shaunlee/beanstalkd-rust/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/shaunlee/beanstalkd-rust/releases/tag/v0.5.0
