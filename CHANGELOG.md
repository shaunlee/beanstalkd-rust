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
  loss and rejoin, membership runbooks (grow 1 → 3 and 3 → 5, shrink,
  replace a failed node or disk, remove the leader, change an address, CA
  rotation), backup and restore, upgrades, monitoring and alerts,
  troubleshooting. Shipped in the release archives with
  `scripts/mkcluster-certs.sh`, which creates a cluster CA and node
  certificates.

- **Cluster startup modes for changing membership**: `[[cluster.peer]]` now lists seeds and address
  overrides (any number; a node need not list itself), and the new
  `cluster.initial_voters` chooses the voters `--cluster-init` creates (all
  peers by default; 1, 3 or 5). A node whose id is not a member yet waits
  until it is added (join), whether it is started before or after the add; a
  node that lost its data rejoins against the cluster's current voters,
  learned from any reachable member, and waits while a membership change is
  in progress.

- **Membership changes at runtime**, run by the leader on requests over the
  cluster port's authenticated admin channel (`beanstalkd-rs cluster`, below):
  add a node as a learner, promote a learner to voter,
  remove a node (the leader too), and change a node's address. Each request
  names the membership it was based on and is refused if that changed; one
  change runs at a time. Guardrails: one voter added or removed per change;
  only a learner that has caught up is promoted; no voter change while a
  voter is rejoining after data loss or does not answer (except removing
  that voter, if it is down); node ids are never reused; fewer than 3 voters
  only when forced; a plaintext address off loopback only when forced. A
  change interrupted by a leader change is finished by the next leader. A
  removed node is not told: stop it. Removing the leader costs one election.

- **`beanstalkd-rs cluster` operator command**: `status` (voters, learners,
  the leader, each node's state and lag), `add`, `promote`, `remove` and
  `set-addr` (`--force` where the cluster allows it), over mTLS with the
  `bstk-admin` certificate from `scripts/mkcluster-certs.sh DIR admin` (or
  `--insecure-plaintext` for test clusters), with `--node` / `--config`
  seeds, `--timeout` and `--json`. It follows the leader, waits for a started
  change to complete, prints the cluster's notes, and exits 0 (done), 1
  (refused or conflicting), 2 (usage), 3 (unreachable) or 4 (accepted but not
  confirmed in time). A removed node that still runs (and still names itself
  or its old leader) is skipped for a member's view. The server's flags are
  unchanged.

- **Membership metrics** on `/metrics` and in the `cluster` object of
  `/admin`: `beanstalkd_cluster_voters`, `_learners`,
  `_member{node,role,addr}`, `_membership_joint`, `_membership_log_index`,
  `_membership_committed`, `_highest_member_id`, `_is_member`, `_joining`
  and, on the leader, `_learner_lag{node}`; `/admin` adds `membership`
  (voters, learners, addresses, joint, log index, committed, highest
  member), `is_member`, `joining`, `learner_lag` and `phase` (`joining`,
  `rejoining`, `starting`, `normal`).

### Changed

- Cluster `/readyz` (and `beanstalkd_cluster_ready`) is 503 on a node that
  is not in the membership: one waiting to be added, or a removed node that
  still runs. A removed node's clients are disconnected, and their
  reservations released, at once (COMPAT C11).
- A cluster node that is still starting (discovering its cluster, waiting
  to be added, rejoining, waiting for a leader) answers `/metrics` and
  `/admin` with its cluster figures only, instead of 503, so
  `beanstalkd_cluster_joining` and `beanstalkd_cluster_rejoining` can be
  watched (before, a rejoining node exported nothing until it had caught
  up). Standalone servers still answer 503 until the binlog is replayed.
- A node restarted with its data after it was removed from the membership
  logs that it was removed (it still waits for a leader that never comes:
  stop it).
- A node started with an empty data directory whose id was removed from the
  cluster (or lies below the highest id ever used) refuses to start: node
  ids are never reused.
- A node that lost its data counts only answers from current voters that
  hold the current membership and are not rejoining themselves: if a
  majority of the voters lost their data at once, they no longer rejoin
  from the survivor (which could silently lose entries committed only on
  them); they wait until data is restored. Rejoins are therefore done one
  node at a time. The number of answers needed is the fewest that meet
  every majority of the voters (1 with 2 voters, 2 with 3 or 4, 3 with
  5), so a wiped voter of a two-voter cluster can rejoin while the other
  voter leads. A node removed from the membership while still rejoining
  exits with an error instead of waiting forever.
- Startup status probes from nodes that are not members are limited to
  one connection per node id, 30 s and 16 probes each; in plaintext test
  mode only from loopback unless `insecure_plaintext_allow_remote = true`.
- **Cluster protocol version 4** (incompatible): nodes of 0.5.x cannot join
  a cluster of later versions, or the reverse. Upgrade a 0.5.x cluster by
  stopping every node, upgrading all of them, then starting them again; a
  rolling upgrade from 0.5.x is not possible. Version 4 adds an extended
  status probe and an authenticated admin channel on the cluster port (for
  membership changes);
  `scripts/mkcluster-certs.sh DIR admin` issues its client certificate
  (SAN `bstk-admin`).
- Log lines carry ANSI color codes only when stderr is a terminal, so
  journald, `docker logs` and log files get plain text.
- **A removed cluster node stops by itself.** Once a majority of the
  remaining voters (of the latest membership it can learn) report a
  committed membership without it, the node logs an `ERROR` naming the
  removal and exits with status 11, whether it was running when it was
  removed or was restarted with its data; before, it ran isolated, or
  waited for a leader, until an operator stopped it. It never exits on its
  own evidence alone: a node that is partitioned, or whose removal was not
  committed (and is later truncated), keeps running. The refusal to start a
  removed id with an empty data directory also exits with 11 now (it was
  1). The packaged systemd unit does not restart on 11
  (`RestartPreventExitStatus=10 11`); Docker's `--restart on-failure`
  does, so remove the container of a removed node. The notes of
  `beanstalkd-rs cluster remove` and the runbooks say so (stopping the node
  yourself is still fine).

### Fixed

- A cluster node whose Raft core stopped on a fatal error (for example the
  openraft race that can kill a leader replicating to a lagging node with a
  higher term, or a storage error) used to keep running without serving
  anything. It now exits with status 21 so that the service manager
  restarts it (the packaged unit already does).
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
- A cluster node restarted before its log had caught up with the last
  connections of its previous process now closes those connections too;
  before, they and their reservations stayed in the cluster state until
  the jobs' TTR expired.

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
