# beanstalkd-rs Operations Guide

How to install, configure, run, back up, upgrade and monitor
beanstalkd-rs, standalone or as a 3- or 5-node cluster. Every command in
this guide was run on macOS (aarch64) or on Debian 13 (aarch64) under
systemd in a container; Linux-only commands are marked. Architecture and
the reasoning behind the behavior described here: [DESIGN.md](DESIGN.md).
Behavior that differs from the reference beanstalkd:
[COMPAT.md](COMPAT.md).

Contents:

1. [Install](#1-install)
2. [Configure](#2-configure)
3. [Persistence (binlog)](#3-persistence-binlog)
4. [Security](#4-security)
5. [Cluster](#5-cluster)
6. [Backup and restore](#6-backup-and-restore)
7. [Upgrades](#7-upgrades)
8. [Monitoring](#8-monitoring)
9. [Troubleshooting](#9-troubleshooting)
10. [Compatibility notes](#10-compatibility-notes)
11. [Known limitations](#11-known-limitations)

## 1. Install

beanstalkd-rs is a single binary, `beanstalkd-rs`, with no runtime
dependencies besides the C library. Pick one of the four ways below.

### 1.1 Release archive

Each release on the
[releases page](https://github.com/shaunlee/beanstalkd-rust/releases)
has one archive per target and a `SHA256SUMS` file:

| Target | Notes |
|---|---|
| `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` | glibc 2.34 or newer |
| `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl` | static, any Linux |
| `aarch64-apple-darwin` | macOS on Apple silicon (development use) |

Archive layout (`beanstalkd-rs-<version>-<target>.tar.gz`):

```
beanstalkd-rs-<version>-<target>/
  beanstalkd-rs                     the server
  LICENSE  README.md  CHANGELOG.md
  docs/beanstalkd-rs.example.toml   every configuration key, commented
  docs/OPERATIONS.md                this guide
  scripts/mkcluster-certs.sh        cluster certificate generator (section 5.2)
  packaging/systemd/beanstalkd-rs.service
  packaging/examples/{standalone,binlog,secure}.toml
  packaging/examples/cluster/node{1,2,3}.toml
```

Verify the download before unpacking. Linux:

```sh
sha256sum -c --ignore-missing SHA256SUMS
tar -xzf beanstalkd-rs-0.5.0-aarch64-unknown-linux-gnu.tar.gz
./beanstalkd-rs-0.5.0-aarch64-unknown-linux-gnu/beanstalkd-rs --version
```

macOS (`shasum` instead of `sha256sum`; a binary downloaded with a browser
is quarantined by Gatekeeper because it is not signed):

```sh
shasum -a 256 -c --ignore-missing SHA256SUMS
tar -xzf beanstalkd-rs-0.5.0-aarch64-apple-darwin.tar.gz
xattr -d com.apple.quarantine beanstalkd-rs-0.5.0-aarch64-apple-darwin/beanstalkd-rs
```

`--ignore-missing` lets you download only the archive you need.

### 1.2 Container image

No image is published to a registry: build it from the repository's
`Dockerfile` (multi-stage, Debian 13 runtime). It runs as
the non-root user `beanstalkd` (uid 10001), keeps data in the `/data`
volume, exposes 11300 and has a `HEALTHCHECK` that sends `stats` to
`127.0.0.1:11300`.

```sh
docker build -t beanstalkd-rs .
docker volume create bstk-data
docker run -d --init --name bstk -p 127.0.0.1:11300:11300 -v bstk-data:/data \
    beanstalkd-rs -l 0.0.0.0 -p 11300 -b /data -V
docker inspect -f '{{.State.Health.Status}}' bstk     # healthy after ~10 s
docker logs bstk
docker stop bstk                                      # SIGTERM: graceful, exit status 0
```

Arguments replace the image's default command (`-l 0.0.0.0 -p 11300`), so
repeat `-l` / `-p` when adding flags. With a configuration file, mount it
read-only and point `binlog.dir` (or `cluster.data_dir`) at `/data`:

```sh
docker run --rm -v "$PWD/etc:/etc/beanstalkd-rs:ro" beanstalkd-rs \
    --config /etc/beanstalkd-rs/config.toml --check-config
docker run -d --init --name bstk-cfg -p 127.0.0.1:11300:11300 -p 127.0.0.1:9180:9180 \
    -v "$PWD/etc:/etc/beanstalkd-rs:ro" -v bstk-data:/data \
    beanstalkd-rs --config /etc/beanstalkd-rs/config.toml
curl -s http://127.0.0.1:9180/readyz
```

where `etc/config.toml` is, for example:

```toml
[[listener]]
addr = "0.0.0.0:11300"

[binlog]
dir = "/data"

[http]
addr = "0.0.0.0:9180"

[log]
level = "info"
format = "json"
```

If the server does not listen on plaintext port 11300 inside the
container, the built-in healthcheck fails: override it (`--health-cmd`,
`--no-healthcheck`) or probe `/healthz` / `/readyz` from the orchestrator.
`scripts/docker-smoke.sh IMAGE` is the automated version of these checks.

### 1.3 systemd (Linux)

`packaging/systemd/beanstalkd-rs.service` is a hardened unit
(`ProtectSystem=strict`, `PrivateUsers=yes`, no capabilities, a system
call filter) that runs `beanstalkd-rs --config
/etc/beanstalkd-rs/config.toml` as the `beanstalkd` user. Its
`StateDirectory=` creates `/var/lib/beanstalkd-rs` (mode 0750, owned by
`beanstalkd`), the only path the service can write: keep `binlog.dir` or
`cluster.data_dir` there. From an unpacked release archive, as root:

```sh
cd beanstalkd-rs-0.5.0-aarch64-unknown-linux-gnu
useradd --system --user-group --home-dir /var/lib/beanstalkd-rs \
    --shell /usr/sbin/nologin beanstalkd
install -m 0755 beanstalkd-rs /usr/local/bin/beanstalkd-rs
install -d -m 0755 /etc/beanstalkd-rs
install -m 0644 packaging/examples/binlog.toml /etc/beanstalkd-rs/config.toml
beanstalkd-rs --config /etc/beanstalkd-rs/config.toml --check-config
install -m 0644 packaging/systemd/beanstalkd-rs.service /etc/systemd/system/
systemd-analyze verify /etc/systemd/system/beanstalkd-rs.service
systemctl daemon-reload && systemctl enable --now beanstalkd-rs
systemctl status beanstalkd-rs --no-pager
```

Day to day:

```sh
systemctl restart beanstalkd-rs
systemctl stop beanstalkd-rs
systemctl show beanstalkd-rs -p ExecMainStatus -p ActiveState   # ExecMainStatus=0 after a clean stop
journalctl -u beanstalkd-rs -o cat --no-pager
```

Notes on the unit:

- `Type=simple`: `systemctl start` returns as soon as the process is
  started, before it listens (and, with a binlog, before replay is done).
  Scripts that connect right after starting must retry, or poll
  `/readyz`.
- There is no reload: the server reads its configuration only at startup,
  and SIGHUP is not handled (it terminates the process without the graceful
  shutdown, exit status 129, like the reference). Apply changes with
  `systemctl restart`.
- `Restart=on-failure` restarts after a crash or an exit status other than
  0, except status 10 (the data directory is locked by another process,
  which a restart cannot fix).
- `LimitNOFILE=65536`: one descriptor per client connection. Raise it for
  more connections.
- Ports below 1024 need `AmbientCapabilities=CAP_NET_BIND_SERVICE` and the
  same capability in `CapabilityBoundingSet=` (see the unit's comments).

### 1.4 Build from source

Requires Rust 1.98 or newer (the workspace's `rust-version`).

```sh
git clone https://github.com/shaunlee/beanstalkd-rust
cd beanstalkd-rust
cargo build --release --locked -p bstk-server
./target/release/beanstalkd-rs --version
```

The binary is `target/release/beanstalkd-rs`. The release profile keeps
line tables for profiling; the release archives and the container image
strip debug info (`CARGO_PROFILE_RELEASE_STRIP=debuginfo`) but keep
symbols.

## 2. Configure

### 2.1 Command line and configuration file

Without a configuration file beanstalkd-rs takes the reference's flags and
behaves like the reference:

```sh
beanstalkd-rs -l 127.0.0.1 -p 11300 -b ./binlog -f 50 -z 65535 -V
beanstalkd-rs --help
```

| Flag | Meaning |
|---|---|
| `-l ADDR` | listen address (IP literal; default `0.0.0.0`) |
| `-p PORT` | listen port (default 11300) |
| `-z BYTES` | maximum job size (default 65535) |
| `-b DIR` | binlog (write-ahead log) directory |
| `-f MS` / `-F` / `-s BYTES` | binlog fsync interval / never fsync / file size (section 3) |
| `-V` | more verbose logging (repeatable: info, debug, trace) |
| `-v`, `--version` | print `beanstalkd-rs <version>` |
| `--config FILE` | read a TOML configuration file |
| `--check-config` | validate the configuration (flags and file), print a summary, exit |
| `--cluster-init` | bootstrap a new cluster (section 5.3) |
| `--threads N` | tokio worker threads (section 2.2) |

`-u USER` is rejected (exit status 5): start the server as the right user
instead (the systemd unit does). SIGUSR1 puts the server in drain mode (new
`put`s get `DRAINING`), as in the reference; SIGTERM and SIGINT shut down
gracefully (stop accepting, sync the binlog, exit 0).

The TOML file adds what flags cannot express: several listeners, TLS,
token authentication, the HTTP endpoints, logging and cluster mode.
[beanstalkd-rs.example.toml](beanstalkd-rs.example.toml) documents every
key; `packaging/examples/` has ready-made files:

| File | Use |
|---|---|
| [standalone.toml](../packaging/examples/standalone.toml) | plaintext, in memory |
| [binlog.toml](../packaging/examples/binlog.toml) | plaintext with a binlog in `/var/lib/beanstalkd-rs` |
| [secure.toml](../packaging/examples/secure.toml) | plaintext on loopback, TLS + token, mTLS, binlog, HTTP |
| [cluster/node1.toml](../packaging/examples/cluster/node1.toml) (and node2, node3) | one node of a 3-node cluster |

Rules: unknown keys are errors; relative paths are relative to the
configuration file's directory; flags override the file for `-z`, `-b`,
`-f` / `-F`, `-s` and `-V` (which can only raise the log level); `-l` /
`-p` cannot be combined with `[[listener]]` entries.

Always check a file before (re)starting with it. `--check-config` also
loads the TLS files and checks that a cluster certificate matches the
node id:

```sh
beanstalkd-rs --config /etc/beanstalkd-rs/config.toml --check-config
```

Output for `packaging/examples/binlog.toml`:

```
configuration OK: /etc/beanstalkd-rs/config.toml
listener 0.0.0.0:11300 (plaintext, auth none)
max job size: 65535
threads: 2 (default)
binlog: /var/lib/beanstalkd-rs (file size 10485760, fsync every 50ms)
http: disabled
log: level warn, format text
```

Errors go to stderr with exit status 1, for example
`beanstalkd-rs: invalid configuration: /etc/beanstalkd-rs/config.toml:3:1:
unknown field `bogus`, expected one of `max_job_size`,
`max_pending_connections`, `threads``.

### 2.2 Worker threads

The default number of tokio worker threads depends on the mode (chosen by
measurement, see [BENCH.md](BENCH.md) "P4-T2" and "P4-T6b"):

| Mode | Default |
|---|---|
| standalone, plaintext only, no binlog | 1 |
| standalone with any TLS listener or a binlog | 2 |
| cluster | 2 |

`--threads N` or `server.threads` (1 to 256) overrides it. One thread
gives the most operations per CPU second; more threads give more
throughput with many connections or TLS, at a higher CPU cost per
operation. With a binlog the engine runs on its own extra thread.

### 2.3 Limits

- **Job size**: `-z` / `server.max_job_size`, default 65535. The flag
  clamps large values like the reference; the file accepts up to
  1073741824 and rejects more. Cluster mode allows at most 33488896 and
  every node must use the same value (section 5.1).
- **Connections**: plaintext connections are limited only by the file
  descriptor limit. At startup the soft `RLIMIT_NOFILE` is raised to the
  hard limit; under systemd that is `LimitNOFILE=` (65536 in the unit).
  `beanstalkd_current_connections` shows the current count.
- **Pending TLS connections**: connections still in their TLS handshake
  (10 s limit) or, on token listeners, not yet authenticated
  (`auth.timeout`, default 10 s) count against
  `server.max_pending_connections` (default 1024, shared by all TLS
  listeners); beyond it new TLS connections are closed at accept
  (`beanstalkd_pending_rejected_total`).
- **Per-connection buffering**: a client blocked in `reserve` that keeps
  pipelining stops being read at 64 KiB (COMPAT D3).
- **Cluster forward queue**: 100,000 inputs or 128 MiB per node; when full,
  new connections to that node are refused and `put` gets `OUT_OF_MEMORY`
  (COMPAT C9).

### 2.4 Logging

Logs go to stderr. `[log] level` is `error`, `warn` (default), `info`,
`debug` or `trace`; each `-V` raises it one step. `[log] format` is `text`
(default) or `json` (one JSON object per line, for log shippers). Colors
(ANSI escape codes) are used only when stderr is a terminal, so journald,
`docker logs` and log files get plain text.

What each level shows:

- `warn`: problems and state changes an operator should see (drain mode,
  binlog tail truncated at startup, cluster rejoin, rejected cluster
  peers, nodes dropped for silence).
- `info`: also listeners, binlog replay, startup and shutdown, failed
  token authentications; in cluster mode also openraft's own messages,
  which are verbose (a full Raft state dump at each start, several lines
  per election, and `WARN ... A message will be ignored because vote
  changed` during normal elections). There is no per-module filter: use
  `warn` in production clusters unless you are investigating.
- `debug`: also failed TLS handshakes of clients
  (`TLS handshake failed: peer sent no certificates`) and cluster dial
  errors.

## 3. Persistence (binlog)

Without a binlog all jobs are in memory and lost when the process exits.
With `-b DIR` (or `[binlog] dir`) every change is appended to a
write-ahead log before its reply is sent, and the log is replayed at
startup.

```sh
beanstalkd-rs -l 127.0.0.1 -p 11300 -b /var/lib/beanstalkd-rs
```

### 3.1 fsync policy

| Flag | `[binlog] fsync` | A reply is sent after the change is... | Lost on power loss / OS crash |
|---|---|---|---|
| `-f0` | `"always"` | written and `fdatasync`ed | nothing acknowledged |
| `-f MS` (default `-f 50`) | `"50ms"` | written to the OS; fsync at most every MS ms | up to the last MS ms of acknowledged changes |
| `-F` | `"never"` | written to the OS; never fsynced by the server | whatever the OS had not written back |

A crash of the process alone (kill -9, a panic, OOM kill) loses nothing
acknowledged under any policy: the write reached the OS before the reply.
After a power loss with `-f MS` or `-F`, the last file can end in a torn
record; startup then truncates it there and logs a warning with the file
and offset (COMPAT D9). Damage anywhere else (an earlier file, a bad
header) refuses to start (exit status 1) rather than silently losing jobs.
The whole set of guarantees: [DESIGN.md](DESIGN.md) section 7.

### 3.2 Files and disk usage

```
/var/lib/beanstalkd-rs/
  binlog.1  binlog.2  ...   log files, preallocated to -s bytes each
  lock                      held (flock) for the lifetime of the process
```

- `-s BYTES` / `[binlog] file_size` sets the file size, default 10 MiB
  (10485760), rounded up to 4096, at most 4 GiB. Each file is preallocated
  in full when created, so disk usage grows in steps of `-s`. A server
  starts with one file and adds a spare with the first `put`: 20 MiB with
  the default.
- Compaction runs continuously: when the log holds three or more times the
  bytes of the live jobs, live jobs are copied out of the oldest file and
  files without live jobs are deleted. Disk usage therefore stays within
  about three times the live data plus two files.
- Every start begins a new file; closed files are truncated to their
  records.
- A job larger than one file cannot be stored: such a `put` gets
  `OUT_OF_MEMORY` (keep `-s` well above `-z`; COMPAT D7).
- Disk full: new `put`s get `OUT_OF_MEMORY` (one spare file is always kept
  for other updates, COMPAT D6). Any write or fsync error while serving
  stops the server with exit status 20 instead of serving without
  persistence (COMPAT D5).
- `stats` and `/metrics` report `binlog-oldest-index` /
  `binlog-current-index` (file numbers), `binlog-max-size` and
  `binlog-records-written`. Disk usage is about
  `(current - oldest + 2) * binlog-max-size`; watch the file system itself
  (section 8.4).
- The format is beanstalkd-rs's own: a reference beanstalkd binlog
  directory cannot be read (and vice versa; startup fails with `bad
  magic`).

### 3.3 Directory lock

One process per directory. A second server on the same directory exits
with status 10:

```
beanstalkd-rs: failed to lock wal dir /var/lib/beanstalkd-rs
```

The lock is an `flock` on `DIR/lock`, released by the kernel when the
process exits, however it exits: a stale `lock` file left after a crash
never blocks a restart, and copying it in a backup is harmless.

## 4. Security

- **Plaintext listeners** have no authentication, like the reference:
  bind them to loopback or a private network, or firewall them.
- **TLS** (`tls = true` on a `[[listener]]`, certificate in `[tls]`):
  TLS 1.2 and 1.3 via rustls. A listener has one of three `auth` modes:
  `none`, `token` (clients send `auth <token>` first; a beanstalkd-rs
  extension, so the client library must support it) or `mtls` (clients
  present a certificate from `[tls] client_ca`; works with any TLS-capable
  client). [secure.toml](../packaging/examples/secure.toml) uses all
  three.
- **Tokens** live in `[auth] tokens` or `tokens_file` (one per line). The
  file must not be readable by others: `--check-config` and startup warn
  `auth.tokens_file ... is accessible by group or others (mode 0644);
  restrict it with chmod 600`. Tokens are compared in constant time and
  never logged. Generate one with:

  ```sh
  (umask 077; openssl rand -hex 32 > /etc/beanstalkd-rs/tokens.txt)
  ```

  Under the systemd unit, make the file readable by the service group:
  `chown root:beanstalkd` and mode 0640 (the same as cluster keys, section
  5.2).
- **Private keys** (`[tls] key`, `[cluster.tls] key`): mode 0600 or 0640,
  readable only by the service user or group.
- **What clients see**: before a TLS handshake (and, on token listeners,
  authentication) completes, a connection never reaches the engine and is
  not counted in `stats`. Failed tokens are counted
  (`beanstalkd_auth_failures_total`) and logged at info with the peer
  address; failed handshakes only at debug.
- **HTTP** (`[http]`) has no authentication. `/admin` and `/metrics` expose
  tube names and counts (never job bodies); keep the listener on loopback
  or a monitoring network. It binds only where `addr` says.
- **Cluster traffic** is always mutual TLS with per-node certificates
  (section 5.2). `insecure_plaintext = true` is for tests: it is refused
  off loopback unless `insecure_plaintext_allow_remote = true`, and then
  anyone who can reach the cluster port can act as a node.
- **Run as non-root**: the systemd unit and the container image do.
  Nothing in the server needs root; ports below 1024 need
  `CAP_NET_BIND_SERVICE`.

Quick checks with `openssl s_client` (the token listener on port 11331,
the mTLS listener on 11332, certificates from `clients/mkcerts.sh`):

```sh
{ printf "auth $(cat tokens.txt)\r\nlist-tubes\r\n"; sleep 1; } | \
    openssl s_client -quiet -no_ign_eof -connect 127.0.0.1:11331 -CAfile tls/ca.pem
{ printf "list-tubes\r\n"; sleep 1; } | openssl s_client -quiet -no_ign_eof \
    -connect 127.0.0.1:11332 -CAfile tls/ca.pem -cert tls/client.pem -key tls/client.key
```

The first prints `AUTHENTICATED` and the tube list, the second the tube
list. A wrong token gets `UNAUTHORIZED` and a close; an mTLS connection
without a client certificate fails with `tlsv13 alert certificate
required`, one with a certificate from another CA with `tlsv1 alert
unknown ca`.

## 5. Cluster

### 5.1 Requirements and behavior

- **3 or 5 nodes**, each with its own configuration file holding the same
  `[[cluster.peer]]` list (ids 1 to 65535 and cluster addresses) and its
  own `node_id`, `listen`, `data_dir` and certificate. A 3-node cluster
  survives the loss of 1 node, a 5-node cluster of 2. (1 node is accepted
  for tests.)
- **The same `-z`** on every node (at most 33488896). A node with another
  value is refused by its peers.
- **No binlog**: `-b` / `[binlog]` is an error with `[cluster]`; the Raft
  log in `data_dir` replaces it.
- **Clients connect to any node** with the unmodified protocol. Every
  node holds the full state; a follower forwards its clients' commands to
  the leader and replies when they are committed. No client-side routing
  or leader discovery is needed; a load balancer may spread clients over
  all nodes (health check: `/readyz`).
- **Every reply waits for a majority commit** (each node fsyncs its log),
  so nothing acknowledged is lost when a minority of nodes fails, and a
  command costs a network round trip more than standalone. Differences
  from a single server that clients can observe: [COMPAT.md](COMPAT.md)
  "Cluster mode" (C1 to C10).
- **Data directory** (`cluster.data_dir`): `log/` (Raft log segments,
  vote), `snapshot/` (a state snapshot every `snapshot_every` = 100,000
  entries; log segments older than the snapshot, except the last 1,000
  entries, are then removed), `conn-ids`, and
  during a rejoin a `rejoin` marker. Watch `beanstalkd_cluster_log_bytes`
  and `beanstalkd_cluster_snapshot_bytes`.

### 5.2 Certificates

Every node has one certificate, used both as the cluster listener's
server certificate and as its client certificate when dialing peers. It
must chain to the cluster CA, carry the SAN DNS name `bstk-node-<id>`
(the CN is not consulted; this is how a node's id is authenticated) and
allow both `serverAuth` and `clientAuth`. `scripts/mkcluster-certs.sh`
(in the repository and in the release archives) creates a CA and the node
certificates (ECDSA P-256, valid 825 days unless `DAYS` is set):

```sh
scripts/mkcluster-certs.sh cluster-tls 1 2 3
```

```
created cluster-tls/cluster-ca.pem
created cluster-tls/node1.pem (SAN DNS:bstk-node-1, valid 825 days)
created cluster-tls/node2.pem (SAN DNS:bstk-node-2, valid 825 days)
created cluster-tls/node3.pem (SAN DNS:bstk-node-3, valid 825 days)
```

The argument `admin` (alone or with node ids) issues the operator
certificate for the cluster port's admin channel, `admin.pem` /
`admin.key`: signed by the same CA, SAN DNS name `bstk-admin` and nothing
else, `clientAuth` only. Nodes refuse it as a peer and refuse node
certificates on the admin channel; it belongs on the operator's machine,
not on the nodes (the membership commands that use it arrive in a later
release; with `insecure_plaintext` the admin channel is open to loopback
only):

```sh
scripts/mkcluster-certs.sh cluster-tls admin
```

```
reusing cluster-tls/cluster-ca.pem
created cluster-tls/admin.pem (SAN DNS:bstk-admin, client only, valid 825 days)
```

Keys are created with mode 0600. Keep `cluster-ca.key` off the nodes:
whoever has it can join the cluster. Run the script again with the same
directory to issue a certificate for one node with the existing CA
(`scripts/mkcluster-certs.sh cluster-tls 2`), for example before the old
one expires. Certificates are read at startup only: replace the files,
then restart the nodes one at a time (section 7.3).

There is no revocation list: every certificate the CA signed for a node
id stays valid until it expires. Before a node has joined, and while it
starts, it asks the others for their status over *probe connections*,
which any node certificate of the CA may open, whether or not its id is a
member. The answers carry the membership (node ids and addresses), each
node's Raft vote and log positions. So a removed node's certificate (or
its key, if it leaked) can still read that, and a malicious holder could
answer probes with a made-up membership or vote and stall or mislead a
node that is starting without its data. When you remove a node you no
longer trust, issue a new CA and new certificates for the remaining nodes
and roll them out (section 7.3). Probe connections are limited (one per
node id, at most 30 s each, 8 at once), and in plaintext test mode they
are accepted from loopback only unless
`insecure_plaintext_allow_remote = true`.

On each node `N`, install its files so that the service group can read
the key (Linux, as root):

```sh
install -d -m 0750 -g beanstalkd /etc/beanstalkd-rs/tls
install -m 0644 cluster-tls/cluster-ca.pem cluster-tls/nodeN.pem /etc/beanstalkd-rs/tls/
install -m 0640 -g beanstalkd cluster-tls/nodeN.key /etc/beanstalkd-rs/tls/
```

### 5.3 Bootstrap

A new cluster is bootstrapped once: start **every** initial node with
`--cluster-init` (in any order), each with an empty `data_dir`. A node
started with `--cluster-init` waits until a majority of the *other* nodes
answer with no state (for 3 nodes: both others), so bootstrapping needs
all nodes of a 3-node cluster up. Afterwards nodes are always started
without `--cluster-init`; with it, a node whose `data_dir` holds state
refuses to start:

```
beanstalkd-rs: --cluster-init: /var/lib/beanstalkd-rs already holds Raft state; a cluster is bootstrapped only once (start this node without --cluster-init to rejoin its cluster)
```

**With systemd** (Linux; tested with three hosts at 172.29.53.11 to .13).
On each node `N`, install the binary, user and unit as in section 1.3 (but
not a standalone `config.toml`), the certificates as in section 5.2, and
a configuration made from `packaging/examples/cluster/nodeN.toml`, whose
example addresses are 10.0.0.1 to 10.0.0.3; replace them with yours:

```sh
sed 's/10\.0\.0\./172.29.53.1/g' packaging/examples/cluster/nodeN.toml > /etc/beanstalkd-rs/config.toml
runuser -u beanstalkd -- beanstalkd-rs --config /etc/beanstalkd-rs/config.toml --check-config
systemctl daemon-reload
systemctl enable beanstalkd-rs
```

Add `--cluster-init` for the first start only, with a drop-in, then start
the service on all nodes:

```sh
mkdir -p /etc/systemd/system/beanstalkd-rs.service.d
printf '[Service]\nExecStart=\nExecStart=/usr/local/bin/beanstalkd-rs --config /etc/beanstalkd-rs/config.toml --cluster-init\n' \
    > /etc/systemd/system/beanstalkd-rs.service.d/cluster-init.conf
systemctl daemon-reload
systemctl start beanstalkd-rs
```

Wait until every node is ready, then remove the drop-in on every node, so
that a later restart (including `Restart=on-failure`) does not run with
`--cluster-init` and fail:

```sh
until curl -fsS http://172.29.53.11:9180/readyz; do sleep 0.5; done
rm /etc/systemd/system/beanstalkd-rs.service.d/cluster-init.conf
systemctl daemon-reload
```

The running process is not affected by the `daemon-reload`.

**Local walkthrough** (one machine, loopback addresses, any OS; from a
source checkout after `cargo build --release -p bstk-server`). The rest of
this section uses it; with systemd, replace the start and stop commands by
`systemctl` and the addresses by yours.

```sh
W=/tmp/bstk-cluster
BIN=$PWD/target/release/beanstalkd-rs
scripts/mkcluster-certs.sh "$W/tls" 1 2 3
for i in 1 2 3; do
  cat > "$W/node$i.toml" <<EOF
[[listener]]
addr = "127.0.0.1:1130$i"

[cluster]
node_id = $i
listen = "127.0.0.1:1140$i"
data_dir = "node$i"

[cluster.tls]
cert = "tls/node$i.pem"
key = "tls/node$i.key"
ca = "tls/cluster-ca.pem"

[[cluster.peer]]
id = 1
addr = "127.0.0.1:11401"
[[cluster.peer]]
id = 2
addr = "127.0.0.1:11402"
[[cluster.peer]]
id = 3
addr = "127.0.0.1:11403"

[http]
addr = "127.0.0.1:918$i"

[log]
level = "info"
EOF
  "$BIN" --config "$W/node$i.toml" --check-config
done
for i in 1 2 3; do
  "$BIN" --config "$W/node$i.toml" --cluster-init > "$W/node$i.log" 2>&1 &
  echo $! > "$W/node$i.pid"
done
for i in 1 2 3; do until curl -fsS "http://127.0.0.1:918$i/readyz"; do sleep 0.5; done; echo " node$i"; done
```

A node logs `cluster membership initialized`, `waiting for a leader` and
`cluster node ready` as it comes up; `/readyz` answers 503 until then.

### 5.4 Clients and the leader

Any node accepts clients. A job put through one node is visible through
the others:

```sh
printf 'use orders\r\nput 0 0 60 5\r\nhello\r\n' | nc -w 1 127.0.0.1 11301
printf 'stats-tube orders\r\n' | nc -w 1 127.0.0.1 11302 | grep current-jobs-ready
```

Which node leads (role, term and leader id per node):

```sh
for i in 1 2 3; do
  curl -s "http://127.0.0.1:918$i/metrics" |
    grep -E '^beanstalkd_cluster_(role\{role="leader"\}|leader_id|term) '
done
```

`/admin` has the same values as JSON under `"cluster"`.

### 5.5 Node loss and failover

- **A follower fails**: nothing changes for clients of the other nodes.
  Its own clients lose their connections. After `2 × node_timeout` (10 s
  by default) the leader drops the node's connections from the shared
  state, so jobs its clients had reserved become ready again (logged on
  the leader: `node is silent: proposing DropNode node=1 connections=1
  silent_for=10.03s`).
- **The leader fails**: the others elect a new leader after 1.0 to 1.2 s
  without heartbeats (the default `heartbeat` and `election_timeout`); a
  failover measured 1.4 s, both on loopback and between containers.
  Clients of the surviving nodes keep their connections and reservations;
  commands sent meanwhile are answered once the new leader commits them
  (each node resends what it had not seen applied; duplicates are
  discarded).
- **The failed node comes back** (same data directory): start it as usual,
  without `--cluster-init`. It logs `mode=Restart`, catches up from the
  leader (by log entries or a snapshot) and serves clients again.

```sh
kill -KILL "$(cat "$W/node3.pid")"          # node 3 was the leader
"$BIN" --config "$W/node3.toml" >> "$W/node3.log" 2>&1 &
echo $! > "$W/node3.pid"
until curl -fsS http://127.0.0.1:9183/readyz; do sleep 0.2; done
```

**Partitions.** A node that cannot reach the leader (or, as leader, a
majority) for `node_timeout` (5 s) closes its client connections and
refuses new ones (`beanstalkd_cluster_isolated` = 1) until it reaches a
leader again; meanwhile its clients' commands are never acknowledged
(COMPAT C8). The majority side elects a leader and carries on; the
minority side cannot commit anything, so the two sides never diverge.
When the partition heals, the minority nodes follow the majority's leader
and serve clients again, without operator action.

Timing knobs (`[cluster]`): `node_timeout` (default 5 s) trades how fast
reservations of a dead node's clients are released against false
positives on a slow network; `heartbeat` / `election_timeout` (100 ms /
500 to 700 ms) set failover time. The longer defaults absorb log fsync
stalls of a few hundred milliseconds; lowering them can cause needless
elections under write load.

### 5.6 Replacing a node or wiping its data (rejoin)

A node whose `data_dir` was lost (a new disk, a replaced machine with the
same id and address, a corrupted log) is brought back by starting it
**without** `--cluster-init` on an **empty** `data_dir`. It then
*rejoins*: it may have acknowledged entries and granted votes it no
longer remembers, so it adopts the highest vote of the other nodes, does
not vote or stand for election, and serves no clients (`/readyz` 503,
`beanstalkd_cluster_rejoining` = 1) until it has caught up with a leader.
A `rejoin` marker in `data_dir` keeps it in this mode across crashes.

```sh
kill -KILL "$(cat "$W/node1.pid")"
rm -rf "$W/node1"
"$BIN" --config "$W/node1.toml" > "$W/node1-rejoin.log" 2>&1 &
echo $! > "$W/node1.pid"
until curl -fsS http://127.0.0.1:9181/readyz; do sleep 0.2; done
```

Under systemd (Linux):

```sh
systemctl stop beanstalkd-rs
rm -rf /var/lib/beanstalkd-rs
systemctl start beanstalkd-rs
```

The log shows the steps (the data directory is created if missing):

```
WARN beanstalkd_rs::cluster: rejoin mode: this node started without Raft state (or did not finish rejoining); ...
INFO beanstalkd_rs::cluster: startup: learned the cluster membership membership=Some(LogId { leader_id: LeaderId { term: 1, node_id: 1 }, index: 0 }) voters=[{1, 2, 3}] nodes=[1, 2, 3]
WARN beanstalkd_rs::cluster: rejoin: adopted the highest vote of the current voters that answered membership=Some(...) voters={1, 2, 3} vote=T2-N2:committed
INFO beanstalkd_rs::cluster: rejoin: caught up with the leader index=25
WARN beanstalkd_rs::cluster: rejoin complete: this node votes and stands for election again
INFO beanstalkd_rs::cluster: cluster node ready node=1 first_local=117381027659776
```

Rules:

- A rejoining node learns the current membership from its peers (any
  reachable member will do) and needs answers from enough of the *other*
  current voters, not rejoining themselves (1 with 2 voters, 2 with 3 or
  4, 3 with 5: enough to meet every majority of the voters), and a
  running leader. Until then it waits and logs `startup: waiting for the
  cluster's status (1 of the 2 answers needed from the current voters
  ...)`; it completes by itself once enough nodes are up. It also waits
  while a membership change is in progress, and a node whose id was
  removed refuses to start. The `rejoin` marker appears only once the node
  has decided to rejoin; before that its data directory holds neither Raft
  state nor marker, and `beanstalkd_cluster_rejoining` is the signal. If
  the wiped node had been the leader, it also waits until the others have
  elected a new one (`the highest vote (T2-N2:committed) is this node's
  own leadership`). If its id is removed from the membership before it
  has caught up, it exits with `cannot finish rejoining node N: ...`.
- Rejoin one node at a time: a node that is still rejoining does not count
  for another, so with 3 voters a second wiped node waits until the first
  has finished.
- With a single voter, a wiped voter can never rejoin (no other node holds
  the data): restore its data directory. With two voters, a wiped voter
  rejoins only while the other one stays leader; if that one restarts or
  loses leadership meanwhile, the cluster stalls (the rejoining node does
  not vote until it has caught up) until the data is restored. Do not stay
  at two voters longer than needed.
- **Never restore an old copy of one node's `data_dir`** into a running
  cluster: a node that forgot only part of its history can break Raft's
  guarantees. Wipe it instead and let it rejoin.
- A replacement machine must keep the node's id and cluster address (the
  peer list is static) and get that node's certificate.
- Do not start a wiped node with `--cluster-init` by mistake: if a peer
  is already established it rejoins anyway (with a warning), but on a
  cluster whose other nodes are also empty it would bootstrap.

### 5.7 Shutting down and restarting the whole cluster

SIGTERM each node (`systemctl stop`); each exits within a fraction of a
second. Start them again without `--cluster-init`, in any order; the
cluster serves once a majority is up, with every committed job, job id,
counter and drain mode preserved (COMPAT C5).

## 6. Backup and restore

### 6.1 Standalone binlog

The binlog directory is the whole state. Safe backups:

- **Stopped server** (simplest): stop it (SIGTERM; the graceful shutdown
  syncs the log), copy the directory, start it again. Linux with systemd:

  ```sh
  systemctl stop beanstalkd-rs
  tar -C /var/lib -czf /root/beanstalkd-rs-backup.tar.gz beanstalkd-rs
  systemctl start beanstalkd-rs
  ```

- **File-system snapshot** (LVM, ZFS, btrfs, a cloud volume snapshot) of a
  running server: an atomic snapshot is equivalent to a crash at that
  instant, which the log is designed to survive (section 3.1); restoring
  it is like restarting after `kill -9`.

Do **not** copy the files of a running server with `cp` or `tar`: the
copy is not atomic across files, while compaction moves jobs between
files and deletes old ones. A copy can miss a file or catch a record half
written in a file that is not the last one; replay then refuses to start
(COMPAT D9) or misses jobs.

Restore: stop the server, replace the directory, start.

```sh
systemctl stop beanstalkd-rs
rm -rf /var/lib/beanstalkd-rs
tar -C /var/lib -xzf /root/beanstalkd-rs-backup.tar.gz
systemctl start beanstalkd-rs
```

Run `tar` as root so the files keep their owner (`beanstalkd`). A backup
can also be started under another directory or on another machine
(`beanstalkd-rs -b /path/to/copy`), which is a convenient way to check
it. The copied `lock` file does not matter (section 3.3). After a
restore, jobs created after the backup are gone and their ids will be
issued again, so clients that remember job ids from that period may
refer to different jobs. Backups contain job bodies: store them with the
same care as the data.

Container: stop the container, then archive the volume, for example with
the image itself (`docker run --rm -v bstk-data:/data ...`) or from the
host's volume directory.

### 6.2 Cluster

A cluster already keeps every committed change on a majority of nodes:
losing a node's data is repaired by wiping it and letting it rejoin
(section 5.6), not by restoring a backup. Backups protect against losing
the whole cluster or a mistake that deleted jobs everywhere.

- **The only safe backup is a cold one of all nodes together**: stop every
  node, copy every node's `data_dir`, start them again. Restore all of
  them together, from the same backup, with every node stopped. Tested on
  three systemd hosts: stop all, back up, start, put a job, stop all,
  restore all, start: the cluster came back without the later job.

  ```sh
  systemctl stop beanstalkd-rs                       # on every node, then:
  tar -C /var/lib -czf /root/beanstalkd-rs-cluster-backup.tar.gz beanstalkd-rs
  systemctl start beanstalkd-rs                      # on every node
  ```

  Restore, on every node with all nodes stopped:

  ```sh
  systemctl stop beanstalkd-rs
  rm -rf /var/lib/beanstalkd-rs && tar -C /var/lib -xzf /root/beanstalkd-rs-cluster-backup.tar.gz
  systemctl start beanstalkd-rs
  ```

- **Never** restore one node's old `data_dir` while other nodes keep
  their newer state, and never mix backups taken at different times.
- A cluster backup cannot be loaded into a standalone server, and a
  standalone binlog cannot seed a cluster.
- Taking the whole cluster down for a backup is an outage. There is no
  online backup or export yet; if one is needed, a planned short downtime
  per backup is the current answer.

## 7. Upgrades

### 7.1 What is checked between versions

- **Binlog files** start with a magic number and format version (now 1);
  a server that does not know the version refuses to start rather than
  misread it.
- **Cluster wire protocol**: every cluster connection starts with a
  hello carrying the protocol version (now 3) and `-z`. A node refuses a
  hello with another version, logging at warn `cluster connection
  rejected: hello from node 1 rejected: unsupported protocol version 4`;
  the dialing side retries with backoff forever (on the leader openraft
  logs `ERROR openraft::replication: ... node 1 at 127.0.0.1:11401:
  rejected: unsupported protocol version`). Nodes on different protocol
  versions therefore never talk to each other, and a rolling upgrade
  across a protocol version change is impossible. Tested by restarting
  one follower of a 3-node cluster with a build whose protocol version
  was raised to 4: that node stayed at `waiting for a leader`, closed
  every client connection, kept `/readyz` at 503, and the other two
  nodes carried on; restarted with the old binary it caught up and served
  again. Two upgraded nodes out of three would instead form a majority
  that cannot reach the third.
- **Raft log entries and snapshots** have their own formats (log segment
  version 1, snapshot file version 2, which still reads version 1 files,
  snapshot payload version 2). Nodes with the same protocol version but
  different releases replicate entries to each other, so a release must
  only add log entry kinds that older nodes of the same protocol version
  never receive, or bump the protocol version.
- **Release notes** ([CHANGELOG.md](../CHANGELOG.md)): read them before
  upgrading; a change of the binlog, cluster data or protocol format is a
  user-facing change and is called out there.

0.5.0 is the first release, so no upgrade between two releases has been
tested yet. What was tested is replacing the binary with a build of the
same version, which exercises the procedures below.

### 7.2 Standalone

```sh
install -m 0755 beanstalkd-rs /usr/local/bin/beanstalkd-rs.new
/usr/local/bin/beanstalkd-rs.new --config /etc/beanstalkd-rs/config.toml --check-config
mv /usr/local/bin/beanstalkd-rs.new /usr/local/bin/beanstalkd-rs
systemctl restart beanstalkd-rs
printf 'stats\r\n' | nc -q 1 127.0.0.1 11300 | grep -E 'current-jobs-ready|^version'
```

The `mv` replaces the file atomically; the running process keeps the old
binary until the restart. Clients are disconnected during the restart (a
second or less plus binlog replay); with a binlog, jobs survive it. Keep
the old binary and a backup (section 6.1) until the new version has run
well: downgrading is only possible while the binlog format is unchanged.

### 7.3 Cluster (rolling restart)

Within one protocol version, upgrade one node at a time, followers first
and the leader last (so a newer leader never replicates to an older node):

1. Find the leader (section 5.4).
2. On a follower: install the new binary as in 7.2, `systemctl restart
   beanstalkd-rs`, and wait for its `/readyz` to return 200 before
   touching the next node. Its clients reconnect to it or to other nodes.
3. Repeat for the other followers, then the leader (a new leader is
   elected among the upgraded nodes in about 1.5 s). If the leader fails
   while some followers still run the old version, an upgraded node may
   become leader over them: finish the remaining nodes right away.

The same procedure applies certificate renewals and configuration
changes. Local walkthrough (a rolling restart of the three nodes, keeping
every job):

```sh
for i in 1 2 3; do
  pid=$(cat "$W/node$i.pid"); kill -TERM "$pid"
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
  "$BIN" --config "$W/node$i.toml" >> "$W/node$i.log" 2>&1 &
  echo $! > "$W/node$i.pid"
  until curl -fsS "http://127.0.0.1:918$i/readyz"; do sleep 0.2; done; echo " node$i"
done
```

If a future release changes the protocol version, the upgrade is a full
cluster restart: stop every node, replace the binaries, start every node;
the release notes will say so.

## 8. Monitoring

### 8.1 HTTP endpoints

Enable with `[http] addr` (off by default; no authentication, section 4).

| Endpoint | Answers |
|---|---|
| `/healthz` | `200 ok` while the process serves HTTP (liveness) |
| `/readyz` | `200 ready` once startup and binlog replay are done; in cluster mode while the node has a leader and has applied everything it knows to be committed (see section 11 for a caveat); otherwise 503 |
| `/metrics` | Prometheus text format |
| `/admin` | read-only JSON: every `stats` key, every tube's `stats-tube`, server-side counters, and in cluster mode a `cluster` object |

The `stats` command over the protocol shows the same server values as the
reference (`printf 'stats\r\n' | nc -w 1 127.0.0.1 11300`).

### 8.2 Key metrics

The full list, with the `stats` key behind each metric:
[DESIGN.md](DESIGN.md) section 6.2. Counters end in `_total`.

| Metric | Watch for |
|---|---|
| `beanstalkd_current_jobs{state}` | backlog (`ready`, `delayed`), stuck work (`reserved`), failed work (`buried`) |
| `beanstalkd_tube_current_jobs{tube,state}` | the same per tube (at most `max_tube_series` tubes) |
| `beanstalkd_commands_total{cmd}`, `beanstalkd_jobs_total` | throughput |
| `beanstalkd_job_timeouts_total` | workers exceeding their TTR |
| `beanstalkd_current_connections`, `beanstalkd_current_workers`, `beanstalkd_current_waiting` | connection counts against `LimitNOFILE` |
| `beanstalkd_draining` | drain mode is on |
| `beanstalkd_binlog_current_index`, `beanstalkd_binlog_oldest_index`, `beanstalkd_binlog_max_size_bytes` | binlog file count and size |
| `beanstalkd_pending_connections`, `beanstalkd_pending_rejected_total`, `beanstalkd_auth_failures_total`, `beanstalkd_auth_timeouts_total` | TLS handshake and token floods |
| `beanstalkd_cluster_role{role}`, `beanstalkd_cluster_leader_id`, `beanstalkd_cluster_term` | leadership and elections |
| `beanstalkd_cluster_ready`, `beanstalkd_cluster_isolated`, `beanstalkd_cluster_rejoining` | whether the node can serve clients |
| `beanstalkd_cluster_replication_lag{peer}` | entries each follower is missing (exported by the leader only) |
| `beanstalkd_cluster_commit_index`, `beanstalkd_cluster_applied_index` | a node falling behind |
| `beanstalkd_cluster_forward_queue`, `beanstalkd_cluster_forward_queue_full`, `beanstalkd_cluster_rejected_puts_total`, `beanstalkd_cluster_refused_connections_total` | back-pressure from the leader |
| `beanstalkd_cluster_resent_inputs_total`, `beanstalkd_cluster_forward_rewinds_total{cause}` | inputs sent to the leader again (leader changes, errors, stalls) |
| `beanstalkd_cluster_log_bytes`, `beanstalkd_cluster_snapshot_bytes` | cluster disk usage |
| `beanstalkd_cluster_drop_node_proposals_total` | nodes the leader dropped for silence |

### 8.3 Suggested alerts

A starting point for Prometheus rules (checked with `promtool check
rules`); tune thresholds to your traffic. The last rule needs
node_exporter.

```yaml
groups:
  - name: beanstalkd-rs
    rules:
      - alert: BeanstalkdDown
        expr: up{job="beanstalkd-rs"} == 0
        for: 1m
      - alert: BeanstalkdNotReady
        expr: beanstalkd_cluster_ready == 0 or beanstalkd_cluster_isolated == 1
        for: 1m
      - alert: BeanstalkdNoLeader
        expr: beanstalkd_cluster_leader_id == 0
        for: 30s
      - alert: BeanstalkdLeaderChurn
        expr: changes(beanstalkd_cluster_term[15m]) > 3
      - alert: BeanstalkdFollowerLag
        expr: max by (instance, peer) (beanstalkd_cluster_replication_lag) > 10000
        for: 2m
      - alert: BeanstalkdResends
        expr: rate(beanstalkd_cluster_resent_inputs_total[5m]) > 10
        for: 10m
      - alert: BeanstalkdForwardQueueFull
        expr: beanstalkd_cluster_forward_queue_full == 1
        for: 1m
      - alert: BeanstalkdRejectingPuts
        expr: increase(beanstalkd_cluster_rejected_puts_total[5m]) > 0
      - alert: BeanstalkdRejoining
        expr: beanstalkd_cluster_rejoining == 1
        for: 10m
      - alert: BeanstalkdConnectionsHigh
        expr: beanstalkd_current_connections > 50000
        for: 5m
      - alert: BeanstalkdPendingRejected
        expr: increase(beanstalkd_pending_rejected_total[5m]) > 0
      - alert: BeanstalkdAuthFailures
        expr: rate(beanstalkd_auth_failures_total[5m]) > 1
        for: 5m
      - alert: BeanstalkdBuriedJobs
        expr: beanstalkd_current_jobs{state="buried"} > 0
        for: 30m
      - alert: BeanstalkdDataDiskFilling
        expr: node_filesystem_avail_bytes{mountpoint="/var/lib/beanstalkd-rs"} / node_filesystem_size_bytes{mountpoint="/var/lib/beanstalkd-rs"} < 0.15
        for: 10m
```

`BeanstalkdConnectionsHigh` assumes `LimitNOFILE=65536`.

### 8.4 Disk

There is no disk-space metric: watch the file system of the binlog or
cluster data directory (node_exporter), and alert well before it is
full. A full disk makes standalone `put`s fail with `OUT_OF_MEMORY` and
any other binlog write error stop the server (section 3.2); in cluster
mode a log write error stops the node.

## 9. Troubleshooting

Startup errors are printed to stderr (the journal under systemd) as
`beanstalkd-rs: ...` with a non-zero exit status: 1 for configuration and
startup errors, 10 for a locked data directory, 20 for a binlog error
while serving, 2 for a command-line syntax error, 5 for `-u`.

| Message | Cause and fix |
|---|---|
| `cannot listen on 127.0.0.1:11300: Address already in use (os error 98)` (os error 48 on macOS) | Another process (often another beanstalkd) has the port. Find it with `ss -ltnp` / `lsof -i :11300`. The same message with `(http)` or `(cluster)` names the HTTP or cluster port. |
| `failed to lock wal dir /var/lib/beanstalkd-rs` (exit 10) | Another beanstalkd-rs is running on the same binlog directory. systemd does not restart on status 10. |
| `failed to lock cluster data dir /var/lib/beanstalkd-rs (/var/lib/beanstalkd-rs/log)` (exit 10) | The same for a cluster `data_dir`. |
| `invalid configuration: FILE:3:1: unknown field `bogus`, expected one of ...` | A typo or a key in the wrong table; see [beanstalkd-rs.example.toml](beanstalkd-rs.example.toml). |
| `invalid configuration: /etc/beanstalkd-rs/missing.pem: cannot load PEM certificates: I/O error: No such file or directory (os error 2)` | A TLS file path is wrong (relative paths are relative to the configuration file) or not readable by the service user. |
| `warning: auth.tokens_file ... is accessible by group or others (mode 0644); restrict it with chmod 600` | Restrict the tokens file (section 4). |
| `failed to replay log in /var/lib/beanstalkd-rs: binlog corrupt: binlog.1 offset 0: bad magic` (exit 1) | The binlog is damaged before its last file, or is not a beanstalkd-rs binlog. Restore a backup (section 6.1). |
| Client: `tlsv13 alert certificate required` / `tlsv1 alert unknown ca`; server (debug level): `TLS handshake failed: peer sent no certificates` / `invalid peer certificate: UnknownIssuer` | An mTLS listener got no client certificate, or one not signed by `[tls] client_ca`. |
| Client: `certificate verify failed` | The client does not trust the server's CA, or connects with a name not in the server certificate. |
| `UNAUTHORIZED` and a close; server: `authentication failed: wrong token` | Wrong token, or a command sent before `auth` on a token listener. |
| `invalid configuration: cluster.tls.cert: certificate is not valid for bstk-node-3` | The node's certificate lacks the SAN `bstk-node-<node_id>` (wrong file for this node). |
| `cluster connection rejected: TLS handshake: ...` on the peers; `invalid peer certificate: BadSignature` / `UnknownIssuer` in their replication errors | The node's certificate is from another CA than `[cluster.tls] ca` on its peers. |
| `cluster peer refused this node: max_job_size mismatch (every node must use the same -z)`; on the peers `cluster hello rejected: max_job_size mismatch: node 1 uses 65535, node 3 uses 1000` | Different `-z` / `server.max_job_size` on this node. |
| `cluster peer refused this node: this node is not a member of the cluster according to node 2`; on the peers `cluster connection rejected: hello from node 4 rejected: node 4 is not a member of the cluster` | The node's id is not in the cluster's Raft membership (it was removed, or not added yet), or, on a node that has no membership yet, not in that node's `[[cluster.peer]]` list. |
| `cluster connection rejected: hello from node 1 rejected: unsupported protocol version 4` | Mixed releases with different cluster protocols (section 7.1). |
| `--cluster-init: DIR already holds Raft state; a cluster is bootstrapped only once ...` | Remove `--cluster-init` (or the systemd drop-in, section 5.3). |
| Stays at `waiting for a leader`, `/readyz` 503 | No majority is reachable: check that enough nodes run, that the cluster ports are open between all nodes (both directions), and the TLS / `-z` errors above. A new cluster started with `--cluster-init` waits until a majority of the other nodes answer (section 5.3). |
| `startup: waiting for the cluster's status (...)` | A joining or rejoining node (section 5.6) is waiting for answers: a rejoin needs `n - quorum(n) + 1` of the other current voters that are not rejoining themselves (1 with 2 voters, 2 with 3 or 4, 3 with 5); start them, or wait until another rejoin finishes. |
| `rejoin: waiting ... (the highest vote (...) is this node's own leadership)` | The wiped node was the leader; it waits until the others elect a new one (a few seconds). |
| Peers log `vote refused: the vote gate is closed` (debug) | Normal while a node rejoins: it does not vote until it has caught up. |
| `INSECURE: cluster.insecure_plaintext = true: ...` | Test configuration; use `[cluster.tls]` in production. |
| A node's clients are disconnected and new connections close immediately; `beanstalkd_cluster_isolated` = 1 | The node cannot reach a leader (partition, peers down); see section 5.5. |

## 10. Compatibility notes

beanstalkd-rs implements the reference beanstalkd's protocol byte for
byte, checked by differential tests against the reference. The
intentional differences (binlog format and fail-stop behavior, graceful
SIGTERM, `-u`, hostnames in `-l`, and cluster-mode differences such as
per-node `stats` identity fields) are listed in [COMPAT.md](COMPAT.md):
"Known differences" (D1 to D14), "Cluster mode" (C1 to C10) and the
extensions (token authentication).

## 11. Known limitations

- **Static cluster membership**: nodes cannot be added or removed, and
  the cluster cannot grow from 3 to 5, without rebuilding it (dynamic
  membership is planned, PLAN P6). A failed machine is replaced by a new
  one with the same id, address and certificate (section 5.6).
- **Cluster CPU cost**: a command costs about 17 to 18 µs of CPU per node
  at 100 connections (see [BENCH.md](BENCH.md) "P4-T6"), several times
  standalone, because every node applies every command and every entry is
  fsynced on a majority. Plan cluster capacity accordingly.
- **Bootstrapping** needs all nodes of a 3-node cluster (a majority of
  the others) up; a rejoin needs `n - quorum(n) + 1` of the other current
  voters, none of them rejoining, and a leader; with two voters the
  survivor must be (and stay) the leader, otherwise the cluster stalls.
- **Snapshot building** briefly stops a node from applying entries (about
  0.2 to 0.4 s for 1 to 2 million jobs), and a snapshot received from the
  leader that fails validation stops the follower until an operator wipes
  it (section 5.6).
- **Backups**: no online backup or export; cluster backups need a full
  stop (section 6.2).
- **No configuration reload**: every change needs a restart (a rolling
  one in a cluster).
- **Logging**: one level for everything; at `info` in cluster mode the
  embedded Raft library is verbose (section 2.4).
