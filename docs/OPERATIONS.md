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
repeat `-l` / `-p` when adding flags. Run cluster nodes with
`--restart on-failure`: a node exits with status 21 when Raft stops on a
fatal error (section 1.3) and expects to be restarted. With a configuration file, mount it
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
  0 (a cluster node also exits with status 21 when Raft stops on a fatal
  error, and so is restarted), except status 10 (the data directory is locked by another process,
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

- **3 or 5 voters**, each node with its own configuration file holding
  its own `node_id` (1 to 65535), `listen`, `data_dir` and certificate,
  and a `[[cluster.peer]]` list: the nodes to bootstrap with, or seeds to
  find a running cluster. A 3-voter cluster survives the loss of 1 node, a
  5-voter cluster of 2. Nodes are added, removed, replaced and moved while
  the cluster runs (sections 5.8 and 5.9); the membership in the Raft log,
  not the configuration files, is then the authority. (1 voter is accepted
  for tests and as a start to grow from.)
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
  "Cluster mode" (C1 to C11).
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
not on the nodes (the `beanstalkd-rs cluster` commands, section 5.8, use
it; with `insecure_plaintext` the admin channel is open to loopback
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
node that is starting without its data. Probe connections are limited
(one per node id, at most 30 s each, 8 at once), and in plaintext test
mode they are accepted from loopback only unless
`insecure_plaintext_allow_remote = true`.

**Rotate the CA** when you remove a node you no longer trust (its
certificate, or a leaked key, could still probe), or when the CA key may
have leaked: issue a new CA and new certificates for the remaining nodes
and the operator, and switch every node at once (a node trusts one CA, so
nodes on different CAs cannot talk). On the walkthrough of section 5.3,
with nodes 2, 4 and 5 left (helpers: section 5.9), the full restart took
0.5 s:

```sh
scripts/mkcluster-certs.sh "$W/tls-new" 2 4 5 admin
for i in 2 4 5; do stop_node $i; done
mv "$W/tls" "$W/tls-old"; mv "$W/tls-new" "$W/tls"
for i in 2 4 5; do start_node $i; done
for i in 2 4 5; do wait_ready $i; done
```

The old certificates are refused from then on (`TLS handshake failed:
invalid peer certificate: BadSignature`).

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
scripts/mkcluster-certs.sh "$W/tls" 1 2 3 admin
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
same id, a corrupted log) is brought back by starting it
**without** `--cluster-init` on an **empty** `data_dir`. It then
*rejoins*: it may have acknowledged entries and granted votes it no
longer remembers, so it adopts the highest vote of the other nodes, does
not vote or stand for election, and serves no clients (`/readyz` 503,
`beanstalkd_cluster_rejoining` = 1) until it has caught up with a leader.
While a node starts, `/metrics` and `/admin` show its cluster figures
only (section 8.1).
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
- A replacement machine keeps the node's id and gets that node's
  certificate; it may get a new address (section 5.9, "Replace a node's
  disk"). The node may also be replaced by one with a new id (section 5.9).
- Do not start a wiped node with `--cluster-init` by mistake: if a peer
  is already established it rejoins anyway (with a warning), but on a
  cluster whose other nodes are also empty it would bootstrap.

### 5.7 Shutting down and restarting the whole cluster

SIGTERM each node (`systemctl stop`); each exits within a fraction of a
second. Start them again without `--cluster-init`, in any order; the
cluster serves once a majority is up, with every committed job, job id,
counter and drain mode preserved (COMPAT C5).

### 5.8 Membership commands (`beanstalkd-rs cluster`)

`beanstalkd-rs cluster <command>` shows and changes the membership of a
running cluster through the cluster port's admin channel (the runbooks
for growing, shrinking, replacing and moving nodes build on it). It is a
separate command line: the server flags (`-l`, `-p`, ...) do not apply to
it and are refused before it. `beanstalkd-rs cluster --help` lists every
option.

| Command | What it does |
|---|---|
| `status` | voters, learners, the leader, the highest member id ever, the membership's log id and whether it is committed, whether a joint configuration is in effect, and for each member its address, applied index, lag behind the leader, and state (`ok`, `rejoining`, `starting`, or unreachable). Exit 0 whenever a node answered. |
| `add ID HOST:PORT` | adds node `ID` as a learner (it replicates, it does not vote). |
| `promote ID [--force]` | makes a caught-up learner a voter. `--force` promotes a learner that is not caught up. |
| `remove ID [--force]` | removes a learner or a voter, the leader too. The node is not told: stop its process. `--force` goes below 3 voters. |
| `set-addr ID HOST:PORT [--force]` | changes a node's cluster address. `--force` allows a non-loopback address over plaintext cluster traffic. |

Options common to all commands (before or after the command):

- `--node HOST:PORT` (repeatable): the cluster address of any member.
  Nodes are asked in order until one answers; the members' addresses
  learned from the answers are tried after them. `--config FILE` takes the
  `[[cluster.peer]]` addresses and the `[cluster.tls]` `ca` from a node's
  configuration file instead (or in addition).
- `--ca FILE`, `--cert FILE`, `--key FILE`: the cluster CA and the
  operator certificate from `scripts/mkcluster-certs.sh DIR admin`
  (section 5.2). A node certificate is refused (exit 2). With
  `insecure_plaintext` clusters (tests only) pass `--insecure-plaintext`
  instead, from the machine that runs the node.
- `--timeout DURATION` (default `30s`): the most a command may take in all.
- `--json`: one JSON object on stdout instead of text, errors included
  (`{"ok": false, "exit": 1, "kind": "refused", "error": "..."}`).

Every change is based on the membership the tool has just read (a
compare-and-set on its log id): if another change got in first, the
command prints the membership it found and exits 1 without changing
anything (`conflict`), and you decide again; nothing is retried. A node
that is not the leader names the leader, and the tool follows it (the
message `... is not the leader; following to node N` on stderr). A change
the leader runs in the background is waited for until the new membership
is committed and final. Notes from the cluster (an even number of voters,
"stop the removed node") are printed as `NOTE:` lines. The guardrails
(one voter per change, no fewer than 3 voters, only a caught-up learner,
ids never reused, no voter change while a voter rejoins) are the cluster's;
the command only reports their refusals.

Exit status:

| Status | Meaning |
|---|---|
| 0 | done (for `status`: a node answered) |
| 1 | refused by the cluster (a guardrail, a conflict, a rejected identity) |
| 2 | usage error, or a certificate or configuration file that cannot be used |
| 3 | no node could be reached, or no leader was found, within the timeout |
| 4 | the change was accepted (or sent) but not seen to complete within the timeout; **it may still complete**: run `status` and look before repeating it |

```sh
beanstalkd-rs cluster status --node 10.0.0.1:11302 \
    --ca cluster-tls/cluster-ca.pem --cert cluster-tls/admin.pem --key cluster-tls/admin.key
```

```
membership (term 1 index 5, committed)
  voters:   1 2 3
  learners: -
leader: node 3 (10.0.0.3:11302), term 1
highest member id ever: 3

ID  ROLE    ADDRESS          APPLIED  LAG  STATE
1   voter   10.0.0.1:11302   4        0    ok
2   voter   10.0.0.2:11302   4        0    ok
3   leader  10.0.0.3:11302   4        0    ok
```

### 5.9 Membership runbooks

Every block below was run as written against the local walkthrough of
section 5.3 (TLS on loopback, macOS, release build); the timings are
from those runs. On real hosts use `systemctl start` / `stop` instead of
`start_node` / `stop_node`, and your addresses. The helpers, for the
shell that holds `W` and `BIN` from section 5.3 (create the operator
certificate first if needed: `scripts/mkcluster-certs.sh "$W/tls" admin`):

```sh
node_config() {   # node_config ID SEED_ID...: a config like section 5.3's
  i=$1; shift
  cat > "$W/node$i.toml" <<EOT
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

[http]
addr = "127.0.0.1:918$i"

[log]
level = "info"
EOT
  for p in "$@"; do
    printf '\n[[cluster.peer]]\nid = %s\naddr = "127.0.0.1:1140%s"\n' "$p" "$p" >> "$W/node$i.toml"
  done
}
start_node() {    # start_node ID [ARGS...]
  i=$1; shift
  "$BIN" --config "$W/node$i.toml" "$@" >> "$W/node$i.log" 2>&1 &
  echo $! > "$W/node$i.pid"
}
stop_node() {     # stop_node ID: SIGTERM and wait for the exit
  pid=$(cat "$W/node$1.pid"); kill -TERM "$pid"
  while kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
}
wait_ready() {    # wait_ready ID
  until curl -fs "http://127.0.0.1:918$1/readyz" > /dev/null; do sleep 0.2; done
}
bcl() {           # beanstalkd-rs cluster with the operator certificate
  "$BIN" cluster --node 127.0.0.1:11401 --node 127.0.0.1:11402 --node 127.0.0.1:11403 \
    --ca "$W/tls/cluster-ca.pem" --cert "$W/tls/admin.pem" --key "$W/tls/admin.key" "$@"
}
settled() {       # every member answers its status and none is rejoining
  local out; out=$(bcl status --json 2>/dev/null) &&
    ! printf "%s" "$out" | grep -qE '"reachable":false|"rejoining":true'
}
```

Rules that hold for every runbook:

- **Ids are never reused.** A new node gets an id above `highest member id
  ever` (`bcl status`), and its own certificate (`scripts/mkcluster-certs.sh
  "$W/tls" ID`, where the CA key is).
- **A new node's `[[cluster.peer]]` list** needs a few running members
  (seeds); it need not list itself, and the other nodes' configurations
  need not list it (they learn its address from the membership). An entry
  in a configuration is also an *address override* for that node (see
  "Change a node's address").
- **Add, then start**: `add` makes the node a learner; started, it catches
  up from the leader and becomes ready (1.1 to 1.5 s here); then `promote`
  makes it a voter. Starting it first also works: it waits, not ready
  (`/readyz` 503, `beanstalkd_cluster_joining` 1), and logs `startup:
  waiting for the cluster's status (join: node 5 is not a member of the
  cluster yet (voters {1, 2, 3, 4}, highest member id 4); waiting until it
  is added as a learner)` until it is added.
- **One voter per change, and an odd count**: each `promote` or `remove`
  of a voter is one change; the cluster notes an even count (`NOTE: 4
  voters: an even count tolerates no more failures than 3 voters; change to
  an odd count`). No voter change runs while a voter does not answer or is
  rejoining, except removing that voter.
- **`bcl` names nodes 1 to 3 as its entry points.** The tool asks them in
  order and learns the other members' addresses from the first answer, so
  it keeps working while at least one of them runs; once all three have
  been removed or replaced, change the `--node` list in `bcl` to nodes that
  are still members (with none answering the command exits 3).
- **`/readyz` can flip to 503 under load.** A node answers 503 while it
  lags behind the commit index it has learned, which happens briefly on a
  busy cluster (a sustained load of thousands of puts per second, a node
  that has just restarted or joined). Poll it, as `wait_ready` does, and do
  not treat one 503 as a failure; a node that stays 503 is the signal.
- **A removed node is not told**: stop it right after `remove`. Until then
  it serves nothing (`/readyz` 503), its clients' connections are dropped
  (their reservations return to ready), it logs failed vote requests
  (`... rejected: not a member of this cluster`) and closes new client
  connections after `node_timeout`. Restarted with its data it logs `this
  node is not in the membership of its own log: it was removed ...` and
  waits forever; with an empty data directory it refuses to start (`cannot
  start node 1: node id 1 is not a member, and ids up to 6 have been used:
  node ids are never reused ...`).

#### Grow from 3 to 5 nodes

Node 4 is added and then started; node 5 is started first and waits until
it is added:

```sh
scripts/mkcluster-certs.sh "$W/tls" 4 5
node_config 4 1 2 3
node_config 5 1 2 3
bcl add 4 127.0.0.1:11404
start_node 4; wait_ready 4
bcl promote 4
start_node 5
sleep 2; curl -s http://127.0.0.1:9185/metrics | grep -E '^beanstalkd_cluster_(joining|is_member|ready) '
bcl add 5 127.0.0.1:11405
wait_ready 5
bcl promote 5
bcl status
```

```
beanstalkd_cluster_ready 0
beanstalkd_cluster_is_member 0
beanstalkd_cluster_joining 1
...
membership (term 1 index 14, committed)
  voters:   1 2 3 4 5
  learners: -
leader: node 3 (127.0.0.1:11403), term 1
highest member id ever: 5

ID  ROLE    ADDRESS          APPLIED  LAG  STATE
1   voter   127.0.0.1:11401  14       0    ok
2   voter   127.0.0.1:11402  14       0    ok
3   leader  127.0.0.1:11403  14       0    ok
4   voter   127.0.0.1:11404  14       0    ok
5   voter   127.0.0.1:11405  14       0    ok
```

Each `add` and `promote` took 30 to 70 ms. Followers answer with the
leader, and the tool follows it (`127.0.0.1:11401 is not the leader;
following to node 3 at 127.0.0.1:11403` on stderr).

#### Grow from 1 to 3 nodes

A single-node cluster (for example a test cluster that becomes
production) is created with only its own entry, then grown one voter at a
time. At 2 voters the cluster tolerates no failure and a wiped voter may
not be able to rejoin: go on to 3 at once.

```sh
scripts/mkcluster-certs.sh "$W/tls" 1 2 3 admin
node_config 1 1
node_config 2 1
node_config 3 1
start_node 1 --cluster-init; wait_ready 1
bcl add 2 127.0.0.1:11402
start_node 2; wait_ready 2
bcl promote 2
bcl add 3 127.0.0.1:11403
start_node 3; wait_ready 3
bcl promote 3
bcl status
```

The whole block took 4 s; `bcl promote 2` notes `2 voters: an even count
tolerates no more failures than 1 voter; change to an odd count; 2
voter(s): below 3, a wiped voter may be unable to rejoin`.

#### Shrink from 5 to 3 nodes

Remove one voter at a time, and stop each removed node:

```sh
bcl remove 5
stop_node 5
bcl remove 4
stop_node 4
bcl status
```

```
remove: done
membership (term 1 index 16, committed)
  voters:   1 2 3 4
  learners: -

NOTE: node 5 is not told it was removed (it isolates itself): stop its process; its id can never be used again; 4 voters: an even count tolerates no more failures than 3 voters; change to an odd count
```

Removing a voter below 3 needs `--force`: `refused: removing node 1 leaves
2 voter(s), fewer than 3: a wiped voter of a two-voter membership rejoins
only while the other leads, and the voter of a one-voter membership never
(use force to do it anyway)`.

#### Replace a failed node with a new id

Node 1 of three voters is dead. Remove it first, then add, start and
promote the replacement (node 6): the cluster refuses every voter change
while a voter does not answer, so a promotion before the removal fails
with `refused: voter N did not answer its status (unreachable: node N:
backing off after 5 failed dial(s)): voter changes need every voter's
answer (one rejoining unseen must not be overlooked)`; removing the dead voter is the one change allowed then, and
it needs `--force` because it leaves 2 voters. Until the promotion the
cluster has 2 voters and tolerates no further failure. (Adding the
replacement as a learner is allowed while a voter is down, so it can also
be added and started before the removal, leaving only the promotion after
it.)

```sh
kill -KILL "$(cat "$W/node1.pid")"   # the failure
bcl remove 1 --force
scripts/mkcluster-certs.sh "$W/tls" 6
node_config 6 2 3
bcl add 6 127.0.0.1:11406
start_node 6; wait_ready 6
bcl promote 6
bcl status
```

```
membership (term 1 index 25, committed)
  voters:   2 3 6
  learners: -
leader: node 3 (127.0.0.1:11403), term 1
highest member id ever: 6
...
```

The replacement was ready 1.1 s after its start. If the failed machine
comes back, do not start node 1 on it (it is removed; with its old data
it would wait forever).

#### Replace a node's disk (same id), optionally at a new address

A node whose data is lost keeps its id and rejoins (section 5.6): stop
it if it still runs, empty its data directory, start it:

```sh
rm -rf "$W/node2"
start_node 2; wait_ready 2
bcl status
```

It was ready 1.3 s after its start. To bring it back at another address
(a new machine), change the address in the membership before starting
it, and its `listen`. No other node's configuration may list it (see
"Change a node's address"):

```sh
kill -KILL "$(cat "$W/node6.pid")"   # the failure
rm -rf "$W/node6"
bcl set-addr 6 127.0.0.1:11416
perl -pi -e 's/^listen = .*/listen = "127.0.0.1:11416"/' "$W/node6.toml"
start_node 6; wait_ready 6
bcl status
```

```
ID  ROLE    ADDRESS          APPLIED  LAG  STATE
2   voter   127.0.0.1:11402  34       0    ok
3   leader  127.0.0.1:11403  34       0    ok
6   voter   127.0.0.1:11416  34       0    ok
```

#### Remove a node that is still running, or the leader

`remove` works on a running node (shrink above): stop it afterwards.
Removing the leader (4 voters, node 3 leading, a client putting jobs in a
loop on each node):

```sh
bcl remove 3
stop_node 3
```

```
127.0.0.1:11402 is not the leader; following to node 3 at 127.0.0.1:11403
127.0.0.1:11403 (node 3) is not a member any more; asking another node
remove: done
membership (term 2 index 1394, committed)
  voters:   2 4 5
  learners: -

NOTE: node 3 (the leader that ran this change) steps down once it is committed; another node leads, and node 3 is not told it was removed: stop its process
```

openraft 0.9 has no leader transfer: the old leader steps down and the
others elect a new one. Replies on the other nodes paused for 1.28 to
1.36 s (two runs) and their connections stayed open; the removed leader's
own clients were disconnected at once. A node whose `node_timeout` is
shorter than an election closes its clients' connections meanwhile (keep
the default 5 s).

#### Change a node's address

`set-addr` changes the address in the membership; every node then dials
the node there, **except** nodes whose configuration lists that node in
`[[cluster.peer]]`: a configured address overrides the membership, read at
startup, and such a node logs `cluster config differs from the
membership: node 2: config address 127.0.0.1:11402 overrides membership
address 127.0.0.1:11412`. A leader with a stale override cannot replicate
to the moved node. So first remove the node's entry from the other nodes'
configurations and restart them one at a time (nothing changes yet: the
membership has the same address), then change the address and restart
the node there. Node 2 below is listed in node 3's and node 6's
configurations:

```sh
perl -0pi -e 's/\[\[cluster\.peer\]\]\nid = 2\naddr = "[^"]*"\n//' "$W/node3.toml" "$W/node6.toml"
stop_node 6; start_node 6; wait_ready 6
stop_node 3; start_node 3; wait_ready 3
bcl set-addr 2 127.0.0.1:11412
stop_node 2
perl -pi -e 's/127\.0\.0\.1:11402/127.0.0.1:11412/' "$W/node2.toml"
start_node 2; wait_ready 2
bcl status
```

The restarts took 0.4 s each and node 2 was ready 0.23 s after its start
at the new address. A non-loopback address over plaintext cluster traffic
needs `--force`.

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
  hello carrying the protocol version (now 4) and `-z`. A node refuses a
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

**From 0.5.x**: 0.5.x speaks cluster protocol version 3, later versions
version 4 (membership changes), so 0.5.x and later nodes cannot be mixed
in one cluster: stop every node, replace every binary, start every node
(section 5.7; the data directories are kept). Standalone servers upgrade
as in 7.2. **Later versions** (same protocol version): a rolling restart
(7.3).

0.5.0 is the first release, so no upgrade between two releases has been
tested yet. What was tested is replacing the binary with a build of the
same version, both by a full stop and start and by a rolling restart,
which exercises the procedures below.

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

Before the next node, also wait until `beanstalkd-rs cluster status`
shows every member reachable and none rejoining (helpers `settled` and the
others: section 5.9). Under load (one client per node putting jobs in a
loop, reconnecting when closed), on the 3-voter walkthrough with nodes 2,
4 and 5 and node 5 leading:

```sh
for i in 2 4 5; do stop_node $i; start_node $i; wait_ready $i; until settled; do sleep 0.5; done; echo "node$i done"; done
```

The three restarts took 2.2 s in all. Each client lost its connection
once (its node restarting) and reconnected; replies paused at most 2.0 s;
59,218 acknowledged puts were all present afterwards (two more jobs than
acknowledged: puts in flight when a connection closed were committed
without their reply reaching the client, as with any disconnect). A
second run: 88,572 acknowledged puts, all present, longest pause 195 ms.

If a release changes the protocol version (as 0.5.x to later versions
does, section 7.1), the upgrade is a full cluster restart: stop every
node, replace the binaries, start every node; the release notes say so.

## 8. Monitoring

### 8.1 HTTP endpoints

Enable with `[http] addr` (off by default; no authentication, section 4).

| Endpoint | Answers |
|---|---|
| `/healthz` | `200 ok` while the process serves HTTP (liveness) |
| `/readyz` | `200 ready` once startup and binlog replay are done; in cluster mode while the node is a member, has a leader, is not isolated, and has applied everything it knows to be committed (see section 11 for a caveat); otherwise 503. A node waiting to be added and a removed node that still runs are never ready. |
| `/metrics` | Prometheus text format; 503 until ready, except that a cluster node that is still starting (joining, rejoining, waiting for a leader) exports its cluster metrics only |
| `/admin` | read-only JSON: every `stats` key, every tube's `stats-tube`, server-side counters, and in cluster mode a `cluster` object (while a cluster node starts, `{"cluster": {...}}` only) |

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
| `beanstalkd_cluster_ready`, `beanstalkd_cluster_isolated`, `beanstalkd_cluster_rejoining`, `beanstalkd_cluster_joining`, `beanstalkd_cluster_is_member` | whether the node can serve clients; a node waiting to be added (`joining`); a removed node still running (`is_member` 0) |
| `beanstalkd_cluster_voters`, `beanstalkd_cluster_learners`, `beanstalkd_cluster_member{node,role,addr}` | the membership as each node sees it (an even voter count, a learner left behind) |
| `beanstalkd_cluster_membership_joint`, `beanstalkd_cluster_membership_committed`, `beanstalkd_cluster_membership_log_index` | a membership change in progress, or stuck |
| `beanstalkd_cluster_learner_lag{node}` | entries a learner is missing before it can be promoted (leader only) |
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
      - alert: BeanstalkdJoining
        expr: beanstalkd_cluster_joining == 1
        for: 10m
      - alert: BeanstalkdRemovedNodeRunning
        expr: beanstalkd_cluster_is_member == 0 and beanstalkd_cluster_joining == 0
        for: 5m
      - alert: BeanstalkdMembershipChangeStuck
        expr: beanstalkd_cluster_membership_joint == 1 or beanstalkd_cluster_membership_committed == 0
        for: 2m
      - alert: BeanstalkdEvenVoterCount
        expr: beanstalkd_cluster_voters % 2 == 0
        for: 30m
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
while serving, 21 for a cluster node whose Raft stopped on a fatal error
(systemd restarts it; if it keeps coming back, read the `ERROR` line above
the exit: a disk or snapshot problem), 2 for a command-line syntax error, 5 for `-u`.

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
| `cluster peer refused this node: this node is not a member of the cluster according to node 2 (removed, or not added yet)`, `... rejected: not a member of this cluster`; on the peers `cluster connection rejected: hello from node 4 rejected: node 4 is not a member of the cluster` | The node's id is not in the cluster's Raft membership (it was removed, or not added yet), or, on a node that has no membership yet, not in that node's `[[cluster.peer]]` list. A node waiting to join logs the join message below instead. |
| `cluster connection rejected: hello from node 1 rejected: unsupported protocol version 4` | Mixed releases with different cluster protocols (section 7.1). |
| `--cluster-init: DIR already holds Raft state; a cluster is bootstrapped only once ...` | Remove `--cluster-init` (or the systemd drop-in, section 5.3). |
| Stays at `waiting for a leader`, `/readyz` 503 | No majority is reachable: check that enough nodes run, that the cluster ports are open between all nodes (both directions), and the TLS / `-z` errors above. A new cluster started with `--cluster-init` waits until a majority of the other nodes answer (section 5.3). |
| `startup: waiting for the cluster's status (join: node 5 is not a member of the cluster yet (voters {1, 2, 3, 4}, highest member id 4); waiting until it is added as a learner)` | The node was started before it was added (`beanstalkd_cluster_joining` 1, `/readyz` 503): add it (`beanstalkd-rs cluster add`, section 5.9) and it continues by itself. |
| `startup: waiting for the cluster's status (1 of the 2 answers needed from the current voters {3, 6} holding the membership at index 29 and not rejoining themselves)` | A rejoining node (section 5.6) is waiting for answers: a rejoin needs `n - quorum(n) + 1` of the other current voters that are not rejoining themselves (1 with 2 voters, 2 with 3 or 4, 3 with 5); start them, or wait until another rejoin finishes. With `... a membership change is in progress` or `... a joint configuration ...: waiting until it is uniform` it waits until the change is committed. |
| `cannot start node 1: node id 1 is not a member, and ids up to 6 have been used: node ids are never reused (a removed node joins again only under a new id above 6)` (exit 1) | A removed id was started with an empty data directory. Use a new id (section 5.9). |
| `this node is not in the membership of its own log: it was removed (or its removal was not committed yet) ...`, then `waiting for a leader` forever | A removed node was restarted with its data. Stop it; it never serves again under this id. |
| A removed node that still runs: ERROR `while requesting vote ... rejected: not a member of this cluster` every election timeout, `/readyz` 503, `beanstalkd_cluster_is_member` 0, and after `node_timeout` `no leader reachable: closing every client connection` | It was not told of its removal and isolates itself. Stop it. |
| `cluster config differs from the membership: node 2: config address 127.0.0.1:11402 overrides membership address 127.0.0.1:11412` | This node's `[[cluster.peer]]` entry overrides the membership address of node 2 (after a `set-addr`): it dials the old address. Remove or correct the entry and restart this node (section 5.9, "Change a node's address"). `members not in the config` and `config peers not in the membership` in the same message are informational. |
| `refused: a membership change is in progress` (CLI exit 1) | Another change is running (a voter change takes two commits; a new leader first finishes a change its predecessor left). Run `status` and the command again. |
| `conflict: the membership changed while this request was being made (it was based on ..., it is now ...); nothing was changed ...` (CLI exit 1) | Another change got in between the tool's read and its request. Check `status` and decide again. |
| `refused: voter N did not answer its status (...): voter changes need every voter's answer ...` / `refused: voter 2 is rejoining: voter changes wait until it has caught up ...` (CLI exit 1) | Bring the voter back or wait for its rejoin; a dead voter can only be removed (section 5.9, "Replace a failed node"). |
| `refused: removing node 1 leaves 2 voter(s), fewer than 3: ...` / `refused: node id 3 is not above 6, the highest id ever used: node ids are never reused (use a new id)` (CLI exit 1) | A guardrail: add a voter first (or `--force`), or use a new id. |
| `beanstalkd-rs cluster: cert is not an admin certificate (SAN bstk-admin): ...`, `--cert and --key (the bstk-admin certificate) are required ...` (CLI exit 2) | Use `admin.pem` / `admin.key` from `scripts/mkcluster-certs.sh DIR admin` (section 5.2). |
| `cannot reach the cluster: 127.0.0.1:11499: Connection refused (os error 61)`, or `... TLS handshake failed: invalid peer certificate: BadSignature (is --ca the cluster CA, ...)` (CLI exit 3) | No `--node` answered: wrong address, nodes down, or a CA or certificate from another (or an old, rotated) CA. |
| `timed out: the change was accepted by ... but not seen to complete within ...; it may still complete` (CLI exit 4) | Run `status` before repeating the command. |
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
"Known differences" (D1 to D14), "Cluster mode" (C1 to C11) and the
extensions (token authentication).

## 11. Known limitations

- **Membership changes** (section 5.9): no leader transfer (openraft
  0.9), so removing the leader costs one election (about 1.3 s); a removed
  node is not told and must be stopped; node ids are never reused; one
  voter per change, and none while a voter is down (except removing it) or
  rejoining; no automatic removal of dead nodes. With 1 or 2 voters a
  wiped voter cannot (1) or can only while the other leads (2) rejoin, so
  grow past 2 at once. Rejoins are serialized: a second wiped node waits
  until the first has caught up. Config `[[cluster.peer]]` addresses
  override the membership's (section 5.9).
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
