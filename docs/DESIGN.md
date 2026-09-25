# beanstalkd-rust Design

> Status: v0.4 (matches P0–P2 as shipped). Reference implementation: C beanstalkd commit `25085c5` (built into `.ref/` by `scripts/build-ref.sh`). See the [changelog](#10-changelog) for what changed since v0.1 and why.

## 1. Goals and Non-goals

**Goals**
1. **Full protocol compatibility**: every command, response, error code and edge-case behavior of the beanstalkd text protocol matches the reference; existing clients work unmodified.
2. **Reliability**: WAL persistence (P1) and Raft-based replicated high availability (P3).
3. **Operability**: TLS, authentication, Prometheus metrics, TOML configuration (P2).
4. **Performance**: throughput ≥ reference, able to use multiple cores.

**Non-goals**
- No pub/sub, message replay or streaming semantics (use Kafka / NATS for that).
- No compatibility with the reference binlog on-disk format (a separate migration tool if needed).

**Source of truth**: `.ref/beanstalkd/prot.c` and friends are authoritative; `doc/protocol.txt` is secondary. Where they disagree we follow the reference's *actual behavior* and record it in `docs/COMPAT.md`.

## 2. Architecture

```
            client (TCP)
                   │
┌──────────────────▼───────────────────────────────┐
│ server (tokio multi-thread runtime)              │
│  conn task × N: BytesMut + ServerCodec::decode    │
│     │  one command in flight per connection       │
│     │  mpsc<EngineMsg> (shared, unbounded)        │
│     ▼                     ▲ per-conn unbounded     │
│  engine actor (sole owner of Engine)              │
│     tokio task, or an OS thread with -b           │
│     per message: engine call, tick(now),          │
│       append journal to WAL, then release replies │
└──────────────────┬───────────────────────────────┘
                   │ JournalEntry / Recovery
            bstk-store (WAL) / raft (P3)
```

### Crates

| crate | Responsibility | Depends on |
|---|---|---|
| `bstk-proto` | Command parsing, response encoding, stats YAML, `ServerCodec`. No I/O, fuzzable (`crates/proto/fuzz`). | bytes, tokio-util |
| `bstk-engine` | Deterministic state machine: jobs, tubes, connection state, queues, timers | bstk-proto |
| `bstk-engine-oracle` | Dev-only frozen copy of the engine before the T6b index refactor; used by an equivalence proptest | bstk-proto |
| `bstk-server` | Binary `beanstalkd-rs`: listeners (plain / TLS), connection tasks, engine actor, CLI and config, auth, HTTP monitoring, system info | tokio, tokio-rustls, hyper, clap, toml, nix, getrandom, tracing |
| `bstk-compat` (`tests/compat`) | Differential harness and `.bt` case corpus against the reference | — |
| `bstk-bench` (`bench/`) | Load generator and benchmark matrix | tokio |
| `bstk-store` | Write-ahead log: segments, CRC records, reservation, compaction, replay | bstk-engine (types), crc32c, nix |
| `bstk-raft` | Raft replication (P3): log entry and RPC types, log / snapshot storage, state-machine wrapper, network | openraft, bstk-engine, postcard |

`clients/` holds real-client smoke tests (Python greenstalk, Go go-beanstalk).

## 3. Concurrency Model

- **Single-owner engine**: all state is owned by one tokio task, so there are no locks. This matches the reference's single-threaded semantics, which lets differential tests compare output byte for byte.
- **Multi-core I/O**: parsing, encoding and socket I/O are spread across tokio workers.
- **Command ordering**: each connection has at most one command in flight to the engine. Pipelined input stays buffered, undecoded, until the previous reply arrives. This guarantees "processed and answered in order".
- **Tick after every message**: the reference runs `prottick` on every event-loop pass, and some replies depend on it (e.g. a reserve that starts waiting inside the DEADLINE_SOON margin, COMPAT engine item 5). The actor therefore calls `tick(now)` after every engine call. This is cheap because `tick` returns at once when nothing is due (§4.3). If the actor ever drains several messages per wake-up, it must still tick between messages.
- **Back-pressure**: replies go out over unbounded per-connection channels and are dropped if the connection is gone, so a slow client never stalls the engine.
- **Actor placement**: without `-b` the actor is a tokio task (no blocking I/O; a thread hop per command cost 9–17% throughput). With `-b` it runs on a dedicated OS thread fed by a std channel with `recv_timeout`, so file writes and fsync never block a tokio worker.
- **Sharding**: not implemented. `reserve` spans multiple tubes and must pick the globally most urgent job, which sharding would break.

## 4. Engine

### 4.1 Determinism and injected time

```rust
pub type Nanos = u64; // monotonic time supplied by the caller (ns since server start)

impl Engine {
    pub fn new(now: Nanos, cfg: EngineConfig, sys: Box<dyn SysInfo>) -> Self;
    pub fn connect(&mut self, now: Nanos, conn: ConnId);
    pub fn disconnect(&mut self, now: Nanos, conn: ConnId, out: &mut Outbox);
    pub fn half_close(&mut self, now: Nanos, conn: ConnId, out: &mut Outbox);
    pub fn put_started(&mut self, now: Nanos, conn: ConnId, too_big: bool);
    pub fn put_rejected(&mut self, now: Nanos, conn: ConnId, why: PutRejection, out: &mut Outbox);
    pub fn handle(&mut self, now: Nanos, conn: ConnId, cmd: Command, out: &mut Outbox);
    pub fn tick(&mut self, now: Nanos, out: &mut Outbox);
    pub fn next_deadline(&self) -> Option<Nanos>;
    pub fn set_draining(&mut self, on: bool);
    pub fn recover(now: Nanos, cfg: EngineConfig, sys: Box<dyn SysInfo>, recovery: Recovery) -> Self;
    pub fn take_journal(&mut self, buf: &mut Vec<JournalEntry>);
    pub fn set_binlog_stats(&mut self, stats: BinlogStats);
    pub fn snapshot(&self, now: Nanos) -> Snapshot;                       // no side effects
    pub fn snapshot_limited(&self, now: Nanos, max_tubes: usize) -> Snapshot;
}
// Outbox = Vec<(ConnId, Response)>: one call may answer several connections.
// EngineConfig { max_job_size, binlog_max_size, journal }.
```

- The engine **never** reads the clock or uses randomness. pid, hostname, rusage etc. come from the server through `SysInfo`; uptime is `now - start`.
- The server supplies `now` as wall-clock time at startup plus monotonic elapsed time: monotonic within a run, and comparable across restarts, so persisted deadlines and `created_at` keep their meaning (as in the reference, which uses `gettimeofday`).
- The same `(now, message sequence)` always yields the same output. This is the prerequisite for Raft replication in P3.
- **Put side effects happen when the command line parses**, before the body arrives (COMPAT proto item 8): `put_started` counts `cmd-put` and, unless the job is too big, marks the producer and allocates the job id. The put then completes through `handle(Command::Put)` or `put_rejected`. `ConnState::pending_put` guarantees the side effects apply exactly once. Without `put_started`, `handle(Put)` and `put_rejected` apply them themselves.

### 4.2 Data structures

| Purpose | Structure | Notes |
|---|---|---|
| All jobs | `HashMap<JobId, JobRec>` | body is `bytes::Bytes` (zero-copy) |
| Tubes | slab `Vec<Option<TubeState>>` + free list, `HashMap<TubeName, TubeId>` | hot paths use integer `TubeId`s |
| Tube list order | `Ms<TubeId>` | faithful `ms.c` multiset: swap-remove, round-robin take |
| Tube ready queue | `BTreeSet<(pri, JobId)>` | same priority → lower id first |
| Tube delayed queue | `BTreeSet<(deadline, JobId)>` | |
| Tube buried queue | `VecDeque<JobId>` | FIFO; kick takes from the front. Removing from the middle is O(n) (known, P4) |
| Tube waiters | `Ms<ConnId>` | reference round-robin order (COMPAT engine item 4) |
| Connection state | `HashMap<ConnId, ConnState>` | used tube, watch `Ms<TubeId>`, waiting flag and deadline, reserved jobs (FIFO and by deadline), producer/worker flags, pending put |
| Connection wake times | `BTreeSet<(Nanos, ConnId)>` | earliest of reserve timeout, DEADLINE_SOON margin and TTR expiry, as in the reference's `conntickat` |
| Delayed-job heads | `BTreeSet<(Nanos, TubeId)>` | each tube's soonest delayed job |
| Paused tubes | `BTreeSet<(unpause_at, TubeId)>` | |
| Dispatchable tubes | `BTreeSet<TubeId>` | tubes with both waiters and ready jobs |

Tube creation and destruction follow the reference's refcounting (use + watch + jobs); `default` is never destroyed.

### 4.3 Timers

- `next_deadline()` is the minimum of the three deadline indexes: O(log n).
- `tick(now)` returns immediately when that minimum is after `now`. Otherwise it moves due delayed jobs to ready, clears expired pauses, and processes due connections, each at most once per call.
- `process_queue` visits only dispatchable tubes, preserving the reference's dispatch order.
- Every state change keeps the indexes in sync. The oracle proptest checks them against a from-scratch scan.

### 4.4 Semantic highlights (prot.c is authoritative; details in COMPAT.md)

- **reserve selection**: among the connection's watched, unpaused tubes, pick the ready job with the smallest `(pri, id)`.
- **DEADLINE_SOON**: inside the 1 s safety margin of a held job, reserve replies `DEADLINE_SOON`; a waiting connection gets it when the margin is reached. The check ignores tube pause (engine item 5).
- **Half-close**: a waiting reserve replies `TIMED_OUT`.
- **TTR of 0** is bumped to 1 s; `pause-tube x 0` pauses for 1 ns.
- **disconnect**: held jobs return to ready; the connection leaves every wait queue; tubes are destroyed in the reference's order.
- **kick** acts on the used tube: buried jobs if any, else delayed.
- **Stats** counters follow prot.c exactly, including counts taken before a command is rejected.

### 4.5 Journal and recovery (P1)

- With `EngineConfig::journal`, the engine records a `JournalEntry` at exactly the reference's binlog transitions: put (full record with tube and body), release with a delay, bury, kick / kick-job (updates), delete. Each update is a snapshot of the job record, so the last record wins on replay.
- `Engine::recover` rebuilds state from `Recovery` (live jobs in first-record order, the next id and the reference's replay tube order), applying the reference's replay rules: a job returns in its last journaled state, delayed jobs past their deadline become ready, replayed buried jobs count one more bury, cumulative counters start at zero. See COMPAT "Binlog".
- The binlog stats fields come from the store through `set_binlog_stats`.

## 5. Protocol (proto)

- `Command`: one variant per protocol command (put carries its body), plus `PauseTubeBadName` for a `pause-tube` whose name fails validation after the reference has already counted it.
- `Response`: one variant per reply; `encode` is byte-identical to the reference.
- `ServerCodec` (tokio-util `Decoder` + `Encoder`) produces `Frame`:
  - `Command(Command)`.
  - `PutStarted { too_big }`: a put line parsed; only when built with `ServerCodec::emit_put_started()` (the server always does).
  - `PutRejected(PutRejection)`: `JobTooBig` (body discarded), `TrailingGarbage` (BAD_FORMAT, body not consumed), `ExpectedCrlf`. These go to the engine because the reference has side effects before rejecting.
  - `Error(Response)`: errors with no engine side effects (BAD_FORMAT, UNKNOWN_COMMAND, ...), written directly by the connection.
- Line scanning, overlong-line discard in 224-byte windows, number parsing (`read_u32` / `read_u64` / `strtoul` for kick) and tube-name validation mirror prot.c; see COMPAT proto items.
- Stats YAML matches the reference format strings byte for byte.

## 6. Server

- One task per connection. It decodes from its own `BytesMut` with `ServerCodec` (not `Framed`), so it controls exactly when decoding happens.
- While a reply is pending it keeps reading only to detect EOF, never decodes, and stops reading once 64 KiB is buffered (COMPAT D3), so blocked pipelining clients hit TCP back-pressure.
- EOF: finish every complete buffered frame first. Half-close is sticky: once EOF is seen, `HalfClose` is re-sent after each dispatched command, so a reserve that only now starts waiting still gets `TIMED_OUT`.
- A drop guard sends `Disconnect` on every exit path.
- `SysInfo` via `nix` (uname, getrusage) and `getrandom`; no unsafe code.
- CLI: `-l addr`, `-p port`, `-z max_job_size` (parsed like the reference's `sscanf("%zu")`), `-V`, `-v`, and for the binlog `-b DIR`, `-f MS` (`-f0` = fsync every write), `-F` (never fsync), `-s BYTES` (segment size), with the reference's defaults (fsync at most every 50 ms) and ordering rules. `-u` is rejected.
- With `-b`, for every message the actor runs the engine call and `tick`, appends the drained journal to the WAL (fsync first with `-f0`), compacts, pushes binlog stats, and only then releases the replies, so no reply is sent for a change that has not reached the OS. A completed put first reserves WAL space; if that fails it completes as `PutRejection::OutOfMemory`. Any WAL error exits with status 20 (fail-stop). Startup replays the binlog before accepting connections; a locked directory exits with status 10.
- SIGUSR1 enters drain mode; SIGINT / SIGTERM exit gracefully. The soft `RLIMIT_NOFILE` is raised to the hard limit at startup (best effort).

### 6.1 Operability (P2)

- **Configuration**: `--config FILE` (TOML, unknown keys rejected) and `--check-config`; CLI flags override file values; `-l` / `-p` cannot be combined with `[[listener]]`. See `docs/beanstalkd-rs.example.toml`.
- **Startup order**: parse and validate config (including TLS files), bind every listener, start HTTP, replay the binlog, then accept.
- **Listeners**: any number of `[[listener]]` entries, each plaintext or TLS (tokio-rustls, aws-lc-rs) with `auth = "none" | "token" | "mtls"`. The connection task is generic over the stream type, so plaintext pays nothing. TLS `close_notify` or EOF maps onto the sticky half-close path; per-connection buffering is bounded by the 64 KiB cap plus one 16 KiB TLS record.
- **When the engine sees a connection**: plaintext at accept; TLS after the handshake; token auth only after `AUTHENTICATED`. Failed handshakes and unauthenticated connections never reach the engine or `stats`.
- **Token auth** (extension): the codec recognizes `auth <token>` only on token listeners (`ServerCodec::recognize_auth`, `Frame::Auth`). Before authentication only `auth` and `quit` are accepted; anything else gets `UNAUTHORIZED` and a close, with no engine message. Tokens are compared in constant time (padded to a fixed width, all tokens checked) and never logged.
- **Pending connections**: TLS connections still in the handshake (10 s limit) or awaiting auth (`auth.timeout`, default 10 s) count against `server.max_pending_connections` (default 1024, shared by all TLS listeners); beyond it new TLS connections are closed at accept.
- **HTTP** (off by default; binds 127.0.0.1 unless configured): `/healthz`, `/readyz` (503 until recovery completes), `/metrics` (Prometheus), `/admin` (read-only JSON). Snapshots come from the engine via `EngineMsg::Snapshot` → `Engine::snapshot_limited` (at most `max_tube_series + 1` tubes) and are cached for `http.snapshot_min_interval` (default 1 s). Up to 256 connections, 2 s header timeout, health endpoints never wait behind the 4-request limit on `/metrics` / `/admin`.
- **Server-side counters** outside `stats` (which stays byte-identical to the reference): pending connections, pending rejections, auth timeouts, auth failures (in `/metrics` and `/admin` `server_rs`).

## 7. Write-Ahead Log (P1, `bstk-store`)

- Segment files `binlog.N`, preallocated to `-s` rounded up to 4096 (at most 4 GiB), and a `lock` file held for the process lifetime.
- Records: length, CRC-32C, then a Put (job record, tube, body), Update (job record) or Delete (id). One positioned write per append.
- Replay: last record wins; first-record order; next id from all surviving records; the reference's tube-list order. A bad record in the last segment holding records truncates there with a warning; earlier corruption refuses to start (COMPAT D9).
- Space: a put reserves room for its put and delete records while one spare preallocated segment always remains for updates (COMPAT D6).
- Compaction: while (allocated − live) / live ≥ 2, move a live job out of the oldest segment; delete segments without live records. Crash-safe at every step.
- fsync: `fdatasync`, per the `-f` / `-F` policy.

## 8. Raft Replication (P3, `bstk-raft`)

Plan and rationale: `docs/PLAN.md` §6. Summary:

- **Replicated inputs**: every engine input is a log entry `Request { now, op }` (`Op::Conn { seq, input }`, `Tick`, `SetDraining`, `DropNode`). Each node applies committed entries to its own `Engine` via `Engine::apply_input` (engine call, then `tick(now)`), so all nodes hold the same state, including connections and reservations. Nothing is acknowledged before it is committed on a majority.
- **Time**: the leader stamps `now = max(its wall-anchored clock, last applied now)`; idle timers are driven by leader-proposed `Tick` entries.
- **Connections**: `ConnId = node_id << 48 | local number`. The owner (the node holding the socket) forwards inputs to the leader (`ForwardRequest`), one in flight per connection, resending unapplied inputs after a leader change; the state machine drops duplicates by `(conn, seq)`. Each node delivers the replies for its own connections from its own apply, so leader changes lose no replies and keep waiting reserves and reservations of surviving nodes.
- **Node loss**: a node without a leader for `node_timeout` closes its client sockets; the leader proposes `DropNode` for a node silent for `2 × node_timeout`, which disconnects its connections (reservations return to ready).
- **Storage**: segmented CRC-checked Raft log with `fdatasync` and group commit, vote file, snapshots of `Engine::export_state` (postcard) every `snapshot_every` entries. `-b` is not used in cluster mode.
- **Transport**: one cluster port, length-prefixed postcard frames (Raft RPCs and forwarding), mTLS by default with the node id bound to the certificate.

Configuration (`[cluster]`; absent = standalone, byte-identical to P2):

```toml
[cluster]
node_id = 1                          # 1..=65535, must appear in [[cluster.peer]]
listen = "10.0.0.1:11400"            # cluster port
data_dir = "/var/lib/beanstalkd-rs"  # raft log, vote, snapshots
node_timeout = "5s"
snapshot_every = 100000              # entries between snapshots
heartbeat = "50ms"
election_timeout = ["150ms", "300ms"]
insecure_plaintext = false           # true only for tests; otherwise [cluster.tls] is required

[cluster.tls]
cert = "node1.pem"                   # certificate subject/SAN must name this node (see T3)
key = "node1.key"
ca = "cluster-ca.pem"                # peers must present certificates from this CA

[[cluster.peer]]
id = 1
addr = "10.0.0.1:11400"
[[cluster.peer]]
id = 2
addr = "10.0.0.2:11400"
[[cluster.peer]]
id = 3
addr = "10.0.0.3:11400"
```

Rules: 3 or 5 peers (1 allowed for tests); `-b` / `binlog` with `[cluster]` is an error; `-z` must match on every node (checked when joining); `--cluster-init` bootstraps membership from `[[cluster.peer]]` once and is refused if `data_dir` already holds state.

## 8a. Later Phases (summary)

- **P4 Performance**: reduce the per-command cross-thread hop (ops per CPU-second is about 0.4× the reference), O(1) buried-job removal, cluster batching and reply computation only on owners, profiling-driven work.

## 9. Compatibility Strategy

- **Differential testing** (`tests/compat`): the same `.bt` script runs against the reference and `beanstalkd-rs`; replies are compared byte for byte after masking volatile fields (pid, uptime, rusage, server id, hostname, version, age, time-left, pause-time-left). 189 cases, part of `scripts/check.sh`.
- **Real clients** (`clients/run-smoke.sh`) and an **engine oracle proptest** complement it.
- Every known difference is recorded in `docs/COMPAT.md` with its reason.

## 10. Changelog

**v0.4 (P2 shipped)**
- TOML config, TLS / mTLS listeners, token auth extension (`Frame::Auth`, `AUTHENTICATED` / `UNAUTHORIZED`), HTTP monitoring (`Engine::snapshot`, `snapshot_limited`), pending-connection limits and auth timeout from the security review.

**v0.3 (P1 shipped)**
- Write-ahead log (`bstk-store`), engine journal and recovery, `Recovery::tube_order` (COMPAT Binlog item 4), `PutRejection::OutOfMemory`.
- Wall-anchored engine time; actor on an OS thread with `-b`; write-before-reply; fail-stop on WAL errors (COMPAT D5–D10).

**v0.2 (P0 shipped)**
- Put side effects moved to parse time: `Frame::PutRejected`, `Frame::PutStarted`, `Engine::put_rejected`, `Engine::put_started` (COMPAT proto item 8).
- `Command::PauseTubeBadName` (COMPAT proto item 12).
- `EngineConfig::binlog_max_size` (the reference reports 10 MiB even without a binlog).
- Deadline indexes, dispatchable-tube set and integer tube ids (T6b; see `docs/BENCH.md`).
- Server: manual decoding instead of `Framed`, 64 KiB read cap while waiting (COMPAT D3), sticky half-close, tick after every message.

**v0.1**: initial P0 design.
