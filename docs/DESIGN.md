# beanstalkd-rust Design

> Status: v0.5 (matches P0–P3 as shipped). Reference implementation: C beanstalkd commit `25085c5` (built into `.ref/` by `scripts/build-ref.sh`). See the [changelog](#10-changelog) for what changed since v0.1 and why.

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
| `bstk-server` | Binary `beanstalkd-rs`: listeners (plain / TLS), connection tasks, engine actor, CLI and config, auth, HTTP monitoring, system info | tokio, bstk-net, tokio-rustls, hyper, clap, toml, nix, getrandom, tracing |
| `bstk-compat` (`tests/compat`) | Differential harness and `.bt` case corpus against the reference | — |
| `bstk-bench` (`bench/`) | Load generator and benchmark matrix | tokio |
| `bstk-store` | Write-ahead log: segments, CRC records, reservation, compaction, replay | bstk-engine (types), crc32c, nix |
| `bstk-raft` | Raft replication (P3): log entry and RPC types, log / snapshot storage, state-machine wrapper, network | openraft, bstk-engine, bstk-net, postcard |
| `bstk-net` | `QuietTcp`, a TCP stream registered for read readiness only (P4-T5b); shared by the server's client path and the cluster transport so neither depends on the other | tokio |

`clients/` holds real-client smoke tests (Python greenstalk, Go go-beanstalk).

## 3. Concurrency Model

- **Single-owner engine**: all state is owned by one tokio task, so there are no locks. This matches the reference's single-threaded semantics, which lets differential tests compare output byte for byte.
- **Multi-core I/O**: parsing, encoding and socket I/O are spread across tokio workers.
- **Command ordering**: each connection has at most one command in flight to the engine. Pipelined input stays buffered, undecoded, until the previous reply arrives. This guarantees "processed and answered in order".
- **Tick after every message**: the reference runs `prottick` on every event-loop pass, and some replies depend on it (e.g. a reserve that starts waiting inside the DEADLINE_SOON margin, COMPAT engine item 5). The actor therefore calls `tick(now)` after every engine call. This is cheap because `tick` returns at once when nothing is due (§4.3). The actor does drain several messages per wake-up (below); it still ticks between messages.
- **Back-pressure**: replies go out over unbounded per-connection channels and are dropped if the connection is gone, so a slow client never stalls the engine.
- **Actor placement**: without `-b` the actor is a tokio task (no blocking I/O; a thread hop per command cost 9–17% throughput). With `-b` it runs on a dedicated OS thread fed by a std channel with `recv_timeout`, so file writes and fsync never block a tokio worker. This is unchanged by P4-T2: `server.threads` sizes the tokio worker pool the *task* form of the actor shares with connection tasks, not the dedicated OS thread `-b` gives it.
- **Worker threads (P4-T2)**: `server.threads` (TOML) / `--threads N` (long-only CLI flag; 1..=256) sets `tokio::runtime::Builder::worker_threads`. A P4-T1 spike found the tokio worker-thread count, not the hand-off design, is the lever for CPU efficiency: fewer workers means fewer cross-thread wake-ups per command, at the cost of the parallelism TLS and many connections can use. See docs/BENCH.md "P4-T2: worker threads" for the numbers behind this session's chosen defaults. Draining several already-queued messages per actor wake-up (`engine_actor::run_task`, `ACTOR_BATCH_LIMIT = 64`; still ticking after each, delivering once per batch) measured no additional throughput or efficiency effect on its own but is harmless and is kept. The default (`server.threads` unset) is chosen by measurement, separately for standalone and cluster mode (`config::{DEFAULT_THREADS_STANDALONE, DEFAULT_THREADS_CLUSTER}`), by this rule:

  > Standalone default = the smallest thread count that meets PLAN §7.5 (efficiency ≥ 0.8x the reference at 10 and 100 connections, throughput ≥ 1.0x every non-pipelined cell) **and** keeps TLS and `-b` throughput ≥ 0.95x of the P3 build **and** keeps burst-scenario plaintext p99 within 2x of the no-burst p99. Cluster mode may use a different default: the best cluster throughput not below P3's numbers (135k via leader / 108k via follower at 100×16, allowing for machine noise measured in the same session).

  Standalone default: **1** (the only thread count that meets the standalone efficiency and throughput gates on this machine; 2 and 4 fail efficiency at every cell, 0.50x–0.78x), **except 2 when any listener is TLS (including mTLS) or the binlog (`-b`) is enabled** (P4-T6b: the P4-T6 matrix and a bisect showed 1 worker saturating at about 108% CPU and landing 6–15% below the P3 build for those modes; 2 workers is within ±5% of P3 at lower CPU, and 4 buys nothing; see docs/BENCH.md "P4-T6b: thread default per mode"). Plaintext without `-b` stays at 1 for efficiency; users who want its extra throughput set `--threads`. The rule is `config::ResolvedConfig::effective_threads`. Cluster default: **2** (best of 1/2/4; all three already beat the P3 baseline measured in the same session). See docs/BENCH.md "P4-T2: worker threads" for the full tables and the trade-offs of the standalone default (pipelined throughput, burst-scenario latency, one consumer-heavy cell's p99). At 1 worker and no `-b`, the actor task and every connection task share that single tokio worker thread; this is safe because nothing on the plaintext path uses `spawn_blocking` / `block_in_place` (checked: none in `crates/server/src`), so there is no separate blocking pool to starve.
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
| All jobs | `HashMap<JobId, Box<JobRec>>` | boxed so empty table slots cost a pointer, not a record; the body is copied out of the connection's read buffer at put (a shared view would pin the whole buffer per live job; see BENCH P4-T4) |
| Tubes | slab `Vec<Option<TubeState>>` + free list, `HashMap<TubeName, TubeId>` | hot paths use integer `TubeId`s |
| Tube list order | `Ms<TubeId>` | faithful `ms.c` multiset: swap-remove, round-robin take |
| Tube ready queue | `BTreeSet<(pri, JobId)>` | same priority → lower id first |
| Tube delayed queue | `BTreeSet<(deadline, JobId)>` | |
| Tube buried queue | `BTreeMap<u64, JobId>` | keyed by `Engine::next_list_seq` at insertion (P4-T3); FIFO iteration order, O(log n) removal of any job via `JobRec::list_seq` |
| Tube waiters | `Ms<ConnId>` | reference round-robin order (COMPAT engine item 4) |
| Connection state | `HashMap<ConnId, ConnState>` | used tube, watch `Ms<TubeId>`, waiting flag and deadline, reserved jobs (`BTreeMap<u64, JobId>`, same `next_list_seq` scheme as buried, plus by-deadline), producer/worker flags, pending put |
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

### 4.6 Scale tests (`scale_tests.rs`, P4-T3)

- The O(log n) buried / reservation removals are guarded by tests at 100k and 1M jobs: a client controls both counts (bury or reserve as many jobs as it likes, then delete, kick or release them in any order), so a scan hiding behind a small n would let a quadratic regression pass unnoticed.
- Every test inserts in a shuffled order that differs from job id order, so a bug that silently sorts by id still fails the order check. Buried order is checked with the real `peek-buried` after every removal (O(log n) each); reservation order has no O(log n) peek, so `Engine::t_conn_reserved` is read at a few checkpoints only.
- The non-`#[ignore]`d tests run at 100k (a quadratic scan would take tens of seconds there). The `#[ignore]`d ones report 100k and 1M timings but assert only an absolute bound on the 1M one: at 100k's sub-100 ms scale a single scheduling hiccup can swing a 100k-versus-1M ratio several-fold, while a quadratic regression misses the 1M bound by orders of magnitude. Timings take the minimum of several runs, since jitter only adds delay. Run them with `cargo test --release -p bstk-engine --ignored -- --test-threads=1`.

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
- A drop guard sends `Disconnect` on every exit path, before the socket closes. Any input a client sends after seeing the close, on any connection, therefore enters the engine channel (in cluster mode, the node's forward queue) after the `Disconnect`, and both are FIFO, so it is processed after the disconnect, as in the single-threaded reference. Closing the socket first left a window in which a `stats` sent after seeing the close could still count the connection (seen in the cluster suites, whose nodes run several worker threads; P5-T1b).
- `SysInfo` via `nix` (uname, getrusage) and `getrandom`; no unsafe code.
- CLI: `-l addr`, `-p port`, `-z max_job_size` (parsed like the reference's `sscanf("%zu")`), `-V`, `-v`, and for the binlog `-b DIR`, `-f MS` (`-f0` = fsync every write), `-F` (never fsync), `-s BYTES` (segment size), with the reference's defaults (fsync at most every 50 ms) and ordering rules. `-u` is rejected. `--threads N` (long-only, P4-T2; 1..=256) sets the tokio worker-thread count; see §3.
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

### 6.2 Monitoring reference (`/metrics`, `/admin`; `metrics.rs`)

Both renderers are pure functions of an engine snapshot, so every value they print is exactly what `stats` / `stats-tube` report at the same instant.

#### Prometheus metrics

This mapping is public API for operators; do not rename metrics
lightly. Counters end in `_total`; everything else is a gauge.

##### Server (`stats`)

| stats key | metric | type |
|---|---|---|
| `current-jobs-urgent` | `beanstalkd_current_jobs{state="urgent"}` | gauge |
| `current-jobs-ready` | `beanstalkd_current_jobs{state="ready"}` | gauge |
| `current-jobs-reserved` | `beanstalkd_current_jobs{state="reserved"}` | gauge |
| `current-jobs-delayed` | `beanstalkd_current_jobs{state="delayed"}` | gauge |
| `current-jobs-buried` | `beanstalkd_current_jobs{state="buried"}` | gauge |
| `cmd-<name>` (all 22) | `beanstalkd_commands_total{cmd="<name>"}` | counter |
| `job-timeouts` | `beanstalkd_job_timeouts_total` | counter |
| `total-jobs` | `beanstalkd_jobs_total` | counter |
| `max-job-size` | `beanstalkd_max_job_size_bytes` | gauge |
| `current-tubes` | `beanstalkd_current_tubes` | gauge |
| `current-connections` | `beanstalkd_current_connections` | gauge |
| `current-producers` | `beanstalkd_current_producers` | gauge |
| `current-workers` | `beanstalkd_current_workers` | gauge |
| `current-waiting` | `beanstalkd_current_waiting` | gauge |
| `total-connections` | `beanstalkd_connections_total` | counter |
| `version` | `beanstalkd_build_info{version="<version>"}` (always 1) | gauge |
| `rusage-utime` | `beanstalkd_cpu_seconds_total{mode="user"}` | counter |
| `rusage-stime` | `beanstalkd_cpu_seconds_total{mode="system"}` | counter |
| `uptime` | `beanstalkd_uptime_seconds` | gauge |
| `binlog-oldest-index` | `beanstalkd_binlog_oldest_index` | gauge |
| `binlog-current-index` | `beanstalkd_binlog_current_index` | gauge |
| `binlog-records-migrated` | `beanstalkd_binlog_records_migrated_total` | counter |
| `binlog-records-written` | `beanstalkd_binlog_records_written_total` | counter |
| `binlog-max-size` | `beanstalkd_binlog_max_size_bytes` | gauge |
| `draining` | `beanstalkd_draining` (0 or 1) | gauge |

`<name>` in `cmd` is the stats key without its `cmd-` prefix, i.e. the
protocol command name: `put`, `peek`, `peek-ready`, `peek-delayed`,
`peek-buried`, `reserve`, `reserve-with-timeout`, `delete`, `release`,
`use`, `watch`, `ignore`, `bury`, `kick`, `touch`, `stats`, `stats-job`,
`stats-tube`, `list-tubes`, `list-tube-used`, `list-tubes-watched`,
`pause-tube`. Note that `urgent` jobs are a subset of `ready` jobs (as in
`stats`), so summing `beanstalkd_current_jobs` over `state` double-counts.

Not exported (identity rather than measurements; see `/admin`): `pid`,
`id`, `hostname`, `os`, `platform`.

The job, connection and tube gauges are named `beanstalkd_current_*`
after their stats keys, which also keeps every gauge name distinct from
every counter's base name (`beanstalkd_jobs_total` is a counter whose
OpenMetrics family would be `beanstalkd_jobs`).

##### Per tube (`stats-tube`), label `tube="<name>"`

| stats-tube key | metric | type |
|---|---|---|
| `current-jobs-urgent` | `beanstalkd_tube_current_jobs{state="urgent"}` | gauge |
| `current-jobs-ready` | `beanstalkd_tube_current_jobs{state="ready"}` | gauge |
| `current-jobs-reserved` | `beanstalkd_tube_current_jobs{state="reserved"}` | gauge |
| `current-jobs-delayed` | `beanstalkd_tube_current_jobs{state="delayed"}` | gauge |
| `current-jobs-buried` | `beanstalkd_tube_current_jobs{state="buried"}` | gauge |
| `total-jobs` | `beanstalkd_tube_jobs_total` | counter |
| `current-using` | `beanstalkd_tube_current_using` | gauge |
| `current-watching` | `beanstalkd_tube_current_watching` | gauge |
| `current-waiting` | `beanstalkd_tube_current_waiting` | gauge |
| `cmd-delete` | `beanstalkd_tube_commands_total{cmd="delete"}` | counter |
| `cmd-pause-tube` | `beanstalkd_tube_commands_total{cmd="pause-tube"}` | counter |
| `pause` | `beanstalkd_tube_pause_seconds` | gauge |
| `pause-time-left` | `beanstalkd_tube_pause_time_left_seconds` | gauge |

Tube counters restart from zero when a tube is destroyed (no users,
watchers or jobs) and later recreated; Prometheus treats that as a
counter reset.

##### Cardinality cap

Per-tube series are emitted for at most `max_tube_series` tubes: the
first ones in `list-tubes` order. Two gauges describe the cap:

| metric | meaning |
|---|---|
| `beanstalkd_tube_series_limit` | the configured `max_tube_series` |
| `beanstalkd_tube_series_truncated` | 1 if some tubes were left out, else 0 |

`beanstalkd_current_tubes` always reports the full tube count.

The snapshot may already hold only some of the tubes (the HTTP listener
asks the engine for `max_tube_series + 1` of them, see
`Engine::snapshot_limited`): "truncated" means that the snapshot has more
tubes than the limit; the one extra tube is what shows this.

##### Server-side (beanstalkd-rs only, not in `stats`)

| metric | type | meaning |
|---|---|---|
| `beanstalkd_pending_connections` | gauge | TLS connections in their handshake or awaiting token authentication |
| `beanstalkd_pending_rejected_total` | counter | TLS connections closed at accept because `server.max_pending_connections` was reached |
| `beanstalkd_auth_timeouts_total` | counter | token connections closed for not authenticating within `auth.timeout` |
| `beanstalkd_auth_failures_total` | counter | wrong tokens and commands sent before authentication |

#### Admin JSON

`{"server": {...}, "server_rs": {...}, "tube_limit": N,
"tubes_truncated": bool, "tubes": [{...}, ...]}` where `server` holds
every `stats` key and each `tubes` entry every `stats-tube` key, with the
reference's key names in the reference's order. Numbers are JSON numbers
(`rusage-utime` / `rusage-stime` as seconds with six decimals), `draining`
is a boolean, and `version`, `id`, `hostname`, `os`, `platform` and the
tube `name` are strings. Tubes appear in `list-tubes` order, at most
`tube_limit` (`http.max_tube_series`) of them; `tubes_truncated` tells
whether some were left out. `server_rs` holds the server-side counters
above as `pending-connections`, `pending-rejected`, `auth-timeouts` and
`auth-failures` (cumulative ones without the `_total` suffix, like the
`stats` keys).

#### Cluster metrics

Cluster figures, added to `/metrics` and `/admin` in cluster mode only (`metrics::ClusterStats`).

| metric | type | meaning |
|---|---|---|
| `beanstalkd_cluster_node_id` | gauge | this node's id |
| `beanstalkd_cluster_role{role}` | gauge | 1 for the current role (`leader`, `follower`, `candidate`, `learner`, `shutdown`), else 0 |
| `beanstalkd_cluster_term` | gauge | current Raft term |
| `beanstalkd_cluster_leader_id` | gauge | leader known to this node (0: none) |
| `beanstalkd_cluster_commit_index` | gauge | last commit index this node learned |
| `beanstalkd_cluster_applied_index` | gauge | last log index applied here |
| `beanstalkd_cluster_last_log_index` | gauge | last log index stored here |
| `beanstalkd_cluster_replication_lag{peer}` | gauge | leader only: entries a peer is missing |
| `beanstalkd_cluster_log_bytes` | gauge | size of the log segments |
| `beanstalkd_cluster_log_segments` | gauge | number of log segments |
| `beanstalkd_cluster_snapshot_index` | gauge | last log index in the snapshot |
| `beanstalkd_cluster_snapshot_bytes` | gauge | size of the stored snapshot |
| `beanstalkd_cluster_forward_queue` | gauge | this node's inputs not yet applied |
| `beanstalkd_cluster_forward_queue_bytes` | gauge | approximate size of those inputs |
| `beanstalkd_cluster_forward_queue_full` | gauge | 1 while the forward queue is at its bound |
| `beanstalkd_cluster_refused_connections_total` | counter | client connections closed at accept (cut off, shutting down, or queue full) |
| `beanstalkd_cluster_rejected_puts_total` | counter | puts answered `OUT_OF_MEMORY` because the forward queue was full |
| `beanstalkd_cluster_resent_inputs_total` | counter | inputs sent to the leader again (duplicates the state machine discards) |
| `beanstalkd_cluster_forward_rewinds_total{cause}` | counter | resends of the forward queue, by cause (`view`, `error`, `stall`, `dropped`) |
| `beanstalkd_cluster_drop_node_proposals_total` | counter | `DropNode` proposals made by this node as leader for silent nodes |
| `beanstalkd_cluster_ready` | gauge | 1 when `/readyz` is 200 |
| `beanstalkd_cluster_isolated` | gauge | 1 while client sockets are closed for lack of a leader |
| `beanstalkd_cluster_rejoining` | gauge | 1 while the node is in rejoin mode (no votes, no clients) |
| `beanstalkd_cluster_votes_refused_total` | counter | vote requests refused in rejoin mode |
| `beanstalkd_cluster_next_local_conn` | gauge | local number of the next client connection (-1 before clients are accepted) |

Absent indexes are exported as -1. `/admin` has the same values under
`"cluster"` (absent ones as `null`).

## 7. Write-Ahead Log (P1, `bstk-store`)

- Segment files `binlog.N`, preallocated to `-s` rounded up to 4096 (at most 4 GiB), and a `lock` file held for the process lifetime.
- Records: length, CRC-32C, then a Put (job record, tube, body), Update (job record) or Delete (id). One positioned write per append.
- Replay: last record wins; first-record order; next id from all surviving records; the reference's tube-list order. A bad record in the last segment holding records truncates there with a warning; earlier corruption refuses to start (COMPAT D9).
- Space: a put reserves room for its put and delete records while one spare preallocated segment always remains for updates (COMPAT D6).
- Compaction: while (allocated − live) / live ≥ 2, move a live job out of the oldest segment; delete segments without live records. Crash-safe at every step.
- fsync: `fdatasync`, per the `-f` / `-F` policy.

### 7.1 Job index and space accounting (`wal.rs`)

- **Segments**: `segs` holds every segment file in index order. `segs[cur]` is the current (write) segment; later ones are preallocated spares without records; earlier ones are closed (truncated to their records). Every open starts a new current segment.
- **Job index (memory use)**: for every live job the store keeps its latest `JobRecord` (needed to write a compaction move), the location of its latest Put record and the bytes it uses: about 120 bytes per job plus hash map overhead. Tubes and bodies stay on disk: a compaction move re-reads the job's Put with a positioned read and re-stamps it with the latest `JobRecord`. Each segment also keeps a queue of (job, offset) entries for its Puts (16 bytes each, cleaned lazily) and the count of live jobs whose latest Put it holds ("anchors").
- **Accounting terms**:
  - `reserved`: one Delete record per live job and per reserved-but-not-yet-written put, plus the Put records of reserved puts.
  - `avail`: unwritten bytes of the current segment plus the capacity of the preallocated spares.
  - `slack`: worst-case bytes lost to rollover fragmentation (records never straddle segments): one Delete record per future rollover plus the pending Put records.
- **Reservation**: `reserve_put` (and a compaction move) succeeds when `reserved + n + slack + one segment's capacity <= avail`, allocating new preallocated segments until it holds. The extra segment is the spare that unreserved Update records and fragmentation consume. If a segment cannot be allocated (disk full, or the test-only size limit) `reserve_put` returns false.
- A put reservation lasts until the end of the next `append`: that call's Put entries consume the pending reservations in order, and leftovers (the put was never journaled, e.g. the engine rejected it) are released.
- `append` never checks reservations; when preallocated space runs out it allocates a segment on the spot and fails only if that fails. A Put larger than a whole segment cannot be reserved; if one is appended anyway it is written at the start of a fresh segment, which grows past its preallocated size.

### 7.2 Compaction and garbage collection (`wal.rs`)

`maintain` computes `ratio = (allocated - live) / live` (integer division) where `allocated` is the bytes of all segment files and `live` is the bytes of live jobs' records (latest Put plus the Updates after it) plus `reserved`. Like the reference it performs `ratio - 1` moves when `ratio >= 2`. A move takes the first live job whose latest Put is in the oldest segment that holds any (and is at least two segments before the current one), re-reads that Put, verifies its CRC, re-stamps it with the job's latest `JobRecord` and writes it to the current segment. Moves stop early if space for them cannot be secured.

GC deletes segments from the head of the list while they are before the current segment and anchor no live job. Before the first unlink it writes out buffered moves and (unless `SyncPolicy::Never`) fsyncs the current segment; with `SyncPolicy::Always` it also fsyncs the directory afterwards.

**Why every on-disk state replays to the same live set**:
- A move is a full Put carrying the latest state, written after all of the job's earlier records, so "last record wins" gives the same state whether or not the old copy still exists (a crash between the move and the unlink leaves the job in two files).
- Only a prefix of the segment list is ever deleted, and only segments without the latest Put of any live job. A live job's latest Put and everything after it survive; surviving records before it are harmless (an Update for an unknown job is ignored; an older Put is overridden). A deleted job's Delete record comes after all its other records, so if any of them survives the Delete survives too: a deleted job is never resurrected.
- A torn move at the tail is dropped by replay, and the old copy is still there because the unlink only happens after the move was written (and fsynced unless the policy is `Never`).

### 7.3 Replay rules (`replay.rs`)

Segments `binlog.N` (N = decimal digits without leading zeros) are read in increasing N. Within a segment, records are read from offset 16 until the first position that is not a valid record:

- a zero length field (or fewer than 8 bytes left, all zero) is the *clean end* if every byte from there to the end of the file is zero;
- anything else (non-zero bytes after the end marker, a length running past the end of the file, a CRC mismatch) makes the segment *torn* at that offset, even if valid-looking records follow it.

A torn segment is accepted only if no later segment contains a valid record, i.e. the damage is in the last segment that has data (later segments can be preallocated spares). `open` then truncates it at the torn offset, fsyncs it and logs a warning with the segment and offset: with `-f N` / `-F`, unsynced writes may reach the disk out of order after a power loss, and losing that unsynced tail is the accepted cost of those modes (like the reference, which warns and continues). Always `Corrupt`: a torn segment followed by a later segment with valid records, a header with the wrong magic or version, and a record whose CRC matches but whose payload is malformed.

A file shorter than the 16-byte header, or whose header is all zero, is a segment whose creation was interrupted: it has no records (non-zero bytes past the header count as torn at offset 16).

Records are applied in file order: the last record of a job wins; a Put for an unknown job creates it; a Put for a known job (a compaction move) replaces its record, tube and body; an Update for an unknown job is ignored (its Put was in a garbage-collected segment, so the job was moved or deleted later); a Delete removes the job. Every record's id counts toward `next_id`, even ignored ones.

`tube_order` mirrors the reference's tube list after replay (excluding `default`): a Put that creates a job appends its tube if no live job uses it yet, and a Delete that removes a tube's last live job swap-removes the tube (`ms_remove`). A move, and an Update or Delete of an unknown job, leave the list alone. After compaction has moved jobs and deleted old segments the surviving records differ from the reference's, so this order (like the job order) may differ from what the reference would produce.

## 8. Raft Replication (P3, `bstk-raft`)

Plan and rationale: `docs/PLAN.md` §6. Summary:

- **Replicated inputs**: every engine input is a log entry `Request { now, op }` (`Op::Conn { seq, input }`, `Tick`, `SetDraining`, `DropNode { node, up_to_local }`, and since P3-FD `Batch(Vec<(seq, input)>)`). Each node applies committed entries to its own `Engine` via `Engine::apply_input` (engine call, then `tick(now)`), so all nodes hold the same state, including connections and reservations. Nothing is acknowledged before it is committed on a majority.
- **Batched proposals (P3-FD)**: the leader proposes connection inputs as `Op::Batch` entries: its cluster actor (its own connections) and its forward handler (other owners' forwards) hand their items, in order, to one proposer task, which turns everything queued into one entry of at most 1,024 items and 1 MiB of bodies (a single larger put alone), with at most one batch outstanding (proposed and not yet applied on the leader, or refused; it stops counting after 500 ms or at a view change); further items wait and go into the next batch. (Without that bound nearly every entry holds a single input again: 35k instead of about 130k ops/s at 100 connections via the leader.) The state machine applies the items of a batch in order, each exactly as an `Op::Conn` with the entry's `now` (same dedup rules, engine call then `tick(now)`), so resends and per-owner order work as before. `Op::Conn` stays valid, and `Batch` is the last `Op` variant, so older logs replay unchanged. Decoding bounds a batch to 1,024 items (`wire::MAX_BATCH_ITEMS`, equal to `MAX_PROPOSAL_ITEMS`) in AppendEntries and control requests; a control request carrying a batch is refused.
- **Time**: the leader stamps `now = max(its wall-anchored clock, last applied now)`; idle timers are driven by leader-proposed `Tick` entries.
- **Connections**: `ConnId = node_id << 48 | local number`. Local numbers come from durably reserved blocks: before handing out `[a, a + 65536)` the node writes `a + 65536` to `data_dir/conn-ids` (temporary file, fsync, rename, directory fsync), and a new process starts at `max(persisted, highest local number in the replicated state + 1, unix_seconds << 16)`, so no number is reused even if a process crashes before any of its `Connect`s is committed. The time floor covers a wiped node, whose file is gone: a process that took its floor at `t0` starts at or above `⌊t0⌋ << 16`, so it has handed out numbers below `t << 16` by time `t`, and a process without the file takes its floor at least 1 s after it started (it waits if needed), hence in a later second than any in which the lost process handed out numbers, and starts above them. Assumptions: the node consumes fewer than 65,536 local numbers per second on average (connections, plus one block per restart), and the wall clock is not set back across a wipe; the floor fits the 48 bits of a local number until 2106 (a start past that is refused). The owner (the node holding the socket) forwards inputs to the leader (`ForwardRequest`), one in flight per connection; the state machine drops duplicates by `(conn, seq)`. Each node delivers the replies for its own connections from its own apply, so leader changes lose no replies and keep waiting reserves and reservations of surviving nodes.
- **Forward queue** (per node, all inputs in order): unapplied inputs are resent only when there is a reason to think they were lost: a leader or term change, a `NotLeader` answer or transport error, or an input applied while one sent before it in the same pass was not. The last-resort stall timer (the oldest input unapplied since it was sent) starts at 2 s and doubles up to 30 s after each stall resend, so an overloaded but working leader is not flooded with duplicates (`beanstalkd_cluster_resent_inputs_total`, `beanstalkd_cluster_forward_rewinds_total{cause}`). The queue is bounded (100,000 inputs, 128 MiB): at the bound new client connections are closed at accept and a put is answered `OUT_OF_MEMORY` (a replicated `PutRejected`, as a reference server does when its binlog is full).
- **Node loss**: every node tracks when the leader last accepted a forward from it; with nothing to forward it pings the leader (an empty forward) every `node_timeout / 4` (50 ms to 1 s). A node closes its client sockets, and closes new ones at accept without creating any state for them, when that is older than `node_timeout`, or when no leader is known (or it leads without a quorum) for `node_timeout`. The leader counts a peer alive only if the peer both answers its replication and sends it forwards or pings, so a one-way partition is detected from both sides; after `2 × node_timeout` without either it proposes `DropNode { node, up_to_local }`, which disconnects that node's connections with a local number up to `up_to_local` (reservations return to ready). The bound is the node's highest local number the proposer saw, and a restarted node first proposes its own bounded `DropNode` and then numbers new connections above that bound, so a `DropNode` that commits late never closes connections accepted after a restart.
- **Startup probes**: a node starts its cluster listener before Raft; until Raft runs the listener answers only *status probes* (`RpcRequest::Status`, wire version 3): the persisted vote, last log id, commit hint and whether there is any Raft state, read from the log store, and only for authenticated peers (same hello / mTLS rules as every request). Raft RPCs, forwards and control requests are refused until then, a Vote RPC after the vote gate (so it is counted).
- **Bootstrap**: every initial node is started with `--cluster-init` and the same `[[cluster.peer]]` list, in any order. `--cluster-init` is refused if `data_dir` holds state or a rejoin marker. Otherwise the node probes the other nodes (each round on that round's answers) and initializes the membership once a majority of the cluster (`m = ⌊n/2⌋ + 1`) of *other* nodes answered with no state at all, or every other node answered and none is *established* (a committed vote, a log entry beyond the bootstrap membership at index 0, or a commit hint); as soon as an answer is established it rejoins instead (with a warning: a wiped node started with `--cluster-init` by mistake); otherwise it waits (logged). The second rule makes the race converge: openraft's `initialize` starts an election at once, so a node that initialized first shows state (index 0 and its own uncommitted vote) but is not established; without the rule the others would all rejoin and nobody could elect it. It needs every other node because the voters of a leader hold its vote uncommitted until its first append, which looks the same; only the leader itself shows a committed vote and index 1. Safety of the first rule: a node that ever led was elected by `m` nodes, at least `m - 1` of them other than this one, each with a persisted vote; `m` answers from the `n - 1` others include one of them since `(m - 1) + m > n - 1`. Consequence: bootstrapping needs `m` other nodes up and matching (for 3 nodes, all three); with fewer it waits.
- **Rejoin**: a node started with an empty `data_dir` without `--cluster-init` (a new disk, or a node that lost its data), with the rejoin marker, or sent here by the bootstrap rule, *rejoins*: it may have acknowledged entries and granted votes it no longer remembers. It writes `data_dir/rejoin` durably, then probes the other nodes (with backoff) until `m` of them answered, takes the highest vote among the answers and its own persisted one (openraft's order, never lower than its own; if the top is incomparable, or is a committed vote naming this node, it asks again later), persists it with the log store's `save_vote`, and only then starts Raft, which loads it, with elections disabled and its vote gate closed, serving no clients. It asks the leader to propose `DropNode(self)` and, once it has applied that entry (an index learned from a leader after it started, so everything committed before is in its log), removes the marker durably, re-enables elections and opens the gate. A crash in between keeps the marker, so the node rejoins (and probes) again. A single-node cluster cannot rejoin (an error). Limits: at most a minority may rejoin at once; a rejoining node waits while fewer than `m` other nodes answer.
- **Why rejoin is safe.** openraft 0.9 facts (checked in its source): a follower accepts an AppendEntries or snapshot only if the leader's vote is `>=` its own (`engine/handler/vote_handler/mod.rs:94-115`, called first by `Engine::append_entries`, `engine/engine_impl.rs:456`), so a follower whose persisted vote equals the leader's committed vote accepts it and one with a higher vote rejects it, and the rejected leader learns the higher vote and steps down; the vote is loaded from `read_vote` at startup (`storage/helper.rs:69`); a granted vote is saved before the grant is answered (`core/raft_core.rs:1648` runs before the queued response). Without the `single-term-leader` feature, votes are ordered by leader id `(term, node_id)` and then `committed` (`vote/vote.rs`, `vote/leader_id/leader_id_adv.rs`), so two leaders may exist in one term and the argument is about votes, not terms. The argument: let `L` be any leader whose entries the rejoining node `R` acknowledged before it lost its data, with vote `V`. `L` was elected by `m` nodes that persisted a vote `>= V` before `L` sent any entry, hence before `R`'s acknowledgement, the wipe and the probe; at least `m - 1` of them are other than `R`, and votes never decrease. `R` hears from `m` of the `n - 1` others, and `(m - 1) + m > n - 1`, so one answer is `>= V`, and so is the adopted vote: `R` rejects every leader below any leader it ever followed, in particular one that lost the election race to a newer leader (finding 6: such a stale leader and `R` formed a majority and overwrote entries committed in the newer term). (Commit acknowledgements do not give this: a leader may finish a commit after the probe by counting `R`'s acknowledgement from before the wipe.) The leader of the adopted vote itself (a committed vote) is accepted, since its vote equals `R`'s; from then on openraft stores each accepted leader's vote, so `R` does not grant a vote in a term where it already accepted another leader. Votes `R` granted before the wipe are covered by the vote gate: `R` votes again only after it has caught up through a leader elected without it, whose election quorum (`m` of the `n - 1` others) intersects the `m - 1` others holding any entry committed with `R`'s help.
- **openraft panic at `core/raft_core.rs:761`** (seen with finding 6): when committed entries diverge, a node can learn a commit `LogId` that is greater (a newer leader id) but has a lower index than its own; openraft's `update_committed` compares whole `LogId`s (`raft_state/mod.rs:275`, leader id first, `log_id/mod.rs:23`), so it emits `Commit { already_committed, upto }` with `upto.index < already_committed.index`, and `apply_to_state_machine` indexes an empty entry list (`core/raft_core.rs:1700` → 761). It is a symptom of the divergence, not a separate bug.
- **Storage**: segmented CRC-checked Raft log with `fdatasync` and group commit, vote file, snapshots of the engine state (postcard, streamed to and from files, see "Streamed snapshots (P4-T5c)") every `snapshot_every` entries. `-b` is not used in cluster mode.
  - *Group commit (P3-FD)*: `append` writes its records and returns; its openraft callback goes to a flush worker (tokio's blocking pool), which `fdatasync`s every segment written since its last sync once and then invokes every callback it covered, in order, so a callback always follows the durability of its entries, and appends queued during a sync share the next one. A failed sync fails its callbacks and every later append. `truncate`, `purge` and `save_vote` first wait until the worker has finished everything queued (so nothing is acknowledged after it was cut off and no removed data is synced later), then run synchronously and are durable when they return. A rolled-over segment is synced inline before the next is created, so only the last segment can have an unsynced tail, which open cuts at the first torn record as before.
  - *openraft 0.9 limit*: the Raft core awaits each append's callback before its next command (`RaftCore::append_to_log`, `core/raft_core.rs:713-731`, "a temp wrapper to make non-blocking append_to_log a blocking"), and it sends heartbeats itself. So the core still waits for every log sync (now without blocking a runtime thread), at most one core append is outstanding, and group commit across appends does not happen in practice; batching happens before the core instead (`Op::Batch`). A sync stall (seen up to about 800 ms on macOS APFS) therefore still delays heartbeats, hence the longer timing defaults below (since P3-FD: heartbeat 100 ms, election timeout 500 to 700 ms). openraft 0.9 starts an election only after `election_timeout_max` (the leader lease) plus the node's random election timeout (drawn once per process from `[min, max]`) without hearing from the leader (`engine/engine_config.rs:54-56`, `core/raft_core.rs:1475-1494`), so the defaults tolerate 1.0 to 1.2 s without a heartbeat (was 450 to 600 ms), and a failover takes about 1.3 to 1.5 s on one machine (the `leader_kill` test bound is 2 s; 500 ms to 1 s would allow up to 2 s before the election alone).
- **Transport**: one cluster port, length-prefixed postcard frames (Raft RPCs and forwarding), mTLS by default with the node id bound to the certificate.
  - *Dialer* (`bstk_raft::client`): one multiplexed connection per peer, shared by replication, votes and forwarding. A request that times out (or whose call openraft drops: it bounds every AppendEntries by `heartbeat_interval`) fails alone; its late answer is discarded. The connection is closed only when stalled: a request goes unanswered while requests have been outstanding with no response at all for `max(stall_timeout = 1 s, 3 × timeout)`. AppendEntries batches of more than one entry are limited to `append_budget` (1 MiB) encoded bytes (`PayloadTooLarge` with a scaled entries hint makes openraft split); a single entry may use the whole frame, so the heartbeat interval must allow the largest job body to cross the link.
  - *Listener* (`bstk_raft::listener`): connections in the TLS handshake or hello have their own budget (16 at once, `max(4, 2 × peers)` per source address, 2 s to finish); an authenticated connection holds its peer's single slot, and a newer authenticated hello from the same peer closes the older connection. Rejections are logged at most about once a second. A rejected hello gets a generic reason (`hello rejected`, `unsupported protocol version`, or the `-z` mismatch); details are logged locally only, and peer-supplied text is logged escaped and truncated.
  - *Vote gate*: `ListenerConfig::vote_gate` (a `VoteGate`); while closed, inbound Vote RPCs are refused before reaching Raft (and counted), so a node that rejoins with an empty data directory cannot help elect a leader that lacks entries it acknowledged before.
  - *Deferred service*: `ClusterListener::spawn_deferred` starts the listener with a status source (`ListenerConfig::status`, the log store) and no Raft; the `ServiceSlot` it returns installs Raft and the forward handler later.
  - *Decoding limits*: at most 4096 entries per AppendEntries, 4096 items per forward, 256 nodes per membership (checked while decoding, before allocating); a snapshot id is at most 256 bytes and a node address 1 KiB, so a received snapshot's meta always fits the 1 MiB the store reads back (`commit` also refuses a larger meta before writing anything).
  - *Snapshots*: a received snapshot (`SnapshotFile`, a temporary file since P4-T5c) accepts chunks at or before the bytes received so far (a retransmit or a restart from 0) and refuses a chunk that would leave a gap; its size is capped (default 4 GiB of disk, `ClusterStateMachine::set_max_snapshot_bytes`), and decoding copies no item larger than `-z` plus 64 KiB. A snapshot whose engine `-z` differs from the local one, or that enables the journal, is refused. `Engine::import_state` refuses a tube slab above 2^24 slots, and engine deadlines saturate instead of overflowing.
  - *Wiped followers*: openraft's `loosen-follower-log-revert` feature is enabled, so a follower that comes back with an empty data directory is re-replicated instead of stopping the leader.

Configuration (`[cluster]`; absent = standalone, byte-identical to P2):

```toml
[cluster]
node_id = 1                          # 1..=65535, must appear in [[cluster.peer]]
listen = "10.0.0.1:11400"            # cluster port
data_dir = "/var/lib/beanstalkd-rs"  # raft log, vote, snapshots
node_timeout = "5s"
snapshot_every = 100000              # entries between snapshots
heartbeat = "100ms"                  # P3-FD: was 50ms
election_timeout = ["500ms", "700ms"]   # P3-FD: was ["150ms", "300ms"]
insecure_plaintext = false           # true only for tests; otherwise [cluster.tls] is required
insecure_plaintext_allow_remote = false  # plaintext only on loopback addresses unless true

[cluster.tls]
# One certificate per node, used both as the listener's server certificate and
# as the dialer's client certificate: it must chain to `ca`, carry the SAN DNS
# name "bstk-node-<node_id>" (the CN is not consulted), and allow both server
# and client authentication (EKU serverAuth and clientAuth). A dialer
# verifies the listener as "bstk-node-<target id>"; a listener requires a client
# certificate from `ca` and, after the hello, checks it is valid for
# "bstk-node-<hello id>" and that the id is a [[cluster.peer]].
cert = "node1.pem"                   # SAN DNS "bstk-node-1"
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

Rules: 3 or 5 peers (1 allowed for tests); `-b` / `binlog` with `[cluster]` is an error; `-z` must match on every node (checked when joining); `--cluster-init` on every initial node bootstraps membership from `[[cluster.peer]]` once (after the status probes above) and is refused if `data_dir` already holds state or a rejoin marker; `[cluster.tls]` and `insecure_plaintext = true` exclude each other; `insecure_plaintext = true` requires `listen` and every peer address to be loopback (`127.0.0.0/8`, `::1`, `localhost`) unless `insecure_plaintext_allow_remote = true`, and logs a warning at startup.

- **Owner-only replies (P4-T5a)**: every node applies every input, but builds replies only for its own connections. `Engine::set_local_conns(Some(LocalConns))` (the state machine sets it with the node id and `CONN_SEQ_BITS` on every engine it creates or restores) makes the engine's `reply` / `reply_with` helpers skip an `Outbox` entry for any other connection; the reply closures of the expensive replies (`stats*`, `list-tubes*`, `peek*`, `reserved` bodies) are not even run. Handlers already counted and mutated before building the reply (builders such as `build_stats_server` take `&self`), and all replies, including `tick`-driven ones (reserve timeouts, `DEADLINE_SOON`, a put waking a reserver), go through the same helpers, so nothing but the `Outbox` differs: a test applies random inputs over three node ids to one engine per scope and requires equal `EngineState` bytes and journals and, per node, exactly the full engine's replies for its connections. Why a runtime setting on the engine: an `Outbox` abstraction would change every engine call site and test, and a predicate in `EngineConfig` or `EngineState` would enter the snapshot payload (which must stay byte-identical on every node), so it is neither; it must be set again after `import_state` (the state machine does). `LocalConns` takes the node shift as a parameter so the engine does not learn the cluster's numbering. Dedup, `Applied` / `Closed` events and journal entries are untouched, and `forward.rs`, the proposer and the actor never saw non-local replies (the state machine's `route` already dropped them), so nothing downstream changes; `route` stays as the delivery guarantee.
- **Fewer wake-ups (P4-T5b)**: at one connection, cluster CPU per operation was dominated by threads waking up, not by work (about two thirds system time). Per entry a follower handles three outside events (the AppendEntries carrying the entry, the log flush worker's callback, and the commit-only AppendEntries openraft 0.9 sends as soon as the commit index moves, `replication/mod.rs:671-685`), the leader about six (the client command, the flush callback, two responses per follower round); tokio's runtime metrics showed 6.6 worker parks per entry on a follower, 3.4 of them finding nothing to do. Changes, none of which touches what is replicated, synced or replied:
  - *Read-only socket registration* (`bstk_net::QuietTcp`, used for cluster connections in both directions and for client connections): tokio registers every `TcpStream` for write readiness too, and kqueue's write filter fires whenever the peer acknowledges data, so each frame written to an otherwise idle connection woke a worker that found nothing to do (a ping-pong test: 2.0 parks per round trip, 1.0 with read-only registration). `QuietTcp` registers reads only and writes directly; a write registration, on a duplicate descriptor, exists only while writes would block, and is dropped at the next write that succeeds at the first try. Registering reports the current state, so space freed between the failed write and the registration is not missed. Linux epoll reports write readiness only after the buffer was full, so the gain there should be smaller (not measured).
  - *View watchers*: the actor, the proposer and the leader duties react to leader, term and role changes through openraft's `server_metrics()` (published only when they change) instead of `metrics()` (published on every Raft core loop iteration, several times per entry). `Core::{leader, is_leader, view}` read the same source, because openraft publishes it before the full metrics, so a task woken by it could still read stale full metrics. The actor marks its receiver changed once at start, so a change between `Actor::new` and the subscription is not missed.
  - *Leader duties* wait for applied-state changes only while leading (followers never propose `Tick`).
  - *Applied events are pulled*: the actor no longer wakes for each `ReplySink::applied`; it drains them whenever it wakes for anything else, before acting, and at least every 50 ms tick. Nothing waits on them alone: sending depends only on new inputs, forward results, view changes and rewinds, and the stall timer, isolation and shutdown checks run on the tick. Consequences: the forward queue's items leave it up to a tick later (bounded by the connections' one command in flight), and a resend triggered by an out-of-order apply can come up to a tick later.
  - *Buffered frame reads* on cluster connections (16 KiB): one read per frame instead of a header read and payload reads.
  - Considered and not done: syncing the log inline in `append` (saves the flush worker's two thread hand-offs per entry and node, about 10% at one connection, but blocks a runtime worker for the whole of a sync stall, which P3-FD moved off on purpose); writing frames from the calling task instead of the connection's writer task (needs a cancel-safe partial-frame writer, as openraft drops calls on its timeout); changing the cluster default of 2 worker threads (P4-T2; with one worker the remaining "found nothing" wake-ups disappear, 3.0 parks per entry on a follower, see docs/BENCH.md P4-T5b). Inherent in openraft 0.9: the two AppendEntries round trips per entry and follower, and the core awaiting each append's flush.
- **Streamed snapshots (P4-T5c)**: building, sending, receiving, installing and restarting from a snapshot hold no copy of the payload in memory; peak memory above the node's own state is one pointer per job plus a few MiB of buffers (6–15 MiB measured, 0.04–0.17 times the payload) instead of 1–5 times the payload (docs/BENCH.md P4-T5c). Pieces:
  - *`TypeConfig::SnapshotData` = `SnapshotFile`*: a window (the payload) of a file in the snapshot directory, through `tokio::fs::File`, so openraft's chunk reads and writes run on tokio's blocking pool. Receiving keeps `SnapshotBuf`'s rules (no gap, a rewind discards the tail on the next write, a size cap) in a `<n>.part.tmp` file that is removed when the `SnapshotFile` is dropped (a stream replaced by another snapshot id, a failed install) or, after a crash, when the store reopens. openraft keeps its receiving state after a rejected chunk, so the file of a rejected stream stays until a different snapshot id arrives, the stream completes or the process exits (at most one file). Each write waits until tokio's `File` has performed it (which otherwise reports a failure on a later call, by when the position is ahead of the file), and after a failed write every write fails until a seek, so the file never differs from the chunks accepted. A sending file shorter than its layout fails with `UnexpectedEof` instead of making openraft send empty chunks forever. Sending opens the current `.snap` file; an open descriptor keeps it readable if a newer snapshot replaces it, and the file's checksum is verified as the payload is read in order, so a file damaged on disk fails the last chunk instead of reaching a follower. `max_snapshot_bytes` now bounds disk, not memory.
  - *Build*: `Engine::state_view` borrows the state (sorting references to the jobs, tube names and connections: 8 bytes per job) and serializes to exactly the bytes of `export_state` (tested on every round-trip proptest); postcard writes it through a 64 KiB `BufWriter` and a CRC-32C counter into the temporary file. Encoding happens under the state-machine lock; `fdatasync`, rename and directory sync after it is released. Lock time is that of the encode plus `write(2)` into the page cache: about the same as `export_state`'s copy took (measured in a 3-node cluster on one host: 160–212 ms before, 197–289 ms after for 1.1 M jobs / 145 MiB; 487–504 ms before, 304–361 ms after for 2 M small jobs / 90 MiB; logged per build at info). Rejected: exporting under the lock and encoding outside it (keeps a copy of all job records, 1–5 times the payload); encoding into memory under the lock and writing outside (one payload of memory for a lock about 20% shorter in isolation). A real fix of M5 needs a copy-on-write engine state.
  - *Install and restart*: the payload is decoded from a `BufReader` over the file through a postcard `Flavor` (`ReadFlavor`) that copies each byte string into a scratch buffer of at most `-z` plus 64 KiB and rejects borrowing (nothing in the payload borrows: `Bytes` and `String` decode into owned copies), keeps the first I/O error (postcard reports any as "unexpected end"), and must consume exactly the payload length; the CRC is computed in the same pass. `EngineState::jobs` holds boxed records, so `import_state` moves each record into the job table instead of copying the whole vector (the decoded state is the final state). The received file becomes the current snapshot only after it decoded and validated: the meta is appended, the header written, then `fdatasync`, rename, directory sync. `postcard::from_io` was not used: its reader maps every I/O error to "unexpected end" and its scratch buffer is consumed by every borrowed item.
  - *Formats*: the payload bytes are unchanged (`PAYLOAD_VERSION` stays 2, and the cluster wire protocol stays version 3: openraft 0.9's chunked transfer already streamed). The snapshot *file* is version 2: payload before meta (the payload is written before the meta is final), checksum over payload, meta, then the length fields. Version 1 files (written before P4-T5c) are still read; the next snapshot is written as version 2. On open, the newest file's checksum is verified by streaming it before older files are removed, as before.
  - *Observed on macOS*: the system allocator keeps freed large blocks dirty for reuse ("Malloc Large (empty)" in `vmmap`), so before P4-T5c a node kept about 500 MiB more after its first snapshot of a 145 MiB payload; streamed snapshots allocate no large blocks.
- **Known limitations (P3)**:
  - openraft 0.9 waits for each log append's flush before its next command (`RaftCore::append_to_log`), so a long fsync stall still delays heartbeats; the 500–700 ms election timeout absorbs the stalls seen on this machine's SSD. Removing the wait needs openraft 0.10 or a patched 0.9 (P4).
  - Building a snapshot holds the state-machine lock while it encodes the state into the page cache (security review M5; P4-T5c removed the memory copies, not the lock time, which is about what `export_state`'s copy took: 0.2–0.4 s for a 1–2 M-job state, during which this node applies nothing). A `write(2)` stalled on a full or slow disk would extend it.
  - An invalid snapshot received from the leader is a storage error, which openraft treats as fatal: the follower stops (fail-stop) until an operator intervenes (security review L5).
  - Membership is static (`[[cluster.peer]]`); nodes cannot be added or replaced at runtime. Bootstrapping needs a majority of the other nodes (all three in a 3-node cluster), and so does a node rejoining after data loss.
  - Plaintext cluster traffic is for tests: it is refused off loopback unless `insecure_plaintext_allow_remote` is set, and then anyone who can reach the cluster port can act as a peer.

## 8a. Later Phases (summary)

- **P4 Performance** (done): per-mode worker-thread defaults (§3; standalone plaintext now at 1.04–1.29× the reference's ops per CPU-second), O(log n) buried-job and reservation removal, owner-only replies, fewer cluster wake-ups and streamed snapshots (§8). Open: cluster CPU per operation (17–18 µs at 100 connections, target 15; one connection 0.55–0.77× of P3, target 0.5×) needs openraft 0.10 (no awaited flush per append), see docs/BENCH.md P4-T6. Dynamic membership is a separate later item.

## 9. Compatibility Strategy

- **Differential testing** (`tests/compat`): the same `.bt` script runs against the reference and `beanstalkd-rs`; replies are compared byte for byte after masking volatile fields (pid, uptime, rusage, server id, hostname, version, age, time-left, pause-time-left). 189 cases, part of `scripts/check.sh`.
- **Real clients** (`clients/run-smoke.sh`) and an **engine oracle proptest** complement it.
- Every known difference is recorded in `docs/COMPAT.md` with its reason.

### 9.1 Chaos history checker (`tests/chaos`, `checker.rs`)

The chaos harnesses (in-process on the simulated network, and multi-process with real servers behind pausable proxies; docs/PLAN.md §6.5) record every client operation and verify the history afterwards.

**Global checks**
- *Job ids are unique*: no two acknowledged puts got the same id.
- *Ids increase in commit order*: if put A was acknowledged before put B was sent, A's id is lower (ids are allocated in log order).
- *Job identity*: every put body is unique, so a `RESERVED` / `FOUND` reply names the put that created the job. The body must belong to a put that got that id, or to an unacknowledged put (which then evidently took effect, with that id). A body seen under two ids means one put created two jobs.
- Replies outside the protocol vocabulary of the command are violations.

**Per-job linearizability.** Each job's operations are checked against a single-server model of that job (a WGL-style depth-first search with memoization). The model's states are absent, ready, reserved(conn, deadline), delayed(until), buried and deleted. Every operation takes effect at one point inside `[send - slack, reply + slack]`; the points follow real time and each connection's program order. The point is modeled as the entry's engine time, which the leader stamps after the command was sent and before the reply; `slack` covers the difference between the clients' clock and engine time (clock skew between nodes, process start-up anchoring). The search places operations at their earliest possible points (`max(current point, send - slack)`): every timing constraint of the model is a lower bound, so placing earlier never loses a valid linearization.

Spontaneous transitions, allowed but never required:
- *TTR expiry*: reserved to ready at any point at or after the reservation's (or last touch's) point + TTR (TTR 0 counts as 1 s, as in the engine).
- *Delay expiry*: delayed to ready at or after the put's / release's point + delay.
- *Disconnect*: reserved(C) to ready once every acknowledged operation of C (on any job) could have taken effect: at or after the send time of C's last acknowledged operation, and only if the client closed or lost C at some point. There is no upper bound: after a kill -9 of the node holding C, the release happens only when the cluster drops that node's connections, long after the client saw its connection reset.
- *Bulk kick*: a `kick` (acknowledged with a non-zero count, or unacknowledged) may have moved a buried or delayed job to ready at a point inside its interval (it does not name its jobs).

Unacknowledged operations are optional: each took effect at a point after its send time, or never. An unacknowledged `reserve` may have reserved any job, so it is part of every job's history. When a job's search fails, the failure is classified (lost job, resurrected job, exclusive holding broken, or another inconsistency) and reported with the job's operations.

## 10. Changelog

**v0.5 (P3 shipped)**: Raft replication (`bstk-raft`, cluster mode in the server), engine state export / import, cluster differential suites and chaos testing; the entries below record the changes made during P3's fix rounds.

**P4-T2 (worker threads)**
- `server.threads` (TOML) / `--threads N` (CLI, long-only, 1..=256): `Cli::threads`, `config::ResolvedConfig::threads`, `config::ResolvedConfig::effective_threads` (see P4-T6b), `config::{DEFAULT_THREADS_STANDALONE, DEFAULT_THREADS_CLUSTER, MIN_THREADS, MAX_THREADS}`; feeds `tokio::runtime::Builder::worker_threads` in `main.rs`. Shown by `--check-config`.
- `engine_actor::run_task` drains up to `ACTOR_BATCH_LIMIT` (64) queued messages per wake-up (still ticking after each) before delivering once per batch; only reachable without a WAL. No measured throughput or efficiency effect on its own (P4-T1); kept because it is harmless.
- `bstk-bench`: `Scenario::HandshakeBurst` (`--rate N`), reporting handshake-completion latency (`latency::HandshakeStats`); `bench/run-matrix.sh` gained a `burst` server mode (a plaintext and a TLS listener on one server, a normal scenario and the handshake burst running concurrently) and an `RS_ARGS` variable (extra arguments for `beanstalkd-rs` only, so `SERVERS="ref rs"` can alternate the reference against a specific thread count in one invocation).

**P4-T5a (owner-only replies)**
- `Engine::set_local_conns(Option<LocalConns>)`, `LocalConns::{new, owns}`; the cluster state machine builds replies only for connections whose `conn >> CONN_SEQ_BITS` is its node id. Not part of `EngineState` / `EngineConfig`; snapshot payload unchanged.

**P4-T5b (fewer wake-ups)**
- `bstk_net::QuietTcp` (`new`, `get_ref`; `AsyncRead` / `AsyncWrite`): a TCP stream registered for read readiness only, used by the cluster dialer and listener and by the server's client connections. Cluster wire format and protocol version unchanged.

**P4-T5c (streamed snapshots)**
- `bstk_raft::SnapshotFile` (module `snapshot_file`, `len`, `is_empty`; `AsyncRead` / `AsyncWrite` / `AsyncSeek`) replaces `SnapshotBuf` as `TypeConfig::SnapshotData`; `DEFAULT_MAX_SNAPSHOT_BYTES` moved to `snapshot_file` (same value, now a disk bound).
- `Engine::state_view() -> EngineStateView<'_>` (opaque, `Serialize`, same bytes as `EngineState`); `EngineState`'s job records are boxed internally (encoding unchanged).
- Snapshot file format version 2 (payload before meta); version 1 still read. Payload version (2) and cluster protocol version (3) unchanged.
- `build_snapshot` logs the lock and total time at info.

**P4-T8 (comment cleanup, `bstk-net`)**
- New crate `bstk-net` (`crates/net`) with `bstk_net::QuietTcp` (moved from `bstk_raft::quiet_tcp`; same API); `bstk-server` and `bstk-raft` depend on it, so the server's client path no longer reaches `QuietTcp` through the Raft crate. Only tokio.
- Removed the doc-only `bstk_engine::_api_doc` and `bstk_engine_oracle::_api_doc`; their per-method contracts now sit on the `Engine` methods.
- Long design reasoning in code comments moved here: §4.6, §6.2, §7.1 to §7.3, §9.1. No behavior change.

**P4-T6b (thread default per mode)**
- `config::effective_threads(threads, cluster)` is replaced by `ResolvedConfig::effective_threads(&self, cluster)`: an explicit `threads` wins; else cluster 2; else 2 when any listener has `tls = true` or the binlog is enabled; else 1. New `config::DEFAULT_THREADS_STANDALONE_TLS_OR_BINLOG` (2); `DEFAULT_THREADS_STANDALONE` (1) now means no TLS listener and no binlog.

**P3-FD (cluster throughput)**
- `Op::Batch(Vec<(u64, EngineInput)>)` (last variant; `Op::Conn` unchanged), `bstk_raft::{MAX_PROPOSAL_ITEMS, MAX_PROPOSAL_BYTES, proposal_item_size, split_batches}`, `wire::MAX_BATCH_ITEMS`; the leader's proposer batches its own and forwarded inputs.
- Log store group commit with a flush worker; `truncate` / `purge` / `save_vote` wait for pending syncs.
- Defaults: `heartbeat` 100 ms (was 50 ms), `election_timeout` 500 to 700 ms (was 150 to 300 ms).

**P3-FC (safe rejoin)**
- Wire protocol version 3: `RpcRequest::Status` / `RpcResponse::Status { vote, last_log_id, committed, has_state }` (`bstk_raft::status`), answered from the log store before Raft runs (`ClusterListener::spawn_deferred`, `ServiceSlot`, `ListenerConfig::status`).
- Rejoin adopts the highest vote of a majority of the other nodes before Raft starts; `--cluster-init` decides from status probes (a bootstrap-only peer does not force a rejoin once every node answered); connection numbers have a time floor instead of the `2^20` gap.

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
