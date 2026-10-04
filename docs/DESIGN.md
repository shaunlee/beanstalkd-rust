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
- CLI: `-l addr`, `-p port`, `-z max_job_size` (parsed like the reference's `sscanf("%zu")`), `-V`, `-v`, and for the binlog `-b DIR`, `-f MS` (`-f0` = fsync every write), `-F` (never fsync), `-s BYTES` (segment size), with the reference's defaults (fsync at most every 50 ms) and ordering rules. `-u` is rejected. `-v` / `--version` (long alias, P5-T4) print `beanstalkd-rs <version>`, the workspace version. `--threads N` (long-only, P4-T2; 1..=256) sets the tokio worker-thread count; see §3.
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

### 6.3 Resource limits (P5-T6)

What one client can make the server hold, and where each bound is tested. Protocol limits follow the reference; the reference has no per-connection caps, so neither do we where a cap would change replies.

| Pressure | Bound | Test |
|---|---|---|
| Long command line (no CRLF) | discarded in 224-byte windows as it arrives, then `BAD_FORMAT` (`STATE_WANT_ENDLINE`) | `resource_limits::an_endless_line_is_discarded_as_it_arrives`; pre-auth: `security_review::hold_pre_auth_overlong_line_is_refused` |
| `put` body over `-z` | discarded as it arrives, then `JOB_TOO_BIG` (`STATE_BITBUCKET`) | `resource_limits::an_oversized_put_body_is_discarded_as_it_arrives` |
| `put` body within `-z` | buffered as it arrives (the codec reserves at most 64 KiB ahead of what was received), so up to `-z` per connection; the reference allocates the whole job at the header | — |
| Pre-auth `put` | refused at the header, body never read | `security_review::hold_pre_auth_put_body_is_not_buffered` |
| Client that never reads | one command in flight; while its reply is unwritten nothing is read, so TCP back-pressure stops the client (64 KiB read cap while a reply is pending, §6) | `resource_limits::a_client_that_never_reads_is_held_by_back_pressure` |
| Idle plaintext connections | none beyond `RLIMIT_NOFILE` (as the reference). With every descriptor in use, `accept` fails with EMFILE and the connection waits in the backlog; the accept loops (plaintext, TLS, HTTP; the cluster listener already did) pause 50 ms per failure. Before P5-T6 they retried at once: on Linux 328,000 log lines and 1.76 s of CPU in 2 s | `resource_limits::accept_backs_off_while_descriptors_are_exhausted` |
| Idle / slow TLS and token connections | `server.max_pending_connections`, 10 s handshake, `auth.timeout` | `security_review::f1_*` |
| Auth floods | a wrong token closes the connection; concurrency is bounded by the pending cap; no rate limit (failures are counted in `beanstalkd_auth_failures_total`) | `auth::wrong_token_gets_unauthorized_and_close`, `security_review::f1_*` |
| HTTP slowloris | 256 connections, 2 s header timeout, health endpoints never queue | `security_review::f4_slow_http_clients_do_not_starve_healthz` |
| `/admin`, `/metrics` size | `max_tube_series`, snapshot cache | `security_review::f3_*` |
| Watched tubes / tubes per connection | unbounded, as in the reference (`watch` and `use` create tubes; a tube is freed with its last reference). Measured with 100,000 `watch`es on one connection: about 300 bytes per tube (the reference: about 690) | — |
| Waiting `reserve`s | one per connection | `resource_limits::a_reserve_storm_hands_each_waiter_one_job` |
| Cluster port | frames up to 32 MiB, read as they arrive; collection sizes bounded before allocation (§8) | fuzzing (§9.2) |

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
- **Startup probes**: a node starts its cluster listener before Raft; until Raft runs the listener answers only *status probes*: `RpcRequest::Status` (wire version 3: the persisted vote, last log id, commit hint and whether there is any Raft state, read from the log store) and `RpcRequest::StatusEx` (version 4: the same plus the node's membership view, see "Cluster protocol v4"), and only for authenticated nodes (same hello / mTLS rules as every request). Raft RPCs, forwards and control requests are refused until then, a Vote RPC after the vote gate (so it is counted). Since P6-T3 a node probes over *probe connections* (`ClientMsg::ProbeHello`), which need the identity but not the membership, so a node that is not a member yet (joining, or checking whether its id was removed) is answered too. Probe connections are bounded so that a certificate holder cannot hold the listener's probe slots (8): one per node id (a newer one takes over the older's slot and closes it), at most 16 probes and 30 s per connection, 5 s idle; in plaintext mode (no identity) only from loopback unless `insecure_plaintext_allow_remote` is set. *Disclosure*: a probe answer carries the membership (ids and addresses), the node's vote, log ids and commit index; it is given to any holder of a certificate of the cluster CA for a node id, including a removed node, until that certificate is retired, i.e. the CA and the node certificates are replaced (there is no revocation list). *Trust*: the rejoin argument below assumes crash faults; a malicious certificate holder answering probes could fabricate a membership (e.g. one that makes a wiped voter wait, or adopt from nodes it names) or a vote. Both are reasons to replace the CA and the certificates when removing a node that is not trusted any more (OPERATIONS). The per-id slot rule does not stop a holder of many certificates (e.g. several removed nodes') from filling the slots, and in plaintext mode the `from` id is not authenticated, so any client allowed to probe can claim any id; both are within the stated trust model.
- **Startup modes (P6-T3)** (`cluster::start`, `cluster::discover`, `bstk_raft::status::startup_decision`): `[[cluster.peer]]` is a list of *seeds* plus local address overrides (any number of entries, the node itself need not be listed); `cluster.initial_voters` (default: every peer) is the voter set `--cluster-init` creates, 1, 3 or 5 of the peers. The mode is chosen from the data directory and the command line:

  | Raft state | rejoin marker | `--cluster-init` | mode |
  |---|---|---|---|
  | yes | no | no | **restart**: Raft starts on its state (the log's membership wins over the config; differences are logged at startup and on every change) |
  | yes | no | yes | refused (`check_init`) |
  | any | yes | yes | refused (`check_init`) |
  | any | yes | no | **discovery** (an unfinished rejoin) |
  | no | no | no | **discovery** |
  | no | no | yes, node in `initial_voters` | **bootstrap** (below); if a probed initial voter is established: discovery |
  | no | no | yes, node not in `initial_voters` | **discovery** (it never initializes: openraft refuses a membership without the node, `Engine::initialize` → `check_members_contain_me`, and the voters would differ from the other initial nodes'). With `--cluster-init` its id must be above every initial voter's (a config error otherwise: it could never be added, see "Ids are never reused") |

  *Discovery* asks the seeds for `StatusEx`, then also every node of the membership they report (dialed at its membership address unless the config overrides it), each round with backoff (50 ms doubling to 2 s), keeping the latest answer per node, until `startup_decision` decides, evaluating in this order (the order is part of the argument below):

  1. `M` = the committed membership with the highest log index among all answers (committed entries lie on the one committed log, so their indexes order them). None: wait. `M` joint: wait (a rejoin would need a quorum of both halves; a joint configuration is left quickly).
  2. `V` = the voters of `M`, `n = |V|`, `m = ⌊n/2⌋ + 1`, `p = n - m + 1` (`status::rejoin_answers`; see "The bound" below: 1, 1, 2, 2, 3 for n = 1..5). If this node is a voter and `V \ {self}` has fewer than `p` nodes (only for a single voter): wait, saying that if this is the current membership it can never rejoin and its data must be restored (not a refusal: `M` may come from a stale seed, and the nodes it names may report a newer membership).
  3. Any answer from `V \ {self}` reporting a membership at a higher index than `M`, whether that node is rejoining or not: wait (a change is in flight; it may be committed without that node knowing). `P` = the answers from `V \ {self}` that do not report `rejoining` and whose membership is `M` itself (same log id); fewer than `p`: wait. (A rejoining voter's answer still counts for `M` and for the newer-membership check: it can only make the node wait.)
  4. This node is a voter *or learner* of `M`: **rejoin**, adopting the highest vote of `P` and its own persisted vote (`adopt_vote`; a committed vote naming this node waits with fresh answers; so would an incomparable top, which is unreachable here, see F3).
  5. Any answer whose membership is newer than `M` and lists this node: wait (it is being added, not yet known committed). Otherwise, among the answers holding `M` itself (any node, voter or not; a node holding a newer, uncommitted membership has its highest member id raised by it), any with `highest_member >= self`: **refuse** to start (a removed or skipped id; exit status 1).
  6. Some answer holding `M` has applied up to `M` (its applied record of the highest member covers every committed membership up to `M`): **join**: wait, logging that the node is not a member yet, until the operator adds it (it then decides *rejoin* as a learner, step 4). Otherwise wait.

  While it waits, the node allows the learned membership's nodes to connect (the leader that adds it need not be one of its seeds) and dials them at their membership addresses; it logs the reason every 5 s with who answered and why the others did not, and serves no clients (`/readyz` 503). A seed list naming no current member therefore waits with "no node that answered knows a committed membership" until the config names one; nothing is guessed. A node with no seed other than itself is an error at once (a single-node cluster cannot rejoin).
- **Bootstrap**: every initial voter is started with `--cluster-init` and the same `[[cluster.peer]]` list and `initial_voters`, in any order. `--cluster-init` is refused if `data_dir` holds state or a rejoin marker. Otherwise the node probes the other initial voters (each round on that round's answers) and initializes the membership with the initial voters once a majority of the initial voter set (`m = ⌊n/2⌋ + 1`, `n = |initial_voters|`) of *other* initial voters answered with no state at all, or every other initial voter answered and none is *established* (a committed vote, a log entry beyond the bootstrap membership at index 0, or a commit hint); as soon as an answer is established it goes to discovery instead (with a warning: a wiped node started with `--cluster-init` by mistake); otherwise it waits (logged). The second rule makes the race converge: openraft's `initialize` starts an election at once, so a node that initialized first shows state (index 0 and its own uncommitted vote) but is not established; without the rule the others would all rejoin and nobody could elect it. It needs every other node because the voters of a leader hold its vote uncommitted until its first append, which looks the same; only the leader itself shows a committed vote and index 1. Safety of the first rule: a node that ever led was elected by `m` nodes, at least `m - 1` of them other than this one, each with a persisted vote; `m` answers from the `n - 1` others include one of them since `(m - 1) + m > n - 1`. Consequence: bootstrapping needs `m` other initial voters up and matching (for 3 voters, all three); with fewer it waits. Peers outside `initial_voters` started with `--cluster-init` join (table above).
- **Rejoin**: a node whose discovery decided *rejoin* may have acknowledged entries and granted votes it no longer remembers (a voter or learner whose data was lost; a learner that never ran takes the same path). It writes `data_dir/rejoin` durably (not earlier: a node waiting to join may be restarted with `--cluster-init`, which refuses the marker), persists the adopted vote with the log store's `save_vote`, then *re-checks*: it asks the voters of `M` again and requires `startup_decision` to decide rejoin with the same membership log id (it saves a higher vote if one appeared); otherwise it decides again with the saved vote as its floor (a membership committed meanwhile; votes only increase, so the saved vote does no harm). Only then it starts Raft, which loads the vote, with elections disabled and its vote gate closed, serving no clients. It asks the leader to propose `DropNode(self)` and, once it has applied that entry (an index learned from a leader after it started, so everything committed before is in its log), removes the marker durably, re-enables elections and opens the gate. A crash in between keeps the marker, so the node goes through discovery (and probes) again. The node reports `rejoining` in `StatusEx` from the start of discovery (P6-T4's guardrails read it); on disk, a node without Raft state or with the marker has not finished (a node still in discovery has neither a vote nor log segments; the multi-process chaos harness counts it so, to keep at most a minority of the voters without their data). If the membership is changed meanwhile so that it no longer lists the node (P6-T4 refuses that, A2), the leader never proposes its `DropNode`: every 3 s the rejoin loop asks the nodes it knows for their status and, if `startup_decision` refuses its id, exits with an error (exit status 1) instead of waiting forever. Limits: rejoins are serialized in effect, since a rejoining voter's answer does not count for another (step 3): with `k` voters rejoining, each needs `p` answers from the `n - 1 - (k - 1)` others that are not; a rejoining voter waits while fewer than `p` other voters holding `M` and not rejoining answer; the voter of a one-voter membership cannot rejoin (restore its data: nothing else holds the cluster's data); in a two-voter membership a wiped voter adopts the other's vote, but it completes only if the other voter is (or stays) the leader: if the survivor has to campaign it needs the rejoining node's vote, whose gate stays closed until it has caught up, so the cluster stalls until the data is restored (keep two-voter memberships short, as in the 1 → 2 → 3 path).
- **Why rejoin is safe.** openraft 0.9 facts (checked in its source): (F1) a follower accepts an AppendEntries or snapshot only if the leader's vote is `>=` its own (`engine/handler/vote_handler/mod.rs:94-115`, called first by `Engine::append_entries`, `engine/engine_impl.rs:456`), so a follower whose persisted vote equals the leader's committed vote accepts it and one with a higher vote rejects it, and the rejected leader learns the higher vote and steps down; accepting saves the leader's vote first. (F2) the vote is loaded from `read_vote` at startup (`storage/helper.rs:69`); a granted vote is saved before the grant is answered (`core/raft_core.rs:1648` runs before the queued response). (F3) without the `single-term-leader` feature, votes are ordered by leader id `(term, node_id)` and then `committed` (`vote/vote.rs:22-24`, `vote/leader_id/leader_id_adv.rs:14-15`), so two leaders may exist in one term and the argument is about votes, not terms; that leader id derives `Ord`, and `Vote::partial_cmp` returns `None` only for incomparable leader ids, so the vote order is total (`Adopt::Incomparable` is unreachable; it is kept for the `single-term-leader` build, where it waits); a candidate's vote has the term after its previous one (`Engine::elect`). (F4) a membership change needs the leader's effective membership to be committed (`ChangeHandler::apply` → `ensure_committed`, `raft_state/membership_state/change_handler.rs:36-63`) and computes one step towards the goal (`Membership::next_coherent`, `membership/membership.rs:279`, called from `Membership::change`, 302-357); `ReplaceAllVoters` (what `change_membership(BTreeSet)` sends) refuses a voter that is not already a node of that membership (`ensure_voter_nodes`, `membership/membership.rs:245`, `LearnerNotFound`). (Line numbers: openraft 0.9.25.) (F5) a quorum of a joint configuration is a majority of each half.
  - *Assumptions* (the test hook keeps them, P6-T4's executor must): (A1) a node becomes a voter only through `change_membership(BTreeSet)` from a membership that lists it already (never `ChangeMembers::AddVoters`), so by F4 a committed membership lists it first; and a leftover joint configuration `(C, C')` is finished towards `C'`, never back to `C`. So, by F4, every membership entry proposed while the committed membership requires a quorum of `V` (a joint `(C, V)`, a uniform `V`, or a learner or address change of it) requires a quorum of `V` as well. (A2) the leader refuses voter changes while any voter reports `rejoining`; the re-check after adoption narrows the window this assumption covers (and turns "removed while rejoining" into a refusal instead of a hang on a `DropNode` the leader refuses from a non-member).
  - *Invariant*: the vote `A` that a rejoining node `R` persists before it starts Raft is at least the vote of every leader elected before `R` lost its data; `R` never votes until it holds every committed entry. Consequently `R` rejects every leader below any leader it ever followed, which is what an intact node does (F1), and its lost acknowledgements cannot be reused by a leader missing the entries they acknowledged.
  - *Setting*: `R` lost its data at `t_w`; discovery's answers all came after `t_w` (a new process). `M` (index `i_M`, uniform voters `V`, `n`, `m`, `p`) and `P` as in "Startup modes"; `t_0` = the time of the earliest answer of `P`; `R ∉ P`; `|P| >= p`.
  - *What a node of `P` is*: it does not report `rejoining`, so it either never lost its data, or finished a rejoin, after which it holds every committed entry it acknowledged before its own data loss (the "Acknowledgements and votes" paragraph below, by induction over rejoins: it applied its `DropNode`, which a leader holding each of them appended after them). Either way it holds every committed entry it ever acknowledged, and its vote is at least every vote it ever persisted. A rejoining node has neither property: it may lack entries it acknowledged before its wipe (that broke Claim 1 when step 3 counted it: five voters, two partitioned holding `M` with a stale vote, one rejoining that holds `M` again but not the entries it had acknowledged later; a node wiped after them would adopt the stale vote from those three and let the stale leader commit a different entry at an index already committed), and while it is in discovery it reports no vote at all (which would break case A of Claim 2).
  - *The bound*: `P` must meet every commit quorum of `V` (Claim 1) and every election quorum of `V` minus `R` (Claim 2, case A). A quorum `Q` of `V` has `m` nodes and may contain `R`; `P ⊆ V \ {R}`, so `P` and `Q` are both subsets of `V` and meet when `|P| + m > n`; within the `n - 1` nodes of `V \ {R}`, `|Q \ {R}| >= m - 1` and `|P| + (m - 1) > n - 1` is the same condition. So `|P| >= n - m + 1 = p`, and no fewer suffices (a set of `n - m` nodes misses the quorum made of the other `m`). For odd `n` this is `m` (n = 3: 2, n = 5: 3), for even `n` it is `m - 1` (n = 2: 1, n = 4: 2); n = 1 needs one other voter, which does not exist.
  - *Claim 1 (`M` was current at `t_0`)*: no membership entry above `i_M` was committed before `t_0`. Otherwise take the first one, `E`: its predecessor in the committed log is `M` (uniform `V`), so by F4 and A1 committing it took a quorum of `V` (F5), which meets `P` ("The bound"); that node acknowledged `E` before its answer, and as a node of `P` it still held `E` when it answered (it holds every committed entry it acknowledged; a committed entry is never removed, and a snapshot carries its membership), so its answer reports a membership above `i_M`, which step 3 excludes.
  - *Claim 2 (the bridge from a static to a changing voter set)*: let `J` be the earliest committed membership entry such that every committed membership entry from `J` to `M` requires a quorum of `V` (the joint `(C, V)` that introduced `V`, or the bootstrap entry). For every leader `L` elected before `t_w`, with vote `V_L`, `A >= V_L`. Case A, `L`'s log held `J` when it was elected: `L`'s effective membership `E_L` is `J` or a later membership entry in its log, and that entry may be uncommitted, at an index above `i_M`. If it is `J`, it requires a quorum of `V` by construction. Otherwise `E_L` was proposed by some leader `L'` whose effective membership `C` was then committed (F4); `C` is the membership entry preceding `E_L` in `L'`'s log, so (log matching) it is `J` or a membership entry after `J` in `L`'s log; it was committed before `L` was elected, so before `t_w <= t_0`, hence its index is at most `i_M` (Claim 1). So `C` is one of the committed entries from `J` to `M`, each of which requires a quorum of `V`, and by A1's clause "every membership entry proposed while the committed membership requires a quorum of `V` requires a quorum of `V` as well", so does `E_L`. Either way `L` was elected by a quorum `Q` of `V` that persisted votes `>= V_L` (F2) before `t_w`. By "The bound" some node of `P` is in `Q \ {R}`; its vote is still `>= V_L` (votes never decrease, and a node that rejoined since adopted at least it, by induction), so some answer of `P` is `>= V_L`, and so is `A`. Case B, `L`'s log lacked `J`: `J` was committed by a leader with vote `V_J`, and by Raft's leader completeness (it held before the wipe; earlier rejoins preserved it, by induction over rejoins) every leader with a higher vote holds `J`, so `V_L <= V_J` (equal only for `J`'s creator, which had not written `J` yet when it was elected). Every node of `P` holds `M` and therefore `J` (log matching, or a snapshot at or past them); it received them from a leader holding `J`, whose vote is `>= V_J` (`J`'s creator, or a node that held `J` and then campaigned with a later term, F3), and accepting saved that vote (F1), which never decreased since; so every answer of `P` is `>= V_J >= V_L`. In both cases `A >= V_L`. In the static case (`V` never changed) `J` is the bootstrap entry, case B is empty, and case A is the P3-FC argument. (`A` may also be an uncommitted candidacy `(T, c)` that `P` saw: then `R` rejects a leader elected in term `T` with a lower node id, as the voter that granted `(T, c)` would, until a later term; this is safe and costs at most one election.)
  - *Acknowledgements and votes*: a leader `L'` that `R` accepts after the rejoin has `V_L' >= A`. Any entry `e` committed with `R`'s lost acknowledgement was committed by a leader `L_e` that `R` followed, so `V_L_e <= A <= V_L'`; a leader with a higher vote holds `e` (leader completeness), and `L'` with an equal vote is `L_e`; a lower leader that lacks `e` is rejected by `R` (finding 6: such a stale leader and `R` formed a majority and overwrote entries committed in the newer term). (Commit acknowledgements do not give this: a leader may finish a commit after the probe by counting `R`'s acknowledgement from before the wipe.) The leader of the adopted vote itself (a committed vote) is accepted, since its vote equals `R`'s; from then on openraft stores each accepted leader's vote, so `R` does not grant a vote in a term where it already accepted another leader. Votes `R` granted before the wipe are covered by the vote gate: `R` votes again only after it has applied an entry proposed after it started, through a leader elected without it. Leader completeness of that leader needs, per configuration, one node of its election quorum that held each committed entry when it voted; election quorums exclude `R` until it has caught up, so the counting `(m - 1) + m > n - 1` gives one in each configuration; configuration changes are committed by leaders holding those entries (induction), so the quorums that accepted them hold the entries as well, and `R`'s acknowledgements after the wipe are truthful (log matching).
  - *(a) A joint configuration in flight*: if `M` itself is joint, discovery waits (step 1); a change in flight past a uniform `M` shows on some voter of `M` as a newer membership (step 3: a committed one by Claim 1's argument; an uncommitted one if an answering voter holds it, and if none does, Claim 1 still holds and the change is case (e)), so discovery waits until it is committed and uniform, then decides against the new voters. A joint configuration left over by a leader change is finished by the next change request (P6-T4); until then a rejoining voter waits, which is safe and logged.
  - *(b) A removed voter trying to rejoin*: its id is in no current membership and at or below the highest member id, so step 5 refuses it (exit status 1, "node ids are never reused"); even if it could start, the listeners of the current members refuse its peer hello (`not a member of this cluster`, P6-T1's allowlist) and the leader closes out any connection it owns. A removed node still running answers probes with its last membership, which lists the removed id at a lower index than `M`: history, not an add (step 5 looks only at memberships newer than `M`).
  - *(c) Growth 1 → 2*: in a one-voter cluster `{1}` the new node joins: it waits until added as a learner, then rejoins as a learner of `M = {1}` + learner, whose voters other than itself are `{1}` and `p = 1`, so it adopts node 1's vote (never needed for a node that never voted, harmless, and it keeps one path), catches up and is promoted. A wiped voter of a two-voter membership adopts the other voter's vote (`p = 1`: every quorum of two contains it) but completes only while that voter leads (see "Rejoin", Limits); a one-voter cluster's voter never rejoins (step 2). So the 1 → 2 → 3 path should not stop at two voters for long.
  - *(d) A stale seed list*: if no seed is a current member, the memberships reported are older than the current one, and the voters they name have moved on: either they answer with a newer membership (the target set grows to its nodes, and discovery proceeds against them) or they are gone or refuse, and discovery waits with "no node that answered knows a committed membership" or "k of the p answers needed", never adopting from non-voters (`P` contains only voters of `M` that hold `M`). Step 2 waits instead of refusing for the same reason.
  - *(e) Adoption racing a membership change*: Claim 2 needs only that `M` was current at `t_0` (Claim 1) and that the leaders in question were elected before `t_w`; a change committed after `t_0` does not invalidate `A`. It matters for liveness and for knowing whom to rejoin: A2 keeps voter changes out while `R` reports `rejoining`, and the re-check after `save_vote` decides again (with the saved vote as floor) when the membership changed, e.g. into a refusal when `R` was removed meanwhile.
  - *(f) A wiped learner*: it rejoins like a voter (step 4), adopting the highest vote of `p` voters of `M` (`V \ {R} = V` has `n` nodes, and "The bound" holds unchanged). A learner's acknowledgements and votes are never counted (openraft counts only voters of the configuration), so this is not needed for a node that was always a learner; it covers a voter demoted to a learner (openraft's `retain`), costs one probe round, and keeps a single path. The vote gate and `DropNode(self)` work as for a voter.
  - *Join and elections*: a joining node does not start Raft until discovery decides rejoin, i.e. until a committed membership lists it, and then starts with elections disabled and the gate closed until it has caught up. openraft would not campaign for it anyway: `RaftCore::handle_tick_election` (`core/raft_core.rs:1450`) returns unless the node is a voter of its effective membership, and `Engine::handle_vote_req` (`engine/engine_impl.rs:282-351`) refuses a candidate with a shorter log before `update_vote`, so a node without a log cannot even raise others' votes (beyond that, a committed vote within the leader lease refuses every candidate). The design does not rely on either.
  - *Ids*: `highest_member` is the highest id that was ever a member (P6-T1, `SmMeta`), a maximum, not a set: an id below it that never was a member is refused too, since P6-T4 will never add it (a removed node's certificate can never be readmitted). Hence a node outside `initial_voters` must have an id above every initial voter.
- **openraft panic at `core/raft_core.rs:761`** (seen with finding 6): when committed entries diverge, a node can learn a commit `LogId` that is greater (a newer leader id) but has a lower index than its own; openraft's `update_committed` compares whole `LogId`s (`raft_state/mod.rs:275`, leader id first, `log_id/mod.rs:23`), so it emits `Commit { already_committed, upto }` with `upto.index < already_committed.index`, and `apply_to_state_machine` indexes an empty entry list (`core/raft_core.rs:1700` → 761). It is a symptom of the divergence, not a separate bug.
- **Storage**: segmented CRC-checked Raft log with `fdatasync` and group commit, vote file, snapshots of the engine state (postcard, streamed to and from files, see "Streamed snapshots (P4-T5c)") every `snapshot_every` entries. `-b` is not used in cluster mode.
  - *Group commit (P3-FD)*: `append` writes its records and returns; its openraft callback goes to a flush worker (tokio's blocking pool), which `fdatasync`s every segment written since its last sync once and then invokes every callback it covered, in order, so a callback always follows the durability of its entries, and appends queued during a sync share the next one. A failed sync fails its callbacks and every later append. `truncate`, `purge` and `save_vote` first wait until the worker has finished everything queued (so nothing is acknowledged after it was cut off and no removed data is synced later), then run synchronously and are durable when they return. A rolled-over segment is synced inline before the next is created, so only the last segment can have an unsynced tail, which open cuts at the first torn record as before.
  - *openraft 0.9 limit*: the Raft core awaits each append's callback before its next command (`RaftCore::append_to_log`, `core/raft_core.rs:713-731`, "a temp wrapper to make non-blocking append_to_log a blocking"), and it sends heartbeats itself. So the core still waits for every log sync (now without blocking a runtime thread), at most one core append is outstanding, and group commit across appends does not happen in practice; batching happens before the core instead (`Op::Batch`). A sync stall (seen up to about 800 ms on macOS APFS) therefore still delays heartbeats, hence the longer timing defaults below (since P3-FD: heartbeat 100 ms, election timeout 500 to 700 ms). openraft 0.9 starts an election only after `election_timeout_max` (the leader lease) plus the node's random election timeout (drawn once per process from `[min, max]`) without hearing from the leader (`engine/engine_config.rs:54-56`, `core/raft_core.rs:1475-1494`), so the defaults tolerate 1.0 to 1.2 s without a heartbeat (was 450 to 600 ms), and a failover takes about 1.3 to 1.5 s on one machine (the `leader_kill` test bound is 2 s; 500 ms to 1 s would allow up to 2 s before the election alone).
- **Transport**: one cluster port, length-prefixed postcard frames (Raft RPCs and forwarding), mTLS by default with the node id bound to the certificate.
  - *Dialer* (`bstk_raft::client`): one multiplexed connection per peer, shared by replication, votes and forwarding. A request that times out (or whose call openraft drops: it bounds every AppendEntries by `heartbeat_interval`) fails alone; its late answer is discarded. The connection is closed only when stalled: a request goes unanswered while requests have been outstanding with no response at all for `max(stall_timeout = 1 s, 3 × timeout)`. AppendEntries batches of more than one entry are limited to `append_budget` (1 MiB) encoded bytes (`PayloadTooLarge` with a scaled entries hint makes openraft split); a single entry may use the whole frame, so the heartbeat interval must allow the largest job body to cross the link.
  - *Listener* (`bstk_raft::listener`): connections in the TLS handshake or hello have their own budget (16 at once, `max(4, 2 × peers)` per source address, 2 s to finish; since P6-T1 at least `2 ×` the allowed nodes); an authenticated connection holds its peer's single slot, and a newer authenticated hello from the same peer closes the older connection. Rejections are logged at most about once a second. A rejected hello gets a generic reason (`hello rejected`, `unsupported protocol version`, or the `-z` mismatch), or, for a node that proved its identity but is not allowed, `not a member of this cluster` (P6-T1); details are logged locally only, and peer-supplied text is logged escaped and truncated. Operator tools use the same port through admin connections (no peer slot; see "Cluster protocol v4 and the admin channel (P6-T2)").
  - *Vote gate*: `ListenerConfig::vote_gate` (a `VoteGate`); while closed, inbound Vote RPCs are refused before reaching Raft (and counted), so a node that rejoins with an empty data directory cannot help elect a leader that lacks entries it acknowledged before.
  - *Deferred service*: `ClusterListener::spawn_deferred` starts the listener with a status source (`ListenerConfig::status`, the log store) and no Raft; the `ServiceSlot` it returns installs Raft and the forward handler later.
  - *Decoding limits*: at most 4096 entries per AppendEntries, 4096 items per forward, 256 nodes per membership (checked while decoding, before allocating); a snapshot id is at most 256 bytes and a node address 1 KiB, so a received snapshot's meta always fits the 1 MiB the store reads back (`commit` also refuses a larger meta before writing anything).
  - *Snapshots*: a received snapshot (`SnapshotFile`, a temporary file since P4-T5c) accepts chunks at or before the bytes received so far (a retransmit or a restart from 0) and refuses a chunk that would leave a gap; its size is capped (default 4 GiB of disk, `ClusterStateMachine::set_max_snapshot_bytes`), and decoding copies no item larger than `-z` plus 64 KiB. A snapshot whose engine `-z` differs from the local one, or that enables the journal, is refused. `Engine::import_state` refuses a tube slab above 2^24 slots, and engine deadlines saturate instead of overflowing.
  - *Wiped followers*: openraft's `loosen-follower-log-revert` feature is enabled, so a follower that comes back with an empty data directory is re-replicated instead of stopping the leader.

Configuration (`[cluster]`; absent = standalone, byte-identical to P2):

```toml
[cluster]
node_id = 1                          # 1..=65535
listen = "10.0.0.1:11400"            # cluster port
data_dir = "/var/lib/beanstalkd-rs"  # raft log, vote, snapshots
node_timeout = "5s"
snapshot_every = 100000              # entries between snapshots
heartbeat = "100ms"                  # P3-FD: was 50ms
election_timeout = ["500ms", "700ms"]   # P3-FD: was ["150ms", "300ms"]
insecure_plaintext = false           # true only for tests; otherwise [cluster.tls] is required
insecure_plaintext_allow_remote = false  # plaintext only on loopback addresses unless true
initial_voters = [1, 2, 3]           # P6-T3: the voters --cluster-init creates (default: every peer)

[cluster.tls]
# One certificate per node, used both as the listener's server certificate and
# as the dialer's client certificate: it must chain to `ca`, carry the SAN DNS
# name "bstk-node-<node_id>" (the CN is not consulted), and allow both server
# and client authentication (EKU serverAuth and clientAuth). A dialer
# verifies the listener as "bstk-node-<target id>"; a listener requires a client
# certificate from `ca` and, after the hello, checks it is valid for
# "bstk-node-<hello id>" and that the id is a member (the effective Raft
# membership; the [[cluster.peer]] seeds before the node has one). Startup
# probes (ProbeHello, P6-T3) need the identity only.
cert = "node1.pem"                   # SAN DNS "bstk-node-1"
key = "node1.key"
ca = "cluster-ca.pem"                # peers must present certificates from this CA

# Seeds and local address overrides (P6-T3): the initial nodes to bootstrap,
# or at least one node of the running cluster to join it.
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

Rules: at least one peer, unique ids and addresses (since P6-T3 any number, and the node itself need not be listed); `initial_voters` (default: every peer) lists 1, 3 or 5 peers (1 for tests), checked when set or with `--cluster-init`; with `--cluster-init`, a node outside `initial_voters` must have an id above every initial voter; `-b` / `binlog` with `[cluster]` is an error; `-z` must match on every node (checked when joining); `--cluster-init` on every initial voter bootstraps membership from `initial_voters` once (after the status probes above) and is refused if `data_dir` already holds state or a rejoin marker; `[cluster.tls]` and `insecure_plaintext = true` exclude each other; `insecure_plaintext = true` requires `listen` and every peer address to be loopback (`127.0.0.0/8`, `::1`, `localhost`) unless `insecure_plaintext_allow_remote = true`, and logs a warning at startup.

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
- **Membership-driven networking (P6-T1)** (`cluster::membership`, `bstk_raft::client::Network`, `bstk_raft::listener::PeerAllowlist`, `cluster::duties`): once a node has a Raft membership, the *effective* membership (the latest membership entry in its log, committed or not; voters and learners of both halves of a joint configuration) is the authority for networking; `[[cluster.peer]]` becomes seeds plus local address overrides.
  - *Address book*: a peer is dialed at its config address if the config lists it (a local override: the per-link proxies of the tests and the multi-process chaos harness give each node its own address for a peer), else at its `BasicNode.addr` in the effective membership (`Network::set_members`, fed by `membership::watch` from openraft's server metrics), else, for Raft RPCs only, at the address openraft passes. Forwards, control requests and status probes resolve the same way, so a node missing from the config is reachable. A connection slot is replaced when its target's resolved address changes, and openraft's per-target client resolves on every RPC (a replication stream may outlive an address change). The last-response time used by liveness is kept per target, not per slot, so a move does not make a node look silent. A node logs a warning, once per change, where its config and the membership differ (an override with another address, config ids that are not members, members missing from the config).
  - *Allowlist*: the listener accepts every node of the effective membership; the config seeds only while the node has no membership (a node without state, or rejoining before its first entries). Before Raft runs (status probes only) it accepts the seeds plus the snapshot's membership, a low-risk window since nothing but status probes is served then. When the allowlist changes, the live connection of every node that left is closed; a hello that raced the change is refused when its connection registers (`PeerAllowlist::set` and the registration check under the same lock). The hello checks identity (the mTLS SAN for `from`) before membership, so only an authenticated node learns that it is not a member (`REJECT_NOT_MEMBER`, logged once per change by the dialer); with `insecure_plaintext` there is no identity and anyone on loopback learns it, which is acceptable for the test-only mode. A node that has no membership yet accepts only its seeds, plus, during discovery (P6-T3), the nodes of the membership it learned, so the leader that adds it need not be in its config.
  - *Liveness*: the silence check runs over the members (not the config). In addition the leader proposes `DropNode { node, up_to_local: highest_local(node) }` for every node that owns connections in the replicated state but is not a member, checked at once after a membership change and then every 100 ms, independently of the silence bookkeeping: a removed node's forwards that the leader had already queued for the proposer may be proposed after that `DropNode` and open connections above its bound, so a `DropNode` is proposed again whenever the owner's highest local number exceeds the bound last proposed, and again after 1 s if its connections remain. The forward handler also refuses forwards and controls from non-members. No state-machine rule changed (a `Connect` of a non-member still applies; the leader closes it out), so replicated semantics are unchanged and a mixed-version cluster cannot diverge.
  - *Quorum checks*: `leader_reachable` counts the effective membership's voters (the larger half of a joint configuration) for its single-voter shortcut. Since P6-T3 nothing reads the config's size: bootstrap counts `initial_voters`, rejoin counts the current voters (see "Startup modes").
  - *Ids are never reused*: the state machine records the highest node id that ever appeared in an applied membership entry (`SmMeta::highest_member`, `StateHandle::highest_member`), deterministically on every node; snapshot payload version 3 carries it; a version 2 payload is still read and takes the highest id of the snapshot's membership. Used by P6-T4's guardrails.
  - *Test-only membership changes*: until the operator interface (P6-T4, P6-T5), nothing reachable by an operator changes membership. The server's `test-hooks` cargo feature (never in a shipped build; `scripts/check.sh` builds it for the membership tests only) polls a command file in the data directory (`cluster::test_hooks`); the Raft crate's tests and the in-process chaos harness call openraft directly.
- **Cluster protocol v4 and the admin channel (P6-T2)** (`bstk_raft::wire`, `bstk_raft::listener`, `bstk_raft::tls`, `bstk_raft::status`, the server's `cluster::StatusView`): the wire protocol is version 4. New messages are appended enum variants (`ClientMsg::{AdminHello, Admin}`, `ServerMsg::Admin`, `RpcRequest::StatusEx`, `RpcResponse::StatusEx`); every version 3 message keeps its encoding (tested byte for byte). Nodes still require an exact version and there is no negotiation: 0.5.0 was never released, so no running cluster needs a rolling upgrade from it, and negotiation would have to be designed, tested and kept for a case nobody has; 0.5.x and later nodes cannot be mixed (CHANGELOG). A version 3 hello gets `REJECT_VERSION`; a version 3 node closes a connection that sends it a version 4 message (an unknown variant is a decode error).
  - *StatusEx*: the durable state of `Status` plus the node's view (`NodeStatusEx`): whether Raft runs, the rejoining flag, term, leader, last applied, the highest member id it knows (the applied `SmMeta::highest_member`, raised to the effective membership's highest id, since before Raft runs only a snapshot is applied), and the effective membership (`MembershipView`: voter sets, two when joint, every node with its address, the entry's log id, and whether it is committed on this node's knowledge: its index is at or below the larger of the log store's commit hint and the last applied index; both come from this node's log, whose committed prefix never diverges). Answered in the same situations as `Status`, i.e. before Raft runs too: the server reads the effective membership from storage once at startup (`StorageHelper::get_membership`, as Raft does when it starts; nothing appends a membership entry before Raft runs), and switches to openraft's metrics once Raft runs. `Network::status_ex` and `StatusTransport::status_ex` are the client calls (for P6-T3's rejoin against the current voters).
  - *Admin channel*: an operator tool sends `AdminHello { version, to }` (`to: None` reaches whichever node answers) instead of a peer hello. It claims no node id and registers no peer slot. Identity: under mTLS the client certificate (already verified against the cluster CA by the TLS handshake) must be valid for `bstk-admin` and carry no other DNS name (`tls::verify_admin_identity`), and `tls::verify_peer_identity` refuses any certificate carrying `bstk-admin`, so the two identities exclude each other even for a certificate naming both: a node key cannot change membership, and the admin key cannot replicate, vote or forward. In plaintext mode (tests only) an admin connection must come from a loopback address (`listener::plaintext_admin_allowed`, canonicalizing `::ffff:127.0.0.1`), even with `insecure_plaintext_allow_remote`: plaintext proves no identity, and anyone who can reach the port could otherwise change membership. As with P6-T1's plaintext peers, any local process can then act as the operator, which is acceptable for the test-only mode. Admin hellos share the handshake budget (16 at once, per source address, 2 s); an accepted admin connection then holds one of `max_admin_conns` (4) admin slots instead, reads requests of at most `wire::ADMIN_MAX_REQUEST_FRAME` (64 KiB; answers may be larger, a maximal membership is about 270 KiB), and closes after `admin_idle_timeout` (60 s) without a request. Connections and requests are logged at info with the source address; refusals go through the rate-limited rejection log with a generic reason (`REJECT_HELLO`, `REJECT_VERSION`, `REJECT_ADMIN_BUSY`).
  - *Separation*: an admin connection that sends a peer request (forward, Raft RPC, control, status probe) or a second hello is closed before anything is served, and so is a peer connection that sends an admin request. The separation is structural (distinct `ClientMsg` variants read by distinct loops), so `check_control` and `check_forward` are unchanged.
  - *Requests*: `AdminRequest::{Membership, AddLearner { id, addr, expect }, Promote { ids, expect }, Remove { id, expect }, SetAddr { id, addr, expect }}`, where `expect` is the membership log id the operator saw (compare-and-set: a change against another membership is `Conflict { current }`). Answers: `Membership(NodeStatusEx)`, `Started`, `Done { log_id }`, `NotLeader { leader, addr }`, `Conflict`, `Refused { reason }`, `Unsupported`. Until P6-T4 every change answers `Unsupported` without reaching openraft; `Membership` is answered from the status source.
  - *Decoding limits*: the membership of a `StatusEx` or `Membership` answer is bounded like a log entry's (`MAX_JOINT_CONFIGS` voter sets of at most `MAX_MEMBERS` ids, `MAX_MEMBERS` nodes, addresses of at most `MAX_NODE_ADDR_LEN` bytes), as are the ids of `Promote` and the addresses of `AddLearner`, `SetAddr` and `NotLeader`; a refusal reason is at most 1 KiB. Answers are bounded too because the dialer and the admin tool decode them.
  - *Probe connections (P6-T3)*: a node at startup sends `ClientMsg::ProbeHello` (the peer `Hello` fields, appended within the unreleased version 4) on a one-shot connection (`Network::probe_status_ex`, used by `StatusTransport::status_ex`; no peer slot, bounded by the connect plus forward timeouts). The listener checks the version, `to`, that `from` is not itself, and the identity exactly as for a peer (`verify_peer_identity`, so the admin certificate is refused), but not the allowlist and not `-z` (nothing but status is served); the connection then holds one of `max_probe_conns` (8) probe slots, carries only `Status` and `StatusEx` requests (anything else, or a hello, closes it), and closes after `probe_idle_timeout` (5 s) without one; refusals are generic (`REJECT_HELLO`, `REJECT_VERSION`, `REJECT_PROBE_BUSY`). A node that is not a member yet can thus learn the membership and the highest member id (join, or refuse a removed id). What a probe reveals (votes, log ids, the membership with addresses) is what any holder of a cluster certificate could have learned while it was a member; with `insecure_plaintext`, anyone on loopback (anyone who can reach the port with `insecure_plaintext_allow_remote`, who can act as a peer anyway). The peer hello's `not a member` rejection is unchanged.
  - *Certificates*: `scripts/mkcluster-certs.sh DIR admin` issues `admin.pem` (SAN `bstk-admin`, `clientAuth` only) from the cluster CA.
- **Known limitations (P3)**:
  - openraft 0.9 waits for each log append's flush before its next command (`RaftCore::append_to_log`), so a long fsync stall still delays heartbeats; the 500–700 ms election timeout absorbs the stalls seen on this machine's SSD. Removing the wait needs openraft 0.10 or a patched 0.9 (P4).
  - Building a snapshot holds the state-machine lock while it encodes the state into the page cache (security review M5; P4-T5c removed the memory copies, not the lock time, which is about what `export_state`'s copy took: 0.2–0.4 s for a 1–2 M-job state, during which this node applies nothing). A `write(2)` stalled on a full or slow disk would extend it.
  - An invalid snapshot received from the leader is a storage error, which openraft treats as fatal: the follower stops (fail-stop) until an operator intervenes (security review L5).
  - Membership is static for operators (`[[cluster.peer]]`); nodes cannot be added or replaced at runtime (P6 adds this; P6-T1 made networking follow the Raft membership, P6-T3 the startup modes). Bootstrapping needs a majority of the other initial voters (all three of 3), and a voter rejoining after data loss a majority of the current voters among the others; a voter of a one- or two-voter membership cannot rejoin.
  - Plaintext cluster traffic is for tests: it is refused off loopback unless `insecure_plaintext_allow_remote` is set, and then anyone who can reach the cluster port can act as a peer.

## 8a. Later Phases (summary)

- **P4 Performance** (done): per-mode worker-thread defaults (§3; standalone plaintext now at 1.04–1.29× the reference's ops per CPU-second), O(log n) buried-job and reservation removal, owner-only replies, fewer cluster wake-ups and streamed snapshots (§8). Open: cluster CPU per operation (17–18 µs at 100 connections, target 15; one connection 0.55–0.77× of P3, target 0.5×) needs openraft 0.10 (no awaited flush per append), see docs/BENCH.md P4-T6. Dynamic membership is a separate later item.

## 9. Compatibility Strategy

- **Differential testing** (`tests/compat`): the same `.bt` script runs against the reference and `beanstalkd-rs`; replies are compared byte for byte after masking volatile fields (pid, uptime, rusage, server id, hostname, version, age, time-left, pause-time-left). 189 cases, part of `scripts/check.sh`.
- **Real clients** (`clients/run-smoke.sh`) and an **engine oracle proptest** complement it.
- Every known difference is recorded in `docs/COMPAT.md` with its reason.

### 9.1 Chaos history checker (`tests/chaos`, `checker.rs`)

The chaos harnesses (in-process on the simulated network, and multi-process with real servers behind pausable proxies; docs/PLAN.md §6.5) record every client operation and verify the history afterwards. Rejoins in the in-process harness run the server's startup decision (`status::startup_decision`) on the simulated nodes' answers, with each node's emulated rejoin marker as its `rejoining` flag and a fixed membership (the bootstrap voters); schedules that change the membership are P6-T7's.

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

### 9.2 Fuzzing (`fuzz/`, P5-T6)

`cargo-fuzz` targets for the four surfaces that decode untrusted bytes. The crate is outside the workspace (nightly; `scripts/check.sh` never builds it); seeds are in `fuzz/seeds` (`cargo run --bin gen_seeds` from `fuzz/` rewrites them from the compat cases and the project's own encoders).

| Target | Input | Checks beyond "no panic" |
|---|---|---|
| `proto_decode` | `ServerCodec` with `-z` 0, 4, 65535 or 1 GiB, auth and put-started on or off, the stream split at fuzzed points | the frames and the leftover buffer do not depend on the split |
| `wal_read` | up to 8 `binlog.N` files (any `N`), optionally with a valid header and recomputed CRCs; `Wal::open` | an opened binlog reopens with the same live jobs, and the next id is above every live id |
| `raft_wire` | one frame as `ClientMsg` or `ServerMsg` (protocol version 4, admin messages included), max frame 32 MiB, 256 bytes or the admin request limit (64 KiB) | `decode` and `read_frame` agree; a decoded message re-encodes to bytes that decode and re-encode identically |
| `snapshot_decode` | a snapshot payload (through `restore_from`), or a `.snap` file (fuzzed meta and payload with a correct header and CRC, or raw bytes) opened by `ClusterStateMachine::open` | an accepted payload re-encodes stably, and the restored engine survives commands, connects, puts, ticks to every deadline and disconnects |

The snapshot entry points are `bstk_raft::storage::state_machine::fuzzing` behind the `fuzzing` feature (never enabled by the server). Run, for example, `cargo +nightly fuzz run -a wal_read corpus/wal_read seeds/wal_read -- -max_total_time=600 -timeout=10 -rss_limit_mb=2048 -malloc_limit_mb=128` from `fuzz/` (`-a`: debug assertions and overflow checks). The weekly workflow runs each target for 5 minutes with `-fork=2` and uploads crashes; it fails on any file in `artifacts/`, because with `-fork` libFuzzer exits 0 after a crash (it passes the child's raw wait status to `exit()`), and in fork mode a seed that crashes is silently skipped.

Findings (P5-T6), each fixed with a regression test in the owning crate:
- `binlog.18446744073709551615` made the next segment number overflow (`wal.rs`; without overflow checks it wrapped to `binlog.0`, which replay never reads), and a number a few below the top let the binlog open once and then run out of numbers. Segment numbers above 2^62 (`MAX_SEGMENT_INDEX`; one is used per segment written) are now `Corrupt`, and new numbers are allocated with a checked add. The snapshot store had the same overflow in its sequence numbers (`snapshot.rs`; found by review, now `Corrupt` / an error).
- A record with job id `u64::MAX` (a forged or damaged file whose CRC matches) set the next id to that live id, so the next put reused it (or overflowed). Records with ids above 2^62 - 1 are now malformed, and `import_state` refuses a next job id above 2^62 (`MAX_NEXT_JOB_ID`), as it already bounded `next_list_seq`.

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

**P5-T6 (hardening)**
- `fuzz/` (cargo-fuzz, outside the workspace) with four targets (§9.2); `bstk-raft` feature `fuzzing` exposes `storage::state_machine::fuzzing::{restore_payload, sample_payload, MAX_JOB_SIZE, NODE}` (`#[doc(hidden)]`, never enabled by the server).
- `Wal::open` returns `Corrupt` for a segment number above `MAX_SEGMENT_INDEX` (2^62) and for records with a job id above `MAX_RECORD_ID` (2^62 - 1); `Engine::import_state` refuses `next_job_id` above `MAX_NEXT_JOB_ID` (2^62); the snapshot store refuses a file named with the largest sequence number. Replies and file formats unchanged.
- The server's accept loops pause 50 ms after a failed `accept` (§6.3).

**P6-T1 (membership-driven networking)**
- `bstk_raft::client::Network::{set_members, address}`; `NetworkConfig::peers` are now overrides (a target is dialed at its config address, else its membership address, else openraft's); `PeerClient` resolves its target on every RPC.
- `bstk_raft::listener::{PeerAllowlist, REJECT_NOT_MEMBER}`, `ClusterListener::allowlist()`; `ListenerConfig::peers` is the initial allowlist and `max_handshakes_per_ip` a floor. The hello checks identity before membership. Wire format and protocol version (3) unchanged; the new rejection is a reason string inside the existing `ServerHello::Rejected`.
- `StateHandle::{highest_member, connection_owners}`; snapshot payload version 3 (`SmMeta::highest_member`), version 2 still read.
- `bstk-server` feature `test-hooks` (test-only membership changes, `cluster::test_hooks`).

**P6-T2 (cluster protocol v4, admin channel)**
- `wire::PROTOCOL_VERSION` = 4, exact match, no negotiation; appended `ClientMsg::{AdminHello, Admin}`, `ServerMsg::Admin`, `RpcRequest::StatusEx`, `RpcResponse::StatusEx`; `wire::{AdminHello, AdminRequest, AdminResponse, MAX_ADMIN_REASON_LEN, ADMIN_MAX_REQUEST_FRAME}`. Version 3 encodings unchanged.
- `status::{NodeStatusEx, MembershipView}`, `StatusSource::status_ex` (default: the durable state only), `StatusTransport::status_ex`, `Network::status_ex`.
- `tls::{ADMIN_DNS_NAME, verify_admin_identity}`; `verify_peer_identity` refuses a certificate carrying `bstk-admin`.
- `listener::{plaintext_admin_allowed, REJECT_ADMIN_BUSY}`, `ListenerConfig::{max_admin_conns, admin_idle_timeout}`.
- Server: `cluster::StatusView` answers status, StatusEx and admin `Membership`; membership changes answer `Unsupported` until P6-T4. `scripts/mkcluster-certs.sh DIR admin`.

**P6-T3 (startup modes: seeds, initial voters, join, rejoin against the current voters)**
- Config: `[[cluster.peer]]` is seeds plus local address overrides (any number ≥ 1, the node itself need not be listed); `cluster.initial_voters` (`ClusterSettings::initial_voters`, default every peer; 1, 3 or 5 peers, checked when set or with `--cluster-init`); with `--cluster-init` a node outside the set needs an id above every initial voter. `cluster_summary` prints the peer count and, with `--cluster-init`, the initial voters.
- Wire: appended `ClientMsg::ProbeHello(Hello)` within version 4; `listener::REJECT_PROBE_BUSY`, `ListenerConfig::{max_probe_conns, probe_idle_timeout}`; `Network::probe_status_ex` (one-shot), which `StatusTransport::status_ex` for `Network` now uses.
- `status::{probe_ex, learned_membership, startup_decision, Startup}`; `adopt_vote` unchanged (used with the current voters' count).
- Server: `cluster::discover` replaces `probe_rejoin_vote`; bootstrap probes and initializes `initial_voters`; a node outside them, or one sent away from bootstrap, goes to discovery; the rejoin marker is written when discovery decides rejoin (no longer before probing); a removed or skipped id exits with status 1. The in-process chaos harness's rejoin calls `startup_decision` with its static membership; the multi-process harness treats a node without a vote or log segment as still rejoining (the marker no longer appears before the decision).
