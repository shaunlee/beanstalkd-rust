# beanstalkd-rust Design

> Status: v0.2 (matches P0 as shipped). Reference implementation: C beanstalkd commit `25085c5` (built into `.ref/` by `scripts/build-ref.sh`). See the [changelog](#9-changelog) for what changed since v0.1 and why.

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
│     loop { select!(msg, timer) }                  │
│     after every message: tick(now), re-arm timer  │
└──────────────────┬───────────────────────────────┘
                   │ (P1) Persist trait
            store (WAL) / raft (P3)
```

### Crates

| crate | Responsibility | Depends on |
|---|---|---|
| `bstk-proto` | Command parsing, response encoding, stats YAML, `ServerCodec`. No I/O, fuzzable (`crates/proto/fuzz`). | bytes, tokio-util |
| `bstk-engine` | Deterministic state machine: jobs, tubes, connection state, queues, timers | bstk-proto |
| `bstk-engine-oracle` | Dev-only frozen copy of the engine before the T6b index refactor; used by an equivalence proptest | bstk-proto |
| `bstk-server` | Binary `beanstalkd-rs`: listener, connection tasks, engine actor, CLI, system info | tokio, clap, nix, getrandom, tracing |
| `bstk-compat` (`tests/compat`) | Differential harness and `.bt` case corpus against the reference | — |
| `bstk-bench` (`bench/`) | Load generator and benchmark matrix | tokio |
| `bstk-store` (P1), `bstk-raft` (P3) | WAL; openraft integration | — |

`clients/` holds real-client smoke tests (Python greenstalk, Go go-beanstalk).

## 3. Concurrency Model

- **Single-owner engine**: all state is owned by one tokio task, so there are no locks. This matches the reference's single-threaded semantics, which lets differential tests compare output byte for byte.
- **Multi-core I/O**: parsing, encoding and socket I/O are spread across tokio workers.
- **Command ordering**: each connection has at most one command in flight to the engine. Pipelined input stays buffered, undecoded, until the previous reply arrives. This guarantees "processed and answered in order".
- **Tick after every message**: the reference runs `prottick` on every event-loop pass, and some replies depend on it (e.g. a reserve that starts waiting inside the DEADLINE_SOON margin, COMPAT engine item 5). The actor therefore calls `tick(now)` after every engine call. This is cheap because `tick` returns at once when nothing is due (§4.3). If the actor ever drains several messages per wake-up, it must still tick between messages.
- **Back-pressure**: replies go out over unbounded per-connection channels and are dropped if the connection is gone, so a slow client never stalls the engine.
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
}
// Outbox = Vec<(ConnId, Response)>: one call may answer several connections.
// EngineConfig { max_job_size, binlog_max_size }.
```

- The engine **never** reads the clock or uses randomness. pid, hostname, rusage etc. come from the server through `SysInfo`; uptime is `now - start`.
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
- CLI: `-l addr`, `-p port`, `-z max_job_size` (parsed like the reference's `sscanf("%zu")`), `-V`, `-v`. P1 adds `-b`, `-f`, `-F`, `-s`.
- SIGUSR1 enters drain mode; SIGINT / SIGTERM exit gracefully. The soft `RLIMIT_NOFILE` is raised to the hard limit at startup (best effort).

## 7. Later Phases (summary)

- **P1 WAL**: append-only segment files, CRC32 per record; record types `Put`, `State`, `Delete`; fsync every write / every N ms / never; compaction; replay on startup through a `Persist` trait. When the engine or protocol types change, decide whether to retire or keep pinning `bstk-engine-oracle`.
- **P2 Operability**: TLS / mTLS via rustls; optional `auth <token>` extension; HTTP `/metrics`, `/healthz`, `/admin`; TOML config.
- **P3 Raft (openraft)**: messages are log entries; the leader proposes `Tick{now}` for time-driven transitions; reservations are replicated; followers proxy to the leader.
- **P4 Performance**: reduce the per-command cross-thread hop (ops per CPU-second is about 0.4× the reference), O(1) buried-job removal, profiling-driven work.

## 8. Compatibility Strategy

- **Differential testing** (`tests/compat`): the same `.bt` script runs against the reference and `beanstalkd-rs`; replies are compared byte for byte after masking volatile fields (pid, uptime, rusage, server id, hostname, version, age, time-left, pause-time-left). 189 cases, part of `scripts/check.sh`.
- **Real clients** (`clients/run-smoke.sh`) and an **engine oracle proptest** complement it.
- Every known difference is recorded in `docs/COMPAT.md` with its reason.

## 9. Changelog

**v0.2 (P0 shipped)**
- Put side effects moved to parse time: `Frame::PutRejected`, `Frame::PutStarted`, `Engine::put_rejected`, `Engine::put_started` (COMPAT proto item 8).
- `Command::PauseTubeBadName` (COMPAT proto item 12).
- `EngineConfig::binlog_max_size` (the reference reports 10 MiB even without a binlog).
- Deadline indexes, dispatchable-tube set and integer tube ids (T6b; see `docs/BENCH.md`).
- Server: manual decoding instead of `Framed`, 64 KiB read cap while waiting (COMPAT D3), sticky half-close, tick after every message.

**v0.1**: initial P0 design.
