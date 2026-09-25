# beanstalkd-rust Design

> Status: v0.1 (P0 design frozen). Reference implementation: C beanstalkd commit `25085c5` (built into `.ref/` by `scripts/build-ref.sh`).

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
            client (TCP / TLS)
                   │
┌──────────────────▼───────────────────────────────┐
│ server (tokio multi-thread runtime)              │
│  conn task × N ── Framed<TcpStream, ServerCodec> │
│     │  decode proto::Command / encode Response    │
│     │  mpsc<EngineMsg>        ▲ per-conn          │
│     ▼                         │ mpsc<Response>    │
│  engine task (sole owner of Engine, actor)        │
│     loop { select!(msg, sleep_until(deadline)) }  │
└──────────────────┬───────────────────────────────┘
                   │ (P1) Persist trait
            store (WAL) / raft (P3)
```

### Crates

| crate | Responsibility | Depends on |
|---|---|---|
| `bstk-proto` | Command parsing, response encoding, stats YAML, tokio codec. No I/O, fuzzable. | bytes, tokio-util (codec) |
| `bstk-engine` | Deterministic state machine: jobs, tubes, connection state, queues, TTR/delay handling | bstk-proto (shared types) |
| `bstk-server` | Binary `beanstalkd-rs`: listener, connection tasks, engine task, CLI, system info | tokio, clap, tracing |
| `bstk-store` (P1) | WAL segments, CRC, compaction, replay | — |
| `bstk-raft` (P3) | openraft integration | — |
| `tests/compat` | Differential test harness and case corpus against the reference | — |

## 3. Concurrency Model

- **Single-owner engine**: all state is owned by one tokio task, so there are no locks. This matches the reference's single-threaded semantics, which lets differential tests compare output byte for byte.
- **Multi-core I/O**: TLS, parsing, encoding and socket I/O are spread across tokio workers. Each engine operation is O(log n); a single core's ceiling is far above what the network layer can feed it.
- **Command ordering**: each connection has at most one command in flight to the engine (send, then wait for the reply before sending the next). This guarantees the protocol's "processed and answered in order" requirement.
- **Sharding**: not implemented. `reserve` spans multiple tubes and must pick the globally most urgent job, which sharding would break. Revisit only if P4 benchmarks prove the engine is the bottleneck.

## 4. Engine

### 4.1 Determinism and injected time

```rust
pub type Nanos = u64; // monotonic time supplied by the caller (e.g. ns since process start)

impl Engine {
    pub fn new(now: Nanos, cfg: EngineConfig, sys: Box<dyn SysInfo>) -> Self;
    pub fn connect(&mut self, now: Nanos, conn: ConnId);
    pub fn disconnect(&mut self, now: Nanos, conn: ConnId, out: &mut Outbox);
    pub fn half_close(&mut self, now: Nanos, conn: ConnId, out: &mut Outbox);
    pub fn handle(&mut self, now: Nanos, conn: ConnId, cmd: Command, out: &mut Outbox);
    pub fn tick(&mut self, now: Nanos, out: &mut Outbox);   // process everything due
    pub fn next_deadline(&self) -> Option<Nanos>;           // next wake-up time
    pub fn set_draining(&mut self, on: bool);
}
// Outbox = Vec<(ConnId, Response)>: one call may answer several connections
// (e.g. a put waking another connection blocked in reserve).
```

- The engine **never** reads the system clock or uses randomness. pid, hostname, rusage etc. for stats come from the server through `SysInfo`; uptime is `now - start`.
- The same `(now, command sequence)` always yields the same output. This is the prerequisite for Raft replication in P3, and lets unit tests simulate time directly.

### 4.2 Data structures

| Purpose | Structure | Notes |
|---|---|---|
| All jobs | `HashMap<JobId, Job>` | body is `bytes::Bytes` (zero-copy) |
| Tube ready queue | `BTreeSet<(pri, id)>` | same priority → lower id first (FIFO) |
| Tube delayed queue | `BTreeSet<(deadline, id)>` | |
| Tube buried queue | `IndexSet<id>` or similar | FIFO; kick takes from the front |
| Global reserved set | `BTreeSet<(ttr_deadline, id)>` | plus per-connection set of reserved jobs |
| Tube waiters | ordered set of `ConnId` | connections blocked in reserve |
| Tube pause | `pause_until: Option<Nanos>` | |
| Connection state | `HashMap<ConnId, ConnState>` | used tube, watch list, waiting state, producer/worker flags |

Tube creation and destruction follow the reference's refcounting: a tube with no jobs and no connection using or watching it is deleted. The `default` tube behaves as in the reference.

### 4.3 Semantic highlights (prot.c is authoritative)

- **reserve selection**: among the connection's watched, unpaused tubes, pick the ready job with the smallest `(pri, id)`.
- **DEADLINE_SOON**: if a job held by the connection has less than 1 s of TTR left (the safety margin), reserve replies `DEADLINE_SOON` immediately; a connection already waiting gets it when the margin is reached.
- **Half-closed connection**: a waiting reserve replies `TIMED_OUT`. The server detects this and notifies the engine.
- **TTR of 0** is bumped to 1 s.
- **Timeouts**: a timed-out job returns to ready and its `timeouts` counter increments.
- **disconnect**: all jobs held by the connection return to ready, and it is removed from every wait queue.
- **kick scope**: only the used tube. If it has buried jobs only buried jobs are kicked, otherwise delayed ones.
- **pause-tube**: the tube hands out no jobs while paused; waiting connections are woken on expiry.
- **Stats**: `cmd-*` counters, `urgent` (pri < 1024), `current-producers`, `current-workers`, `current-waiting` etc. are computed exactly as in prot.c.

## 5. Protocol (proto)

- `Command`: one variant per protocol command; put carries its body (`Bytes`).
- `Response`: one variant per reply; `encode(&self, &mut BytesMut)` produces output byte-identical to the reference.
- `ServerCodec` (tokio-util `Decoder` + `Encoder`) decodes into `Frame`:
  - `Frame::Command(Command)`.
  - `Frame::Error(Response)`: errors decided at decode time (BAD_FORMAT, UNKNOWN_COMMAND, JOB_TOO_BIG, EXPECTED_CRLF, ...).
  - Lines longer than 224 bytes, discarding the body after JOB_TOO_BIG, etc. must behave exactly like the reference.
- **Number parsing** follows `read_u32` / `read_u64` / `read_duration`: leading spaces accepted, minus sign rejected, overflow is BAD_FORMAT. **Tube names** follow `NAME_CHARS` / `is_valid_tube`.
- **Stats formatting**: `StatsJob`, `StatsTube` and `StatsServer` emit YAML with the exact field order and format of the reference's `STATS_FMT` etc.

## 6. Server

- One task per connection: read a frame, send it to the engine, await the reply (`oneshot` or per-connection `mpsc`), write it to the socket.
- While blocked in reserve the task keeps reading the socket to detect EOF / half-close:
  - EOF → tell the engine to disconnect.
  - Half-close → TIMED_OUT per the reference semantics.
- CLI (compatible subset of the reference): `-l addr`, `-p port`, `-z max_job_size`, `-V` (verbose), `-v` (version). P1 adds `-b`, `-f`, `-F`, `-s`.
- SIGUSR1 enters drain mode; subsequent puts reply `DRAINING`.
- Structured logging via tracing.

## 7. Later Phases (summary)

- **P1 WAL**: append-only segment files, CRC32 per record.
  - Record types: `Put`, `State`, `Delete`.
  - fsync policy: every write, every N ms, or never.
  - Compaction: when a segment's live ratio is low, move its live jobs forward.
  - On startup, replay segments to rebuild engine state. The engine emits state changes through a `Persist` trait.
- **P2 Operability**:
  - TLS via rustls, including mTLS.
  - Optional `auth <token>` extension command, only active when enabled in config.
  - A separate HTTP port serving `/metrics` (Prometheus), `/healthz` and `/admin` JSON.
  - TOML config; configurable job size limit up to 1 GB (same cap as the reference).
- **P3 Raft (openraft)**:
  - `Cmd` is the log entry. Time-driven transitions are advanced by the leader periodically proposing `Tick{now}`.
  - Reservations are replicated by default so reserved state survives failover.
  - Followers proxy commands to the leader, so clients can connect to any node.
- **P4 Performance**: benchmarks against the reference, flamegraph hot spots, sharding only if needed.

## 8. Compatibility Strategy

- **Differential testing**: the same script is sent to both the reference and `beanstalkd-rs`, and replies are compared byte for byte.
  - Volatile fields are masked first: pid, uptime, rusage, id, hostname, version, age, time-left, etc.
  - Time-dependent cases use second-granularity delays only and tolerate ±1 s.
- Every known difference is recorded in `docs/COMPAT.md` with its reason and whether it is intentional.
