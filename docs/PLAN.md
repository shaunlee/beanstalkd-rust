# Development, Test and Acceptance Plan

> Companion to `docs/DESIGN.md`. Roles: the lead (architecture owner) owns planning, interface contracts, integration and acceptance; subagents implement and test.

## 0. Common Rules (every task)

- **Toolchain**: Rust ≥ 1.98, edition 2024, workspace `resolver = "3"`.
- **Quality gate** `scripts/check.sh` must pass before hand-off:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo build --workspace`
  - `cargo test --workspace`
- **Interface contracts are frozen**: `Command`, `Response`, `Frame` in `bstk-proto` and the public API of `bstk-engine` are defined by the lead. Any change must be justified in the hand-off report; the lead decides.
- **The reference code is authoritative**: `.ref/beanstalkd/*.c` (run `scripts/build-ref.sh` first). When in doubt read the source — never guess. If the reference has a bug, or protocol.txt disagrees with it, follow the reference and record it in `docs/COMPAT.md`.
- **Parallel builds**: each subagent uses its own `CARGO_TARGET_DIR` to avoid cargo lock contention.
- **Forbidden**: `unsafe`; `unwrap()` outside tests (use `expect("reason")` where failure is impossible); reading the clock or using randomness inside the engine.

## 1. P0 Task Breakdown

| ID | Task | Owner | Depends on | Wave |
|---|---|---|---|---|
| T0 | Workspace skeleton, interface contracts, check script | lead | — | 0 |
| T1 | `bstk-proto`: parsing, encoding, codec, stats YAML | subagent A | T0 | 1 |
| T2 | `bstk-engine`: full state machine | subagent B | T0 | 1 |
| T3 | `tests/compat`: differential harness and case corpus | subagent C | T0 | 1 |
| T4 | `bstk-server`: networking, engine actor, CLI | subagent D | T1, T2 | 2 |
| T5 | Run differential tests, fix mismatches, add edge cases | subagent E | T3, T4 | 3 |
| T6 | Real-client smoke tests, concurrency stress, benchmarks | subagent F | T4 | 3 |
| T6b | Engine timer/index performance fix (added after T6 profiling) | subagent G | T5, T6 | 3b |
| T7 | P0 acceptance | lead | all | 4 |

### T1 bstk-proto

**Deliverables**
- `parse_line(&[u8]) -> Result<Command, Response>` for command lines (without the put body).
- `ServerCodec`:
  - Put body reading, discarding the body on JOB_TOO_BIG, EXPECTED_CRLF.
  - Handling of command lines longer than 224 bytes.
  - Behavior matches `scan_line_end`, `_skip` and `fill_extra_data` in prot.c.
- `Response::encode` byte-identical to the reference.
- YAML output of `StatsJob`, `StatsTube`, `StatsServer` and tube lists byte-identical to `STATS_FMT` and friends.

**Tests**
- Valid cases for all 25 commands, plus error cases:
  - wrong argument count
  - non-numeric values
  - minus sign
  - u32 / u64 overflow
  - leading or extra whitespace
  - invalid tube names; length exactly 200 and 201
  - unknown commands; wrong case
- Codec tests:
  - correct decoding under arbitrary fragmentation (1 byte at a time, random splits)
  - pipelining (several commands in one write)
  - the next command still decodes correctly after a JOB_TOO_BIG body is discarded
- proptest: for random valid commands, `encode_command → parse` round-trips (needs a test-only client-side encoder).
- A cargo-fuzz target under `fuzz/` (nightly; not part of check.sh).

**Acceptance**: check passes; `cargo llvm-cov -p bstk-proto` line coverage ≥ 90% (manual spot check if the tool is unavailable); every listed case has a test.

### T2 bstk-engine

**Deliverables**
- The full semantics of `docs/DESIGN.md` §4, including:
  - reserve / reserve-with-timeout waiting and wake-up
  - reserve-job
  - DEADLINE_SOON
  - TTR expiry
  - delay expiry
  - pause-tube
  - kick and kick-job
  - tube creation and garbage collection
  - releasing a connection's jobs on disconnect
  - half-close
  - drain mode
  - all stats counters

**Tests** (all with simulated time)
- At least one case for each command's success path and for each of its error replies.
- Every state transition: ready, reserved, delayed, buried, deleted.
- Time-related:
  - TTR expiry (check the timeouts counter and that the job returns to ready)
  - both DEADLINE_SOON triggers
  - reserve-with-timeout with 0 and with > 0
  - pause expiry waking waiters
- Multi-connection:
  - waiters are woken in arrival order
  - watching several tubes picks the globally most urgent job
- proptest state-machine test: random sequences of commands and time advances; after every step check the invariants:
  - (a) every job is in exactly one container
  - (b) counters match container sizes
  - (c) every reserved job belongs to a live connection
  - (d) no tube has both ready jobs and an unpaused waiter

**Acceptance**: check passes; all of the above tests exist and pass; the invariant test runs ≥ 10,000 cases.

### T3 tests/compat Differential Harness

**Deliverables**
- A runner that starts the reference and `beanstalkd-rs` on random ports, runs the same case script against both, records replies, masks volatile fields and compares byte for byte. On mismatch it prints a clear diff.
- A case DSL in `tests/compat/cases/*.bt`:

  ```
  # comment
  @c1 send "put 0 0 5\r\nhello\r\n"    # escapes: \r \n \\ \" \xNN
  @c1 recv                            # read one complete reply (incl. body), up to 3 s
  @c1 recv_none 200ms                 # nothing must arrive within this time
  @c2 send "reserve\r\n"
  sleep 1100ms
  @c1 shutdown_write                  # half-close
  @c1 close
  ```

  Connections open automatically on first reference.
- Both server binary paths can be overridden via environment variables. A missing reference binary must **fail loudly**, never skip.
- A corpus of ≥ 60 cases covering:
  - all 25 commands and their error replies
  - protocol-level errors such as overlong lines and JOB_TOO_BIG
  - time-dependent behavior
  - multiple connections
  - stats contents

**Acceptance**: the runner has self-tests (DSL parsing, masking); running reference-vs-reference passes fully, proving the harness itself is stable.

### T4 bstk-server

**Deliverables**
- Binary `beanstalkd-rs`.
- Connection tasks: at most one command in flight per connection; EOF and half-close are still detected while blocked in reserve.
- Engine actor: `select!` between `recv` and `sleep_until(next_deadline)`.
- `SysInfo`: pid, hostname, uname, rusage, uptime, random server id.
- CLI flags `-l`, `-p`, `-z`, `-V`, `-v`; SIGUSR1 enters drain mode.
- Graceful shutdown on SIGINT / SIGTERM.

**Tests**: integration tests start the server and drive it over raw TCP:
- basic flow
- a blocked reserve woken by another connection's put
- jobs released on disconnect
- half-close → TIMED_OUT
- pipelining

**Acceptance**: check passes; integration tests pass; after 1,000 connections open and then close, no tasks are left behind and the connection count returns to zero (verified via stats).

### T5 Compatibility Convergence

- Run every T3 case; classify each mismatch as a proto, engine or server issue; fix and re-run.
- Add new edge cases discovered along the way.

**Acceptance**: 100% of differential cases pass; any intentional difference is recorded in `docs/COMPAT.md` with its reason (target: zero).

### T6 Real Clients and Stress

**Real-client smoke tests**
- At least one real client, Python greenstalk preferred; a second one if feasible.
- Run the full flow — put, reserve, delete, release, bury, kick, stats, tubes — and get identical results from both servers.

**Stress test**
- A Rust load generator under `bench/`.
- Scenario: N connections (100) doing a put/reserve/delete loop for 30 s.
- Record ops/s and p99 latency for both servers.

**Acceptance**:
- Client tests pass.
- No errors or hangs during the stress run; afterwards the job count in stats is 0.
- Numbers recorded in `docs/BENCH.md`. P0 only requires ≥ 0.8× the reference's throughput; if not met, include an analysis and defer to P4.

### T6b Engine Performance Fix

T6 found per-operation cost growing linearly with the number of tubes and connections: `tick` and `next_deadline` scan every tube and connection after every message, `process_queue` visits every tube, and tube names are hashed and copied on hot paths. This violates the O(log n) design (DESIGN §4.2) and misses the 0.8× bar for 100 connections on separate tubes.

**Deliverables**
- Reproducible optimized reference build for benchmarking (`scripts/build-ref.sh` option).
- Re-baseline on the current HEAD with an otherwise idle machine.
- Indexed deadlines (connections, delayed jobs, paused tubes) so `tick` returns immediately when nothing is due and `next_deadline` is O(log n); `process_queue` visits only tubes with waiters; further hot-path fixes (e.g. integer tube ids) only if still needed. Engine public API unchanged; semantics unchanged (tick still runs after every message).
- A scaling scenario in `bstk-bench`: ~10,000 idle connections and ~10,000 tubes holding delayed jobs, with 10 active connections.

**Tests**
- Oracle proptest: a frozen copy of the pre-change engine and the new engine receive identical `(now, message)` sequences, including nanosecond-level advances and exact deadline ties; outboxes and stats must match at every step.
- Invariant: the indexed `next_deadline()` equals a from-scratch scan.

**Acceptance**
- Every non-pipelined cell of the T6 matrix ≥ 0.8× the optimized reference. Pipelined cells should reach it too; any that do not must be justified with profile evidence in `docs/BENCH.md`.
- Scaling scenario throughput ≥ 0.8× of the same scenario with no idle connections or tubes.
- 189/189 differential cases pass in 3 consecutive runs; check.sh green; smoke tests pass; `docs/BENCH.md` updated.

## 2. P0 Acceptance Checklist (T7, run by the lead)

- [x] `scripts/check.sh` passes (fmt, clippy, build, test): 285 tests
- [x] All 25 protocol commands implemented, covered at the proto, engine and differential levels (line coverage: proto 96.0%, engine 98.5%)
- [x] ≥ 60 differential cases, 100% passing; `docs/COMPAT.md` up to date: 189/189, 3 consecutive runs
- [x] Engine invariant proptest passes with ≥ 10,000 cases, plus a 10,000-case oracle proptest against the pre-T6b engine
- [x] 1,000-connection concurrency test passes with no resource leaks
- [x] Real-client smoke test passes (Python greenstalk, Go go-beanstalk)
- [x] `docs/BENCH.md` records performance, meeting the 0.8× bar or with an analysis: every cell ≥ 0.88× the -O2 reference
- [x] Code review: no unsafe; no unwrap outside tests; no clock or randomness in the engine

## 3. Later Phases (summary; detailed when each phase starts)

| Phase | Main tasks | Acceptance focus |
|---|---|---|
| P1 | Write-ahead log; see §4 | see §4.5 |
| P2 | Operability; see §5 | see §5.5 |
| P3 | Raft replication; see §6 (done) | see §6.6 |
| P4 | Performance; see §7 | see §7.5 |

## 4. P1: Write-Ahead Log (detailed plan)

### 4.1 Reference behavior (probed against `.ref` with `-b`)

| Topic | Reference behavior |
|---|---|
| Journaled transitions | put, release **with** delay, bury, kick / kick-job, delete. **Not** journaled: reserve, touch, release with delay 0, TTR timeout, delay expiry, pause-tube, use/watch. |
| Record content | Full job record (tube + body) the first time, short records later; the last record of a job wins on replay. |
| Replay | A job reserved at crash time comes back in its last journaled state with that record's counters (ready after a plain put; buried or delayed if its last record says so; changes from unjournaled transitions such as `release … 0` are lost). Delayed with a passed deadline → ready (`delay` still reported). Jobs are replayed in first-record order, which sets buried FIFO order and ready ties. Tubes with no jobs are not recreated. The tube list order is the result of replaying every record: a tube is appended at a job's full record and swap-removed when a delete record frees its last job. Replaying a buried job increments `buries` again (shows 2 after one bury). |
| Counters after restart | Per-job counters (`reserves`, `releases`, ...) come from the last journaled record. Cumulative server and tube counters (`cmd-*`, `total-jobs`) reset; recovered jobs don't count toward `total-jobs`. |
| Time | Wall clock (`gettimeofday`): `created_at` and delay deadlines persist, so `age` and remaining delays continue across downtime. |
| Job ids | Next id = highest id seen in the binlog + 1 (a deleted job's id is not reused while its records remain). |
| Files | `binlog.N` plus a `lock` file (fcntl lock). A second instance on the same directory exits with status 10. Files are preallocated to `-s` rounded up to 4096; `binlog-max-size` reports `-s` unrounded. A new file is started on startup. |
| Space reservation | Space for a job's future records is reserved at put time; if it cannot be reserved the put replies `OUT_OF_MEMORY`. A write error silently disables the WAL. |
| Compaction | Ratio-based: while (allocated − live) / live ≥ 2, move one live job out of the oldest file. |
| fsync | `-f MS`: at most once per MS ms (default 50), not awaited before replying. `-f0`: fsync on every write, before the reply. `-F`: never. |

### 4.2 Design decisions

1. **Time**: the server passes `now = wall-clock nanoseconds at startup + monotonic elapsed`, so time is monotonic but wall-anchored. Deadlines and `created_at` persist as they are, as in the reference.
2. **Journal**: when enabled (`EngineConfig::journal`, on only with `-b`), the engine appends a `JournalEntry` to an internal buffer at exactly the reference's journaled transitions; the actor drains it after every engine call (no signature changes). Each entry is a snapshot of the job record (the body and tube only in the first entry), so the last entry wins.
3. **Recovery**: `Engine::recover(now, cfg, sys, recovered)` takes recovered jobs in the reference's replay order, each in its final state, plus the next job id computed by the store (highest id in any surviving record + 1, including deleted jobs whose records remain), and applies the replay rules in 4.1 (including the `buries` quirk).
3a. **Binlog stats**: the store owns `binlog-oldest-index`, `binlog-current-index`, `binlog-records-written` and `binlog-records-migrated`; the actor pushes them to the engine with `Engine::set_binlog_stats` after each write, and the engine reports them in `stats`. In the reference, compaction moves also count as records written.
4. **Write before reply**: the actor moves to a dedicated OS thread. It writes the drained journal synchronously before releasing that call's replies; with `-f0` it also fsyncs first. This holds in every mode, so a reply is never sent for a change that has not reached the OS.
5. **Space reservation**: before passing a completed put to the engine, the actor reserves WAL space; on failure the put completes as a new `PutRejection::OutOfMemory` (put side effects already applied, `OUT_OF_MEMORY` reply). The oracle crate is updated mechanically for the new variant.
6. **Write errors**: fail-stop (log and exit non-zero) instead of silently disabling the WAL, so no reply is ever sent for an unpersisted change. Recorded as a COMPAT difference.
7. **Layout**: our own file format (CRC-checked records, torn-tail truncation). File numbering and compaction moves may differ from the reference, so the following are masked in differential tests: `file`, `binlog-oldest-index`, `binlog-current-index` and `binlog-records-migrated`. `binlog-records-written` stays compared in cases that never trigger compaction (it also counts compaction moves), which checks that we journal the same transitions. Order after a compaction (buried FIFO, `list-tubes`) may differ from the reference; declared in COMPAT, and restart cases stay pre-compaction.

### 4.3 Tasks

| ID | Task | Owner | Depends on | Wave |
|---|---|---|---|---|
| P1-T0 | Contracts: `JournalEntry`, `EngineConfig::journal`, `Engine::recover`, `Engine::set_binlog_stats`, `PutRejection::OutOfMemory`, `bstk-store` API, time anchor | lead | — | 0 |
| P1-T1 | `bstk-store`: segments, lock, records + CRC, reservation, compaction, fsync policy, recovery | subagent | T0 | 1 |
| P1-T2 | Engine: journal at the reference's transitions, `recover`, replay quirks | subagent | T0 | 1 |
| P1-T3 | Harness: `restart` / `crash` directives, per-server `-b` directories, `-b` mode for the whole corpus, masks, restart cases | subagent | T0 | 1 |
| P1-T4 | Server: `-b -f -F -s`, actor on an OS thread, write-before-reply, reservation, fail-stop, lock handling | subagent | T1, T2 | 2 |
| P1-T5 | Differential convergence with `-b`; crash, corruption and compaction tests; WAL benchmarks | subagent(s) | T3, T4 | 3 |
| P1-T6 | P1 acceptance | lead | all | 4 |

### 4.4 Tests

- **Store**: unit tests; truncation of the last record at every byte offset; CRC corruption; crash in the middle of compaction (the same job in two files, last wins); a deleted job is never resurrected while its delete record survives; proptest of random operation sequences with recovery at arbitrary points.
- **Engine**: proptest that runs random sequences, recovers from the journal, and compares with the expected post-restart state derived from the live state (reserved → ready, counters per 4.1).
- **Differential**: the whole corpus with `-b`; new restart and crash cases covering 4.1 (orders, counters, delays across downtime, ids).
- **Durability**: kill -9 at random points under load in every fsync mode: every acknowledged `INSERTED` job exists after restart; every acknowledged `DELETED` job is gone; acknowledged bury and release states persist. For `-f0`, an instrumented file layer proves the order write → fsync → reply.
- **Compaction**: long churn with a small `-s`: disk usage stays bounded and the state after restart is correct.
- **Performance**: benchmarks with `-b` against the reference with the same `-f` / `-F` settings.

### 4.5 Acceptance

- [x] `scripts/check.sh` green, including the `-b` differential mode and restart cases, 3 consecutive runs (382 tests, 148 s)
- [x] Recovery robustness tests pass (truncation at every offset, CRC, mid-compaction crash)
- [x] Durability: zero lost acknowledged changes across ≥ 100 random kill -9 runs per fsync mode (100 rounds each, about 4M acknowledged operations, 0 violations; `-f0` write → fsync → reply order proven)
- [x] Compaction churn: disk usage bounded, correct state after restart (1M put/delete pairs, peak 2.1 MB)
- [x] Real-client smoke tests pass with `-b` as well, including a kill -9 and restart
- [x] Engine oracle and invariant proptests still pass; throughput without `-b` not regressed (±5%)
- [x] Throughput with `-b` ≥ 0.8× the reference in every fsync mode (lowest cell 0.98×, `-f0` with 4 KiB bodies, fsync-bound on both)
- [x] `docs/COMPAT.md`, `docs/DESIGN.md` and `docs/BENCH.md` updated

## 5. P2: Operability (detailed plan)

### 5.1 Scope

- **TLS**, optionally with client certificates (mTLS), on listeners of its own; the protocol is unchanged over TLS.
- **Token authentication** as an opt-in protocol extension (`auth <token>`), accepted only on TLS listeners.
- **HTTP endpoints** on a separate, opt-in listener: `/metrics` (Prometheus), `/healthz`, `/readyz`, and a read-only `/admin` JSON view.
- **TOML configuration file** covering all of the above plus the existing flags.
- **Invariant**: with no configuration file and no new flags, behavior is byte-identical to P1 (all differential suites unchanged), and plaintext throughput stays within ±5%.

### 5.2 Facts checked before planning

- Real clients over TLS need no changes: go-beanstalk has `NewConn(io.ReadWriteCloser)` (accepts a `tls.Conn`); greenstalk accepts a ready `socket.socket` (Python's `ssl.SSLSocket` is one).
- rustls (default aws-lc-rs provider) builds and completes a handshake on this machine.
- No existing beanstalkd client library sends an `auth` command; mTLS gives client identity with no protocol change.
- stunnel is not installed (available via Homebrew), so there is no TLS baseline for the reference yet.

### 5.3 Design decisions

1. **Listeners**: `[[listener]]` entries in the config, each with `addr`, `tls` (bool) and `auth` (`none`, `token` or `mtls`). Without a config file, `-l` / `-p` define one plaintext listener as today. New CLI flags are long-only (`--config`, …) and never reuse a reference short flag (including the removed `-c` / `-n`). CLI values override the file; unknown TOML keys are errors.
2. **TLS**: rustls via tokio-rustls, certificate and key from PEM files; `client_ca` enables mTLS (client certificate required). The connection task becomes generic over the stream type (monomorphized, no `Box<dyn>`), so plaintext pays nothing. A TLS close_notify or TCP shutdown maps onto the existing sticky half-close; the 64 KiB read cap still bounds memory.
3. **Token auth**: the codec recognizes `auth <token>` only when the listener uses token auth (`ServerCodec` option, new `Frame::Auth`), so it never reaches the engine and default parsing is unchanged. Before authentication, only `auth` and `quit` are accepted; any other input (including `put`, `stats`, `list-tubes`) gets one `UNAUTHORIZED` reply and the connection is closed, before any engine message (so no counters change and no job id is consumed). A wrong token gets `UNAUTHORIZED` and a close. Tokens come from the config (or a file it names), are compared in constant time and never logged. Token auth on a plaintext listener is a configuration error.
4. **Engine snapshot**: new `Engine::snapshot(now)` returning server stats and per-tube stats, without touching any counter (`cmd-stats` stays unchanged) and without job bodies.
5. **HTTP**: off by default; when enabled binds 127.0.0.1 unless an address is given. Started before binlog replay: `/healthz` = process alive, `/readyz` = 503 until recovery completes. `/metrics` renders the snapshot (server gauges and counters, per-tube series up to a configurable cap, binlog fields). `/admin` is read-only JSON of the same data. No admin actions in P2.
6. **Logging**: level and text/JSON format configurable.

### 5.4 Tasks

| ID | Task | Owner | Depends on | Wave |
|---|---|---|---|---|
| P2-T0 | Contracts: `Frame::Auth` + codec option, `Engine::snapshot`, config schema | lead | — | 0 |
| P2-T1 | Config parsing and validation module (standalone, not wired) | subagent | T0 | 1 |
| P2-T2 | `Engine::snapshot` + metrics / admin rendering from snapshots (standalone) | subagent | T0 | 1 |
| P2-T3 | Harness TLS mode: every case over TLS to ours vs plaintext to the reference | subagent | T0 | 1 |
| P2-T4 | Server wiring: config, listeners, TLS / mTLS, token auth, HTTP endpoints | subagent | T1, T2 | 2 |
| P2-T5 | Tests and benchmarks: TLS differential, real clients over TLS, auth bypass tests, metrics vs `stats`, performance | subagent | T3, T4 | 3 |
| P2-T6 | Adversarial security review of TLS, auth and HTTP code | subagent | T4 | 3 |
| P2-T7 | P2 acceptance | lead | all | 4 |

Only one agent at a time edits the server's wiring (T4); T1–T3 work in separate modules or crates.

### 5.5 Acceptance

- [x] Default configuration: all four differential suites pass unchanged; plaintext throughput within ±5% of P1
- [x] TLS differential mode passes for every case, including the half-close cases
- [x] Real clients (Python greenstalk, Go go-beanstalk) pass the smoke tests over TLS, and with mTLS
- [x] Auth tests: pipelined `auth wrong` followed by a command, `put` before auth, `stats` before auth; none reaches the engine (counters unchanged); constant-time comparison; tokens never logged
- [x] `/metrics` values equal what `stats` / `stats-tube` report; `/readyz` is 503 during binlog replay
- [x] Config: precedence (CLI over file), unknown keys rejected, invalid combinations rejected (token auth without TLS)
- [x] TLS throughput recorded in `docs/BENCH.md`
- [x] Security review findings resolved or documented
- [x] `docs/DESIGN.md`, `docs/COMPAT.md`, README updated

## 6. P3: Raft Replication (detailed plan)

### 6.1 Scope

- **Cluster mode** (opt-in `[cluster]` section): 3 or 5 nodes replicate every engine input through Raft (openraft), so the cluster behaves like one reference server that survives the loss of a minority of nodes.
- **Clients are unchanged**: a client may connect to any node, over any listener type from P2; the protocol, replies and ordering are those of a single server.
- **Membership is static** (listed in the config); the cluster is bootstrapped once with `--cluster-init`. Dynamic membership changes (adding or replacing nodes at runtime) are out of scope for P3.
- **Invariant**: without `[cluster]`, behavior is byte-identical to P2 (all differential suites unchanged) and throughput stays within ±5%.

### 6.2 Facts checked before planning

- **Library**: openraft `0.9.25` (2026-07) is the current stable line and is maintained; `0.10` is still alpha with near-weekly releases; tikv's `raft` crate has had no release since 0.7.0 (2023). We pin `openraft = "=0.9.25"` with the `serde` and `storage-v2` features.
- **openraft 0.9 API**: `RaftLogStorage` / `RaftStateMachine` (storage v2; `apply` receives committed entries in order and returns one response per entry), `RaftNetwork` / `RaftNetworkFactory`, `client_write` and `client_write_ff`, `ensure_linearizable`, `initialize`, `change_membership`. `openraft::testing::Suite::test_all` is a conformance suite for storage implementations. Defaults: heartbeat 50 ms, election timeout 150–300 ms.
- **Commit propagation**: when the commit index advances, the leader immediately sends it to followers (a heartbeat is filled in if nothing else is pending), so a follower applies a committed entry about one round trip after the leader.
- **Engine determinism across processes**: engine `HashMap`s are used for lookups only (iteration appears only in `#[cfg(test)]` helpers); every order-carrying structure is a `Vec`, `Ms` or `BTreeSet`. Time arithmetic uses saturating or signed subtraction, so a lower `now` cannot panic.
- **Engine state is plain data**: all fields of `Engine` except `sys` are integers, `Bytes`, `Vec`, `BTreeSet`, `HashMap` or small structs, so a full-state serialization via serde is straightforward. `sys` is read only when rendering `stats`.
- **No iptables on macOS**: network faults need a user-space mechanism (an in-process simulated network, and a pausable TCP proxy between processes).

### 6.3 Design decisions

1. **What is replicated: every engine input.** A log entry is `{now, input}` where `input` is one of `Connect`, `Disconnect`, `HalfClose`, `PutStarted`, `PutRejected`, `Command`, `Tick`, `SetDraining`, `DropNode`. Applying an entry runs the engine call and then `tick(now)` on every node, the same "tick after every message" rule as today. Because the engine is deterministic, all nodes hold identical state, including connections, watch lists, waiting reserves and reservations. Every reply is therefore linearizable: nothing is acknowledged before it is committed on a majority (commit-before-reply replaces P1's write-before-reply). Considered and rejected: replicating only the binlog journal plus a leader lease; it is faster but loses reservations on failover and relies on clock bounds for safety.
2. **Connections belong to nodes.** `ConnId` packs `(node_id, local_seq)` into the existing `u64` (high 16 bits = node id), so ids are unique across the cluster and never reused. The node holding the socket (the *owner*) forwards the connection's inputs to the leader; each input carries `(conn, seq)` and the state machine ignores duplicates, so an owner may safely resend after a leader change. Every node computes every reply during apply, and **each node delivers the replies for its own connections from its own apply**. As a result, a leader change loses no replies, and a waiting `reserve` on a surviving node stays waiting. Only one input per connection is in flight at a time, as today.
3. **Losing a node.** If a node cannot reach a leader for `cluster.node_timeout` (default 5 s), it closes all of its client connections. If the leader has not heard from a node for `2 × node_timeout`, it proposes `DropNode(id)`, which applies `Disconnect` to every connection that node owns: their reservations return to ready, as when a client disconnects from the reference. An owner that applies a `Disconnect` or `DropNode` for a connection it still holds closes that socket. Connections on surviving nodes, including their reservations, are unaffected by leader changes.
4. **Time.** The leader stamps each entry with `now = max(local wall-anchored clock, last applied now)`, so engine time never goes backwards across leader changes, even with clock skew between nodes. When the leader is idle it proposes `Tick{now}` at `next_deadline()`; followers never tick on their own.
5. **Persistence.** In cluster mode the Raft log plus snapshots replace the binlog; `-b` together with `[cluster]` is a configuration error. The log is a new segmented append-only store in `bstk-raft`: CRC-32C records like `bstk-store`, `fdatasync` before acknowledging an append, batched appends (group commit). A snapshot is the full serialized engine state (serde + postcard), taken every `cluster.snapshot_every` entries (default 100,000) and before purging the log.
6. **Transport and security.** A dedicated cluster port carrying length-prefixed postcard frames over TCP: Raft RPCs plus input forwarding. Cluster traffic requires mTLS by default, reusing P2's TLS code and certificate settings, and a peer's node id must match its certificate. Plaintext is allowed only with an explicit `cluster.insecure_plaintext = true` (tests). No gRPC / tonic.
7. **Protocol-visible behavior in cluster mode.**
   - `stats` shows the pid, hostname, id and rusage of the node the client is connected to.
   - `uptime` counts from the cluster's first entry.
   - The `binlog-*` fields report 0, except `binlog-max-size`.
   - SIGUSR1 on any node proposes a cluster-wide `SetDraining`.
   - A client whose node is cut off from the majority gets no replies until the node rejoins or `node_timeout` closes its connection; it never gets a reply for an uncommitted change.
   - These are recorded in COMPAT as cluster-mode differences.
8. **Monitoring.** `/readyz` is 200 on a node that can reach a leader and has applied up to the commit index it last learned. `/metrics` and `/admin` add role, term, leader id, commit/applied indexes, per-peer replication lag, and snapshot / log sizes.
9. **Two network implementations behind one trait.** The real TCP/TLS transport lives in `bstk-raft`; the in-process simulated network (per-link drop, delay, duplication, partition; seeded) is compiled only for tests or with the `sim` feature, which the server never enables, so no test harness code ships in the binary.

### 6.4 Tasks

| ID | Task | Owner | Depends on | Wave |
|---|---|---|---|---|
| P3-T0 | Contracts: `EngineInput` + `Engine::apply_input`, `EngineState` (serde) + `Engine::export_state` / `Engine::import_state`, `ConnId` packing, serde for the `bstk-proto` types in inputs, `bstk-raft` crate skeleton (type config, entry and RPC types), `[cluster]` config schema | lead | — | 0 |
| P3-T1 | Engine: state export/import with validation, determinism tests (two engines with different hash seeds; restore mid-run and continue ≡ uninterrupted run), oracle proptest still green | subagent | T0 | 1 |
| P3-T2 | `bstk-raft` storage: segmented log store, vote store, snapshot store, state-machine wrapper (apply → engine, `(conn, seq)` duplicate filter, reply routing by owner node); passes `openraft::testing::Suite`; crash / torn-write tests | subagent | T0 | 1 |
| P3-T3 | `bstk-raft` network: framed RPC codec, `RaftNetwork` over TCP/TLS, input forwarding with resend on leader change; simulated network and 3-node in-process cluster harness for tests | subagent | T0 | 1 |
| P3-T4 | Server: cluster mode wiring: config, `--cluster-init`, engine handle (local actor vs cluster), owner-side reply delivery, leader tracking, `Tick` proposals, node timeout / `DropNode`, drain, `/readyz` and metrics | subagent | T1–T3 | 2 |
| P3-T5 | Differential and clients: harness cluster mode (3 local nodes; whole corpus through the leader and through a follower), real-client smoke tests against a cluster, including a leader kill mid-run | subagent | T4 | 3 |
| P3-T6 | Chaos: seeded fault schedules on the in-process cluster; multi-process harness with kill -9, SIGSTOP/SIGCONT and a pausable TCP proxy for partitions; history checker (§6.5) | subagent | T4 | 3 |
| P3-T7 | Cluster benchmarks; adversarial security review of the cluster port and forwarding | subagent(s) | T4 | 3 |
| P3-T8 | P3 acceptance | lead | all | 4 |

Only one agent at a time edits the server's wiring (T4); T1–T3 work in separate crates or modules.

### 6.5 Tests

- **Engine**: proptests that (a) random input sequences give identical outputs and states on two engines built independently (different `HashMap` seeds), (b) exporting and importing the state at any point and continuing gives the same outputs as not doing so, (c) a duplicate `(conn, seq)` input is a no-op.
- **Storage**: the openraft conformance suite; truncation at every byte offset of the last record; restart after crash mid-append and mid-snapshot.
- **Differential**: the whole `.bt` corpus against a 3-node cluster, connected to the leader and, separately, to a follower, compared with the reference (masking the binlog fields and anything COMPAT lists for cluster mode).
- **History checker** (used by T6): every client operation is recorded with send and reply times. A run fails if any of these hold:
  1. An acknowledged `INSERTED` job is missing although no acknowledged `DELETE` removed it.
  2. An acknowledged `DELETED` job reappears.
  3. Job ids are not unique, or not increasing in commit order.
  4. A job is held by two connections at once: a connection gets an acknowledged `DELETED` / `RELEASED` / `BURIED` / `TOUCHED` for a job that another connection reserved after it (per-job linearizability against a model with TTR expiry and disconnect).
  5. Any reply is inconsistent with a single-server execution of the per-job history.

  TTR expiry followed by a new reservation is legitimate beanstalkd behavior and is allowed by the model.
- **Faults** (both harnesses): leader kill, follower kill, kill -9 of all nodes and restart, minority / majority partitions, asymmetric partitions, message loss and delay, paused processes, clock skew between nodes, and a node rejoining with an empty data directory (installed from a snapshot).

### 6.6 Acceptance

- [x] Standalone mode unchanged: all 7 differential suites pass, plaintext throughput within ±5% of P2
- [x] The whole differential corpus passes against a 3-node cluster, through the leader and through a follower
- [x] Real clients (Python, Go) pass the smoke tests against a cluster, including a leader kill mid-run
- [x] Chaos: ≥ 1,000 seeded in-process fault schedules and ≥ 100 multi-process fault runs with zero checker violations (final run with data wipes on: 2,000 in-process seeds, 500 of them 5-node, and 100 multi-process runs, 0 failures; six bugs found and fixed on the way, see the P3 commits and DESIGN §8)
- [x] Failover: after a leader kill, a new leader serves within 2 s (1.3–1.5 s measured); connections and reservations on surviving nodes are kept; connections on the lost node are closed
- [x] A wiped node rejoins from a snapshot and converges; a full-cluster kill -9 and restart loses no committed change
- [x] Engine determinism and state export/import proptests pass; openraft storage conformance suite passes
- [x] Cluster throughput and latency recorded in `docs/BENCH.md`; target ≥ 50k ops/s for put-reserve-delete with 100 connections on a 3-node cluster on one machine (135k via the leader, 108k via a follower)
- [x] Security review of the cluster port resolved or documented
- [x] `docs/DESIGN.md`, `docs/COMPAT.md`, README updated

## 7. P4: Performance (detailed plan)

### 7.1 Scope

- **Standalone efficiency**: close the CPU-efficiency gap to the reference without losing throughput or multi-core use for TLS and many connections.
- **Complexity**: no per-operation cost that grows with a count a client controls (buried jobs, reservations held by one connection).
- **Footprint**: memory per job and binlog bytes written per operation, measured against the reference.
- **Cluster efficiency**: CPU per operation in cluster mode, especially at low load, and snapshot memory (security review M5).
- **Invariants**: all differential suites, chaos acceptance and smoke tests stay green; no protocol-visible change.
- **Out of scope**: openraft 0.10 (still alpha), dynamic membership.

### 7.2 Facts checked before planning

- **Throughput vs the reference** (T6b, P2, P3 benchmarks): every non-pipelined standalone cell is at 0.88x to 1.13x of the reference, and pipelined cells at 1.40x to 1.60x.
- **CPU efficiency** is the real gap: at 100 connections we do about 52k operations per CPU-second, against about 124k for the reference (0.42x); we use 1.8 to 4.2 cores where the reference uses one.
- **Where our time goes**: the T6b profile after the index work shows the engine actor on-CPU about 7% of the time. Most process time is tokio worker park/unpark and socket syscalls: every command crosses threads twice (connection task → engine actor → connection task).
- **Linear-time removals**: removing a buried job (`TubeState::buried`, a `VecDeque`, `engine.rs` ~1129) and removing a job from a connection's reservations (`ConnState::reserved_fifo`, a `Vec`, ~1188) are O(n). The reference uses intrusive linked lists (`dat.h` `Job *prev, *next`), so both are O(1) there. A client can make them quadratic, e.g. by deleting 1M buried jobs, or by releasing jobs out of order on a connection holding 100k.
- **Cluster CPU**: about 20 µs per operation at 100 connections (standalone about 10 µs). At one connection it is about 260 µs, mostly waking parked threads on every node for every entry. Every node builds every reply, including `stats` YAML, even for connections it does not own.

### 7.3 Design decisions

1. **Standalone hand-off**: decided by measurement. A spike (P4-T1) builds two prototypes outside the main tree:
   - (a) the engine actor drains many messages per wake-up (still ticking between messages), and connection tasks batch their reply writes, to cut wake-ups;
   - (b) an opt-in single-threaded mode (`server.threads = 1`: tokio current-thread runtime, engine called inline by connection tasks, no channels), like the reference's event loop.

   The lead picks by the numbers: adopt what reaches the targets in 7.5. If both help, (a) becomes the default and (b) the opt-in. The multi-threaded runtime stays the default either way, because TLS and many connections need several cores.
2. **O(1) removals**: buried lists and per-connection reservations become order-preserving structures with O(1) or O(log n) removal (e.g. an index-linked list in a slab, or a `BTreeMap` keyed by insertion sequence). The observable order stays exactly the reference's (buried FIFO for kick and peek-buried, reservation order for DEADLINE_SOON and disconnect release). The engine oracle proptest guards equivalence, as in T6b.
3. **Footprint**: measure first (1M jobs of 16 B and 1 KiB; bytes per job, RSS after a delete-all), then fix only what is clearly above the reference. The same goes for binlog bytes written per operation under churn with default and small `-s`.
4. **Cluster**:
   - `Engine::apply_input` gains a way to skip building replies for connections the node does not own. State and timers are unaffected, since replies are pure. Determinism tests compare state, and replies only where they are built.
   - Fewer wake-ups at low load (coalescing notifications between the Raft core, the state machine and the actors).
   - Snapshots are streamed to and from files rather than held as several in-memory copies.

### 7.4 Tasks (sequential: one subagent at a time; the lead picks each subagent's model)

| ID | Task | Owner |
|---|---|---|
| P4-T0 | Plan; bench additions for ops per CPU-second, memory and bytes written | lead |
| P4-T1 | Spike: prototypes (a) and (b), profile both, report numbers; no merge | subagent |
| P4-T2 | Implement the chosen hand-off design in the server | subagent |
| P4-T3 | Engine: O(1) buried and reservation removal; 1M-scale tests; oracle equivalence | subagent |
| P4-T4 | Footprint measurement (memory per job, binlog bytes per op) vs reference, and fixes if needed | subagent |
| P4-T5 | Cluster efficiency: owner-only replies, fewer wake-ups, streamed snapshots | subagent |
| P4-T6 | Full benchmark matrix (standalone, `-b`, TLS, cluster), chaos acceptance re-run, smoke tests | subagent |
| P4-T7 | P4 acceptance | lead |

### 7.5 Acceptance

- [ ] Standalone plaintext: ops per CPU-second ≥ 0.8x the reference at 10 and 100 connections, and throughput ≥ 1.0x the reference in every non-pipelined matrix cell (was 0.88x at worst)
- [ ] TLS, `-b` and cluster throughput not below their P3 numbers (±5%)
- [ ] Deleting or kicking 1M buried jobs, and releasing 100k reservations of one connection in random order, run in time linear in the count; the oracle proptest still passes
- [ ] Memory per job ≤ 1.5x the reference and binlog bytes written per operation ≤ 1.2x the reference (or a documented reason)
- [ ] Cluster: CPU per operation at 100 connections ≤ 15 µs (was 20); one-connection CPU per operation at least halved; snapshot peak memory ≤ 1.5x the state size
- [ ] All differential suites, chaos acceptance (≥ 1,000 in-process seeds, ≥ 100 multi-process runs) and smoke tests green
- [ ] `docs/BENCH.md`, `docs/DESIGN.md` updated
