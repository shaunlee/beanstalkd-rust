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
| P2 | TLS / mTLS, auth extension, HTTP metrics / healthz / admin, TOML config | differential tests 100% with default config; real clients connect over TLS |
| P3 | openraft, Tick proposals, replicated reservations, follower proxy | on a 3-node cluster, random kills / partitions lose no acknowledged job and never double-deliver a reserved job (Jepsen-style tests) |
| P4 | Profiling and optimization (incl. compaction write amplification: about one extra record per operation under churn with small `-s`) | throughput ≥ 1× reference; multi-core scaling curve |

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
