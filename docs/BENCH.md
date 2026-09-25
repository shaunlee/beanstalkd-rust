# Benchmarks (P0, task T6)

`beanstalkd-rs` against the reference C beanstalkd (commit `25085c5`),
driven by the `bstk-bench` load generator (`bench/`).

**Summary.** At 1 to 10 connections, and at 100 connections on a single
shared tube, `beanstalkd-rs` reaches 0.87x to 1.11x of the reference's
throughput. The **0.8x bar is missed in one workload**: 100 connections
each on **its own tube** (`put-reserve-delete`, 100 conns) gets 0.64x to
0.67x, and 0.39x when pipelined. Profiling shows the single engine actor is
saturated. Every command pays for several O(#tubes + #connections) scans in
`Engine::tick`, `Engine::next_deadline` and `process_queue`, and each scan
SipHash-es and clones tube names (see [Hot spots](#hot-spots)). Our server
also uses 2 to 3.3 cores where the reference uses 1. The fixes are
engine-internal and are deferred to P4, as PLAN.md T6 allows.

## Environment

| | |
|---|---|
| Machine | Apple M6, 12 cores (hw.perflevel0 "Super": 2, hw.perflevel1 "Performance": 4, hw.perflevel2 "Efficiency": 6), 32 GB RAM |
| OS | macOS 27.0 (Darwin 27.0.0, arm64) |
| Rust | rustc 1.98.1 (48a229cea 2026-09-01); `beanstalkd-rs` and `bstk-bench` built with `cargo build --release` (workspace `[profile.release]`: opt-level 3, `debug = 1`) |
| Reference | `.ref/beanstalkd` sources copied and rebuilt with `make CFLAGS=-O2 beanstalkd` (Apple clang 21.0.0). The repo's `scripts/build-ref.sh` builds **without** `-O` (`-g` only), which would be an unfair baseline. The smoke tests use that default build. |
| Load generator | `bstk-bench` on the same machine, tokio multi-thread runtime with its default of 12 workers, loopback TCP, `TCP_NODELAY` |
| Servers | `-l 127.0.0.1 -p <free port>`, default `-z`; a fresh server process for every run |
| Background load | **Noisy**: another agent was compiling on the same machine throughout. The 1-minute load average was 5.7 to 18 during the matrix. Cells whose first-pass runs spread by more than 20% were re-run (see below). |
| `ulimit -n` | 1048576 |

Commands (the scratch paths are specific to this run):

```sh
export CARGO_TARGET_DIR=...                   # any
cargo build --release -p bstk-server -p bstk-bench
rsync -a --exclude .git --exclude '*.o' --exclude beanstalkd .ref/beanstalkd/ /tmp/ref-O2/
make -C /tmp/ref-O2 -j12 CFLAGS=-O2 beanstalkd

# Full matrix: 3 scenarios x conns {1,10,100} x body {16,4096} x 3 runs x 2 servers, 5 s each.
REF_BIN=/tmp/ref-O2/beanstalkd OUT_CSV=matrix.csv bench/run-matrix.sh
# Pipelined extra cells.
REF_BIN=/tmp/ref-O2/beanstalkd OUT_CSV=matrix.csv \
  SCENARIOS=put-reserve-delete CONNS="10 100" BODIES=16 PIPELINES=16 bench/run-matrix.sh
bench/summarize.py matrix.csv

# Stress (task T6): 100 connections, 30 s, each server.
bstk-bench --addr 127.0.0.1:PORT --conns 100 --duration 30 --scenario put-reserve-delete
```

Each run of `bench/run-matrix.sh` starts a fresh server and runs
`bstk-bench ... --json`. The two servers alternate run by run, so drift in
the background load hits both equally.

The raw per-run CSVs are in `bench/results/`:

- `2026-09-25-matrix-first-pass.csv`: the whole first pass.
- `2026-09-25-matrix.csv`: the table below. Five cells that spread by more
  than 20% in the first pass (put-reserve-delete 1x4096 and 10x{16,4096};
  producers-consumers 2x16 and 100x16) were re-run 3 more times, and those
  runs replace the first pass. All re-runs were within 20%.

## What is measured

- **ops/s**: completed protocol operations per second (each put, reserve
  and delete counts as one) inside the measured window. The whole run fails
  on any unexpected reply or a reply slower than 10 s. At the end, `stats`
  must show 0 ready/reserved/delayed/buried jobs. Every run in this document
  passed both checks.
- **put-reserve-delete**: every connection loops put → reserve → delete on
  **its own tube**, so N connections means N tubes.
- **producers-consumers**: N/2 producers `put` into one shared tube.
  N/2 consumers loop `reserve-with-timeout 1` → `delete`. Consumers do two
  ops per job, so a ready backlog builds up. They drain it after the
  deadline, and that drain is not measured.
- **put-only**: all connections `put` into one shared tube. The jobs are
  drained afterwards, and the drain is not measured.
- **pipe**: `--pipeline D` writes D commands of one kind back to back
  (D puts, then D reserves, then D deletes). Latency is measured from the
  batch write to each reply.
- **CPU %**: server `rusage-utime + rusage-stime` (from `stats`) over the
  measured window. 100% is one core.
- Values are the **median of 3 runs**, 5 s each. Latencies are the median
  of the per-run p99s.

## Results

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | ref CPU % | rs CPU % | ref put p99 µs | rs put p99 µs | ref reserve p99 µs | rs reserve p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 1 | 16 | 1 | 46,066 | 46,217 | 1.00 | 34 | 38 | 53 | 50 | 54 | 51 |
| put-reserve-delete | 1 | 4096 | 1 | 36,265 | 33,467 | 0.92 | 33 | 37 | 75 | 79 | 76 | 80 |
| put-reserve-delete | 10 | 16 | 1 | 112,642 | 104,839 | 0.93 | 87 | 202 | 195 | 180 | 195 | 182 |
| put-reserve-delete | 10 | 4096 | 1 | 109,204 | 103,637 | 0.95 | 89 | 207 | 223 | 184 | 187 | 184 |
| **put-reserve-delete** | **100** | 16 | 1 | 133,744 | 85,612 | **0.64** | 98 | 258 | 1,045 | 1,496 | 1,045 | 1,529 |
| **put-reserve-delete** | **100** | 4096 | 1 | 119,596 | 79,814 | **0.67** | 98 | 265 | 1,445 | 1,519 | 1,022 | 1,603 |
| producers-consumers | 2 | 16 | 1 | 47,318 | 48,707 | 1.03 | 47 | 61 | 99 | 94 | 101 | 96 |
| producers-consumers | 2 | 4096 | 1 | 53,392 | 46,437 | 0.87 | 48 | 62 | 83 | 97 | 80 | 97 |
| producers-consumers | 10 | 16 | 1 | 110,792 | 108,796 | 0.98 | 87 | 195 | 195 | 174 | 196 | 174 |
| producers-consumers | 10 | 4096 | 1 | 106,073 | 102,752 | 0.97 | 88 | 206 | 222 | 184 | 183 | 184 |
| producers-consumers | 100 | 16 | 1 | 127,850 | 124,810 | 0.98 | 99 | 309 | 1,043 | 1,068 | 1,046 | 1,067 |
| producers-consumers | 100 | 4096 | 1 | 132,068 | 129,728 | 0.98 | 98 | 325 | 1,263 | 1,038 | 882 | 1,037 |
| put-only | 1 | 16 | 1 | 37,758 | 35,369 | 0.94 | 33 | 36 | 73 | 76 | - | - |
| put-only | 1 | 4096 | 1 | 33,598 | 31,203 | 0.93 | 34 | 39 | 77 | 82 | - | - |
| put-only | 10 | 16 | 1 | 110,137 | 107,545 | 0.98 | 85 | 185 | 196 | 178 | - | - |
| put-only | 10 | 4096 | 1 | 101,484 | 103,127 | 1.02 | 91 | 209 | 203 | 182 | - | - |
| put-only | 100 | 16 | 1 | 132,966 | 126,742 | 0.95 | 99 | 293 | 1,018 | 1,038 | - | - |
| put-only | 100 | 4096 | 1 | 110,986 | 123,504 | 1.11 | 99 | 330 | 1,181 | 1,104 | - | - |
| put-reserve-delete | 10 | 16 | 16 | 210,372 | 215,578 | 1.02 | 99 | 319 | 967 | 876 | 834 | 903 |
| **put-reserve-delete** | **100** | 16 | 16 | 202,158 | 78,698 | **0.39** | 99 | 208 | 8,957 | 19,497 | 8,717 | 26,751 |

### Stress run (T6 acceptance)

`put-reserve-delete`, 100 connections, 30 s, 16-byte bodies, pipeline 1,
one run per server:

| server | ops/s | put p50 / p99 / p999 / max (µs) | server CPU | RSS after | errors / hangs | jobs left (stats) |
|---|---:|---|---:|---:|---|---|
| reference (-O2) | 156,813 | 627 / 931 / 1,112 / 1,865 | 99% | 2.1 MB | none | 0 |
| beanstalkd-rs | 89,332 | 1,165 / 1,488 / 2,095 / 18,105 | 257% | 5.6 MB | none | 0 |

Both servers pass: no unexpected replies, no reply slower than 10 s, and
`stats` shows `current-jobs-{ready,reserved,delayed,buried}` = 0 after the
run. Reserve and delete latencies are within 1% of put's for both servers.

## Analysis

- **Single tube or few connections: at parity.** With 1 or 10 connections,
  and with 100 connections sharing one tube (producers-consumers,
  put-only), `beanstalkd-rs` gets 0.87x to 1.11x, with similar or better
  p99. The noisiest cells (0.87x at 2x4096 producers-consumers, 1.11x at
  100x4096 put-only) sit within the run-to-run noise of this machine.
- **Many tubes: below the bar.** With 100 connections each on its own tube,
  throughput drops to 0.64x to 0.67x, and to 0.39x when pipelined. The
  pipelined case keeps the engine busiest, so it suffers most. The
  reference is barely affected by the number of tubes (134k vs 128k to
  133k ops/s).
- **CPU.** The reference never exceeds one core. `beanstalkd-rs` uses about
  the same CPU at 1 connection (37% vs 34%), but 2x to 3.3x more at
  10 to 100 connections for about the same throughput. At 100 connections,
  ops per CPU-second are about 0.3x the reference's (e.g. 126.7k ops/s at
  293% vs 133.0k ops/s at 99% for put-only). At 10 connections the figure
  is about 0.45x. The cost is mostly cross-thread hand-offs:
  each command goes connection task → mpsc → engine actor → mpsc →
  connection task. Two tokio wake-ups per command, often across worker
  threads, plus idle workers parking and unparking (`__psynch_cvwait` and
  `__psynch_cvsignal` dominate the process-wide profile).
- **Scaling.** The engine actor is one task, so the ceiling is one core of
  engine work. Today that core is spent mostly on scans that do not depend
  on the command (below), not on the command itself.

## Hot spots

Profile: `sample <pid> 8` on `beanstalkd-rs` during
`put-reserve-delete --conns 100` (release build with `debug = 1`).
The engine actor task was on-CPU for 2,592 of 2,660 samples per thread,
i.e. **one core saturated**. Inclusive shares of the engine actor's time:

| inclusive | where | cause |
|---:|---|---|
| 44% | `Engine::tick` | called **after every message** by `engine_actor::run`. It walks all delayed queues (`soonest_delayed_job`), clones `tube_order.items` (a `Vec<TubeName>`, i.e. 100 `String` clones) to check pause expiry, then scans **all connections** via `conn_tickat` |
| 21% | `Engine::next_deadline` | called after every message. It scans all tubes (`soonest_delayed_job` and pauses) and **all connections** again |
| 23% | `process_queue` (inside `Engine::handle`, on put) | clones `tube_order.items` and does a `HashMap<TubeName, _>` lookup for every tube, on every loop iteration |
| 40% (overlaps the above) | SipHash `hash_one::<TubeName>` + `memcmp` | the per-tube `self.tubes.get(name)` lookups inside those scans |
| 24% (overlaps) | `soonest_delayed_job` | O(#tubes) with a `TubeName` clone per improvement, reached from both `tick` and `next_deadline` |
| 16% / 9% / 7% | `clone` / `malloc` / `free` | the `tube_order` and `TubeName` clones above |

With N tubes and N connections, each command costs about 3 x O(N) hashed
lookups plus about 2N small allocations. The reference also runs
`prottick` once per event-loop iteration (`serv.c: srvserve`), but that
pass is cheap. Connections sit in a heap ordered by `tickat`, so only the
head is examined. Tubes are walked by pointer (`tubes.items[i]`), with no
hashing, no cloning and no allocation. Tick and next-deadline are a single
pass, not two.

### Suggestions (P4, engine and actor; not applied here)

1. **Make the per-message tick nearly free.** Cache the next deadline and
   skip `tick` unless `now >= cached_deadline`. Merge `tick` and
   `next_deadline` into one pass that returns the next deadline, as
   `prottick` does. Together with item 3, this removes most of the
   44% + 21%.
   DESIGN.md / COMPAT.md engine item 5 requires an immediate tick in some
   boundary cases. Make that conditional on the deadline actually being due
   rather than unconditional.
2. **Batch the actor loop.** After `rx.recv()`, drain everything already
   queued with `try_recv` (bounded, e.g. 64 messages), then run
   tick/next_deadline and reset the `Sleep` **once per batch**, not once per
   message. This also amortizes the timer re-arm (`Sleep` is about 4%).
3. **Index deadlines instead of scanning.**
   - Keep a `BTreeSet<(tickat, ConnId)>` of connection deadlines, updated
     when a connection's reserved set or waiting state changes, instead of
     iterating `self.conns` in both `tick` and `next_deadline`.
   - Keep a global `BTreeSet<(deadline, tube_order_index, JobId)>` for
     delayed jobs. It preserves the reference's first-tube-wins tie-break
     and removes the per-tube scan in `soonest_delayed_job`.
   - Keep a set of paused tubes.
4. **Make `process_queue` proportional to the tubes that can match.** Keep
   the set of tubes that have waiting connections (usually tiny), iterate
   indices instead of cloning `tube_order.items`, and skip the loop when
   that set is empty (the common case for put-reserve-delete: nobody is
   blocked in reserve).
5. **Cheaper tube identity.** Intern tubes as a small integer `TubeId` (a
   slab or `Vec` index) or `Arc<str>`, and use an integer-keyed map or a
   faster hasher (FxHash or ahash) for `tubes` and `jobs`. This removes the
   SipHash + `memcmp` + `String` clone cost everywhere.
6. **CPU efficiency (server).** Consider a `current_thread` runtime, or a
   small worker count, for the engine plus connections when the connection
   count is low. Alternatively, handle replies without a second hop, e.g.
   a `oneshot`/`Notify` per connection kept alive across commands, to cut
   cross-thread wake-ups. Measure `ops/CPU-s`, not only ops/s.

Items 1 to 4 are local to `bstk-engine` / `engine_actor.rs` and should
restore the many-tube case to the same level as the single-tube case
(0.95x to 1.1x). Re-run with `bench/run-matrix.sh` to confirm.

## Reproducing on a quiet machine

The numbers above were taken while another build was running. For
publishable numbers, re-run the matrix on an idle machine (check `uptime`
first). Consider `--threads` to cap the load generator's workers, and
compare `ops/s` together with CPU %.
