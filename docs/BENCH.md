# Benchmarks

`beanstalkd-rs` against the reference C beanstalkd (commit `25085c5`),
driven by the `bstk-bench` load generator (`bench/`). The newest numbers
are from task P1-T5 (write-ahead log, `-b`), first below. The T6b section
(engine performance fix) follows, and the T6 first pass, whose profile
motivated T6b, is kept at the end.

## P1: write-ahead log (`-b`)

### Summary (P1-T5)

- **Every `-b` cell reaches at least 0.8x of the optimized reference, in
  every fsync mode.** With `-F` and the default `-f 50`, beanstalkd-rs runs
  at 1.25x to 1.73x of the reference. One noisy cell reached 2.44x: `-F`
  10x16, where the reference's own runs spread by more than 20%. With `-f0`, the 16-byte cells run at
  1.23x to 1.73x. The 4 KiB `-f0` cells are fsync-bound on both servers
  (about 21k to 31k ops/s, neither server uses half a core), so they come
  out at parity: 0.98x to 1.04x over 5 runs each. Their first 3-run pass gave
  0.81x to 1.20x, with runs spread by more than 20%.
- **No regression without `-b`.** In the same session the no-`-b` cells are
  1.12x to 1.33x of the reference. In T6b they were 0.88x to 1.11x (see below).
  Without `-b` the engine actor is still a tokio task; only `-b` moves it to
  an OS thread.
- Why `-b` costs us less than it costs the reference: the reference's
  `filewrjobshort` / `filewrjobfull` (file.c) issue one `write()` per field.
  That is 2 syscalls per update record and 4 per full put record, all on its
  single event-loop thread. beanstalkd-rs encodes one message's records into
  a buffer and issues one `pwrite` per engine message (wal.rs `flush`), on
  the engine thread, while connection I/O runs on other threads.
  Its CPU use is higher (1.6 to 3 cores vs at most 1), as without `-b`.

### Environment (P1-T5)

| | |
|---|---|
| Machine | Apple M6, 12 cores, 32 GB RAM; APFS on the internal SSD (binlog directories under `/private/tmp`) |
| OS | macOS 27.0 (Darwin 27.0.0, arm64) |
| Rust | rustc 1.98.1; release build (opt-level 3, `debug = 1`) |
| Reference | `.ref/beanstalkd-opt/beanstalkd` (`scripts/build-ref.sh --optimized`, `-O2`) |
| Load generator | `bstk-bench`, same machine, loopback, 5 s per run, a fresh server process and a fresh empty binlog directory per run, ref and rs runs alternating |
| fsync | Both servers use plain `fsync`/`fdatasync`, not `F_FULLFSYNC` (docs/COMPAT.md, Binlog §10). On macOS this reaches the drive cache, not stable storage, so the absolute `-f0` numbers are optimistic for both servers. |
| Background load | **Not idle.** There was no compilation or test activity during the runs. An OrbStack VM owned by another user (a Docker soak test) was running. The 1-minute load average was 6.1 at the start and 5.7 at the end, and 3.4 to 12.3 across runs (CSV `load_avg` column). Because the runs alternate, drift affects both servers alike. |

### Commands (P1-T5)

```sh
scripts/build-ref.sh --optimized
cargo build --release -p bstk-server -p bstk-bench
# SERVER_MODES (new): none | F | default | f0, applied to both servers;
# every run gets a fresh -b directory under BINLOG_ROOT, removed afterwards.
OUT_CSV=p1-matrix.csv SCENARIOS="put-reserve-delete producers-consumers" \
  CONNS="10 100" BODIES="16 4096" RUNS=3 DURATION=5 \
  SERVER_MODES="none F default f0" bench/run-matrix.sh
# Re-run of the noisy fsync-bound cells with 5 runs.
OUT_CSV=p1-f0-rerun.csv SCENARIOS="put-reserve-delete producers-consumers" \
  CONNS="10 100" BODIES=4096 RUNS=5 SERVER_MODES=f0 bench/run-matrix.sh
bench/summarize.py p1-matrix.csv
```

The CSVs have a new `server_mode` column. `summarize.py` groups by it and
reads old CSVs, which lack it, as mode `none`. Server arguments: `-b <dir>`
with default `-s` (10 MiB), plus `-F` or `-f0`. Raw data:
`bench/results/2026-09-25-p1-matrix.csv` and
`bench/results/2026-09-25-p1-f0-rerun.csv`.

### Results (P1-T5)

Medians of 3 runs (`*` = runs spread by more than 20%). The 4 KiB `-f0`
rows show the 5-run re-run; the first 3-run pass is in brackets.

| mode | scenario | conns | body | ref ops/s | rs ops/s | rs/ref | ref CPU % | rs CPU % | ref put p99 µs | rs put p99 µs |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | put-reserve-delete | 10 | 16 | 65,984* | 87,947* | 1.33 | 94 | 163 | 244 | 212 |
| F | put-reserve-delete | 10 | 16 | 47,346* | 115,759* | 2.44 | 83 | 176 | 397 | 208 |
| default | put-reserve-delete | 10 | 16 | 75,990* | 110,669* | 1.46 | 82 | 181 | 233 | 206 |
| f0 | put-reserve-delete | 10 | 16 | 32,846* | 56,801* | 1.73 | 65 | 160 | 548 | 298 |
| none | put-reserve-delete | 10 | 4096 | 135,948 | 175,792 | 1.29 | 73 | 138 | 131 | 109 |
| F | put-reserve-delete | 10 | 4096 | 105,610 | 134,342 | 1.27 | 95 | 172 | 210 | 142 |
| default | put-reserve-delete | 10 | 4096 | 106,388 | 132,542 | 1.25 | 95 | 170 | 202 | 144 |
| f0 | put-reserve-delete | 10 | 4096 | 26,852* | 27,399* | 1.02 (1.04) | 35 | 52 | 395 | 306 |
| none | put-reserve-delete | 100 | 16 | 190,102 | 219,985 | 1.16 | 99 | 180 | 709 | 617 |
| F | put-reserve-delete | 100 | 16 | 126,124 | 200,634 | 1.59 | 98 | 242 | 1,292 | 718 |
| default | put-reserve-delete | 100 | 16 | 128,791 | 202,455 | 1.57 | 99 | 236 | 1,207 | 729 |
| f0 | put-reserve-delete | 100 | 16 | 50,246 | 70,486 | 1.40 | 62 | 122 | 4,456 | 2,112 |
| none | put-reserve-delete | 100 | 4096 | 180,961 | 202,883 | 1.12 | 99 | 206 | 941 | 647 |
| F | put-reserve-delete | 100 | 4096 | 110,235 | 175,790 | 1.59 | 99 | 273 | 8,545 | 1,865 |
| default | put-reserve-delete | 100 | 4096 | 109,082 | 173,493 | 1.59 | 98 | 266 | 8,838 | 2,064 |
| f0 | put-reserve-delete | 100 | 4096 | 23,087* | 24,022* | 1.04 (0.93) | 31 | 50 | 10,397 | 3,221 |
| none | producers-consumers | 10 | 16 | 141,413 | 179,357 | 1.27 | 70 | 133 | 111 | 105 |
| F | producers-consumers | 10 | 16 | 111,961 | 140,186 | 1.25 | 91 | 167 | 155 | 128 |
| default | producers-consumers | 10 | 16 | 111,475 | 139,965 | 1.26 | 90 | 167 | 163 | 130 |
| f0 | producers-consumers | 10 | 16 | 50,463 | 61,887 | 1.23 | 63 | 118 | 288 | 235 |
| none | producers-consumers | 10 | 4096 | 128,196 | 166,333 | 1.30 | 73 | 151 | 132 | 124 |
| F | producers-consumers | 10 | 4096 | 98,096 | 124,803 | 1.27 | 97 | 185 | 213 | 170 |
| default | producers-consumers | 10 | 4096 | 96,175 | 124,815 | 1.30 | 96 | 181 | 236 | 174 |
| f0 | producers-consumers | 10 | 4096 | 23,987* | 23,718* | 0.99 (1.20) | 34 | 50 | 469 | 363 |
| none | producers-consumers | 100 | 16 | 170,879* | 199,859* | 1.17 | 94 | 207 | 751 | 688 |
| F | producers-consumers | 100 | 16 | 106,897* | 164,339* | 1.54 | 98 | 268 | 1,468 | 983 |
| default | producers-consumers | 100 | 16 | 105,839* | 183,411* | 1.73 | 98 | 269 | 1,489 | 774 |
| f0 | producers-consumers | 100 | 16 | 46,453 | 60,590 | 1.30 | 59 | 116 | 2,908 | 2,148 |
| none | producers-consumers | 100 | 4096 | 173,899 | 195,662 | 1.13 | 98 | 232 | 955 | 699 |
| F | producers-consumers | 100 | 4096 | 101,023 | 168,223 | 1.67 | 98 | 297 | 8,483 | 1,560 |
| default | producers-consumers | 100 | 4096 | 100,728 | 164,667 | 1.63 | 98 | 292 | 8,589 | 1,552 |
| f0 | producers-consumers | 100 | 4096 | 26,770* | 26,306* | 0.98 (0.81) | 36 | 55 | 9,921 | 3,673 |

- **Acceptance (≥ 0.8x in every `-b` cell): met.** The lowest `-b` cell is
  `-f0` producers-consumers 100x4096: 0.98x over 5 runs, 0.81x in the first
  3-run pass.
- **The 4 KiB `-f0` cells are bound by the device.** Each server
  fsyncs once per journaled write: the reference once per record from
  `walwrite`, beanstalkd-rs once per engine message (usually one record).
  Throughput is the same (about 21k to 31k ops/s) at 10 and 100
  connections. The spread between identical runs is 20 to 50% on both
  servers, and neither is CPU-bound (reference 31 to 37%, beanstalkd-rs 46
  to 67%). The 3-run medians (0.81x to 1.20x) are within that noise. The
  5-run re-run gives 0.98x to 1.04x, and the individual runs overlap
  completely (reference 20.2k to 29.0k, beanstalkd-rs 21.0k to 31.4k ops/s;
  `bench/results/2026-09-25-p1-f0-rerun.csv`). The 16-byte `-f0` cells are
  faster (47k to 70k), and there beanstalkd-rs leads by 1.23x to 1.73x.
- **No `-b`: no regression.** Ratios in this session are 1.12x to 1.33x for
  these 8 cells, vs 0.88x to 1.11x in T6b. Absolute ops/s differ between
  sessions because of background load: the reference itself moved, for
  example 121k to 190k at 100x16 put-reserve-delete. The engine actor still
  runs as a tokio task without `-b`, so nothing on that path changed in P1.

### Durability and compaction (P1-T5)

These are correctness results, recorded here with their run times. The
tests are in `crates/server/tests/durability.rs`, and the module docs give
the commands.

- **Kill -9 torture, 100 rounds per fsync mode, release build.** 4
  concurrent clients per round. The server is SIGKILLed at a random moment
  (0 to 1.5 s into the round) and restarted on the same directory, with `-s`
  alternating between 64 KiB and 256 KiB. Every model job is checked after
  each restart (state, pri, tube, body, and the `stats` totals).

  | mode | rounds | acked journaled ops | ins / del / bury / release+delay / kick | unjournaled acks | unacked at kill | lost-reply puts found on disk | max binlog | violations |
  |---|---:|---:|---|---:|---:|---:|---:|---:|
  | `-F` | 100 | 1,688,790 | 434,736 / 433,807 / 303,416 / 281,366 / 235,465 | 1,069,146 | 348 | 6 | 2.35 MB (33 files) | **0** |
  | default | 100 | 1,321,708 | 340,152 / 339,343 / 237,833 / 219,679 / 184,701 | 839,437 | 356 | 10 | 2.14 MB (33 files) | **0** |
  | `-f0` | 100 | 1,004,659 | 258,988 / 258,183 / 180,698 / 166,787 / 140,003 | 639,791 | 366 | 20 | 2.21 MB (34 files) | **0** |

- **Compaction churn (`-s 65536`).** A steady set of 1,000 jobs (60%
  ready, 20% buried, 20% delayed, 7 tubes, about 382 KB of records), one
  steady job replaced every 500 churn pairs, and 1,000,000 put + delete
  pairs from 4 pipelining connections (bodies 16 B to 4,000 B). Throughput
  was about 126k ops/s. The binlog peaked at 2.10 MB (33 files) during the
  1M churn, and 1.16M compaction moves ran. After SIGTERM the directory held
  1.9 MB, and after a further 100k pairs and SIGKILL it held 2.0 MB. The
  exact steady set (state, pri, tube, body and `stats` counts) was
  recovered after both restarts.

## Summary (T6b)

- **Every matrix cell now reaches at least 0.88x of the optimized (-O2)
  reference.** The cell that failed in T6, 100 connections each on its own
  tube, went from 0.50x/0.54x to 1.04x/1.11x. Pipelined `put-reserve-delete`
  went from 0.93x/0.34x (10/100 conns) to 1.40x/1.60x.
- **Scaling scenario:** 10 active connections alongside 10,000 idle
  connections and 10,000 tubes holding delayed jobs run at **0.96x** of the
  same load with no idle connections or tubes (116,964 vs 121,293 ops/s).
  Before T6b the same scenario collapsed to 724 ops/s (0.007x). The reference
  drops to 11,018 ops/s (0.09x of its own zero-idle 120,151 ops/s) because
  its `prottick` still walks every tube on every event-loop iteration.
- **What changed:** `Engine::tick` and `Engine::next_deadline` no longer scan
  every tube and connection. Deadlines live in ordered indexes, so `tick`
  returns at once when nothing is due. `process_queue` visits only tubes that
  have both waiters and ready jobs. Tubes are slab entries with integer ids.
  The engine actor re-arms its timer only when the deadline changes. See
  [What changed](#what-changed-t6b). There are no observable changes: 189/189
  differential cases pass, and a 10,000-case proptest compares the new
  engine step by step with a frozen copy of the old one.

## Environment (T6b)

| | |
|---|---|
| Machine | Apple M6, 12 cores (2 "Super" + 4 "Performance" + 6 "Efficiency"), 32 GB RAM |
| OS | macOS 27.0 (Darwin 27.0.0, arm64) |
| Rust | rustc 1.98.1; `beanstalkd-rs` and `bstk-bench` built with `cargo build --release` (opt-level 3, `debug = 1`) |
| Reference | `scripts/build-ref.sh --optimized`: a copy of `.ref/beanstalkd` built with `make CFLAGS=-O2` at `.ref/beanstalkd-opt/beanstalkd`. The Makefile appends its own flags (`override CFLAGS+=-Wall -Werror -Wformat=2 -g`), so the compile line is `cc -O2 -Wall -Werror -Wformat=2 -g`. The default debug build at `.ref/beanstalkd/beanstalkd`, used by the differential and smoke tests, is untouched. |
| Load generator | `bstk-bench` on the same machine, tokio multi-thread runtime (12 workers), loopback TCP, `TCP_NODELAY` |
| Servers | `-l 127.0.0.1 -p <free port>`, default `-z`; a fresh server process for every run; runs alternate ref / rs |
| `ulimit -n` | 1048576 (soft); `kern.maxfilesperproc` 122880 |
| Background load | **Not idle.** Nothing was compiling during the measurements, but an OrbStack VM owned by another user kept 1.5 to 5.4 cores busy throughout (`top`). 1-minute load average from the CSV `load_avg` column: before matrix 4.8 to 12.7, after matrix 7.3 to 12.3, pipelined-before 9.9 to 12.6, scaling 8.4 to 11.0. Because ref and rs runs alternate, the drift affects both servers equally. The ratios are therefore comparable, but absolute ops/s are not comparable between the "before" and "after" sessions (the reference itself moved, e.g. 161k to 121k at 100x16). |

## Commands (T6b)

```sh
export CARGO_TARGET_DIR=...                 # any
scripts/build-ref.sh --optimized            # -> .ref/beanstalkd-opt/beanstalkd
cargo build --release -p bstk-server -p bstk-bench

# Full matrix: 3 scenarios x conns {1,10,100} x body {16,4096} x 3 runs x 2 servers, 5 s each.
# run-matrix.sh now defaults REF_BIN to .ref/beanstalkd-opt/beanstalkd.
OUT_CSV=matrix.csv bench/run-matrix.sh
# Pipelined cells.
OUT_CSV=matrix.csv SCENARIOS=put-reserve-delete CONNS="10 100" BODIES=16 PIPELINES=16 bench/run-matrix.sh
# Scaling scenario: zero-idle baseline and 10,000 idle conns + 10,000 delayed tubes, 3 runs each.
OUT_CSV=scaling.csv SCENARIOS=put-reserve-delete CONNS=10 BODIES=16 \
  IDLE_CONNS="0 10000" DELAYED_TUBES="0 10000" bench/run-matrix.sh
bench/summarize.py matrix.csv
bench/summarize.py scaling.csv
```

"Before" is the release build of commit 4304ef6 (HEAD before T6b). Its
pipelined cells and its scaling runs were driven by the new `bstk-bench`
binary: the old binary lacks the `--idle-conns` / `--delayed-tubes` flags
that `run-matrix.sh` now passes. The load the bench generates is otherwise
unchanged.

Raw per-run CSVs in `bench/results/`:

- `2026-09-25-t6b-before-matrix.csv`, `2026-09-25-t6b-before-pipelined.csv`
- `2026-09-25-t6b-after-matrix.csv` (includes the pipelined cells)
- `2026-09-25-t6b-before-scaling.csv`, `2026-09-25-t6b-after-scaling.csv`

Measurement definitions (ops/s, CPU %, median of 3 runs of 5 s,
correctness checks) are the same as in T6; see
[What is measured](#what-is-measured).

## Results (T6b): before / after

Medians of 3 runs. rs/ref ratios below 0.8 are in bold.

| scenario | conns | body | pipe | before: ref ops/s | before: rs ops/s | before rs/ref | after: ref ops/s | after: rs ops/s | after rs/ref | rs CPU % before | rs CPU % after | ref CPU % after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 1 | 16 | 1 | 47,894 | 45,560 | 0.95 | 35,170 | 34,735 | **0.99** | 38 | 36 | 33 |
| put-reserve-delete | 1 | 4096 | 1 | 48,172 | 46,701 | 0.97 | 32,903 | 36,920 | **1.12** | 38 | 37 | 33 |
| put-reserve-delete | 10 | 16 | 1 | 137,938 | 139,843 | 1.01 | 101,314 | 110,347 | **1.09** | 174 | 177 | 86 |
| put-reserve-delete | 10 | 4096 | 1 | 135,063 | 126,936 | 0.94 | 105,570 | 106,837 | **1.01** | 181 | 182 | 88 |
| put-reserve-delete | 100 | 16 | 1 | 161,307 | 80,672 | **0.50** | 121,492 | 126,058 | **1.04** | 224 | 244 | 98 |
| put-reserve-delete | 100 | 4096 | 1 | 132,005 | 71,824 | **0.54** | 115,506 | 128,087 | **1.11** | 232 | 252 | 99 |
| producers-consumers | 2 | 16 | 1 | 58,802 | 59,816 | 1.02 | 47,748 | 47,857 | **1.00** | 61 | 59 | 47 |
| producers-consumers | 2 | 4096 | 1 | 56,783 | 48,920 | 0.86 | 45,919 | 45,168 | **0.98** | 63 | 61 | 49 |
| producers-consumers | 10 | 16 | 1 | 122,591 | 120,722 | 0.98 | 102,298 | 111,723 | **1.09** | 190 | 178 | 87 |
| producers-consumers | 10 | 4096 | 1 | 129,906 | 128,517 | 0.99 | 102,772 | 104,847 | **1.02** | 187 | 187 | 90 |
| producers-consumers | 100 | 16 | 1 | 165,482 | 144,679 | 0.87 | 145,215 | 127,672 | **0.88** | 320 | 245 | 99 |
| producers-consumers | 100 | 4096 | 1 | 147,898 | 133,964 | 0.91 | 116,287 | 123,993 | **1.07** | 339 | 264 | 99 |
| put-only | 1 | 16 | 1 | 42,761 | 41,238 | 0.96 | 35,356 | 34,519 | **0.98** | 38 | 35 | 33 |
| put-only | 1 | 4096 | 1 | 36,824 | 36,976 | 1.00 | 33,971 | 32,616 | **0.96** | 40 | 39 | 34 |
| put-only | 10 | 16 | 1 | 128,220 | 120,836 | 0.94 | 113,217 | 111,862 | **0.99** | 184 | 177 | 86 |
| put-only | 10 | 4096 | 1 | 113,978 | 111,912 | 0.98 | 95,382 | 101,440 | **1.06** | 206 | 202 | 88 |
| put-only | 100 | 16 | 1 | 151,996 | 131,882 | 0.87 | 136,112 | 126,254 | **0.93** | 320 | 244 | 99 |
| put-only | 100 | 4096 | 1 | 123,962 | 124,071 | 1.00 | 106,795 | 120,902 | **1.13** | 353 | 280 | 98 |
| put-reserve-delete | 10 | 16 | 16 | 242,576 | 224,970 | 0.93 | 227,565 | 319,567 | **1.40** | 304 | 371 | 96 |
| put-reserve-delete | 100 | 16 | 16 | 214,011 | 71,958 | **0.34** | 207,574 | 332,190 | **1.60** | 176 | 418 | 96 |

Full "after" table with latencies (`bench/summarize.py
bench/results/2026-09-25-t6b-after-matrix.csv`; `*` = runs spread by more
than 20%):

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | ref CPU % | rs CPU % | ref put p99 µs | rs put p99 µs | ref reserve p99 µs | rs reserve p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 1 | 16 | 1 | 35,170 | 34,735 | 0.99 | 33 | 36 | 76 | 76 | 76 | 77 |
| put-reserve-delete | 1 | 4096 | 1 | 32,903* | 36,920* | 1.12 | 33 | 37 | 82 | 80 | 82 | 80 |
| put-reserve-delete | 10 | 16 | 1 | 101,314* | 110,347* | 1.09 | 86 | 177 | 226 | 174 | 226 | 174 |
| put-reserve-delete | 10 | 4096 | 1 | 105,570 | 106,837 | 1.01 | 88 | 182 | 232 | 178 | 195 | 176 |
| put-reserve-delete | 100 | 16 | 1 | 121,492 | 126,058 | 1.04 | 98 | 244 | 1,132 | 1,065 | 1,133 | 1,068 |
| put-reserve-delete | 100 | 4096 | 1 | 115,506 | 128,087 | 1.11 | 99 | 252 | 1,460 | 1,067 | 1,039 | 1,069 |
| producers-consumers | 2 | 16 | 1 | 47,748* | 47,857* | 1.00 | 47 | 59 | 93 | 97 | 94 | 96 |
| producers-consumers | 2 | 4096 | 1 | 45,919* | 45,168* | 0.98 | 49 | 61 | 102 | 104 | 95 | 104 |
| producers-consumers | 10 | 16 | 1 | 102,298 | 111,723 | 1.09 | 87 | 178 | 222 | 170 | 223 | 171 |
| producers-consumers | 10 | 4096 | 1 | 102,772 | 104,847 | 1.02 | 90 | 187 | 242 | 189 | 193 | 187 |
| producers-consumers | 100 | 16 | 1 | 145,215 | 127,672 | 0.88 | 99 | 245 | 1,048 | 1,082 | 1,051 | 1,088 |
| producers-consumers | 100 | 4096 | 1 | 116,287 | 123,993 | 1.07 | 99 | 264 | 1,393 | 1,118 | 987 | 1,119 |
| put-only | 1 | 16 | 1 | 35,356 | 34,519 | 0.98 | 33 | 35 | 75 | 83 | - | - |
| put-only | 1 | 4096 | 1 | 33,971 | 32,616 | 0.96 | 34 | 39 | 76 | 80 | - | - |
| put-only | 10 | 16 | 1 | 113,217 | 111,862 | 0.99 | 86 | 177 | 193 | 168 | - | - |
| put-only | 10 | 4096 | 1 | 95,382 | 101,440 | 1.06 | 88 | 202 | 232 | 187 | - | - |
| put-only | 100 | 16 | 1 | 136,112 | 126,254 | 0.93 | 99 | 244 | 1,066 | 1,070 | - | - |
| put-only | 100 | 4096 | 1 | 106,795* | 120,902* | 1.13 | 98 | 280 | 1,317 | 1,148 | - | - |
| put-reserve-delete | 10 | 16 | 16 | 227,565 | 319,567 | 1.40 | 96 | 371 | 1,092 | 626 | 949 | 629 |
| put-reserve-delete | 100 | 16 | 16 | 207,574 | 332,190 | 1.60 | 96 | 418 | 8,959 | 6,062 | 9,157 | 6,042 |

- **Acceptance.** Every non-pipelined cell is at least 0.88x. The lowest
  is producers-consumers 100x16 at 0.88x, where the reference run was
  unusually fast (145k vs 116k to 136k for its neighboring 100-connection
  cells). Both pipelined cells exceed the reference (1.40x, 1.60x).
- **CPU.** The reference stays at one core or less. `beanstalkd-rs` still
  uses 1.8 to 4.2 cores at 10 to 100 connections: the per-command
  cross-thread hand-offs described in T6 remain (connection task → engine
  actor → connection task). The engine is no longer the bottleneck. At 100
  connections on separate tubes, throughput per CPU-second rose from about
  36k ops (80,672 ops/s at 224%) to about 52k (126,058 at 244%), versus
  about 124k for the reference. At 100 connections on one shared tube, CPU
  fell from 320 to 353% to 244 to 280%. Cutting the hand-offs (T6
  suggestion 6) is left to P4.

### Scaling scenario

`put-reserve-delete`, 10 active connections (each on its own tube), 16-byte
bodies. "Scaled" adds `--idle-conns 10000 --delayed-tubes 10000`: 10,000
connections that stay idle, plus 10,000 tubes each holding one job delayed by
3600 s. Both are set up before the clock starts, and the delayed jobs are
deleted afterwards. Medians of 3 runs.

| server | zero-idle ops/s | scaled ops/s | scaled / zero-idle | CPU % zero-idle / scaled | put p99 µs zero-idle / scaled |
|---|---:|---:|---:|---:|---:|
| beanstalkd-rs after T6b | 121,293 | 116,964 | **0.96** | 170 / 172 | 160 / 166 |
| beanstalkd-rs before T6b | 109,058 | 724 | 0.007 | 198 / 100 | 184 / 24,740 |
| reference (-O2), after session | 120,151 | 11,018 | 0.09 | 87 / 96 | 188 / 2,352 |
| reference (-O2), before session | 116,509 | 11,469 | 0.10 | 88 / 96 | 191 / 2,376 |

Before T6b every message paid O(10,000 tubes + 10,000 connections) twice.
The reference's `prottick` (prot.c) also calls `soonest_delayed_job()` and
walks `tubes.items` for pauses on every event-loop iteration. That is
O(#tubes) per event, so it slows down in this scenario too, and it accepts
the 10,000 connections slowly once the tubes exist. The bench therefore
opens the idle connections before it creates the tubes.

## What changed (T6b)

1. **Optimized reference build** (`scripts/build-ref.sh --optimized`, or
   `REF_OPT=1`): builds an rsync'd copy of the pinned sources with
   `CFLAGS=-O2` into `.ref/beanstalkd-opt/`. `bench/run-matrix.sh` uses it by
   default.
2. **Oracle first.** `crates/engine-oracle` (`bstk-engine-oracle`,
   `publish = false`) is a frozen copy of the pre-T6b engine. Its only
   changes are that its test modules are dropped and its stats builders are
   made `pub`. It is only a dev-dependency of `bstk-engine`. The proptest
   `oracle_tests::new_engine_matches_frozen_oracle` (10,000 cases, 20 to 99
   steps each) drives both engines with identical `(now, message)`
   sequences. The messages cover 6 connections and 4 tubes, and include
   connect, disconnect, half-close, `put_started`, every put rejection,
   every command, drain mode and tick (after 80% of messages, plus explicit
   ticks). Time moves by 1 to 3 ns, by 0.5/1/2/5 s and by 1 s ± 1 ns, and
   jumps to exactly `next_deadline()` - 1, + 0 and + 1 ns, so TTR expiry
   (`<`), the DEADLINE_SOON margin (`>=`), delays, 1 ns pauses and reserve
   timeouts are all hit on their boundaries. After every step the test
   asserts identical outboxes, `next_deadline()`, server stats, stats for
   every tube and every job id, tube order and every watch list. It also
   asserts that each index equals a from-scratch recomputation, including
   `next_deadline()` against the old full scan. The existing invariant
   proptest runs the same index check. As a sanity check, four deliberately
   planted bugs were each caught: the wrong delayed-job tie-break, a `<`
   instead of `<=` in the tick fast path, a missing re-index after `touch`,
   and no pause clearing in `process_queue`.
3. **Indexed deadlines** (`crates/engine/src/engine.rs`):
   `conn_ticks: BTreeSet<(conntickat, ConnId)>` (as in the reference's
   connection heap), `delay_heads: BTreeSet<(deadline, TubeId)>` (one
   entry per tube: its soonest delayed job) and
   `pauses: BTreeSet<(unpause_at, TubeId)>`. They are updated by one helper
   per index, called from every state change (reserve, unreserve, touch,
   wait/unwait, delayed insert/remove, pause, tube destruction).
   `next_deadline()` is the minimum of three `first()`s. `tick(now)` returns
   immediately when that is after `now`. Otherwise it runs the same three
   phases as before, in the same order. Ties between equal delayed
   deadlines in different tubes still go to the tube earliest in the
   `tube_order` list, which is looked up among the tied entries via each
   tube's stored position. A reserve that starts waiting inside its safety
   margin gets a `conntickat` at or before `now`, so the server's tick right
   after `handle` still sends DEADLINE_SOON (COMPAT engine item 5).
4. **`process_queue`** iterates only the `dispatchable` set: tubes that have
   waiting connections and ready jobs, usually empty. The minimum `(pri, id)`
   is unique, so the visiting order cannot change which job is dispatched.
   The reference's side effect of clearing every expired pause on each
   `process_queue` call is kept (it is visible in `stats-tube`). The expired
   pauses are popped from `pauses` first.
5. **Integer tube ids.** Tubes live in a slab (`Vec<Option<TubeState>>`)
   indexed by `TubeId`. Jobs, connections (`use`, watch list), `tube_order`
   and the indexes hold ids. Names are hashed only when a command names a
   tube. The per-message `TubeName` clones and SipHash lookups from the T6
   profile are gone. This was done together with item 3, whose index keys
   need stable integer tube ids, so there is no separate measurement for it.
   `current-jobs-delayed` is now a counter instead of a sum over all tubes.
6. **Engine actor** (`crates/server/src/engine_actor.rs`): one pinned
   `Sleep`, reset only when `next_deadline()` changes, instead of a new timer
   per message. Replies are moved out of the outbox instead of cloned. It
   still handles one message per wake-up and ticks after each one.
7. **Not needed:** batching the connection task's flushes (step d). The
   pipelined cells already exceed the reference.

**Profile after** (`sample <pid> 5` during `put-reserve-delete --conns
100`): the engine actor task was on-CPU for about 7% of the sample window
(103 of 1,422 samples), versus 97% (2,592 of 2,660) in T6, so it is no
longer saturated. Within the actor, `Engine::handle` was about 29%,
`next_deadline` about 7% and `tick` about 4%. In T6, `tick` alone was 44%
and `next_deadline` 21%. Most process time is now in tokio worker
park/unpark (`__psynch_cvwait`) and socket syscalls.

## T6 first pass (historical)

The results and analysis below are from task T6 (commit 4304ef6, noisy
machine, reference built by hand with `-O2`). They are superseded by the
T6b tables above.

`beanstalkd-rs` against the reference C beanstalkd (commit `25085c5`),
driven by the `bstk-bench` load generator (`bench/`).

**Summary.** At 1 to 10 connections, and at 100 connections on a single
shared tube, `beanstalkd-rs` reaches 0.87x to 1.11x of the reference's
throughput. The **0.8x bar is missed in one workload**: 100 connections
each on **its own tube** (`put-reserve-delete`, 100 conns) gets 0.64x to
0.67x, and 0.39x when pipelined. Profiling shows the single engine actor is
saturated. Every command pays for several O(#tubes + #connections) scans in
`Engine::tick`, `Engine::next_deadline` and `process_queue`, and each scan
SipHash-es and clones tube names (see Hot spots below). Our server
also uses 2 to 3.3 cores where the reference uses 1. The fixes are
engine-internal and are deferred to P4, as PLAN.md T6 allows.

### Environment

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

### What is measured

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

### Results

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

#### Stress run (T6 acceptance)

`put-reserve-delete`, 100 connections, 30 s, 16-byte bodies, pipeline 1,
one run per server:

| server | ops/s | put p50 / p99 / p999 / max (µs) | server CPU | RSS after | errors / hangs | jobs left (stats) |
|---|---:|---|---:|---:|---|---|
| reference (-O2) | 156,813 | 627 / 931 / 1,112 / 1,865 | 99% | 2.1 MB | none | 0 |
| beanstalkd-rs | 89,332 | 1,165 / 1,488 / 2,095 / 18,105 | 257% | 5.6 MB | none | 0 |

Both servers pass: no unexpected replies, no reply slower than 10 s, and
`stats` shows `current-jobs-{ready,reserved,delayed,buried}` = 0 after the
run. Reserve and delete latencies are within 1% of put's for both servers.

### Analysis

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

### Hot spots

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

#### Suggestions (P4, engine and actor; items 1, 3, 4 and 5 applied in T6b, item 2 not needed)

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

### Reproducing on a quiet machine

The numbers above were taken while another build was running. For
publishable numbers, re-run the matrix on an idle machine (check `uptime`
first). Consider `--threads` to cap the load generator's workers, and
compare `ops/s` together with CPU %.
