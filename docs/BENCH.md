# Benchmarks

`beanstalkd-rs` against the reference C beanstalkd (commit `25085c5`),
driven by the `bstk-bench` load generator (`bench/`). The newest numbers
are from task P5-T2 (Linux, ratios only), then P4-T6 (the final P4 matrix), then P4-T6b (thread default per mode), then P4-T5c (snapshot memory), then P4-T5b
(cluster wake-ups at low load), then
P4-T4 (footprint: memory per job, binlog bytes per operation). P4-T2 (tokio worker-thread count and its
default, the lever the P4-T1 spike found for CPU efficiency) follows,
then P3-FD (Raft cluster mode after batched proposals and group commit,
and the standalone regression check), then P2-T5 (TLS / mTLS), then
P1-T5 (write-ahead log, `-b`), then the T6b section (engine performance
fix); the T6 first pass, whose profile motivated T6b, is kept at the
end.

## Environment notes for new measurements

- **Reference build.** `scripts/build-ref.sh` (both the debug and the `--optimized` tree, which the benchmarks use) patches one comparison in the reference's `conn_timeout` (docs/COMPAT.md D14) and, where the compiler knows it, passes `-Wno-error=stringop-truncation` (gcc 14). Neither changes the hot paths, so the numbers below, measured before the patch existed, stay comparable.
- **Linux container.** Copy the tree in with `COPYFILE_DISABLE=1` (no `._*` files from macOS tar) and leave out `clients/python/.venv`, `target/`, `.ref/` and `.git`; rebuild `.ref/` inside the container. Run it with `docker run --init`, so that orphaned servers are reaped.

## P8-T1: where cluster CPU per operation goes (2026-10-06)

Binary: HEAD `4c3e3fc`, `cargo build --release` with
`CARGO_PROFILE_RELEASE_DEBUG=line-tables-only`. 3 nodes on loopback
(plaintext cluster traffic, default 2 tokio workers, default `[cluster]` settings, data dirs under `/private/tmp`),
`bstk-bench --scenario put-reserve-delete --body-size 16`, 24 s runs. macOS (Apple M6, 12 cores), shared machine:
**load average 5 to 12 during all runs** (per-run values in T1); numbers drift by up to 2x with load, so rely on shares and ratios.

### Method

- **CPU per op per node**: `rusage-utime + rusage-stime` from each node's `/admin` before and after an unsampled 6 s window
  (t = 4 s to 10 s of the run), divided by the operations applied in the window (`cmd-put + cmd-reserve + cmd-delete` of
  the node; every node applies all ops). Log entries from `last_log_index`.
- **Component shares**: `/usr/bin/sample` (1 ms, 8 s, run on all three nodes at the same time, later in the same run, after
  the rusage window), call trees folded into stacks (`parse.py`), Rust v0 symbols demangled with `rustfilt`
  (scratch install). Idle stacks (`__psynch_cvwait`; `kevent` under tokio's park path) are dropped; shares are of the
  remaining "busy" samples and are multiplied with the node's rusage us/op to get us/op per component (so absolute
  per-component numbers carry the error of both). A frame is assigned from the leaf upward: the first frame that matches
  a component pattern wins (`analyze.py`); socket/file syscalls are attributed to the code that called them. A second
  view classifies by what the leaf was doing (syscall kind, allocation, clock, user code).
  Two sampled runs per scenario (`s1`, `s2`) are merged. Caveat: `sample` under-counts running time (it sees 35 to 65% of the
  rusage CPU in the busy leader) and cannot see kernel time spent in blocking calls' entry/exit, context switches and
  wake-ups, which is large at one connection (rusage: sys is 65 to 70% of the CPU there). Treat the one-connection
  shares as "visible user-space + syscall time", and use T3 counters for the wake-up chain.
- **Counters per entry** (T3): a 40-line C interposer (`interpose.c`, `libcnt.dylib`, `DYLD_INSERT_LIBRARIES`) counts libc
  `fdatasync/pwrite/pread/recvfrom/sendto/kevent/pthread_cond_signal` per process; separate runs (`c1`, `c2`),
  deltas over the same 6 s window. These count calls, not time. AppendEntries RPCs per entry: a follower does exactly
  2 `recvfrom` and 2 `sendto` per entry in every scenario, i.e. 2 RPCs (one with the entry, one commit-only) per entry.
- Raw data: `raw/<run>/` (`sample{1,2,3}.txt`, `snap*.txt`, `cnt*.txt`, `bench1.json`, `ids.txt`, `load.txt`);
  scripts `run.sh cluster.sh account.py analyze.py parse.py mkmd.py top.py standalone.sh thr.sh all.sh`.
- Scenarios: (a) 100 connections via the leader, (b) 1 connection via the leader, (c) 100 via a follower, (d) 1 via a
  follower. Run names: `s*`/`c*` + `<conns>-<leader|follower>`; thread-count runs `t<threads>-r<rep>-...`.

### Headline findings

1. **At 100 connections via the leader the cluster is already near its floor, and the floor is client socket I/O.**
   Quiet-window totals: 15.3 to 16.0 us/op (c1, c2, s2; leader 11.5 to 12.0 + 2 x ~2.0 for the followers); 21.7 us/op in the
   loaded run s1 (load 5 to 8 but with competing compile jobs: leader 16.3). The leader burns 2/3 of its CPU (9.3 of 14.1 us/op in
   the merged sample view) in `recvfrom`/`sendto` of client connections: exactly 1 recv + 1 send per op (T3: 54.3 recv per
   entry for 50.1 ops per entry), the same cost a standalone server pays. Standalone with the same tool and load: 5.9 to 6.1 us/op at
   the standalone default of 1 worker, **8.2 to 12.2 us/op with 2 workers** (`standalone.txt`). So
   leader ~ standalone-with-2-workers; the cluster-only extras on the leader are about 1.2 (log) + 0.7 (apply) + 0.4 (glue) +
   0.9 (replication network) + 0.2 (openraft) us/op. The two followers add 2 x 1.9 to 2.4 us/op.
2. **openraft core is nearly free**: 1.3 to 3.5% of busy samples on every node and scenario (0.2 us/op leader at 100 conn;
   1.5 to 3.9 us per entry at one connection). Our wire codec (postcard) is 1 to 5%. Nothing here is worth optimizing
   in openraft's algorithms; the cost is syscalls, thread hand-offs and per-entry file I/O.
3. **At one connection every log entry (= one op) costs ~206 us of CPU on three nodes (four default-setting runs: 206 to 212; thread-count
   runs at 2 workers 191 to 257; 354 in the busiest run c2)**: leader 92 to 96, each follower 57, of which sys is 65 to 70%. Per entry and node the counters are fixed:
   1 `fdatasync`, 2 `pwrite`, 1 to 3 `pread`, 2 AppendEntries in and out (4 socket calls on a follower, 4 + client I/O on the leader),
   4.2 kevent (4.1 blocking = parks) and 3.0 `pthread_cond_signal` (leader 4.1 to 4.8) per entry.
   Then ~34% of a follower's visible time is `fdatasync` + `pwrite`/`pread`, ~28% socket calls, 12 to 17% scheduler/park/wake.
4. **At 100 connections the per-entry cost is amortized over 50 ops** (33 via a follower), so per-entry savings count
   1/50 as much in us/op as at one connection; the 100-connection levers are per-op: client I/O (66% on the leader) and the
   followers' apply (0.5 us/op each) and per-op share of log/network (1 to 1.4 us/op each).
5. **Entries**: no entries at idle (no tick or heartbeat entries; checked 20 s idle: 0). `Op::Tick` is not a factor in this
   workload. ops per entry: 50 via the leader, 33 via a follower (the follower's forward round-trip limits the batch).
6. **Worker threads matter more than any single code path**: `--threads 1` on all nodes: 154 to 207 us/op at one connection vs 191
   to 257 at 2 workers vs 203 to 296 at 3 (-19% in both pairs / baseline / +7 to +15%; runs `t1-*`, `t2-*`, `t3-*`); at 100 connections 12.5 to 13.2 vs 12.7 to
   19.3 vs 14.9 to 22.4 us/op, but 1 worker caps throughput at ~100k ops/s (the 2-worker leader runs at 148 to 154% CPU
   and reaches up to 154k in a quiet moment; observed 91k to 154k with the same build). The existing invariant (no throughput loss > 5%)
   rules out a plain switch to 1 worker.


### Tables

#### T1. CPU per operation per node (rusage over an unsampled 6 s window; all runs listed)

| scenario | run | load (start / end) | ops/s | ops per log entry | leader us/op | target-follower us/op | plain-follower us/op (each) | total us/op |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 1-follower | c1 (syscall counters) |  10.76 8.91 7.75  to  11.75 9.26 7.91  | 3199 | 1.0 | 108.4 | 84.7 | 63.6 | 256.6 |
| 1-leader | c1 (syscall counters) |  6.29 8.23 7.39  to  6.73 8.19 7.40  | 6272 | 1.0 | 92.3 | - | 56.9 | 206.2 |
| 100-follower | c1 (syscall counters) |  7.46 8.21 7.44  to  6.80 8.01 7.39  | 111273 | 33.7 | 4.8 | 12.5 | 2.7 | 20.1 |
| 100-leader | c1 (syscall counters) |  6.81 8.57 7.45  to  7.72 8.67 7.52  | 125777 | 50.1 | 11.9 | - | 2.0 | 15.9 |
| 1-follower | c2 (syscall counters) |  9.65 9.32 8.19  to  8.33 9.04 8.12  | 3043 | 1.0 | 155.7 | 124.6 | 93.3 | 373.6 |
| 1-leader | c2 (syscall counters) |  9.40 8.90 7.89  to  9.09 8.87 7.91  | 3796 | 1.0 | 165.4 | - | 94.5 | 354.3 |
| 100-follower | c2 (syscall counters) |  10.50 9.18 8.05  to  11.75 9.60 8.24  | 76777 | 32.6 | 7.4 | 18.7 | 4.2 | 30.3 |
| 100-leader | c2 (syscall counters) |  9.19 8.90 7.83  to  8.61 8.78 7.82  | 130407 | 50.1 | 11.5 | - | 1.9 | 15.3 |
| 1-follower | s1 (sampled later) |  8.01 8.24 7.48  to  11.61 9.05 7.79  | 2223 | 1.0 | 125.6 | 101.7 | 73.3 | 300.6 |
| 1-leader | s1 (sampled later) |  7.18 8.54 7.48  to  6.49 8.30 7.41  | 6101 | 1.0 | 94.7 | - | 57.6 | 209.9 |
| 100-follower | s1 (sampled later) |  6.67 8.15 7.40  to  8.03 8.33 7.48  | 106999 | 34.0 | 5.0 | 13.1 | 2.9 | 21.0 |
| 100-leader | s1 (sampled later) |  4.72 8.37 7.34  to  7.22 8.68 7.48  | 91197 | 50.2 | 16.3 | - | 2.7 | 21.7 |
| 1-follower | s2 (sampled later) |  11.53 9.59 8.24  to  10.05 9.39 8.21  | 3038 | 1.0 | 159.8 | 122.4 | 93.5 | 375.7 |
| 1-leader | s2 (sampled later) |  8.24 8.70 7.79  to  7.89 8.59 7.77  | 5827 | 1.0 | 96.6 | - | 57.6 | 211.9 |
| 100-follower | s2 (sampled later) |  8.60 8.77 7.88  to  10.02 9.07 8.01  | 77203 | 33.1 | 7.1 | 18.1 | 4.1 | 29.3 |
| 100-leader | s2 (sampled later) |  11.04 9.16 7.88  to  9.56 8.97 7.85  | 125154 | 50.1 | 12.0 | - | 2.0 | 16.0 |

#### T2. Component shares of busy samples, with us/op (share x the role's mean rusage CPU of the s1,s2 runs)

**100-leader** (cpu us/op: leader 14.1 (user 4.0, sys 10.1), plain follower 2.4 (user 1.1, sys 1.3))

| component | leader share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|
| client connection + protocol | 65.5% | 9.25 | 0.0% | 0.00 |
| state machine apply / engine | 5.2% | 0.74 | 19.8% | 0.47 |
| our cluster glue (actor/proposer/forward) | 2.9% | 0.41 | 0.3% | 0.01 |
| openraft core | 1.3% | 0.19 | 2.6% | 0.06 |
| our network: client/listener/IO | 6.5% | 0.92 | 21.3% | 0.50 |
| our network: codec (postcard/wire) | 0.9% | 0.13 | 5.1% | 0.12 |
| log storage / flush worker | 8.8% | 1.24 | 41.5% | 0.98 |
| tokio scheduler/park/wake/sync | 8.9% | 1.26 | 9.3% | 0.22 |
| other | 0.0% | 0.00 | 0.2% | 0.00 |

| leaf class (what the sampled instruction was doing) | leader share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|
| syscall: socket read/write | 66.2% | 9.35 | 16.0% | 0.38 |
| syscall: file pread/pwrite | 2.6% | 0.36 | 16.5% | 0.39 |
| syscall: fdatasync | 3.7% | 0.52 | 18.4% | 0.43 |
| syscall: thread signal/lock | 1.5% | 0.21 | 2.8% | 0.07 |
| memory allocation | 2.1% | 0.30 | 5.2% | 0.12 |
| clock reads | 1.9% | 0.27 | 1.6% | 0.04 |
| kevent | 0.3% | 0.05 | 1.2% | 0.03 |
| user code | 21.7% | 3.07 | 38.3% | 0.90 |

**1-leader** (cpu us/op: leader 95.7 (user 34.0, sys 61.6), plain follower 57.6 (user 16.9, sys 40.7))

| component | leader share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|
| client connection + protocol | 10.6% | 10.10 | 0.0% | 0.00 |
| state machine apply / engine | 1.7% | 1.61 | 1.7% | 0.97 |
| our cluster glue (actor/proposer/forward) | 1.7% | 1.61 | 0.3% | 0.16 |
| openraft core | 3.1% | 2.95 | 2.6% | 1.50 |
| our network: client/listener/IO | 31.4% | 30.08 | 32.6% | 18.81 |
| our network: codec (postcard/wire) | 1.5% | 1.44 | 2.2% | 1.27 |
| log storage / flush worker | 36.3% | 34.71 | 48.5% | 27.96 |
| tokio scheduler/park/wake/sync | 13.6% | 13.02 | 11.8% | 6.79 |
| other | 0.1% | 0.13 | 0.3% | 0.16 |

| leaf class (what the sampled instruction was doing) | leader share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|
| syscall: socket read/write | 37.5% | 35.92 | 28.2% | 16.26 |
| syscall: file pread/pwrite | 12.2% | 11.68 | 15.9% | 9.17 |
| syscall: fdatasync | 16.1% | 15.37 | 26.4% | 15.19 |
| syscall: thread signal/lock | 4.6% | 4.36 | 3.5% | 2.04 |
| memory allocation | 2.5% | 2.42 | 2.5% | 1.47 |
| clock reads | 3.5% | 3.39 | 2.2% | 1.29 |
| kevent | 1.1% | 1.04 | 1.2% | 0.67 |
| user code | 22.5% | 21.48 | 20.0% | 11.54 |

**100-follower** (cpu us/op: leader 6.1 (user 2.5, sys 3.5), target follower 15.6 (user 3.9, sys 11.7), plain follower 3.5 (user 1.3, sys 2.2))

| component | leader share | us/op | target follower share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|---:|---:|
| client connection + protocol | 0.0% | 0.00 | 64.6% | 10.07 | 0.0% | 0.00 |
| state machine apply / engine | 11.6% | 0.71 | 5.3% | 0.83 | 15.1% | 0.53 |
| our cluster glue (actor/proposer/forward) | 2.3% | 0.14 | 3.1% | 0.48 | 0.6% | 0.02 |
| openraft core | 3.5% | 0.21 | 0.8% | 0.12 | 2.8% | 0.10 |
| our network: client/listener/IO | 31.4% | 1.90 | 7.5% | 1.17 | 22.8% | 0.80 |
| our network: codec (postcard/wire) | 4.4% | 0.26 | 1.2% | 0.18 | 4.5% | 0.16 |
| log storage / flush worker | 34.0% | 2.06 | 9.2% | 1.43 | 40.4% | 1.42 |
| tokio scheduler/park/wake/sync | 12.8% | 0.77 | 8.3% | 1.29 | 13.5% | 0.47 |
| other | 0.2% | 0.01 | 0.0% | 0.00 | 0.3% | 0.01 |

| leaf class (what the sampled instruction was doing) | leader share | us/op | target follower share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|---:|---:|
| syscall: socket read/write | 24.5% | 1.48 | 65.1% | 10.15 | 18.1% | 0.64 |
| syscall: file pread/pwrite | 10.5% | 0.63 | 2.5% | 0.38 | 14.7% | 0.52 |
| syscall: fdatasync | 13.1% | 0.80 | 5.4% | 0.83 | 18.4% | 0.65 |
| syscall: thread signal/lock | 3.3% | 0.20 | 1.4% | 0.22 | 3.9% | 0.14 |
| memory allocation | 5.2% | 0.32 | 2.1% | 0.32 | 3.6% | 0.13 |
| clock reads | 3.6% | 0.22 | 1.8% | 0.28 | 2.5% | 0.09 |
| kevent | 1.2% | 0.07 | 0.2% | 0.03 | 1.5% | 0.05 |
| user code | 38.6% | 2.34 | 21.6% | 3.37 | 37.4% | 1.31 |

**1-follower** (cpu us/op: leader 142.7 (user 54.5, sys 88.2), target follower 112.0 (user 35.8, sys 76.3), plain follower 83.4 (user 25.7, sys 57.7))

| component | leader share | us/op | target follower share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|---:|---:|
| client connection + protocol | 0.0% | 0.00 | 8.6% | 9.60 | 0.0% | 0.00 |
| state machine apply / engine | 2.6% | 3.67 | 3.1% | 3.45 | 2.6% | 2.18 |
| our cluster glue (actor/proposer/forward) | 2.4% | 3.49 | 3.5% | 3.96 | 0.9% | 0.73 |
| openraft core | 2.7% | 3.85 | 3.1% | 3.45 | 2.9% | 2.42 |
| our network: client/listener/IO | 36.2% | 51.69 | 32.7% | 36.61 | 30.1% | 25.09 |
| our network: codec (postcard/wire) | 1.9% | 2.78 | 1.5% | 1.68 | 2.6% | 2.18 |
| log storage / flush worker | 38.9% | 55.46 | 33.9% | 37.96 | 43.0% | 35.88 |
| tokio scheduler/park/wake/sync | 15.3% | 21.77 | 13.3% | 14.90 | 17.3% | 14.42 |
| other | 0.0% | 0.00 | 0.4% | 0.42 | 0.6% | 0.48 |

| leaf class (what the sampled instruction was doing) | leader share | us/op | target follower share | us/op | plain follower share | us/op |
|---|---:|---:|---:|---:|---:|---:|
| syscall: socket read/write | 29.1% | 41.57 | 32.2% | 36.11 | 23.4% | 19.51 |
| syscall: file pread/pwrite | 10.9% | 15.50 | 11.6% | 13.05 | 13.5% | 11.27 |
| syscall: fdatasync | 14.8% | 21.14 | 15.8% | 17.68 | 21.8% | 18.18 |
| syscall: thread signal/lock | 5.1% | 7.26 | 3.5% | 3.87 | 4.9% | 4.12 |
| memory allocation | 3.4% | 4.84 | 4.7% | 5.30 | 3.8% | 3.15 |
| clock reads | 2.3% | 3.23 | 2.2% | 2.44 | 1.7% | 1.45 |
| kevent | 0.8% | 1.16 | 1.1% | 1.18 | 2.3% | 1.94 |
| user code | 33.6% | 48.02 | 28.9% | 32.41 | 28.5% | 23.76 |

#### T3. Per-entry counters (interposed libc calls over the same 6 s window; mean of c1,c2 runs)

| scenario | node role | fdatasync | pwrite | pread | recvfrom | sendto | kevent (blocking) | cond_signal | entries/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 100-leader | leader | 1.00 | 2.00 | 3.00 | 54.3 | 54.1 | 14.1 (12.1) | 4.79 | 2555 |
| 100-leader | plain follower | 1.00 | 2.00 | 1.00 | 2.0 | 2.0 | 4.2 (4.1) | 2.98 | 2555 |
| 1-leader | leader | 1.00 | 2.00 | 3.00 | 5.0 | 5.0 | 7.4 (7.1) | 4.09 | 5034 |
| 1-leader | plain follower | 1.00 | 2.00 | 1.00 | 2.0 | 2.0 | 4.2 (4.1) | 2.97 | 5034 |
| 100-follower | leader | 1.01 | 2.01 | 3.00 | 6.1 | 6.1 | 8.3 (8.0) | 4.02 | 2829 |
| 100-follower | target follower | 1.00 | 2.00 | 1.01 | 37.7 | 37.5 | 10.5 (9.2) | 4.05 | 2829 |
| 100-follower | plain follower | 1.00 | 2.01 | 1.00 | 2.0 | 2.0 | 4.3 (4.1) | 3.01 | 2829 |
| 1-follower | leader | 1.00 | 2.00 | 3.01 | 5.0 | 5.0 | 7.3 (7.0) | 4.55 | 3121 |
| 1-follower | target follower | 1.00 | 2.01 | 1.00 | 4.0 | 4.0 | 6.7 (6.4) | 3.00 | 3121 |
| 1-follower | plain follower | 1.00 | 1.99 | 1.00 | 2.0 | 2.0 | 4.2 (4.1) | 2.97 | 3121 |

Notes on T3: AppendEntries RPCs per entry per follower = 2 (recvfrom 2.0, sendto 2.0 on a plain follower); `kevent (blocking)`
is the number of kevent calls with a non-zero timeout (parks of the IO driver); a leader at 100 connections also does
~1.0 recv + 1.0 send per op for clients (54.3 recvfrom per entry = 50.1 client + 4 from the two followers). Leader preads:
2 for the two replication streams + 1 for apply; follower: 1 for apply. The 2 pwrites per entry and node are the log append and
`save_committed` (an unsynced hint, `log_store.rs:725`). One `fdatasync` per entry and node: group commit already merges ops
into entries; it cannot merge entries at one connection.

#### T4. Reference: standalone and thread counts (same tool, same load window)

Standalone (`standalone.txt`, 1 node, put-reserve-delete, rusage over 8 s):

| conns | threads | us/op (user + sys), 2 runs |
|---:|---|---|
| 100 | 1 (standalone default) | 5.9 (1.1 + 4.8); 6.1 (1.2 + 4.9) |
| 100 | 2 | 12.2 (2.4 + 9.8); 8.2 (1.6 + 6.6) |
| 1 | 1 | 9.7 (2.5 + 7.2); 7.0 (1.8 + 5.2) |
| 1 | 2 | 8.9 (2.3 + 6.6); 8.1 (2.1 + 6.0) |

Cluster, 3 nodes via the leader, `--threads N` on every node (`t<N>-r<rep>-*`; total us/op over the 3 nodes; 6 s window):

| conns | threads | rep 1: ops/s, total us/op (leader CPU%) | rep 2: ops/s, total us/op (leader CPU%) |
|---:|---:|---|---|
| 100 | 1 | 99.7k, 13.2 (91%) | 105k, 12.5 (91%) |
| 100 | 2 | 107k, 19.3 (154%) | 154k, 12.7 (148%) |
| 100 | 3 | 104k, 22.4 (179%) | 148k, 14.9 (172%) |
| 1 | 1 | 5.4k, 207 (48%) | 7.9k, 154 (52%) |
| 1 | 2 | 5.4k, 257 (63%) | 7.8k, 191 (70%) |
| 1 | 3 | 4.9k, 296 (70%) | 7.7k, 204 (76%) |

(The two reps differ by load; compare within a rep. P3 reference at one connection: 343.6 us/op via the leader, so the
"0.5x" gate is 172 us/op: reached only by 1 worker in a quiet window, 154.)

### Per-role reading

- **Leader, 100 connections**: 11.5 to 16.3 us/op depending on load; 2/3 is client socket recv/send (1 + 1 per op), 9% scheduler, 9% log
  (append + 3 preads + the flusher), 5% apply, 3% proposer/actor glue, 1.3% openraft, 7% replication network + codec. It runs at
  148 to 154% CPU, i.e. both workers are busy: it is the throughput bottleneck, so every leader us/op saved is also throughput.
- **Plain follower, 100 connections**: 1.9 to 2.8 us/op = ~100 to 140 us per entry of 50 ops: 41% log (fdatasync 18%, pwrite 15%
  incl. `save_committed` 4%, `read_entries` 4%), 20% apply (0.47 us/op: engine `handle` 12%, `refresh_conn_tick` 3.4%,
  `process_queue` 3.4%, decode of the `EngineInput`s 2.8%), 21% listener socket calls + 5% codec, 9% scheduler. One
  follower is ~25% busy, so its 2nd worker is mostly idle weight (1 worker would save ~19% of its one-connection CPU, nothing measurable at 100).
- **Target follower (100 via a follower)**: 12.5 to 18.7 us/op: 65% client socket I/O (same as a leader), plus forward/glue 3%;
  the leader then costs 4.8 to 7.4 us/op (no client I/O but 33 ops per entry instead of 50, so per-entry costs weigh 1.5x).
  Totals via a follower: 20.1 / 29.3 / 30.3 / 21.0 (c1, s2, c2, s1) vs 15.3 to 21.7 via the leader.
- **One connection**: per entry the leader spends 92 to 96 us (35% log incl. fdatasync 15, 31% replication/listener socket calls, 14% scheduler,
  10% client socket) and each follower 57 (48% log: fdatasync 15 + pwrite/pread 9 + flusher hand-off, 33% socket calls, 12% scheduler),
  with sys time 65 to 70%. Ten of the ~14 libc calls per entry on a follower are not "work": 2 kevent-parks, 3 cond-signals,
  the AppendEntries pair carrying no entry, and the `save_committed` write.

### Ranked candidate savings

Savings are estimates (us per op at 100 connections via the leader, 3 nodes together; us per op = per entry at one connection,
out of ~206), derived from the per-entry counters (T3) times the per-call costs in the profile (socket call 4.1 us on a follower,
`pread`/`pwrite` ~3 us, `fdatasync` 15 to 18 us, park/wake chain ~5 us) and the component inclusive shares; they are not measured
gains and are listed from largest expected gain at 100 connections (the open gate) down.

| # | candidate | est. save at 100 conn via leader (us/op) | est. save at 1 conn (us/op) | needs openraft 0.9 patch? | evidence |
|---|---|---:|---:|---|---|
| 1 | **Defer / coalesce the commit-only AppendEntries** (empty AE sent right after each commit; 2 RPCs per entry per follower today). In `PeerClient::append_entries` answer a no-entry, no-new-info AE locally with the matching success and carry `leader_commit` on the next real AE or within a short bound (heartbeats must still pass; a follower with forwarded inputs outstanding, i.e. the via-follower target, must not be delayed). | 0.8 to 1.1 (5 to 7%) | 40 to 55 (20 to 27%) | no (network layer only; relies on how openraft 0.9 treats the reply to an empty AE, to be verified by the differential and chaos suites) | T3: 2.0 recvfrom + 2.0 sendto per entry on every follower at every concurrency; follower socket calls = 28% of its visible time, leader replication socket calls ~31% at 1 conn; each RPC also costs a wake-up chain (cond_signal 3.0/entry, parks 4.1/entry on a follower). |
| 2 | **Reduce 2-worker cross-thread cost of client I/O on the leader** (experiments, not a known fix): standalone 2 workers cost 8.2 to 12.2 us/op vs 5.9 to 6.1 with 1; the leader at 100 conn is 66% socket syscalls at 4.5 us per call vs ~3 at one worker. Ideas to test one by one: separate runtime/thread for the Raft chain (RaftCore, replication, flusher callbacks, `serve_peer`) so client workers are not woken for Raft events; `disable_lifo_slot`; fewer wake-ups between connection task and actor. | 2 to 4 (if it reaches the 1-worker figure; unproven) | about 40 (-19% in both 1-worker pairs, T4) | no (runtime/config) | T4: cluster 1 worker 12.5 to 13.2 us/op vs 12.7 to 19.3 at 2, but 1 worker loses throughput (91% CPU, ~100k vs up to 154k); the gate "no throughput loss > 5%" forbids simply setting 1 worker. |
| 3 | **Tail cache of recent decoded log entries** instead of `pread` + postcard decode per read: 3 reads per entry on the leader (2 replication streams + apply), 1 per follower. | 0.3 to 0.4 | 15 to 25 (7 to 12%) | no (our `LogStore`) | T3: pread 3.0 leader / 1.0 follower per entry; `read_entries` 4% of a follower (1.8% at 1 conn), leader file pread/pwrite leaf 0.36 us/op; `Entry`/`EngineInput` deserialize 1.9 to 2.8% on followers. |
| 4 | **Cheaper apply per item**: the state machine applies each batch item as engine call + `tick(now)` + `refresh_conn_tick` + `process_queue` (3.4% each on a follower). Do the tick/refresh once per entry (same `now` for all items of an entry). Must be shown idempotent under the differential suites; if not, it needs a versioned `Op`. Also avoid re-decoding items (`EngineInput` deserialize 2.8%). | 0.3 to 0.5 (leader 0.74 + follower 0.47 x 2 us/op of apply; guess a third of it) | 1 to 3 | no | `apply_conn` 16% of a follower busy at 100 conn (0.38 to 0.47 us/op), leader apply 5.2% (0.74 us/op). |
| 5 | **Stop writing `save_committed` on every commit** (unsynced hint, one `pwrite` per entry and node): write it at most every N entries or every few ms and at shutdown/leader change. Semantics: the hint must stay safe if stale (module doc in `storage`). | 0.1 to 0.2 | 6 to 12 (3 to 6%) | no (our `LogStore`; openraft keeps calling it) | T3: pwrite 2.0 per entry (append + hint); `save_committed` 3.9 to 4.3% of a follower's visible time. |
| 6 | **Persistent flush thread** (a dedicated thread + condvar/channel) instead of `spawn_blocking` per flush run: today each idle -> busy transition goes through tokio's blocking pool (spawn, mutex, condvar signal), then the callback wakes RaftCore across threads. | 0.1 to 0.2 | 5 to 10 | no | `Flusher::push/submit` 2.0%, `spawn_blocking` 1.9%, `Condvar::notify_one_slow` 2.0% of a follower at 1 conn; 4% (of which ~2.5% non-sync) at 100. |
| 7 | **Larger batches via a follower** (33 vs 50 ops per entry via the leader): let more items ride in one forward message / batch. Applies to the via-follower mode only. | 0 via the leader; 1 to 2 of the 20 to 30 via a follower | 0 | no | via-follower leader costs 4.8 to 7.4 us/op vs ~2 to 5 non-client us/op via the leader; entries/s 2.8k vs 2.6k although ops/s is lower. |
| - | Not a candidate: `fdatasync` (1 per entry and node, 15 to 18 us; 45 to 54 us per entry = 22 to 26% at 1 conn, 0.4 to 0.65 us/op at 100); needed for durability; only amortized by bigger batches, which cost latency. openraft core (1.3 to 3.5%), postcard codec (1 to 5%), allocation (2 to 5%), clock reads (1.6 to 3.6%, nearly all inside tokio's worker/time driver), no tick or heartbeat entries. | | | | |

Combined (1 + 3 + 5 + 6, none overlapping, all without patching openraft): ~65 to 100 us of ~206 per op at 1 conn (32 to 48%),
which would meet the "<= 0.5x of P3 = 172 us" gate with margin (the combined estimate is ~106 to 141 us; the 2-worker runs at 191 to 257
us need at least ~20 to 85 us). At 100 conn via the leader the same set saves ~1.4 to 1.9 us/op (9 to 12%): 15.3 to 16.0 -> ~13.5 to 14.5
in a quiet window. Reaching "<= 15 us" robustly under load needs item 2 (the client-I/O floor is 66% of the leader and the leader is the
throughput bottleneck at 148 to 154% CPU), because the cluster-only part (everything but client socket calls) is only ~5 us/op on the
leader and ~4 us/op on the two followers together.

### Gate status from these data

- 100 connections via the leader, total CPU per op: 15.3, 15.9, 16.0 (quiet-ish), 21.7 (noisy); 12.5 to 13.2 with 1 worker; 12.7 with 2
  workers in the quietest run (t2-r2, 154k ops/s). So the 15 us gate is within noise of the current build; the lead should take the
  acceptance numbers from a quiet window and compare interleaved A/B runs.
- 1 connection via the leader: 206 to 212 (default 2 workers, load 6 to 8), 191 (t2-r2); P3 343.6 -> 0.56 to 0.62x; gate 172.
  Counters and the ranking say items 1, 3, 5, 6 can close that without touching openraft.

## P7-T3: openraft 0.10 evaluation (2026-10-05)

Question: does `openraft 0.10.0-alpha.36` reach the cluster CPU targets of
PLAN §7.5, and what would moving to it cost? A port was made on a
throwaway branch (`eval-openraft-0.10`, not merged).

**Port.** About an hour of work: 38 files, about 600 net lines. The
changes are mostly mechanical: type parameters, the storage-v2 apply
stream and its responders, metrics watch types, and
`allow_log_reversion = Some(true)` in place of the `loosen-follower-log-revert`
feature. Chunked snapshots keep the 0.9 wire format through the companion
crate `openraft-legacy` (`network_v1::Adapter`). `PayloadTooLarge` is
gone, so the client trims a batch itself and answers `PartialSuccess`.
Leader transfer exists (`trigger().transfer_leader`) but needs our own
RPC. On the branch, the bstk-raft tests (155), the cluster integration
tests (37) and in-process chaos (`full` and `membership`, 300 seeds each)
passed. The fuzz crate was not ported, and leader transfer was compiled
but not run.

**Source findings.** The append is no longer awaited per command
(`core/raft_core.rs` `run_append_entries`). The "replication channel
closed" path behind DESIGN §8 "Fatal Raft stop" no longer exists: a
closed replication channel is ignored, and snapshots go through a
separate transmitter task.

**CPU per operation.** The machine was shared (load average 11–18), so
absolute values are higher than in P4-T6, and only the ratios are
meaningful. Method: 3-node cluster, put-reserve-delete, release builds,
14 interleaved rounds alternating 0.9 and 0.10 (the order flipped every
round), CPU summed over the three node processes. The table shows the
median and Q1–Q3.

| Configuration | 0.9 µs/op | 0.10 µs/op | 0.10 / 0.9 (paired) |
|---|---:|---:|---:|
| via leader, 100 connections | 24.1 (22.9–26.5) | 25.1 (24.0–25.8) | 1.03 (0.92–1.16) |
| via follower, 100 connections | 30.9 (30.2–32.0) | 31.5 (29.7–33.7) | 1.04 (1.00–1.07) |
| via leader, 1 connection | 394 (342–412) | 457 (406–470) | 1.13 (1.06–1.17) |
| via follower, 1 connection | 398 (371–479) | 497 (430–521) | 1.12 (1.06–1.27) |

Throughput differences were within the noise (interquartile ranges
spanned 1.5–3×).

**Conclusion.** 0.10 does not lower CPU per operation. It is level at
100 connections and costs 12–13% more at one connection, so it does not
reach the §7.5 targets by itself. The remaining cluster cost is in our
use of Raft (an entry per operation, replication rounds), not in the
awaited flush. Its benefits are leader transfer and removal of the fatal
race, which the exit-on-fatal supervisor (P7) already contains.
Recommendation: stay on 0.9.25 and revisit when 0.10.0 is released
(alphas still change the replication core: 15 files from alpha.35 to
alpha.36).

## P6-T8: hot-path regression check (2026-10-05)

HEAD `3ce7157` against the accepted P5 build (`5004d6e`, built from a git
worktree with its own target directory), macOS, release builds, 100
connections, 16-byte bodies, `bench/run-matrix.sh` with the P5 binary in
the `ref` slot (standalone, alternated) and as separate order-flipped
rounds (3-node cluster, plaintext cluster traffic, 6 s per run, ops/s
through the leader and through a follower). Criterion: within +/-5%.

The shared Mac was loaded by system daemons for most of the session (1-minute
load 25 to 150 early, 6 to 12 in the final sets), and the early sets spread 3x
(cluster runs from 14k to 65k ops/s), too much to tell 5% from noise. The
table is the last, quietest set (medians; the first rounds of it, at about
100k and 85k ops/s with 1% spread, agree with it); the earlier sets are
listed below it.

| mode | runs per build | P5 ops/s | HEAD ops/s | HEAD/P5 | cluster CPU per op HEAD/P5 |
|---|---:|---:|---:|---:|---:|
| cluster, via the leader | 12 | 100,204 | 97,961 | 0.98 | 1.01 |
| cluster, via a follower | 12 | 85,430 | 83,983 | 0.98 | 1.02 |
| standalone put-reserve-delete | 10 | 139,875 | 153,088 | 1.09 | 0.89 (server CPU per op) |
| standalone producers-consumers | 10 | 140,777 | 139,526 | 0.99 | 1.01 |

Result: no regression; standalone and cluster are within 5% except
standalone put-reserve-delete, where HEAD is faster (that cell spreads
80k to 190k on this machine).

Earlier sets on the loaded machine (not conclusive): standalone, 7 and 15
alternated runs, put-reserve-delete 1.04 and 0.97, producers-consumers 1.01 and
1.05 (medians of the ratio of medians); cluster, 8 rounds (10 s) and 12 rounds
(6 s), via the leader 1.46 and 1.07, via a follower 1.06 and 0.99, with
interquartile ranges of 2x; CPU per operation in those sets HEAD/P5 0.92 to
1.05. The 0.77 to 0.92 paired-ratio medians of the follower sets came from
runs where one build happened to hit a load spike; the quiet set shows 0.99.

Commands: `REF_BIN=<P5 binary> RS_BIN=target/release/beanstalkd-rs
SERVERS="ref rs" SCENARIOS="put-reserve-delete producers-consumers" CONNS=100
BODIES=16 RUNS=10 DURATION=4 bench/run-matrix.sh`; cluster: `RS_BIN=<binary>
SERVERS=rs SCENARIOS=put-reserve-delete CONNS=100 BODIES=16 RUNS=1 DURATION=6
CLUSTER_NODES=3 SERVER_MODES="cluster-leader cluster-follower"` for each build
in turn, order flipped every round, 12 rounds.

## P5-T2: Linux (ratios only)

### Summary

Linux aarch64 in a `rust:1.98.1-slim` container (`--cpus=12`, kernel
7.0 under OrbStack) on the same shared Mac, server and `bstk-bench` in the
container over loopback. The container host is the loaded VM (1-minute
load 3 to 15), so only alternated or paired ratios are reported; absolute
Linux figures need real hardware. The stock `run-matrix.sh` reads CPU with
`ps -o cputime` (1 s resolution on Linux) and load with `sysctl`; the runs
used a container copy reading `/proc/PID/stat` and `/proc/loadavg`.
Cluster cells use node data on `/dev/shm`: on the container's overlay
filesystem each fsync costs about 2 ms and dominates every cluster number.

| Criterion | Linux result |
|---|---|
| Standalone ops per CPU-second ≥ 0.8x the reference at 10 and 100 connections | 0.98 to 1.23x: met |
| Standalone throughput ≥ 1.0x the reference at 10 and 100 connections | after the `QuietTcp` fix below: 1.01 to 1.22x in 7 of 8 cells; producers-consumers 10×16 0.98x (a high-spread cell) |
| `-b` not below P3 (±5%) | 0.96 to 1.05x paired: met |
| TLS not below P3 (±5%) | 16 B cells 0.975 to 0.98x paired: met; 100×4096 0.91 to 0.94x in three sets, at 0.87x of P3's CPU (P3 runs 12 workers here, ours 2; `--threads 4` gives 0.97x) |
| Cluster throughput not below P3 | via the leader 1.06 to 1.20x, via a follower 1.03 to 1.08x (paired, tmpfs) |
| Cluster CPU per operation at 100 connections ≤ 15 µs | via the leader 11.8 µs (P3 14.2): met; via a follower 15.9 (P3 16.3) |
| One-connection cluster CPU per operation halved | 0.78x via the leader, 0.80x via a follower: not met |
| 1 and 2 connections vs the reference | 0.94 to 0.97x, the same pre-existing gap as on macOS |

### `QuietTcp` on epoll

The first Linux run found that `QuietTcp` (P4-T5b) made standalone
plaintext slower than tokio's `TcpStream`: +13 to 16% CPU per operation,
-7 to -14% throughput. `strace -c` at 10 connections showed 2.00
`recvfrom` calls per command, one of them failing with `EAGAIN`:
`QuietTcp` kept read readiness after a short read, which tokio's
`TcpStream` clears. With that fixed (`d4306ab`), 1.005 `recvfrom` per
command and no `EAGAIN`; alternated against the unfixed build (6 runs,
put-reserve-delete):

| conns | body | throughput fixed/base | server µs/op fixed/base |
|---:|---:|---:|---:|
| 1 | 16 | 1.05 | 0.91 |
| 1 | 4096 | 1.04 | 0.93 |
| 10 | 16 | 1.10 | 0.90 |
| 10 | 4096 | 1.10 | 0.88 |
| 100 | 16 | 1.16 | 0.87 |
| 100 | 4096 | 1.03 | 1.04 (noise) |

In cluster mode the read-only registration still pays on Linux: plain
`TcpStream` everywhere costs 5 to 13% more cluster CPU per operation at
one connection (macOS: 24 to 31%), as P4-T5b expected for epoll.

### Fixed build vs the reference (5 runs, alternated)

| scenario | conns | body | throughput | ops per CPU-s |
|---|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 1.09 | 1.07 |
| put-reserve-delete | 10 | 4096 | 1.02 | 1.03 |
| put-reserve-delete | 100 | 16 | 1.22 | 1.23 |
| put-reserve-delete | 100 | 4096 | 1.18 | 1.18 |
| producers-consumers | 10 | 16 | 0.98 | 0.98 |
| producers-consumers | 10 | 4096 | 1.01 | 1.04 |
| producers-consumers | 100 | 16 | 1.15 | 1.07 |
| producers-consumers | 100 | 4096 | 1.01 | 1.05 |

### Worker threads on Linux

One worker is not CPU-bound here (59 to 76% of a core). Two workers give
1.20 to 1.44x plaintext throughput for 1.15 to 1.24x CPU per operation,
which would still be about 0.83 to 0.91x the reference's efficiency
(an estimate from two ratios, not measured directly); TLS 10×4096 with
one worker is 0.75x of two. The per-mode defaults (P4-T6b) stand; on
Linux, `--threads 2` is a reasonable choice for plaintext throughput.

## P4-T6: final P4 matrix

### Summary

Run at `36d3db1` (before P4-T6b; the TLS and `-b` cells at the new default
are in the P4-T6b section below) against the optimized reference and the
P3 build (`e9cbb1f`). The machine was loaded throughout (OrbStack VM;
1-minute load 4 to 14, median about 7.5), so absolute numbers are 40 to 50%
below P4-T2's; ratios come from alternated or order-flipped runs.

| Criterion (PLAN §7.5) | Result |
|---|---|
| Standalone ops per CPU-second ≥ 0.8x the reference at 10 and 100 connections | 1.04 to 1.29x: met |
| Standalone throughput ≥ 1.0x the reference, non-pipelined cells | 1.01 to 1.40x at 2, 10 and 100 connections; **0.95 to 0.97x at 1 connection** (pre-existing since P3, see P4-T6b) |
| `-b` and TLS not below P3 (±5%) | at one worker 0.91 to 0.93x at 10 connections; with the P4-T6b default (2 workers) 0.98 to 1.07x: met |
| Cluster throughput not below P3 (±5%) | 1.06 to 1.52x via leader and follower at 1, 10 and 100 connections; mTLS via the leader at 100 connections 0.94x (one block, not rerun) |
| Cluster CPU per operation at 100 connections ≤ 15 µs | **17.4 to 17.8 µs** via the leader (P3: 24 to 26), 25.3 via a follower (P3: 33); the target node alone 13.0 to 13.4 / 15.9: not met |
| One-connection cluster CPU per operation halved | **0.73 to 0.77x** of P3 via the leader, 0.55x via a follower: not met |
| Memory per job ≤ 1.5x, binlog bytes per operation ≤ 1.2x the reference | worst 1.19x and 0.95x: met |
| Chaos: 1,000 in-process seeds, 100 multi-process runs | 0 failures each (2,288 snapshot installs in-process): met |
| Smoke tests | all modes pass (TLS modes after the `mkcerts.sh` authority-key-identifier fix in P4-T6b) |

Why the cluster targets are missed: what remains per entry is mostly
openraft 0.9's own work, two AppendEntries round trips per entry and
follower (the second only advances the commit index) and the Raft core
awaiting each append's flush, plus thread wake-ups for each (P4-T5b).
CPU per operation also rises with machine load (best run 14.1 µs at load
5.5, worst 20.4 at 9.7). One worker per node removes the idle wake-ups
but cost 100-connection throughput in the loaded runs (P4-T5b), so the
cluster default stays at two. Reaching the targets needs openraft 0.10
(no awaited flush per append) or measurements on a quiet machine to
revisit the worker count.

### Standalone plaintext vs the reference (alternated, 5 runs)

| scenario | conns | body | rs/ref | ops per CPU-s rs/ref |
|---|---:|---:|---:|---:|
| put-reserve-delete | 1 | 16 | 0.91 (rerun 0.89) | 0.89 |
| put-reserve-delete | 1 | 4096 | 0.93 (rerun 0.96) | 0.91 |
| put-reserve-delete | 10 | 16 | 1.18 | 1.13 |
| put-reserve-delete | 10 | 4096 | 1.09 | 1.04 |
| put-reserve-delete | 100 | 16 | 1.18 | 1.16 |
| put-reserve-delete | 100 | 4096 | 1.06 | 1.06 |
| producers-consumers | 2 | 16 | 1.04 | 0.95 |
| producers-consumers | 2 | 4096 | 1.01 | 0.96 |
| producers-consumers | 10 | 16 | 1.17 | 1.12 |
| producers-consumers | 10 | 4096 | 1.40 | 1.29 |
| producers-consumers | 100 | 16 | 1.02 | 1.01 |
| producers-consumers | 100 | 4096 | 1.10 | 1.09 |
| put-reserve-delete, pipelined x16 | 10 | 16 | 1.03 | 1.14 |
| put-reserve-delete, pipelined x16 | 100 | 16 | 1.26 | 1.27 |

The 1-connection cells were inflated by load: a later bisect measured
0.95 to 0.97x for every build since P3 (P4-T6b).

### Cluster vs P3 (3 nodes, 10 s, 5 order-flipped rounds)

| mode | conns | ops/s cur/P3 | cluster µs/op P3 → cur | target node µs/op P3 → cur |
|---|---:|---:|---|---|
| leader | 1 | 1.15 | 343.6 → 251.5 | 162.4 → 116.6 |
| follower | 1 | 1.52 | 409.7 → 224.1 | 130.7 → 76.2 |
| leader | 10 | 1.14 | 92.0 → 64.1 | 48.8 → 33.6 |
| follower | 10 | 1.22 | 129.0 → 86.2 | 51.1 → 34.3 |
| leader | 100 | 1.12 | 26.0 → 17.8 | 20.0 → 13.4 |
| follower | 100 | 1.06 | 33.1 → 25.3 | 21.5 → 15.9 |
| mtls-leader | 100 | 0.94 | 26.5 → 20.9 | 20.3 → 15.7 |
| leader (rerun, load 5.6 to 7.9) | 1 | 1.05 | 296.6 → 228.6 | 137.1 → 105.5 |
| leader (rerun) | 100 | 1.01 | 24.2 → 17.4 | 18.8 → 13.0 |

"Cluster µs/op" is the CPU of all three nodes per operation. No resent
inputs, rewinds or term changes in any run.

### Commands

As in P4-T2 "Commands" (blocks A to D) with `CONNS="1 10 100"`, no
`--threads`, and TLS and cluster blocks run as `RUNS=1` rounds with the
binary order flipped each round; footprint as in P4-T4; chaos:
`BSTK_CHAOS_SEEDS=1000 cargo test --release -p bstk-chaos --test inprocess full -- --ignored`
and `BSTK_CHAOS_MP_RUNS=100 cargo test -p bstk-chaos --test multiprocess full -- --ignored`.

## P4-T6b: thread default per mode

### Summary

- **Default worker threads by mode**: 1 for standalone with no TLS listener and no binlog; 2 for standalone with any TLS listener (including mTLS) or with `-b`; 2 in cluster mode (unchanged). An explicit `--threads` / `server.threads` always wins. The rule is `ResolvedConfig::effective_threads`.
- **Why**: the P4-T6 matrix and a bisect showed the P4-T2 default of 1 worker costs throughput against the P3 build (e9cbb1f) for TLS and `-b` at 10 connections: one worker saturates at about 108% CPU. At 1 thread, `-b` at 10 connections is 0.92–0.94x P3 and TLS 10×4096 is 0.85–0.93x. With 2 workers every TLS and `-b` cell is within ±5% of P3 (TLS 0.98–1.06x, `-b` 1.02–1.07x) at lower CPU than P3. 4 workers buys nothing over 2.
- **Plaintext without `-b` stays at 1** for efficiency against the reference; users who want its throughput set `--threads`.

### Results (P4-T6b)

put-reserve-delete against the P3 build e9cbb1f. The machine was under load (1-minute average 6–14). TLS: 12 rotated rounds per cell; each cell shows the ratio of medians / the median paired ratio. `-b`: alternated, 6 runs. CPU is percent of one core, this build vs P3.

TLS, `--threads 2`:

| Cell (conns × bytes) | Ratio of medians / median paired ratio | CPU (this build vs P3) |
|---|---|---|
| 10×16 | 1.056 / 1.045 | 134% vs 162% |
| 10×4096 | 1.044 / 1.015 | 141% vs 173% |
| 100×16 | 1.033 / 1.037 | 169% vs 232% |
| 100×4096 | 0.988 / 0.978 | 173% vs 245% |

TLS, `--threads 4`: 10×16 1.021 / 1.009, 10×4096 1.027 / 1.025, 100×16 1.005 / 1.000, 100×4096 1.020 / 1.005, at about P3's CPU (160%, 173%, 219%, 232%). That is no gain over 2 workers for more CPU.

`-b`, `--threads 2`:

| Cell (conns × bytes) | Ratio of medians / median paired ratio | CPU (this build vs P3) |
|---|---|---|
| 10×16 | 1.015 / 1.025 | 162% vs 194% |
| 10×4096 | 1.060 / 1.047 | 160% vs 192% |
| 100×16 | 1.048 / 1.039 | 208% vs 254% |
| 100×4096 | 1.072 / 1.060 | 226% vs 264% |

At 1 thread (T6 and bisect): `-b` at 10 connections 0.92–0.94x, TLS 10×4096 0.85–0.93x.

### Why plaintext stays at 1 thread

Plaintext 16 B, current build:

| Connections | 1 thread | 2 threads |
|---|---|---|
| 10 | 99.6k ops/s at 81% CPU (8.2 µs/op) | 112.0k at 126% (11.4 µs/op) |
| 100 | 110.9k at 88% (7.9 µs/op) | 136.3k at 159% (11.6 µs/op) |

2 threads buys 12–23% throughput for about 1.4–1.5x the CPU per op, so plaintext stays at 1 for the efficiency target against the reference.

### Note: single-connection plaintext

The single-connection plaintext put-reserve-delete cell is 0.95–0.97x the reference at every build since P3 (bisect: reference vs P3 0.969 / 0.954 at 16 / 4096 B; P3 vs HEAD 1.007 / 1.005), with CPU equal to the reference. It is a latency-bound gap from the connection to engine task hop. It is pre-existing and not addressed in P4.

## P4-T5c: snapshot memory

### Summary

- **Target** (docs/PLAN.md §7.5): snapshot peak memory at most 1.5x the
  state size. *State size* here is the postcard payload of the snapshot
  (the `payload_len` field of the `.snap` header): the bytes a leader
  streams to a follower. *Extra* is a node's peak physical footprint
  during the operation minus its settled footprint holding the same
  state before it.
- **Result**: every phase now needs one pointer per job plus a few MiB
  of buffers, 0.04–0.17x the state size; before, 1–5x. Lock hold time of
  a build is about unchanged (it was `export_state`'s copy, now it is the
  encode into the page cache), shorter for small jobs, slightly longer for
  the mixed state; see docs/DESIGN.md §8 "Streamed snapshots (P4-T5c)".
- **Retention**: with the default macOS allocator, a node kept about
  500 MiB more after its first build of a 145 MiB payload (freed large
  blocks stay dirty in malloc's cache); now 1–9 MiB.

### Results (P4-T5c)

3 nodes on loopback, `MallocLargeCache=0` (see Method), 2 runs per row,
both runs shown (min–max). MiB of physical footprint.

Mixed state: 1,000,000 jobs × 16 B + 100,000 × 1 KiB in `default`;
payload 145.5 MiB, live footprint of the state 355.7–356.0 MiB.

| phase | before (HEAD 6671480) | after | before / after vs payload |
|---|---:|---:|---:|
| (a) build, extra on each node | 327.3–327.4 | 8.5 | 2.25x / 0.06x |
| (b) send to a wiped follower, extra on the leader | 151.4–151.6 | 6.0–6.1 | 1.04x / 0.04x |
| (c) receive + install on the wiped follower, peak over the state | 289.2–289.3 | 6.3–6.5 | 1.99x / 0.04x |
| (d) restart from its own snapshot, peak over the state | 286.3–286.4 | 6.2–6.3 | 1.97x / 0.04x |
| build: state lock held (ms, max of 3 nodes) | 160–212 | 197–289 | |
| build: total (ms) | 274–488 | 197–290 | |
| (c) wiped node start to caught up (s) | 1.42–1.78 | 1.15–1.43 | |
| (d) restart to ready (s) | 1.03–1.23 | 0.72–0.83 | |
| retained after the build, default allocator | 499–501 | 1.0–8.5 | |

Small jobs only: 2,000,000 jobs × 16 B; payload 89.6 MiB, live footprint
475.8–476.0 MiB (the engine holds about 5.3x the payload).

| phase | before | after | before / after vs payload |
|---|---:|---:|---:|
| (a) build | 452.2–452.5 | 15.4 | 5.05x / 0.17x |
| (b) send | 95.8 | 5.8–6.1 | 1.07x / 0.07x |
| (c) install | 347.5–347.7 | 13.4 | 3.88x / 0.15x |
| (d) restart | 347.5–347.6 | 13.5–13.6 | 3.88x / 0.15x |
| build: state lock held (ms) | 487–504 | 304–361 | |
| build: total (ms) | 723–746 | 304–362 | |

Where it went before: (a) `export_state` (a copy of every job record,
bodies shared), the payload `Vec` grown by doubling, and a second copy
of it for the file; (b) the whole file read into memory per send; (c)
the received buffer, the decoded `EngineState` whose job vector was
copied into the job table, and the file copy; (d) the file read twice
(once to verify), then the same decode. After: (a) a vector of
references to sort the jobs (8 B per job: 8.5 MiB for 1.1 M jobs, 15.4
for 2 M); (b) openraft's 3 MiB chunk plus tokio's file buffer; (c), (d)
a vector of boxed records (8 B per job) and the decoder's buffers.

Lock times are from a log line each node writes per build (for "before",
a scratch copy of HEAD with the same line added around
`export_state`); all three nodes build at the same moment on one host,
so they compete for CPU. In isolation (one process, the mixed state, 3
runs): `export_state` 115–187 ms; `state_view` encoded straight to a
file 138–175 ms; `state_view` encoded into a `Vec` 114–131 ms.

### Method and commands (P4-T5c)

`bench/snapshot_mem.py` (new): per run, a fresh 3-node cluster
(`snapshot_every` above the load, info logging), the jobs put through the
leader over 32 connections, then every node restarted with
`snapshot_every` 2,000 entries above its log, so each phase starts from a
settled process holding only the state. (a) single `use` commands until
every node has a snapshot; (b)+(c) kill -9 a follower, delete its data
directory, restart it (it rejoins; the leader's log is purged below the
snapshot, so it gets the snapshot); (d) kill -9 the other follower and
restart it. A thread samples `ri_phys_footprint` (`proc_pid_rusage`,
which counts compressed pages, unlike `ps`) of each node every 3 ms;
for the restarted processes the kernel's lifetime maximum is used too.

`MallocLargeCache=0` in the nodes' environment turns off macOS malloc's
cache of freed large blocks; with the cache, the first operation's
leftovers hide the next operation's peak (for HEAD: the 500 MiB kept
after the build absorbed the 145 MiB file read of the send, which then
showed as +3 to +7 MiB). `--large-cache` keeps the default, used for the
"retained" row.

```sh
cargo build --release -p bstk-server   # HEAD copied aside first
python3 bench/snapshot_mem.py --bin $BIN --scratch $DIR --reps 2
python3 bench/snapshot_mem.py --bin $BIN --scratch $DIR --reps 2 --large-cache
python3 bench/snapshot_mem.py --bin $BIN --scratch $DIR --reps 1 --small 2000000 --large 0
```

### Environment (P4-T5c)

Same machine and toolchain as P4-T5b (Apple M6, 12 cores, 34 GiB,
macOS 27.0, rustc 1.98.1, release with `debug = 1`), openraft 0.9.25,
plaintext cluster traffic on loopback, data directories under
`/private/tmp` (APFS). An OrbStack VM of another user was running
throughout; footprint numbers repeated within 0.1 MiB across runs except
the default-allocator rows.

## P4-T5b: cluster wake-ups at low load

### Summary

- **Where one-connection cluster CPU went** (3 nodes on loopback, 2 worker
  threads, put-reserve-delete, 16-byte bodies, one command in flight):
  about two thirds system time, spent waking threads, not doing work.
  Per entry (one client command) a follower used 12.4 context switches and
  25 syscalls, the leader 17.7 and 44; tokio's runtime metrics (a
  `tokio_unstable` scratch build) showed 6.6 worker parks per entry on a
  follower, 3.4 of them "no-op" (woken, nothing to run), and 11.1 / 4.2
  on the leader. A follower has three outside events per entry (the
  AppendEntries with the entry, the flush worker's callback, and the
  commit-only AppendEntries openraft 0.9 sends right after every commit),
  the leader about six. `fdatasync` itself costs about 16 µs of system
  time per call on this machine (a standalone Python loop), once per entry
  and node.
- **Dominant avoidable cause: macOS write readiness.** Every write on a
  tokio `TcpStream` produced a spurious wake-up: kqueue's write filter
  fires on each acknowledgement. A minimal ping-pong (one worker) does
  2.0 parks per round trip with `TcpStream` and 1.0 with a read-only
  registration. Fixed with `QuietTcp` (docs/DESIGN.md §8, "Fewer
  wake-ups"), together with fewer task wake-ups (Raft view watchers on
  `server_metrics`, followers' leader duties not woken per apply, the
  actor pulling applied events) and buffered frame reads.
- **Counters per entry, before → after** (2 worker threads, via the
  leader; load-independent): follower parks 6.6 → 5.0 (no-op 3.4 → 1.8),
  context switches 12.4 → 10.4, syscalls 25 → 21; leader parks 11.1 →
  8.7 (no-op 4.2 → 2.1), context switches 17.7 → 16.9, syscalls 44 → 38.
  With 1 worker thread and the changes, a follower parks exactly 3.0
  times per entry (its three events, no no-op wake-ups) and the leader
  5.7.
- **CPU per operation at one connection** (medians of 3 interleaved
  rounds, `ps` CPU of all three nodes / ops): via the leader 343 → 235 µs
  (-31%), target node 157 → 110 µs; via a follower 367 → 280 µs (-24%).
  Throughput rose with it (4,124 → 5,742 and 3,453 → 3,991 ops/s). **The
  "at least halved" target is not met at the default 2 worker threads.**
  With `--threads 1` the new build measured 235 µs/op via the leader in
  the matrix (base 280), and 158 µs/op in a separate quieter run (load 7,
  `meas.sh`, below): the remaining gap is the second worker's no-op
  wake-ups plus the openraft-0.9 message pattern.
- **100 connections:** unchanged within noise (leader 18.4 → 18.6 µs/op at
  2 workers; 15.5 → 15.7 at 1 worker; follower 25.1 → 22.3 / 21.5 →
  21.7). The ≤ 15 µs gate is not reached at this machine load at either
  thread count (P4-T2 measured 9.6 µs/op at 1 worker on a quieter day).
- **Failover** (leader `kill -9` to a put via a surviving follower
  answered `INSERTED`, release builds, alternating, 4 runs each): base
  1.20–1.29 s, new 1.22–1.31 s; no change. Heartbeat and election
  timeouts are unchanged.
- **Standalone** (the client-socket change applies there too; 3
  interleaved runs, load 9–19): 1 connection 23.8k → 32.2k ops/s, 12.7 →
  9.8 µs/op; 100 connections 118.7k → 105.3k ops/s (ranges 94.7k–121.7k
  and 41.8k–116.8k: one run hit a load spike of 18), 7.4 → 8.3 µs/op.
  Within this session's noise; worth re-checking in P4-T6's full matrix.
- **Not done** (DESIGN §8): inline log sync (about -10% on followers in a
  prototype, but blocks a runtime worker through a sync stall), a
  cancel-safe direct frame writer, and changing the cluster thread
  default (P4-T2's decision; the data here favours 1 worker for CPU per
  operation, at a throughput cost at 100 connections on this loaded
  machine: 76k vs 101k ops/s via the leader).

### Results (P4-T5b)

`base` = HEAD `63aac83`, `new` = the P4-T5b changes; `def` = default
threads (2 in cluster mode), `t1` = `--threads 1`. Rounds alternate
base/new (and def/t1) through `bench/run-matrix.sh` with `RUNS=1`,
`DURATION=10`. Medians with ranges; "cluster µs/op" is all nodes' `ps`
CPU over ops, "target µs/op" the target node's `stats` CPU over ops;
"load" lists each run's 1-minute load average. No resends, rewinds,
term or leader changes in any run.

| mode | conns | build | ops/s | cluster CPU % | cluster µs/op | target µs/op | load |
|---|---:|---|---:|---:|---:|---:|---|
| cluster-leader | 1 | base-def | 4,124 (3,946–4,180) | 142 (139–143) | 343.4 (342.1–352.7) | 156.7 (154.6–159.9) | 9.27 9.55 12.78 |
| cluster-leader | 1 | new-def | 5,742 (4,421–6,243) | 130 (129–135) | 235.3 (208.2–292.7) | 110.4 (95.8–134.4) | 9.98 11.28 10.21 |
| cluster-leader | 1 | base-t1 | 4,095 (4,036–4,393) | 115 (113–124) | 280.0 (262.5–303.0) | 122.4 (114.5–129.9) | 9.32 9.88 11.32 |
| cluster-leader | 1 | new-t1 | 4,408 (4,336–5,906) | 104 (104–106) | 235.2 (175.6–244.9) | 105.5 (76.0–107.9) | 8.64 10.51 14.13 |
| cluster-follower | 1 | base-def | 3,453 (2,874–3,825) | 127 (122–127) | 366.9 (332.0–426.3) | 118.1 (106.4–138.8) | 9.43 11.95 11.59 |
| cluster-follower | 1 | new-def | 3,991 (3,613–5,259) | 112 (110–121) | 279.9 (230.5–303.3) | 94.0 (76.3–101.3) | 8.90 10.78 9.88 |
| cluster-follower | 1 | base-t1 | 3,486 (3,266–3,662) | 105 (102–111) | 311.6 (287.6–318.1) | 106.5 (98.0–108.4) | 8.61 9.68 10.91 |
| cluster-follower | 1 | new-t1 | 3,623 (3,210–3,785) | 95 (92–97) | 261.4 (255.2–287.3) | 89.1 (88.5–98.5) | 9.21 9.99 12.62 |
| cluster-leader | 100 | base-def | 112,840 (97,256–113,162) | 207 (203–208) | 18.4 (18.0–21.4) | 13.4 (13.1–15.7) | 9.27 11.20 11.19 |
| cluster-leader | 100 | new-def | 101,036 (89,472–135,799) | 196 (166–202) | 18.6 (14.9–19.4) | 13.8 (11.3–14.6) | 8.38 9.71 9.20 |
| cluster-leader | 100 | base-t1 | 77,942 (52,343–82,256) | 121 (81–123) | 15.5 (15.0–15.5) | 10.9 (10.5–11.0) | 7.98 10.52 10.66 |
| cluster-leader | 100 | new-t1 | 76,250 (68,477–85,623) | 120 (118–121) | 15.7 (14.2–17.3) | 11.3 (10.2–12.5) | 8.95 12.90 11.37 |
| cluster-follower | 100 | base-def | 89,886 (85,226–123,883) | 226 (222–237) | 25.1 (17.9–27.9) | 15.2 (11.0–16.7) | 9.98 10.88 11.23 |
| cluster-follower | 100 | new-def | 95,313 (85,687–96,387) | 215 (202–228) | 22.3 (21.2–26.6) | 14.1 (13.4–16.7) | 7.66 10.06 10.24 |
| cluster-follower | 100 | base-t1 | 77,015 (69,829–98,762) | 166 (166–180) | 21.5 (18.2–23.7) | 11.1 (9.4–12.2) | 8.12 10.19 14.33 |
| cluster-follower | 100 | new-t1 | 72,256 (67,190–72,764) | 156 (154–158) | 21.7 (21.6–22.9) | 11.8 (11.7–12.4) | 8.74 13.21 10.70 |

Two further one-connection rounds ran at load 13–28 (another user's VM
was busy); their throughput fell to 1.4k–2.5k ops/s in places, so they
are left out of the table (raw CSVs keep them only in the scratch
directory).

Per-node counters, one connection via the leader, 10 s (`top` CSW and
BSD syscalls, `ps -M` thread CPU, tokio worker metrics), followers / leader:

| build | threads | CPU µs/op | ctx switches/op | syscalls/op | parks/op | no-op parks/op | load |
|---|---:|---|---|---|---|---|---:|
| base | 2 | 99, 99 / 166 | 12.4 / 17.7 | 25 / 44 | 6.6 / 11.1 | 3.4 / 4.2 | 12 |
| new | 2 | 68, 68 / 114 | 10.4 / 16.9 | 21 / 38 | 5.0 / 8.7 | 1.8 / 2.1 | 9 |
| new | 1 | 46, 46 / 66 | 8.2 / 7.7 | 17 / 30 | 3.0 / 5.7 | 0.0 / 1.2 | 7 |

(base's parks are from a scratch build of the first two changes, which
did not move the counters; base CPU and counters from the base binary.)

### Method and commands (P4-T5b)

```sh
cargo build --release -p bstk-server -p bstk-bench   # base, copied aside first
# A/B rounds (RS_BIN swapped per invocation; 3 rounds, def and t1):
RS_BIN=$BIN RS_ARGS="$ARGS" SERVERS=rs SCENARIOS=put-reserve-delete CONNS="1 100" \
  BODIES=16 RUNS=1 DURATION=10 CLUSTER_NODES=3 \
  SERVER_MODES="cluster-leader cluster-follower" OUT_CSV=$OUT bench/run-matrix.sh
# Standalone: the same with SERVER_MODES=none.
```

Counters: a 3-node cluster started by hand; `top -l 1 -pid P -stats
pid,csw,sysbsd` and `ps -M -p P` before and after a 10 s `bstk-bench`
run; tokio metrics from a scratch build (`RUSTFLAGS="--cfg
tokio_unstable"`, a thread printing `worker_park_count` /
`worker_noop_count` per second), not part of the tree. Failover: kill
-9 the leader, time a `put` via a follower until `INSERTED`.

### Environment (P4-T5b)

Same machine and toolchain as P4-T2 (Apple M6, 12 cores, macOS 27.0,
rustc 1.98.1, release with `debug = 1`), openraft 0.9.25, plaintext
cluster traffic on loopback, data directories under `/private/tmp`.
Background load 7–14 during the table's runs (an OrbStack VM of another
user), higher (13–28) during the extra rounds.

## P4-T4: footprint

### Method

- **Tool**: `bench/footprint.py` (subcommands `mem` and `binlog`), a
  standalone script rather than a `bstk-bench` scenario: it needs exact
  job counts and per-connection tube assignment (not a duration-based
  load), and for `binlog` it reads a process's own I/O counters, which
  `bstk-bench` has no reason to know about. `bench/pidrusage.c` is a
  ~40-line C helper around macOS's `proc_pid_rusage`; see "Binlog bytes"
  below for why. Both are reusable: `python3 bench/footprint.py mem
  --help` / `binlog --help`.
- **Memory per job**: for each (body size, job count, tube count) cell,
  starts a server fresh with no `-b`, samples RSS (`ps -o rss=`, polled
  until two reads 300 ms apart agree within 1%), pipelined-puts `n` jobs
  of that body size (content is a random slice per job, not `b'x' * n`,
  so identical pages can't be folded), confirms `current-jobs-ready == n`
  via `stats`, samples RSS again, deletes every job (ids are `1..n` on a
  fresh server, both servers), confirms `current-jobs-ready == 0`, and
  samples RSS a third time (retention/fragmentation). Bytes/job =
  `(loaded - baseline) * 1024 / n`. 3 repetitions, medians reported (a
  few cells, in some runs, show one rep well off the other two -- e.g. an
  earlier run of this same build (with the pre-fix tube loader below,
  so `1,000` there meant 512 tubes) read `rs 16×1,000,000×1,000` rep 2 at
  176.8 B/job (loaded RSS 180,656 KiB) against its other two reps' ~286.5
  (loaded ~287,000 KiB each), with that same rep's *after-delete* RSS
  (254,320 KiB) coming in *above* its own loaded RSS. `stats` confirmed
  all jobs were present before every "loaded" sample, so this isn't the
  settle-loop returning early; it's consistent with the memory compressor
  paging part of the process out under the host's memory pressure (the
  busy Docker VM, see Environment) around the "loaded" sample -- `ps`'s
  RSS excludes compressed pages. The shipped run
  (`bench/results/2026-09-27-p4-t4-mem.csv`) happened to be a clean one:
  every cell's 3 reps agree within 2.9%, so the median in the table below
  isn't masking a swing this large, but the mechanism is real and worth
  knowing about if a future run looks noisier).
- **Binlog bytes**: macOS has no per-process write-byte counter in `ps`
  or `/usr/bin/time -l`, and `getrusage` doesn't have one either.
  `bench/pidrusage.c` reads `proc_pid_rusage(pid, RUSAGE_INFO_V4, ...)`,
  which has both `ri_logical_writes` (bytes passed to `write`/`pwrite`,
  counted the instant the syscall returns) and `ri_diskio_byteswritten`
  (actual block I/O, which lags behind writeback and is undercounted if
  read too soon -- confirmed by writing 64 MiB with no `fsync`: logical
  read the full 64 MiB immediately, diskio read 56 MiB a second later).
  `logical_writes` is the metric the acceptance gate uses; `diskio` is
  reported alongside as a loose cross-check. A control run (`ftruncate`
  to 64 MiB, no `write`) showed 0 on both counters, confirming neither
  counts sparse preallocation -- moot here since both the reference
  (`rawfalloc`, `file.c`) and beanstalkd-rs (`preallocate`, `wal.rs`)
  preallocate by writing real zero bytes, not `ftruncate`.
  For each (workload, body size, segment size) cell: starts a server
  fresh with `-b <fresh dir> -s <size>` (default fsync; fsync policy
  doesn't affect `logical_writes`, which counts the `write()` calls
  themselves, not their durability), reads `pidrusage` once after a
  0.2 s settle (`_wait_ready` only requires the port to accept
  connections, and both servers `listen()` before finishing their WAL
  setup -- `make_server_socket` before `srv_acquire_wal` in the
  reference's `main.c`, and step 3 before step 5 in our own `main.rs` --
  so either server's first-segment preallocation could still be
  in flight when the "before" `pidrusage` sample is taken; the largest
  possible effect is the whole preallocation landing inside the window,
  ~52 B/cycle at the 10 MiB segment size (10,485,760 / 200,000) and
  under 1 B/cycle at 64 KiB -- not enough to change any conclusion
  below either way), runs 200,000 cycles of the workload on one connection
  (ids are sequential and deterministic, so the whole run is pipelined
  in chunks of 200 rather than round-tripped one command at a time),
  reads `pidrusage` again after a 1 s settle (for the `diskio`
  cross-check). "B/op" in the table below is logical bytes ÷ 200,000
  *cycles*, not ÷ command count: `churn` is one cycle = 3 commands,
  put→reserve→delete (2 journaled records: put, delete); `mixed` is one
  cycle = 9 commands,
  put→reserve→bury→kick-job→reserve→release(delay=1)→kick-job→reserve→delete
  (6 journaled records: put, bury, kick, release, kick, delete;
  `kick-job` targets a specific id so the next step is exact). In both
  workloads 1 job in 50 is put and buried but never revisited, so the
  binlog keeps some live records across many segment rotations --
  otherwise a small `-s` only exercises file turnover, never an actual
  compaction move of live data forward (COMPAT "Binlog" item 7). A
  single run per cell (the byte count is deterministic given the op
  sequence, not a timing measurement); `binlog-records-written` from
  `stats` is logged alongside as a sanity check (it isn't expected to
  match exactly once compaction runs, since compaction moves are
  masked in the differential suites too, but it is expected to be the
  same order of magnitude, and it is: 401,907-517,859 for churn vs the
  reference's 402,152-494,243).
- **Environment**: Apple M6, 12 cores, 34 GiB RAM, macOS 27.0 (Darwin
  27.0.0). The host also runs a busy Docker VM intermittently during the
  session (`sysctl vm.loadavg` 1-minute figures from 15 to 71 were
  observed across these runs, well above the machine's core count, and
  rising over the session). Byte counts are exact given the same op
  sequence, so CPU/scheduling noise can't change what either server
  *wrote*; RSS is a different story -- it *is* sensitive to memory
  pressure through macOS's memory compressor (see the outlier example
  above), which is presumably more active with the Docker VM competing
  for memory. The settle-loop only guards against an in-progress
  allocation, not a compressed page; 3 reps and taking the median is the
  actual defense (see the Method note above for a case where it mattered
  in an earlier run). The shipped run's reps agree within 2.9% in every
  cell. Reference binary:
  `.ref/beanstalkd-opt/beanstalkd` (`scripts/build-ref.sh --optimized`).
  Raw data: `bench/results/2026-09-27-p4-t4-mem.csv`,
  `bench/results/2026-09-27-p4-t4-binlog.csv`.

### Cause found, and fixed

A 100k×16 B smoke test (before any fix) measured memory per job at 17.6x
the reference under pipelined loading (the loader `bench/footprint.py`
recommends, and any realistic producer uses): ~4,377 B/job against the
reference's ~249. A non-pipelined loader (one put per round trip, same
unfixed build) cost only ~565 B/job, isolating pipelining as the
trigger.

**Cause**: `ServerCodec::decode` (`crates/proto/src/codec.rs`, unchanged
by this task) builds a put's body with `src.split_to(need).freeze()` --
a *view* into the connection's read buffer (`rbuf`), not a copy.
`bytes::BytesMut::reserve` can only grow a buffer in place when nothing
else references its backing allocation; as soon as any live job body is
a view into it (true for essentially every job, since jobs commonly
outlive the connection that created them by a lot), every later
`reserve` on `rbuf` must allocate a fresh buffer instead and abandon the
old one -- which stays resident for as long as that one job body is
alive, i.e. forever, until the job is deleted. Under pipelining, the
server reads many commands per syscall, so this happened roughly once
per buffer refill: confirmed by writing a 20,000-put probe with and
without pipelining (565 vs 4,589 B/job) and by the fact that 4,377 is
close to `INITIAL_BUF_CAPACITY` (4 KiB, `conn.rs`) -- each abandoned
buffer was close to one buffer-full's worth, retained by whichever job
happened to still reference it.

This task's edit scope is `crates/engine` and `crates/store`, so the fix
does not touch `codec.rs`; instead it breaks the sharing where the body
enters the engine (`Engine::cmd_put`, `crates/engine/src/engine.rs`),
which is equally effective because a connection only ever has one put
in flight at a time (`await_reply` in `crates/server/src/conn.rs` never
decodes a further command while a reply is outstanding): the mechanism
is not that decoding runs ahead of the engine, but that one socket read
commonly delivers many pipelined puts at once, and every body sliced out
of that one read shares its buffer; copying each one out in `cmd_put`,
right before it is stored, removes the sharing before the job outlives
the read that produced it. A `crates/proto/src/codec.rs` fix at the
source (copy at decode time instead of at the engine) was prototyped
first and measured: 100k×16 B 272.8, 1M×16 B 285.7 B/job -- both within
1% of the final numbers below -- before being reverted for scope; it is
a cleaner fix in that it also avoids the transient reallocation, and is
worth the lead's consideration for a follow-up outside P4-T4's scope.

**Fix** (`crates/engine/src/engine.rs`, `cmd_put`): copy the body into
its own allocation (`Bytes::copy_from_slice`) as the first thing
`cmd_put` does, before it's ever stored. This costs one extra memcpy per
put (proportional to body size). Measured with only this fix in place
(no `Box`, and using the `codec.rs` variant, which is what P4-T4 had
built at the time): 100k×16 B pipelined 4,377 → 440.6 B/job (ratio
1.79x); 1M×16 B 660.5 B/job (ratio 2.73x) -- the copy fixes the
retention, but the 1.79x/2.73x remaining was still over the 1.5x gate,
especially at 1,000,000 jobs, so a second fix followed:

**Fix 2** (`crates/engine/src/engine.rs`): box the hash map's values,
`HashMap<JobId, JobRec>` → `HashMap<JobId, Box<JobRec>>`. `hashbrown`
resizes at 7/8 load and (like `std::collections::HashMap`) stores each
value inline in its slot array, so every slot -- empty or occupied --
costs a full `JobRec` (136 B) until the next resize, not just a few
bytes; at 1,000,000 jobs the table had *just* crossed a doubling (from
1,048,576 to 2,097,152 slots for 1,142,858+ needed) and stays only
47.7% full until about 835k more inserts (7/8 of 2,097,152 is
1,835,008), which is why the 1M
cells were worse than the 100k ones even after the copy fix. The
reference's chained hash table (`job.c`) only ever costs one 8-byte
pointer per slot; the `Job` itself is a separate, exactly-sized
allocation either way. Boxing does the same: the table's per-slot cost
drops from 136 B to a pointer (8 B), and the `JobRec` becomes its own
allocation, sized exactly, unaffected by the table's load factor.
`EngineState` (the P3 snapshot / cluster-transfer type) still stores
jobs as `Vec<JobRec>`, not `HashMap`, so the `Box` never reaches
serialization and the on-wire snapshot format is unchanged by
construction, not merely by test coverage.

The measured drop from adding this fix is bigger than the load-factor
arithmetic alone predicts: at 1M jobs, 2,097,152 slots going from 145
B/slot (`JobId` + inline `JobRec` + a control byte) to 17 B/slot
(`JobId` + a pointer + a control byte), holding 1,000,000 boxed
`JobRec`s on the side, works out to about 124 B/job saved; the measured
drop was ~375 B/job (660.5 → 285.7). At 100k the same arithmetic gives
~24 B/job against a measured ~168 B/job (440.6 → 272.8). The remainder
is consistent with -- not separately confirmed here -- the *previous*
table generations from earlier doublings (512k, 256k, 128k, ... slots,
each freed when the map outgrew it): their sizes sum to roughly the
current table's size at the old 145 B/slot cost, and macOS's allocator
doesn't necessarily return that memory to the OS immediately, so `ps`'s
RSS can keep counting it. This doesn't change any conclusion below (if
anything it makes the reported rs figures slightly conservative, by
perhaps another ~36 B/job at 1M once that stale memory is eventually
reclaimed). With both fixes together: 272.8 B/job at 100k×16 B and
285.7 at 1M×16 B (ratios 1.10x-1.18x on that run, 1.10x-1.20x on the
final matrix below) -- the two job counts now behave alike instead of
1M being the visibly worse case.

Both fixes are engine-internal: no protocol-visible change, verified by
the full differential and oracle-proptest suites (`cargo test
--workspace` via `scripts/check.sh`, 45 test binaries, all green; one
`bstk-chaos` multi-process test and one `bstk-server` cluster test each
failed once with a connection reset during this session's runs and
passed on every other attempt, including 5 back-to-back reruns of the
cluster one -- consistent with this host's load rather than these
changes, see Environment) and a rough throughput/CPU sanity check
(put-reserve-delete, 100×16, 1 worker thread; `bstk-bench`, not the
full P4-T6 matrix): four quick 5 s runs at varying host load
(`vm.loadavg` 24-71 during these checks) gave ops/s ratios of
0.88x-1.22x against the reference (gate ≥1.0x; 2 of 4 runs below it)
and ops/CPU-second ratios of 0.96x-1.17x (gate ≥0.8x; every run above
it). This host, at this load, cannot answer whether the two extra
per-put allocations (the body copy, the `Box`) cost real throughput --
the swing between runs is bigger than any effect they could plausibly
have. It is reported as-is rather than smoothed over; a formal re-check
under controlled load is P4-T6's job, not this one's, and the lead may
want it prioritized given this uncertainty.

A cheap A/B narrows this a little: the pre-P4-T4 build (`git archive
HEAD`, built separately) alternated against the current build -- always
pre-fix first, current second, within each pair, so drift within a pair
isn't balanced out -- same scenario, 100 conns, 1 worker thread, 7 valid
5 s runs each (host load 17-33, one pair discarded for a parse error):
median ops/s ratio (current/pre-fix) 1.16x, median ops/CPU-second ratio
1.06x, with one outlier pair at 0.53x/0.58x (a load spike coincident
with the "current" half of that pair, not a trend -- the other 6 pairs
range 0.94x-1.40x throughput, 0.99x-1.29x efficiency). A 1.16x median
"speedup" from a change that only adds work (a memcpy, a heap
allocation) is itself a sign this is measuring host noise, not a real
effect: no evidence of a regression, but the fixed pre-fix-first
ordering means this can't rule one out either. Not a substitute for
P4-T6's controlled matrix.

No binlog fix was needed or attempted (see the table below): every
workload/body/segment cell already came in at or under 1.0x. Whether the
copy happens in the codec or the engine cannot change binlog bytes
either way (the WAL only ever sees the `JobRec` the engine stores,
already an owned copy in both variants); a spot check of one cell
(`churn`, 16 B, 10 MiB segments) after settling on the engine-side fix
read 268.6 B/op against the table's 268.9, confirming this.

The same class of bug was checked for on binlog recovery
(`crates/store/src/replay.rs`, where `Engine::recover` gets a job's body
back from a segment read): both sites that hand a body to a `RecoveredJob`
already use `Bytes::copy_from_slice(body)`, not a slice of the read
buffer, so recovered jobs don't pin whole segments the way pipelined puts
pinned read buffers. No fix needed there.

### Memory per job

Medians of 3 reps; ratio = rs / ref; "after delete" is the third RSS
sample (retention/fragmentation after every job is deleted, not itself
gated).

| body | n | tubes | ref B/job | rs B/job | ratio | ref after-delete | rs after-delete |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 | 100,000 | 1 | 247.2 | 272.5 | 1.10 | 26,128 KiB | 34,592 KiB |
| 16 | 100,000 | 1,000 | 251.3 | 266.2 | 1.06 | 26,336 KiB | 34,336 KiB |
| 16 | 1,000,000 | 1 | 241.9 | 285.5 | 1.18 | 238,592 KiB | 254,240 KiB |
| 16 | 1,000,000 | 1,000 | 240.2 | 287.4 | 1.20 | 237,184 KiB | 256,192 KiB |
| 1,024 | 100,000 | 1 | 1,390.7 | 1,298.4 | 0.93 | 137,648 KiB | 134,880 KiB |
| 1,024 | 100,000 | 1,000 | 1,396.4 | 1,311.2 | 0.94 | 138,224 KiB | 136,800 KiB |
| 1,024 | 1,000,000 | 1 | 1,387.3 | 1,299.4 | 0.94 | 1,357,040 KiB | 1,244,704 KiB |
| 1,024 | 1,000,000 | 1,000 | 1,386.1 | 1,301.6 | 0.94 | 1,355,872 KiB | 1,246,928 KiB |

("1,000 tubes" is exactly that: each worker connection owns a disjoint
slice of the 1,000 named tubes and puts that tube's exact share, `n /
1,000` jobs, rather than round-robining large batches across tubes: an
earlier version of the loader cycled one 2,000-job batch per tube per
iteration, needing only 4 iterations at 100k jobs / 16 connections and
32 at 1M, so it only ever reached 64 or 512 of the requested 1,000
tubes; `current-tubes` is now checked against `tubes + 1` after loading
to catch a regression here.)

Worst case 1.20x, against the 1.5x gate. 1 KiB bodies run *under* 1.0x
(the body itself dominates, and beanstalkd-rs's per-job structure
overhead is smaller). 1,000 tubes vs. 1 tube makes no material
difference for either server at these job counts (a `TubeState` is
~250 B, amortized over 100-1,000 jobs/tube at 100k-1M jobs). `rs`
after-delete stays well above baseline: hashbrown's jobs table doesn't
shrink on removal (its ~2^21-slot, 17 B/slot array for the 1,000,000-job
cells alone accounts for ~36 MB of the ~246 MB retained), and the rest
is ordinary allocator behavior -- freed small blocks (`BTreeSet`/`Box`
allocations) kept for reuse rather than returned to the OS. The
reference's after-delete also stays near its loaded figure, but not for
the same reason: deleting every job does drop its table below the 1/16
load `rehash(0)` shrinks at, so its hash table itself goes back down;
what's retained there is the allocator holding on to the freed `Job`,
body and `Heap` array blocks instead of returning them to the OS -- the
same generic allocator behavior, just without the table contribution.

### Binlog bytes per operation

Medians are not applicable (1 run/cell, see Method); ratio = rs / ref
on `logical_writes`, the gated metric. "B/op" = logical bytes ÷ 200,000
cycles (3 commands/cycle for `churn`, 9 for `mixed`; see Method).

| workload | body | seg size | ref B/op | rs B/op | ratio | ref records | rs records |
|---|---:|---:|---:|---:|---:|---:|---:|
| churn | 16 | 64 KiB | 572.5 | 395.3 | 0.69 | 494,243 | 517,859 |
| churn | 16 | 10 MiB | 393.3 | 268.9 | 0.68 | 402,152 | 401,907 |
| churn | 1,024 | 64 KiB | 3,886.3 | 3,793.5 | 0.98 | 489,757 | 490,973 |
| churn | 1,024 | 10 MiB | 2,985.5 | 2,510.5 | 0.84 | 425,417 | 423,201 |
| mixed | 16 | 64 KiB | 1,506.7 | 1,210.1 | 0.80 | 1,420,711 | 1,513,470 |
| mixed | 16 | 10 MiB | 1,186.9 | 795.2 | 0.67 | 1,193,959 | 1,190,808 |
| mixed | 1,024 | 64 KiB | 4,944.7 | 4,534.8 | 0.92 | 1,297,295 | 1,295,152 |
| mixed | 1,024 | 10 MiB | 3,815.6 | 3,119.1 | 0.82 | 1,216,674 | 1,213,875 |

Worst case 0.98x, against the 1.2x gate -- every cell already favors
`beanstalkd-rs`, so no fix was attempted. This tracks the on-disk record
formats: the reference's short record (used for bury, kick, release and
delete, `filewrjobshort`, `file.c`) is `int nl=0` (4 B) + the in-memory
`Jobrec` reused as-is (80 B) = 84 B regardless of the transition, and
its full (put) record is 4 + tube_len + 80 + body_len; beanstalkd-rs's
delete record is a flat 17 B (kind + id, `format.rs`) and its
update record (bury/kick/release) is 66 B, put is 67 + tube_len +
body_len -- about 20% smaller on puts and far smaller on
delete/update, because the on-disk `JobRecord` (57 B) was sized for the
format rather than reusing the in-memory struct, and delete doesn't
carry a full record at all. A small `-s` (64 KiB) narrows the margin
(the fixed per-segment preallocation, identical real zero-byte writes
on both sides, is a larger fraction of the total at that size) but
never crosses 1.0x in these runs.

## P4-T2: worker threads

### Summary

- **Standalone default: 1 worker thread.** It is the only thread count of {1, 2, 4} that meets the standalone efficiency gate (≥ 0.8x the reference's ops per CPU-second at 10 and 100 connections): 2 workers gives 0.63x–0.78x and 4 workers 0.50x–0.69x in the same 8 cells (monotonically worse with more workers, as the P4-T1 spike found). All three meet the throughput gate (≥ 1.0x every non-pipelined cell: 1.02x–1.37x), and 1 worker also clears the `-b` and TLS gates (≥ 0.95x the P3 build) and the burst gate (plaintext p99 within 2x of no-burst) with room to spare, so the "smallest thread count that meets every gate" is also the only one that meets the first.
- **TLS's "needs several cores" assumption (§7.3) does not hold on this machine**: at 1 worker and 100 TLS connections, throughput is 1.06x the P3 build (206k vs 195k ops/s) at 0.43x the CPU (98% vs 226%, P3's default 12 workers). `-b` shows the same pattern (147% vs 263% of a core at 100×16): the WAL's dedicated OS thread is now the *second* thread instead of the *thirteenth*.
- **Cluster default: 2 worker threads.** All three thread counts already beat the P3 build's cluster throughput measured in the same session (100×16: P3 130.9k/105.2k via leader/follower; 1 worker 142.3k/117.4k; 2 workers 147.3k/119.3k; 4 workers 135.9k/109.5k), so the rule picks the best: 2 workers, at 215% total node CPU for +3.5% leader throughput over 1 worker's 136%. At 1 worker, cluster CPU per operation is already about 2.9 core-seconds / 142k ops ≈ 9.6 µs, ahead of P4-T5's ≤ 15 µs target; this is a P4-T5 note, not a P4-T2 decision.
- **What 1 worker gives up**, none of it gated: pipelined throughput (1.28x–1.38x vs 2 workers' 1.50x–1.78x — the only cells where more workers win on both throughput and efficiency); burst-scenario cost (plaintext p99 +35% under an 800/s TLS handshake burst, vs +18% at 2 workers and +1% at 4; ops/s -10% vs -5%/-5%); and the `producers-consumers` 100×16 cell, the one standalone cell where rs put p99 (886 µs) is higher than the reference's (716 µs) — 100 consumers sharing one worker queue behind each other's reserve wake-ups.
- **Noise**: a few cells are flagged by `summarize.py` (> 20% spread across 5 runs), all consistent with host jitter rather than a systematic effect, medians unaffected. `producers-consumers` 100×16 at 1 worker: run 1 was low on *both* the reference (153k vs 189k–193k the other 4 runs) and `beanstalkd-rs` (182k vs 190k–205k) — a transient spike at the start of that invocation, caught evenly by alternation. `producers-consumers` 10×16 at 1 worker: only `beanstalkd-rs` dipped, in run 4 (134k vs 154k–184k the other 4 runs; the reference stayed within 134k–140k throughout); `load_avg` was unremarkable for that run (4.96, mid-range of 4.9–6.0 across the 5). `producers-consumers` 100×16 at 2 workers: the reference's run 2 dipped to 109k (vs 150k–195k the other 4 runs) while `beanstalkd-rs` stayed in a narrower 164k–220k band the same runs, during a period when `load_avg` climbed from 7.2 to 12.2 (the highest measurement on both sides came at the highest load, so this is not a simple load correlation either).
- The P3-FD table (below) reported 135,256 / 108,313 ops/s via leader/follower at 100×16; this session's P3 rerun gives 130,918 / 105,243 (about 3% lower), consistent with the higher background load (1-minute average 3.6 climbing to 7.6 across the run, vs P3-FD's 4.7 starting load) — the "allowing for machine noise" the rule anticipated. All thread-count comparisons below use this session's own P3 rerun as the baseline, not the P3-FD figures.

### Standalone plaintext vs the optimized reference

1 worker (the chosen default), full matrix, `bench/summarize.py --efficiency`:

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | ref ops/CPU-s | rs ops/CPU-s | eff rs/ref | ref CPU % | rs CPU % | ref put p99 µs | rs put p99 µs | ref reserve p99 µs | rs reserve p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 1 | 143,710 | 186,675 | 1.30 | 203,493 | 222,763 | 1.09 | 71 | 84 | 109 | 104 | 110 | 104 |
| put-reserve-delete | 10 | 4096 | 1 | 135,970 | 179,304 | 1.32 | 189,544 | 211,002 | 1.11 | 72 | 85 | 124 | 111 | 111 | 111 |
| put-reserve-delete | 100 | 16 | 1 | 189,708 | 208,501 | 1.10 | 193,480 | 211,676 | 1.09 | 98 | 99 | 706 | 618 | 707 | 618 |
| put-reserve-delete | 100 | 4096 | 1 | 180,154 | 206,996 | 1.15 | 183,084 | 209,643 | 1.15 | 98 | 99 | 953 | 650 | 671 | 652 |
| producers-consumers | 10 | 16 | 1 | 138,730* | 172,661* | 1.24 | 190,040 | 201,236 | 1.06 | 73 | 84 | 125 | 129 | 126 | 129 |
| producers-consumers | 10 | 4096 | 1 | 126,708 | 154,318 | 1.22 | 171,691 | 178,816 | 1.04 | 74 | 86 | 149 | 150 | 127 | 150 |
| producers-consumers | 100 | 16 | 1 | 191,820* | 197,127* | 1.03 | 195,535 | 202,805 | 1.04 | 98 | 97 | 716 | 886 | 718 | 886 |
| producers-consumers | 100 | 4096 | 1 | 168,569 | 191,188 | 1.13 | 173,962 | 195,191 | 1.12 | 97 | 98 | 1,155 | 816 | 816 | 829 |

Pipelined (10/100×16×16, informational, no gate):

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | eff rs/ref | ref put p99 µs | rs put p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 16 | 289,724* | 371,836* | 1.28 | 1.29 | 867 | 667 |
| put-reserve-delete | 100 | 16 | 16 | 277,202 | 383,101 | 1.38 | 1.34 | 8,388 | 5,309 |

2 workers, same matrix (efficiency fails every cell; kept for the trade-off numbers in the summary above):

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | ref ops/CPU-s | rs ops/CPU-s | eff rs/ref | ref CPU % | rs CPU % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 1 | 141,662 | 187,782 | 1.33 | 198,483 | 153,794 | 0.77 | 72 | 122 |
| put-reserve-delete | 10 | 4096 | 1 | 134,929* | 173,275* | 1.28 | 185,342 | 136,216 | 0.73 | 73 | 127 |
| put-reserve-delete | 100 | 16 | 1 | 188,214* | 217,593* | 1.16 | 192,251 | 144,773 | 0.75 | 98 | 150 |
| put-reserve-delete | 100 | 4096 | 1 | 179,943 | 222,297 | 1.24 | 183,428 | 142,542 | 0.78 | 98 | 157 |
| producers-consumers | 10 | 16 | 1 | 138,682 | 189,332 | 1.37 | 196,525 | 153,803 | 0.78 | 71 | 124 |
| producers-consumers | 10 | 4096 | 1 | 124,124* | 166,090* | 1.34 | 165,384 | 123,855 | 0.75 | 73 | 134 |
| producers-consumers | 100 | 16 | 1 | 156,577* | 175,175* | 1.12 | 167,104 | 106,784 | 0.64 | 94 | 160 |
| producers-consumers | 100 | 4096 | 1 | 149,364* | 174,128* | 1.17 | 157,890 | 98,709 | 0.63 | 95 | 176 |

2 workers pipelined (the best pipelined cells measured):

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | eff rs/ref |
|---|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 16 | 267,534* | 477,395* | 1.78 | 0.90 |
| put-reserve-delete | 100 | 16 | 16 | 309,046 | 462,873 | 1.50 | 0.82 |

4 workers (ratio-only; efficiency fails every cell, worse than 2 workers, throughput still ≥ 1.0x): put-reserve-delete 10×16 1.25x / 10×4096 1.02x* / 100×16 1.12x / 100×4096 1.14x; producers-consumers 10×16 1.28x / 10×4096 1.32x / 100×16 1.08x / 100×4096 1.17x; efficiency 0.50x–0.69x throughout. Pipelined: 10×16×16 1.61x (eff 0.62x), 100×16×16 1.66x (eff 0.63x).

### `-b` and TLS vs the P3 build

`-b` (default fsync), alternated against the P3 build (raw CSVs: `b-default-alt{1,2,4}.csv`); gate is ≥ 0.95x, all three thread counts pass every cell:

| threads | 10×16 | 10×4096 | 100×16 | 100×4096 |
|---:|---:|---:|---:|---:|
| 1 | 1.01 (168%→106% CPU) | 0.98 | 1.09* (263%→147%) | 1.05 |
| 2 | 1.08 | 1.07 | 1.13* | 1.14 |
| 4 | 0.99 | 1.01 | 1.15* | 1.06* |

TLS, P3's own listener via the `rs` slot vs the current build (`b-tls-p3.csv` relabeled, `b-tls-rs{1,2,4}.csv`); same gate, all pass:

| threads | 10×16 | 10×4096 | 100×16 | 100×4096 |
|---:|---:|---:|---:|---:|
| 1 | 1.00* (144%→83% CPU) | 0.97 | 1.06 (226%→98%) | 1.02 |
| 2 | 1.08* | 1.11 | 1.11 | 1.15 |
| 4 | 1.05* | 1.05 | 1.04 | 1.04 |

### Connection/handshake-burst scenario

100 plaintext connections running `put-reserve-delete` (16 B) while a TLS handshake burst (target 800/s, ≤ 64 outstanding, one `use bench-burst` per connection) runs against a second listener on the same server; `none` is the same invocation without the burst client. Gate: burst p99 within 2x of no-burst p99. 0 `burst_errors` and ~797/800 handshakes/s achieved at every thread count (no client-side port exhaustion).

| threads | ops/s (none) | ops/s (burst) | put p99 (none) µs | put p99 (burst) µs | p99 ratio | handshake p50/p99 ms |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 207,528 | 186,753 | 648 | 876 | 1.35 | 1.33 / 10.78 |
| 2 | 219,237 | 207,693 | 619 | 730 | 1.18 | 1.28 / 7.75 |
| 4 | 203,944 | 194,618 | 733 | 740 | 1.01 | 1.32 / 3.77 |

(reserve and delete p99 move with put p99 within 1% at every thread count.)

### Cluster mode vs the P3 build

3 nodes, `cluster-leader` / `cluster-follower`, 10/100×16, P3 build vs the current build via the `rs` slot (`d-p3.csv` relabeled, `d-rs{1,2,4}.csv`). Rule: best throughput not below the P3 numbers (this session's rerun, see "Summary" for the cross-check against P3-FD's historical figures).

| target | P3 (this session) | 1 worker | 2 workers | 4 workers |
|---|---:|---:|---:|---:|
| leader, 10×16 | 33,878 | 34,267 | 34,776 | 34,355 |
| follower, 10×16 | 28,442 | 27,241 | 29,586 | 28,362* |
| leader, 100×16 | 130,918 | 142,315 | **147,338** | 135,905 |
| follower, 100×16 | 105,243 | 117,397* | **119,325** | 109,520 |

All three thread counts beat P3; 2 workers is best at both connection counts and is the chosen cluster default (215% total node CPU vs 1 worker's 136%, for +3.5% leader throughput at 100×16 — noted for P4-T5, not a reason to change the pick).

### Chosen defaults

Superseded for TLS and `-b` by P4-T6b (above): the standalone default is 2 workers when any listener is TLS or the binlog is enabled; the text below describes the P4-T2 decision.

Applying the rule from `docs/PLAN.md` §7.5 / the lead's decision text:

- **Standalone** (`server.threads` unset, no `[cluster]`): **1**. It is the smallest — and, on this machine, the *only* — thread count of {1, 2, 4} that meets the standalone efficiency gate (≥ 0.8x) at every 10- and 100-connection cell, while also meeting the throughput gate (≥ 1.0x every non-pipelined cell), the `-b` and TLS gates (≥ 0.95x the P3 build) and the burst gate (plaintext p99 within 2x of no-burst). No thread count needed to be reported to the lead as a no-clean-winner case: 1 clears every gate; 2 and 4 fail the first one outright.
- **Cluster** (`[cluster]` present): **2**. The standalone-vs-reference gates do not apply in cluster mode (the rule allows a different default); among {1, 2, 4}, all beat the P3 build's cluster throughput measured in the same session, and 2 is the best.

### Environment (P4-T2)

| | |
|---|---|
| Machine | Apple M6, 12 cores, 32 GB RAM |
| OS | macOS 27.0 (Darwin 27.0.0, arm64) |
| Rust | rustc 1.98.1; release build (opt-level 3, `debug = 1`) |
| beanstalkd-rs | commit `f7ad41b` plus the P4-T2 changes (uncommitted at measurement time) |
| P3 baseline | commit `f7ad41b` (`git archive HEAD` before the P4-T2 changes), built the same way, own `CARGO_TARGET_DIR` |
| Reference | `.ref/beanstalkd-opt/beanstalkd`, `scripts/build-ref.sh --optimized` (commit `25085c5`, `-O2`) |
| Load generator | `bstk-bench`, same machine, loopback, 5 s per run, 5 runs per cell; connections set up before the clock starts |
| Background load | Not idle (an OrbStack VM of another user and system services, as in earlier sections). The 1-minute load average was 3.6 at the start of the ~54-minute run used for the tables below and 7.6 at the end (it climbed steadily, not in step with any particular cell); see the per-row `load_avg` column for the value at each run, and "Noise" below for the cells this affected. |

### Method

- **Alternation**: within one `bench/run-matrix.sh` invocation, `SERVERS="ref rs"` interleaves a reference run and a `beanstalkd-rs` run for every cell (ref first), so machine-load drift affects both sides alike. This works for the standalone-vs-C-reference matrix and the `-b` (default fsync) vs P3 matrix (`RS_ARGS="--threads N"` reaches only the `rs` side; a P3-era or reference binary without `--threads` is never given it). It does **not** work for TLS or cluster mode: `run-matrix.sh`'s `ref` slot always means "plaintext plus an external stunnel" for TLS modes, and cluster modes skip `server = ref` unconditionally (cluster mode is ours only) — so those two comparisons run as separate blocks, one binary at a time, both labeled `rs` in their own CSV, relabeled (`sed 's/^rs,/ref,/'`) before feeding both files to `summarize.py` together.
- **`RS_ARGS`** (`bench/run-matrix.sh`, P4-T2): extra arguments appended for `beanstalkd-rs` only (never the reference, never an `RS_BIN` standing in for it), so a sweep can alternate a fixed reference against several `--threads N` values in one invocation.
- **`burst` server mode** (`bench/run-matrix.sh`, P4-T2, ours only): a generated `--config` gives the server both a plaintext listener (the normal `$SCENARIOS` traffic) and a TLS listener (`auth = "none"`); `bstk-bench`'s new `handshake-burst` scenario (`--rate`, `BURST_RATE` / `BURST_CONNS` env vars) runs concurrently against the TLS listener, repeatedly connecting, running one cheap command (`use bench-burst`) and closing. The `none` mode cell in the same invocation (same build, same session) is the no-burst comparison point.
- **Cluster mode vs P3**: `cluster-leader` / `cluster-follower` server modes, `RS_BIN` swapped between the P3 binary and the current build (`RS_ARGS="--threads N"` on the latter only); both labeled `rs`, compared as described above under Alternation.

### Commands

```sh
cargo build --release -p bstk-server -p bstk-bench
REF="$PWD/.ref/beanstalkd-opt/beanstalkd"          # scripts/build-ref.sh --optimized
CUR=target/release/beanstalkd-rs                     # current build (has --threads)
# P3 baseline: HEAD before the P4-T2 changes, its own target dir.
git archive HEAD | tar -x -C p3 && \
  (cd p3 && CARGO_TARGET_DIR=target cargo build --release -p bstk-server)
P3=p3/target/release/beanstalkd-rs

# A: standalone plaintext vs the reference, alternated, for N in 1 2 4.
REF_BIN=$REF RS_BIN=$CUR SERVERS="ref rs" RS_ARGS="--threads $N" \
  SCENARIOS="put-reserve-delete producers-consumers" CONNS="10 100" BODIES="16 4096" \
  RUNS=5 DURATION=5 SERVER_MODES=none OUT_CSV=a-alt$N-main.csv bench/run-matrix.sh
REF_BIN=$REF RS_BIN=$CUR SERVERS="ref rs" RS_ARGS="--threads $N" \
  SCENARIOS=put-reserve-delete CONNS="10 100" BODIES=16 PIPELINES=16 \
  RUNS=5 DURATION=5 SERVER_MODES=none OUT_CSV=a-alt$N-pipe.csv bench/run-matrix.sh
bench/summarize.py --efficiency a-alt$N-main.csv
bench/summarize.py --efficiency a-alt$N-pipe.csv

# C: connection/handshake-burst scenario, for N in 1 2 4.
RS_BIN=$CUR SERVERS=rs SERVER_ARGS="--threads $N" SCENARIOS=put-reserve-delete \
  CONNS=100 BODIES=16 RUNS=5 DURATION=5 SERVER_MODES="none burst" \
  BURST_RATE=800 BURST_CONNS=64 OUT_CSV=c-rs$N.csv bench/run-matrix.sh
bench/summarize.py --baseline none c-rs$N.csv

# B: -b (default fsync) vs the P3 build, alternated, for N in 1 2 4.
REF_BIN=$P3 RS_BIN=$CUR SERVERS="ref rs" RS_ARGS="--threads $N" \
  SCENARIOS=put-reserve-delete CONNS="10 100" BODIES="16 4096" \
  RUNS=5 DURATION=5 SERVER_MODES=default OUT_CSV=b-default-alt$N.csv bench/run-matrix.sh
bench/summarize.py b-default-alt$N.csv

# B: TLS vs the P3 build (P3's own TLS listener; block, not alternated).
RS_BIN=$P3 SERVERS=rs SCENARIOS=put-reserve-delete CONNS="10 100" BODIES="16 4096" \
  RUNS=5 DURATION=5 SERVER_MODES=tls OUT_CSV=b-tls-p3.csv bench/run-matrix.sh
RS_BIN=$CUR SERVERS=rs SERVER_ARGS="--threads $N" SCENARIOS=put-reserve-delete \
  CONNS="10 100" BODIES="16 4096" RUNS=5 DURATION=5 SERVER_MODES=tls \
  OUT_CSV=b-tls-rs$N.csv bench/run-matrix.sh
sed 's/^rs,/ref,/' b-tls-p3.csv > b-tls-p3-as-ref.csv
bench/summarize.py b-tls-p3-as-ref.csv b-tls-rs$N.csv

# D: cluster mode vs the P3 build (ref slot is skipped for cluster modes).
RS_BIN=$P3 SERVERS=rs SCENARIOS=put-reserve-delete CONNS="10 100" BODIES=16 \
  RUNS=5 DURATION=5 CLUSTER_NODES=3 SERVER_MODES="cluster-leader cluster-follower" \
  OUT_CSV=d-p3.csv bench/run-matrix.sh
RS_BIN=$CUR SERVERS=rs SERVER_ARGS="--threads $N" SCENARIOS=put-reserve-delete \
  CONNS="10 100" BODIES=16 RUNS=5 DURATION=5 CLUSTER_NODES=3 \
  SERVER_MODES="cluster-leader cluster-follower" OUT_CSV=d-rs$N.csv bench/run-matrix.sh
sed 's/^rs,/ref,/' d-p3.csv > d-p3-as-ref.csv
bench/summarize.py d-p3-as-ref.csv d-rs$N.csv
```

New CSV columns (empty outside `burst` mode): `burst_attempted`,
`burst_completed`, `burst_errors` (handshake succeeded but the one
command after it did not, or the handshake itself failed — not simply
`attempted - completed`), `burst_rate_achieved` (completed handshakes
per measured second), `burst_hs_p50_ms` / `burst_hs_p99_ms` (handshake-
completion latency, measured right after the TLS handshake, before the
one command).

## P3: cluster mode

### Summary (P3-FD)

- **Target met: 135k ops/s, ≥ 50k.** put-reserve-delete with 100
  connections via the leader of a 3-node cluster on one machine runs at
  135,256 ops/s (median of 5; 16-byte bodies; 2.7x the target, 4.8x
  P3-T7b's 28.4k), 115,007 ops/s with 4 KiB bodies (was 20.4k). Through a
  follower: 108,313 and 94,904 ops/s (were 21.1k and 18.0k). That is
  0.47x to 0.66x of standalone in the same session (200k to 206k ops/s).
  With 10 connections: 34.8k / 22.4k via the leader and 28.8k / 18.1k via
  a follower (were 22.2k / 10.8k and 18.2k / 11.2k).
- **What changed (P3-FD):** (1) the leader proposes connection inputs in
  `Op::Batch` entries: its own and forwarded inputs go through one
  proposer that turns everything queued into one entry (at most 1,024
  items and 1 MiB), with one batch outstanding at a time, so one log
  write, one `fdatasync` and one replication round cover many inputs;
  (2) the log store's `fdatasync` moved to a flush worker with group
  commit (the Raft core no longer blocks a runtime thread on the disk,
  but openraft 0.9 still awaits every append's flush, see "Anomalies");
  (3) timing defaults `heartbeat` 100 ms and `election_timeout` 500 to
  700 ms (were 50 ms and 150 to 300 ms).
- **No elections, no resends.** In all 135 cluster runs (matrix and
  1-connection) `term_changes`, `leader_changes`, `resent_inputs`,
  `forward_rewinds`, `drop_node_proposals`, `refused_connections` and
  `rejected_puts` were 0 and no node reported `isolated`; before, 16 of
  80 4-KiB runs had an election.
- **Latency:** put p50 / p99 at 100x16 via the leader 734 / 982 µs (was
  3,579 / 4,579 µs); with one connection 148 µs via the leader and
  170 µs via a follower (were 252 and 308 µs), against 17 µs standalone.
- **Standalone mode is unchanged**: 0.99x to 1.01x against the P3-T7b
  binary (commit `5534189`) in all eight cells, 3 alternating runs each.
- mTLS on the cluster port and 5 nodes were not re-measured (their
  P3-T7b numbers are kept below).

### Before P3-FD (P3-T7b)

The first measurement (same matrix, commit `5534189`) gave 28.4k ops/s at
100x16 via the leader (20.4k with 4 KiB bodies, 21.1k and 18.0k via a
follower), with elections in 16 of 80 4-KiB runs. A `sample` of the
leader showed its Raft core task busy 80% of the wall time, 77% of that
inside `LogStore::append` (`fdatasync` 50%, `pwrite` 24%): openraft 0.9
runs its engine commands after every API message
(`RaftCore::process_raft_msg`, `raft_core.rs:990-1013`), so every
`client_write_ff` was appended and synced on its own (about 26 µs per
entry, a ceiling of about 38k entries/s), and every put was two entries.
`fdatasync` stalls of 50 to 800 ms on this machine's APFS volume
(reproduced without beanstalkd-rs) blocked the Raft core, and with it the
heartbeats, past the 150 to 300 ms election timeout. The P3-T7b raw CSVs
are kept (see "Commands").

### Environment (P3-FD)

| | |
|---|---|
| Machine | Apple M6, 12 cores, 32 GB RAM; APFS on the internal SSD (cluster data directories under `/private/tmp`) |
| OS | macOS 27.0 (Darwin 27.0.0, arm64) |
| Rust | rustc 1.98.1; release build (opt-level 3, `debug = 1`) |
| beanstalkd-rs | commit `5534189` plus the P3-FD changes (uncommitted at measurement time); openraft 0.9.25 |
| Cluster | 3 nodes, one process each, on one machine, loopback only; every node started with `--cluster-init`; generated `--config` per node: a plaintext client `[[listener]]`, `[http]` (for `/readyz` and `/admin`), `[cluster]` with defaults (P3-FD: `heartbeat` 100 ms, `election_timeout` 500 to 700 ms, `node_timeout` 5 s, `snapshot_every` 100,000) and `insecure_plaintext = true`, or `[cluster.tls]` with per-node certificates for the mTLS rows. A fresh cluster with empty data directories for every run. |
| fsync | The Raft log uses plain `fdatasync`, not `F_FULLFSYNC` (like `-b`); on macOS this reaches the drive cache, not stable storage. All three nodes write to the same SSD. |
| Load generator | `bstk-bench` (unchanged since P2), same machine, loopback, 5 s per run; it starts once every node answers `/admin` with `ready: true` and a leader is known; connections are set up before the clock starts |
| Background load | **Not idle**, as in P3-T7b (an OrbStack VM of another user and system services). The 1-minute load average was 4.7 at the start; across runs it was 4.0 to 10.1 in the cluster matrix (median 5.6), 4.9 to 8.4 in the 1-connection runs and about 6 in the regression check. Runs alternate between modes, so drift affects them alike. |

### Commands (P3-FD)

```sh
cargo build --release -p bstk-server -p bstk-bench
# SERVER_MODES: cluster-leader | cluster-follower | cluster-mtls-leader |
#   cluster-mtls-follower (ours only; SERVERS=rs skips the reference).
#   CLUSTER_NODES (default 3); KEEP_LOGS=DIR keeps each run's node logs.
SERVERS=rs OUT_CSV=p3-fd-cluster.csv SCENARIOS="put-reserve-delete producers-consumers" \
  CONNS="10 100" BODIES="16 4096" RUNS=5 DURATION=5 \
  SERVER_MODES="none cluster-leader cluster-follower" bench/run-matrix.sh
SERVERS=rs OUT_CSV=p3-fd-cluster-1conn.csv SCENARIOS=put-reserve-delete CONNS=1 BODIES=16 \
  RUNS=5 SERVER_MODES="none cluster-leader cluster-follower" bench/run-matrix.sh
bench/summarize.py --baseline none p3-fd-cluster.csv
# Standalone regression check: the P3-T7b binary (HEAD 5534189) as the
# reference.
mkdir p3 && git archive 5534189 | tar -x -C p3 && \
  (cd p3 && CARGO_TARGET_DIR=p3/target cargo build --release -p bstk-server)
REF_BIN=p3/target/release/beanstalkd-rs OUT_CSV=p3-fd-regress.csv \
  SCENARIOS="put-reserve-delete producers-consumers" CONNS="10 100" \
  BODIES="16 4096" RUNS=3 DURATION=5 SERVER_MODES=none bench/run-matrix.sh
```

New CSV columns (empty in standalone rows): `nodes`, `target_role`
(the node bstk-bench talks to), `cluster_cpu_pct` (all nodes together),
`node_cpu` (`id:role:pct` per node, `L` leader, `F` follower, `*` the
target), and per-run deltas over all nodes from `/admin`:
`resent_inputs`, `forward_rewinds` (all causes), `drop_node_proposals`,
`refused_connections`, `rejected_puts`, `term_changes` (the leader's term
after minus before), `leader_changes` (1 if the leader moved) and
`isolated_after` (nodes reporting `isolated` after the run). Per-node CPU
is the `ps` cputime delta over the whole `bstk-bench` run (setup
included, so slightly diluted); for the target node it agrees with the
`stats` rusage over the measured window (`server_cpu_pct`) within about
1%. `/admin` counters are read before and after each run, so a transient
`isolated` state in between would be missed (`refused_connections` would
show its effect). Raw data (P3-FD): `bench/results/2026-09-26-p3-fd-cluster.csv`,
`…-p3-fd-cluster-1conn.csv` and `…-p3-fd-regress.csv`; the P3-T7b
files (`…-p3-cluster.csv`, `…-p3-cluster-mtls.csv`,
`…-p3-cluster-1conn.csv`, `…-p3-cluster-5node.csv`, `…-p3-regress.csv`)
are kept.

### Results (P3-FD)

Medians of 5 runs (`*` = runs spread by more than 20%). "vs standalone" is
ops/s divided by the `none` row of the same cell (same session). "target
CPU %" is the node bstk-bench talks to (from `stats`); "leader CPU %" the
leader, "follower CPU %" the mean of the followers (in follower mode
including the target), "all nodes CPU %" the sum (`ps`); "anomalies"
sums resent inputs / forward-queue rewinds / term changes over the 5
runs.

| mode | scenario | conns | body | ops/s | vs standalone | put p50 µs | put p99 µs | reserve p50 µs | reserve p99 µs | target CPU % | leader CPU % | follower CPU % | all nodes CPU % | anomalies |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | put-reserve-delete | 10 | 16 | 180,456 | - | 53 | 104 | 53 | 104 | 131 | - | - | - | - |
| cluster-leader | put-reserve-delete | 10 | 16 | 34,838 | 0.19 | 284 | 380 | 284 | 379 | 109 | 108 | 51 | 210 | 0/0/0 |
| cluster-follower | put-reserve-delete | 10 | 16 | 28,806 | 0.16 | 328 | 513 | 327 | 512 | 92 | 89 | 72 | 233 | 0/0/0 |
| none | put-reserve-delete | 10 | 4096 | 175,771 | - | 55 | 109 | 55 | 108 | 138 | - | - | - | - |
| cluster-leader | put-reserve-delete | 10 | 4096 | 22,361* | 0.13 | 318 | 1,120 | 308 | 1,124 | 75 | 74 | 35 | 144 | 0/0/0 |
| cluster-follower | put-reserve-delete | 10 | 4096 | 18,098* | 0.10 | 356 | 856 | 349 | 832 | 61 | 60 | 47 | 155 | 0/0/0 |
| none | put-reserve-delete | 100 | 16 | 206,390 | - | 478 | 652 | 478 | 653 | 200 | - | - | - | - |
| cluster-leader | put-reserve-delete | 100 | 16 | 135,256 | 0.66 | 734 | 982 | 734 | 984 | 212 | 210 | 33 | 276 | 0/0/0 |
| cluster-follower | put-reserve-delete | 100 | 16 | 108,313 | 0.52 | 920 | 1,226 | 920 | 1,224 | 189 | 52 | 109 | 270 | 0/0/0 |
| none | put-reserve-delete | 100 | 4096 | 199,966 | - | 495 | 669 | 494 | 670 | 211 | - | - | - | - |
| cluster-leader | put-reserve-delete | 100 | 4096 | 115,007 | 0.58 | 866 | 1,136 | 862 | 1,125 | 200 | 197 | 34 | 266 | 0/0/0 |
| cluster-follower | put-reserve-delete | 100 | 4096 | 94,904 | 0.47 | 1,050 | 1,418 | 1,044 | 1,411 | 176 | 64 | 104 | 271 | 0/0/0 |
| none | producers-consumers | 10 | 16 | 179,569 | - | 53 | 105 | 54 | 105 | 134 | - | - | - | - |
| cluster-leader | producers-consumers | 10 | 16 | 34,343 | 0.19 | 287 | 383 | 288 | 384 | 109 | 93 | 47 | 187 | 0/0/0 |
| cluster-follower | producers-consumers | 10 | 16 | 28,652 | 0.16 | 329 | 516 | 329 | 516 | 93 | 81 | 63 | 207 | 0/0/0 |
| none | producers-consumers | 10 | 4096 | 169,219 | - | 57 | 116 | 57 | 115 | 149 | - | - | - | - |
| cluster-leader | producers-consumers | 10 | 4096 | 22,832* | 0.13 | 333 | 2,848 | 334 | 2,857 | 82 | 78 | 39 | 156 | 0/0/0 |
| cluster-follower | producers-consumers | 10 | 4096 | 18,556* | 0.11 | 377 | 6,849 | 375 | 6,961 | 65 | 60 | 46 | 152 | 0/0/0 |
| none | producers-consumers | 100 | 16 | 197,298 | - | 494 | 771 | 494 | 773 | 212 | - | - | - | - |
| cluster-leader | producers-consumers | 100 | 16 | 128,286 | 0.65 | 768 | 1,092 | 769 | 1,092 | 211 | 172 | 33 | 238 | 0/0/0 |
| cluster-follower | producers-consumers | 100 | 16 | 101,023* | 0.51 | 968 | 1,568 | 969 | 1,564 | 189 | 55 | 93 | 240 | 0/0/0 |
| none | producers-consumers | 100 | 4096 | 182,626 | - | 526 | 860 | 527 | 860 | 241 | - | - | - | - |
| cluster-leader | producers-consumers | 100 | 4096 | 101,082 | 0.55 | 968 | 1,394 | 969 | 1,402 | 196 | 165 | 34 | 232 | 0/0/0 |
| cluster-follower | producers-consumers | 100 | 4096 | 84,074* | 0.46 | 1,157 | 1,812 | 1,158 | 1,807 | 170 | 63 | 90 | 243 | 0/0/0 |

One connection (put-reserve-delete, 16-byte bodies):

| mode | scenario | conns | body | ops/s | vs standalone | put p50 µs | put p99 µs | reserve p50 µs | reserve p99 µs | target CPU % | leader CPU % | follower CPU % | all nodes CPU % | anomalies |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | put-reserve-delete | 1 | 16 | 57,683 | - | 17 | 34 | 17 | 34 | 38 | - | - | - | - |
| cluster-leader | put-reserve-delete | 1 | 16 | 6,494 | 0.11 | 148 | 249 | 148 | 249 | 80 | 80 | 44 | 167 | 0/0/0 |
| cluster-follower | put-reserve-delete | 1 | 16 | 5,691 | 0.10 | 170 | 281 | 170 | 278 | 48 | 67 | 42 | 152 | 0/0/0 |

mTLS on the cluster port and 5 nodes, **P3-T7b numbers (before P3-FD,
not re-measured)**; put-reserve-delete, 100 connections:

| mode | nodes | body | ops/s | put p50 µs | put p99 µs | target CPU % | leader CPU % | follower CPU % | all nodes CPU % | anomalies |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cluster-leader | 3 | 16 | 28,080 | 3,619 | 4,626 | 99 | 98 | 7 | 113 | 0/0/0 |
| cluster-mtls-leader | 3 | 16 | 28,542 | 3,532 | 4,598 | 98 | 97 | 7 | 112 | 0/0/0 |
| cluster-mtls-follower | 3 | 16 | 21,115 | 4,732 | 6,081 | 42 | 38 | 24 | 87 | 0/0/0 |
| cluster-leader | 3 | 4096 | 23,098* | 3,838 | 11,100 | 89 | 89 | 8 | 104 | 0/0/0 |
| cluster-mtls-leader | 3 | 4096 | 22,820 | 3,850 | 8,480 | 87 | 86 | 8 | 102 | 0/0/0 |
| cluster-mtls-follower | 3 | 4096 | 19,536 | 5,090 | 6,734 | 43 | 42 | 26 | 94 | 0/0/0 |
| cluster-leader | 5 | 16 | 27,325 | 3,717 | 4,716 | 105 | 105 | 8 | 138 | 0/0/0 |
| cluster-follower | 5 | 16 | 20,786 | 4,789 | 6,155 | 44 | 45 | 16 | 111 | 0/0/0 |

### CPU (P3-FD)

- **The target node is now the busiest process, as in standalone mode.**
  At 100x16 via the leader, the leader uses 2.1 cores (standalone: 2.0
  cores at 206k ops/s) and each follower 0.33 cores; the whole cluster
  2.8 cores for 135k ops/s, about 20 µs of CPU per operation in total
  (P3-T7b: 40 µs; standalone: 10 µs). Via a follower, the target
  (follower) uses 1.9 cores, the leader 0.5, the other follower 0.3.
- Per operation the cluster costs roughly 2x standalone CPU at 100
  connections, and much more at low load (1 connection: 1.7 cores for
  6.5k ops/s, about 260 µs per operation, mostly waking parked threads on
  every node for every entry).
- The remaining gap to standalone at 100 connections (0.66x) is latency
  along the commit path (leader append and sync, one replication round
  trip, follower append and sync, apply) with one batch outstanding, not
  a saturated core: the leader is at 2.1 of 12 cores. Allowing more
  batches outstanding was slower on this machine (2: about 120k, 4: 90k
  to 120k, unbounded: 35k ops/s in a 2-run tuning pass), because the
  Raft core then appends several small entries instead of one large one.

### Anomalies (P3-FD)

- **None of the P3-T7b anomalies recurred**: no term change, resend,
  rewind, `DropNode`, refused connection, rejected put or `isolated`
  node in any of the 135 cluster runs, including every 4 KiB run (16 of
  80 had elections before). Two changes contribute: an entry now covers
  up to 1,024 inputs, so the log does about 1/100th of the syncs, and the
  timing defaults tolerate 1.0 to 1.2 s without a heartbeat (openraft
  0.9 waits `election_timeout_max` plus the node's random election
  timeout; it was 450 to 600 ms). The disk stalls themselves are
  unchanged: the 10-connection 4 KiB cells still show put p99 of 0.9 to
  7 ms and spreads over 20% (`*`).
- **The Raft core still waits for each log flush.** openraft 0.9.25's
  `RaftCore::append_to_log` (`core/raft_core.rs:713-731`) awaits the
  append's `LogFlushed` callback before running its next command, so the
  flush worker only stops the sync from blocking a runtime thread; at
  most one core append is outstanding, and its group commit (tested with
  concurrent appends in `appends_coalesce_into_few_syncs`) does not
  combine appends in practice. A long stall still delays heartbeats; the
  longer election timeout absorbs about a second of it.
- Via a follower is 0.80x to 0.83x of via the leader at 100 connections
  (was 0.74x to 0.88x): one forward in flight per node adds a round trip
  per forward batch.

### Standalone regression check (P3-FD)

"ref" is `beanstalkd-rs` built from commit `5534189` (P3-T7b), "rs" the
P3-FD build, both started with `-l 127.0.0.1 -p PORT`, alternating, a
fresh process per run, medians of 3 runs:

| scenario | conns | body | pipe | ref ops/s | rs ops/s | rs/ref | ref CPU % | rs CPU % | ref put p99 µs | rs put p99 µs | ref reserve p99 µs | rs reserve p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 1 | 174,816 | 176,232 | 1.01 | 134 | 134 | 116 | 116 | 118 | 116 |
| put-reserve-delete | 10 | 4096 | 1 | 172,082 | 170,359 | 0.99 | 140 | 140 | 118 | 120 | 117 | 119 |
| put-reserve-delete | 100 | 16 | 1 | 201,457 | 203,590 | 1.01 | 206 | 202 | 736 | 675 | 736 | 675 |
| put-reserve-delete | 100 | 4096 | 1 | 191,139* | 192,205* | 1.01 | 220 | 220 | 809 | 790 | 808 | 788 |
| producers-consumers | 10 | 16 | 1 | 174,598 | 175,261 | 1.00 | 136 | 136 | 115 | 114 | 116 | 115 |
| producers-consumers | 10 | 4096 | 1 | 169,304 | 168,941 | 1.00 | 148 | 148 | 113 | 114 | 113 | 113 |
| producers-consumers | 100 | 16 | 1 | 204,119 | 204,548 | 1.00 | 202 | 203 | 655 | 656 | 657 | 657 |
| producers-consumers | 100 | 4096 | 1 | 194,417 | 195,571 | 1.01 | 233 | 233 | 699 | 694 | 698 | 694 |

**Conclusion: standalone throughput, CPU and latency are unchanged
(0.99x to 1.01x).**


## P2: TLS, mTLS and the plaintext regression check

### Summary (P2-T5)

- **TLS costs beanstalkd-rs 3% to 10% of its plaintext throughput** (ours
  TLS / ours plaintext = 0.90x to 0.97x in the 16 cells of the TLS and mTLS
  rows). mTLS costs the same as TLS once connected: the client certificate
  is only checked during the handshake, which happens before the clock
  starts. Server CPU rises by 7% to 22% (for example 130% to 141% at
  10x16, 208% to 253% at 100x4096).
- **The reference behind stunnel reaches 24k to 38k ops/s**, so ours over
  TLS runs at **4.0x to 4.7x** of it at 10 connections and **7.4x to 8.4x**
  at 100 connections. stunnel, not the reference, is the bottleneck: the
  reference uses 22% to 41% of a core, while stunnel uses 3.7 to 10.2 cores
  (see "CPU" below). This compares our built-in TLS with the obvious way
  to put TLS in front of the reference, not with an ideal TLS proxy.
- **TLS with `-b` (default fsync, 50 ms)** runs at 0.69x to 0.86x of our
  plaintext no-`-b` throughput and at 0.91x to 0.96x of our plaintext `-b`
  throughput measured in the same session; versus the reference behind
  stunnel with `-b` it is 3.1x to 7.5x.
- **Plaintext without a configuration file is unchanged.** Against the
  P1 binary (commit `ca545ac`, the last before P2) in the same session:
  0.99x to 1.00x in a focused 7-run re-run of the 16-byte cells, 0.95x to
  1.01x in a noisier 5-run pass of all eight cells, 0.99x to 1.01x in a
  3-run pass before the security fixes; inside the ±5% acceptance band
  (see "Plaintext regression check").

### Environment (P2-T5)

| | |
|---|---|
| Machine | Apple M6, 12 cores, 32 GB RAM (same as P1-T5) |
| OS | macOS 27.0 (Darwin 27.0.0, arm64) |
| Rust | rustc 1.98.1; release build (opt-level 3, `debug = 1`) |
| beanstalkd-rs | commit `eceb065` (includes the P2-T6b security fixes); TLS via rustls 0.23 / tokio-rustls 0.26, aws-lc-rs provider; generated config with one listener and defaults otherwise (`max_pending_connections` 1024 is far above the 100 connections here) |
| Reference | `.ref/beanstalkd-opt/beanstalkd` (`scripts/build-ref.sh --optimized`, `-O2`) |
| TLS proxy for the reference | stunnel 5.82 (Homebrew), OpenSSL 4.0.2, default thread-per-connection model, `TCP_NODELAY` on both sides, `debug = 3` |
| Certificates | generated per matrix run by `clients/mkcerts.sh`: ECDSA P-256 CA and server certificate, client certificate for mTLS; both servers use the same files |
| Negotiated | TLS 1.3, `TLS_AES_256_GCM_SHA384`, for both beanstalkd-rs and stunnel (checked with `openssl s_client`) |
| Load generator | `bstk-bench --tls` (tokio-rustls, same provider), same machine, loopback, 5 s per run, a fresh server (and stunnel) process per run, ref and rs runs alternating; connections and handshakes are set up before the clock starts |
| Background load | **Not idle.** An OrbStack VM owned by another user was running, and system services at times used up to 175% CPU. The 1-minute load average was 5.0 at the start and 23 at the end of the matrix, and 5.3 to 37 across runs (median 13.6; CSV `load_avg`); part of that is the benchmark itself, since stunnel alone keeps up to 10 cores busy in the reference's TLS runs. No cell spread by more than 20% over its 3 runs. Because runs alternate, drift affects both servers alike. |

### Commands (P2-T5)

```sh
scripts/build-ref.sh --optimized
brew install stunnel                       # TLS baseline for the reference
cargo build --release -p bstk-server -p bstk-bench
# SERVER_MODES (new): tls | mtls | tls-default. Ours runs from a generated
# --config (one TLS listener, auth none or mtls); the reference runs behind
# stunnel with the same certificate (and verifyChain/requireCert for mtls).
OUT_CSV=p2-matrix.csv SCENARIOS="put-reserve-delete producers-consumers" \
  CONNS="10 100" BODIES="16 4096" RUNS=3 DURATION=5 \
  SERVER_MODES="none tls mtls default tls-default" bench/run-matrix.sh
bench/summarize.py --baseline none p2-matrix.csv
# Plaintext regression check: the P1 binary in place of the reference.
git worktree add /tmp/p1 ca545ac && \
  (cd /tmp/p1 && CARGO_TARGET_DIR=/tmp/p1/target cargo build --release -p bstk-server)
REF_BIN=/tmp/p1/target/release/beanstalkd-rs OUT_CSV=p2-regress.csv \
  SCENARIOS="put-reserve-delete producers-consumers" CONNS="10 100" \
  BODIES="16 4096" RUNS=5 DURATION=5 SERVER_MODES=none bench/run-matrix.sh
```

A single TLS run by hand:

```sh
clients/mkcerts.sh /tmp/certs
bstk-bench --addr 127.0.0.1:11301 --tls --ca /tmp/certs/ca.pem \
  [--client-cert /tmp/certs/client.pem --client-key /tmp/certs/client.key] \
  [--token TOKEN] --conns 10 --duration 5 --scenario put-reserve-delete
```

`bstk-bench` itself changed for `--tls` (the plaintext client now reads
and writes through one `BufReader` over a plain/TLS stream enum instead
of split TCP halves). Driving the same server (commit `e6df034`, in the same
session), the P1 `bstk-bench` binary and the new one measure the same
plaintext throughput (put-reserve-delete 16 B, medians of 3: 180.6k vs
180.8k ops/s at 10 connections, 207.9k vs 207.4k at 100),
so the load generator change does not affect comparisons with P1.

Raw data: `bench/results/2026-09-25-p2-matrix.csv` (new column
`proxy_cpu_pct`: stunnel's CPU), and for the regression check
`bench/results/2026-09-25-p2-regress.csv`,
`bench/results/2026-09-25-p2-regress-rerun.csv` and
`bench/results/2026-09-25-p2-regress-e6df034.csv`.

### Results (P2-T5)

Medians of 3 runs. "rs/ref" is ours vs the reference in the same mode
(for `tls` / `mtls` / `tls-default`: ours with built-in TLS vs the
reference behind stunnel). "rs/rs[none]" is ours in that mode vs ours in
plaintext without `-b`, same cell. "stunnel CPU %" is the proxy's CPU in
front of the reference, not included in "ref CPU %"; "rs CPU %" includes
our TLS work.

| mode | scenario | conns | body | ref ops/s | rs ops/s | rs/ref | rs/rs[none] | ref CPU % | stunnel CPU % | rs CPU % | ref put p99 µs | rs put p99 µs |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| none | put-reserve-delete | 10 | 16 | 138,188 | 178,108 | 1.29 | 1.00 | 71 | - | 130 | 128 | 111 |
| tls | put-reserve-delete | 10 | 16 | 36,548 | 170,065 | 4.65 | 0.95 | 36 | 627 | 141 | 440 | 116 |
| mtls | put-reserve-delete | 10 | 16 | 37,080 | 172,757 | 4.66 | 0.97 | 36 | 615 | 141 | 429 | 111 |
| default | put-reserve-delete | 10 | 16 | 103,769 | 147,140 | 1.42 | 0.83 | 91 | - | 167 | 185 | 124 |
| tls-default | put-reserve-delete | 10 | 16 | 38,577 | 139,596 | 3.62 | 0.78 | 54 | 514 | 174 | 442 | 132 |
| none | put-reserve-delete | 10 | 4096 | 135,547 | 176,777 | 1.30 | 1.00 | 72 | - | 138 | 122 | 107 |
| tls | put-reserve-delete | 10 | 4096 | 36,089 | 158,269 | 4.39 | 0.90 | 38 | 643 | 156 | 442 | 126 |
| mtls | put-reserve-delete | 10 | 4096 | 37,123 | 159,855 | 4.31 | 0.90 | 39 | 628 | 156 | 436 | 121 |
| default | put-reserve-delete | 10 | 4096 | 105,766 | 133,822 | 1.27 | 0.76 | 95 | - | 170 | 182 | 139 |
| tls-default | put-reserve-delete | 10 | 4096 | 36,674 | 121,475 | 3.31 | 0.69 | 54 | 550 | 181 | 475 | 157 |
| none | put-reserve-delete | 100 | 16 | 186,566 | 209,089 | 1.12 | 1.00 | 99 | - | 196 | 768 | 617 |
| tls | put-reserve-delete | 100 | 16 | 24,061 | 199,330 | 8.28 | 0.95 | 22 | 1,012 | 221 | 83,711 | 678 |
| mtls | put-reserve-delete | 100 | 16 | 24,065 | 201,199 | 8.36 | 0.96 | 22 | 1,017 | 223 | 84,144 | 656 |
| default | put-reserve-delete | 100 | 16 | 126,500 | 187,942 | 1.49 | 0.90 | 99 | - | 265 | 1,090 | 682 |
| tls-default | put-reserve-delete | 100 | 16 | 24,048 | 179,160 | 7.45 | 0.86 | 36 | 1,003 | 283 | 107,777 | 778 |
| none | put-reserve-delete | 100 | 4096 | 179,614 | 201,362 | 1.12 | 1.00 | 99 | - | 208 | 858 | 747 |
| tls | put-reserve-delete | 100 | 4096 | 24,128 | 187,798 | 7.78 | 0.93 | 23 | 1,008 | 253 | 79,436 | 710 |
| mtls | put-reserve-delete | 100 | 4096 | 24,447 | 187,416 | 7.67 | 0.93 | 24 | 1,015 | 254 | 79,156 | 722 |
| default | put-reserve-delete | 100 | 4096 | 106,739 | 173,954 | 1.63 | 0.86 | 98 | - | 268 | 9,148 | 2,134 |
| tls-default | put-reserve-delete | 100 | 4096 | 24,211 | 161,974 | 6.69 | 0.80 | 42 | 986 | 305 | 91,699 | 2,174 |
| none | producers-consumers | 10 | 16 | 137,352 | 180,287 | 1.31 | 1.00 | 69 | - | 134 | 116 | 104 |
| tls | producers-consumers | 10 | 16 | 36,100 | 171,121 | 4.74 | 0.95 | 36 | 463 | 144 | 426 | 112 |
| mtls | producers-consumers | 10 | 16 | 36,315 | 166,824 | 4.59 | 0.93 | 36 | 461 | 144 | 433 | 122 |
| default | producers-consumers | 10 | 16 | 107,860 | 141,026 | 1.31 | 0.78 | 90 | - | 168 | 176 | 135 |
| tls-default | producers-consumers | 10 | 16 | 38,703 | 133,489 | 3.45 | 0.74 | 52 | 373 | 175 | 431 | 142 |
| none | producers-consumers | 10 | 4096 | 128,094 | 169,826 | 1.33 | 1.00 | 73 | - | 149 | 127 | 113 |
| tls | producers-consumers | 10 | 4096 | 36,681 | 154,789 | 4.22 | 0.91 | 39 | 450 | 166 | 444 | 126 |
| mtls | producers-consumers | 10 | 4096 | 38,308 | 154,767 | 4.04 | 0.91 | 41 | 441 | 166 | 424 | 126 |
| default | producers-consumers | 10 | 4096 | 97,808 | 126,993 | 1.30 | 0.75 | 97 | - | 178 | 199 | 159 |
| tls-default | producers-consumers | 10 | 4096 | 37,149 | 116,971 | 3.15 | 0.69 | 59 | 369 | 188 | 463 | 168 |
| none | producers-consumers | 100 | 16 | 190,935 | 206,709 | 1.08 | 1.00 | 98 | - | 203 | 683 | 637 |
| tls | producers-consumers | 100 | 16 | 24,434 | 199,741 | 8.17 | 0.97 | 23 | 881 | 226 | 93,835 | 663 |
| mtls | producers-consumers | 100 | 16 | 24,410 | 199,782 | 8.18 | 0.97 | 23 | 886 | 225 | 82,013 | 668 |
| default | producers-consumers | 100 | 16 | 118,685 | 184,357 | 1.55 | 0.89 | 98 | - | 269 | 1,261 | 728 |
| tls-default | producers-consumers | 100 | 16 | 24,603 | 177,218 | 7.20 | 0.86 | 39 | 870 | 289 | 106,746 | 784 |
| none | producers-consumers | 100 | 4096 | 172,757 | 193,406 | 1.12 | 1.00 | 98 | - | 236 | 941 | 744 |
| tls | producers-consumers | 100 | 4096 | 24,188 | 177,791 | 7.35 | 0.92 | 25 | 862 | 276 | 81,014 | 818 |
| mtls | producers-consumers | 100 | 4096 | 24,212 | 179,532 | 7.41 | 0.93 | 24 | 864 | 274 | 89,062 | 786 |
| default | producers-consumers | 100 | 4096 | 99,259 | 163,930 | 1.65 | 0.85 | 97 | - | 294 | 8,812 | 1,582 |
| tls-default | producers-consumers | 100 | 4096 | 23,921 | 154,145 | 6.44 | 0.80 | 46 | 829 | 322 | 76,468 | 1,626 |

### CPU (P2-T5)

- **Ours**: TLS adds 7% to 22% server CPU for the same or slightly lower
  throughput: 130% to 141% (10x16), 138% to 156% (10x4096), 196% to 221%
  (100x16), 208% to 253% (100x4096) for put-reserve-delete. Per operation
  that is 7.3 to 10.3 µs of server CPU in plaintext and 8.3 to 13.5 µs
  over TLS: about 1 to 2 µs more at 16 B, 2 to 3 µs more at 4 KiB, where
  encrypting the body dominates. mTLS uses the same CPU as TLS.
- **Reference behind stunnel**: the reference itself drops to 22% to 41% of
  a core because it is starved by stunnel, which burns 3.7 to 6.4 cores at
  10 connections and 8.3 to 10.2 cores at 100 connections for 24k to 38k
  ops/s. That is 100 to 420 µs of proxy CPU per operation, against ours'
  total of about 8 to 14 µs per operation. A `sample` profile of stunnel
  at 10 connections shows its threads almost always in `poll` (4,661 of
  4,909 samples), with `ps` showing system time at 96% of its CPU: the
  thread-per-connection model spends its time in the kernel waking up and
  polling, not in cryptography.
- **Latency**: ours over TLS has put p99 within 10% of plaintext (111 →
  116 µs at 10x16; 617 → 678 µs at 100x16). Behind stunnel the reference's
  p99 goes from 128 µs to 440 µs at 10 connections and to 76 to 108 ms at
  100 connections.

### Plaintext regression check (P2-T5)

"P1" is `beanstalkd-rs` built from commit `ca545ac` (the last commit
before P2) and "P2" commit `eceb065` (with the P2-T6b security fixes), both started with `-l 127.0.0.1 -p PORT` and no
configuration file, run by `bench/run-matrix.sh` with the P1 binary as
`REF_BIN` (so the "ref" columns of the CSVs are P1). The acceptance band is
±5%.

Full matrix, medians of 5 runs (load average 4.9 to 11.1):

| scenario | conns | body | P1 ops/s | P2 ops/s | P2/P1 | P1 CPU % | P2 CPU % | P1 put p99 µs | P2 put p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 173,614 | 165,433 | 0.95 | 134 | 137 | 127 | 144 |
| put-reserve-delete | 10 | 4096 | 172,524 | 173,891 | 1.01 | 139 | 139 | 119 | 116 |
| put-reserve-delete | 100 | 16 | 218,731 | 208,280 | 0.95 | 182 | 197 | 652 | 664 |
| put-reserve-delete | 100 | 4096 | 204,128 | 204,014 | 1.00 | 208 | 210 | 659 | 658 |
| producers-consumers | 10 | 16 | 179,179 | 179,332 | 1.00 | 133 | 134 | 107 | 107 |
| producers-consumers | 10 | 4096 | 169,496 | 170,772 | 1.01 | 146 | 148 | 120 | 113 |
| producers-consumers | 100 | 16 | 195,556* | 186,699* | 0.95 | 207 | 214 | 810 | 884 |
| producers-consumers | 100 | 4096 | 163,229* | 158,801* | 0.97 | 254 | 253 | 966 | 1,042 |

Three cells came out at 0.95x; their individual runs were spread widely
(put-reserve-delete 10x16: 148k to 181k ops/s for P2, 161k to 178k for
P1). A focused re-run of the four 16-byte cells
right after it, 7 runs each (load average 5.1 to 7.9):

| scenario | conns | body | P1 ops/s | P2 ops/s | P2/P1 | P1 CPU % | P2 CPU % | P1 put p99 µs | P2 put p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| put-reserve-delete | 10 | 16 | 180,227 | 180,758 | 1.00 | 132 | 132 | 106 | 105 |
| put-reserve-delete | 100 | 16 | 207,858* | 205,634* | 0.99 | 199 | 201 | 674 | 731 |
| producers-consumers | 10 | 16 | 179,302 | 178,707 | 1.00 | 132 | 134 | 105 | 106 |
| producers-consumers | 100 | 16 | 205,620 | 205,554 | 1.00 | 200 | 202 | 639 | 641 |

and an earlier 3-run pass of the full matrix on commit `e6df034` (before
the security fixes, load average 4.3 to 7.3,
`bench/results/2026-09-25-p2-regress-e6df034.csv`) gave 0.99x to 1.01x
in all eight cells. **Conclusion: plaintext throughput, CPU and latency
without a configuration file are at parity with P1.** The 0.95x cells are
noise from the shared machine (`*` = runs spread by more than 20%).

The no-config numbers here (about 178k to 209k ops/s) are higher than the
"none" rows of the P1-T5 table (66k to 220k ops/s) for both servers
most likely because that session ran under heavier background load:
only same-session comparisons are meaningful.


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
