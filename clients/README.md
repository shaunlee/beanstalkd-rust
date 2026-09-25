# Real-client smoke tests

These tests drive `beanstalkd-rs` with unmodified, third-party beanstalkd
client libraries and check that it behaves exactly like the reference C
beanstalkd.

| Client | Library | Script |
|---|---|---|
| Python | [greenstalk](https://pypi.org/project/greenstalk/) | `python/smoke.py` |
| Go | [go-beanstalk](https://github.com/beanstalkd/go-beanstalk) v0.2.0 | `go/main.go` |

Each client runs a full flow and asserts on every result: `use` / `watch` /
`ignore` on several tubes (including `NOT_IGNORED`), `put` with priorities
and delays, `reserve`, `reserve-with-timeout` (0 and > 0, `TIMED_OUT`),
`reserve-job`, `delete` (including `NOT_FOUND`), `release` with and without
delay, `bury`, `kick` (buried first, then delayed), `kick-job`, `touch`,
`peek` / `peek-ready` / `peek-delayed` / `peek-buried`, `stats`,
`stats-tube`, `stats-job`, `list-tubes` and `pause-tube` (including a
reserve blocked until the pause expires). It also prints a transcript of
every call and every stats field.

## Running

```sh
scripts/build-ref.sh          # once: builds .ref/beanstalkd/beanstalkd
clients/run-smoke.sh
```

The runner:

1. builds `beanstalkd-rs` (`cargo build --release -p bstk-server`, honouring
   `CARGO_TARGET_DIR`) unless `BSTK_RS_BIN` is set;
2. for each client, starts a fresh reference server and a fresh
   `beanstalkd-rs` on free ports bound to 127.0.0.1, and runs the client
   against each under a watchdog;
3. normalizes both transcripts with `normalize.py` (masks pid, version,
   rusage, uptime, hostname, os, platform, the server `id`, job `age`,
   `time-left` and `pause-time-left` -- the same list as
   `tests/compat/src/mask.rs` -- and renumbers job ids in order of first
   appearance) and diffs them.

It exits non-zero if any client assertion fails, a client hangs, or the
normalized transcripts differ; the diff is printed.

### Binlog mode (`-b`)

```sh
SMOKE_BINLOG=1 clients/run-smoke.sh
SMOKE_BINLOG=1 SMOKE_SERVER_ARGS=-f0 clients/run-smoke.sh
SMOKE_BINLOG=1 SMOKE_SERVER_ARGS=-F clients/run-smoke.sh
```

Both servers run with `-b <fresh dir under SMOKE_OUT>` (plus
`SMOKE_SERVER_ARGS`), and `normalize.py --binlog` additionally masks the
binlog layout fields of docs/COMPAT.md D8: `file` (stats-job),
`binlog-oldest-index`, `binlog-current-index` and `binlog-records-migrated`.
`binlog-records-written` and `binlog-max-size` stay compared (the flows are
far too small to trigger compaction). The client flow is unchanged.

### Restart mode

```sh
SMOKE_RESTART=1 clients/run-smoke.sh
SMOKE_RESTART=1 SMOKE_SERVER_ARGS=-F clients/run-smoke.sh
```

Implies `SMOKE_BINLOG=1`. For each (client, server) pair:

1. the client runs the full flow with `--leave-jobs`: afterwards it leaves
   jobs in tube `smoke-keep` / `smoke-go-keep` in known journaled states
   (ready; delayed 3600s; two buried, buried in the opposite order of their
   puts; buried then kicked; released with a 7200s delay; a deleted one),
   plus two jobs it keeps **reserved** (one whose last record is its put,
   one whose last record is a 1s-delay release that has expired), then
   prints `HOLDING` and blocks on stdin;
2. the runner kills the server with SIGKILL (the reference has no SIGTERM
   handler, so this is the fair choice for both) while the reservations are
   held, then closes the client's stdin;
3. it restarts the server on the same binlog dir (on a new port) and runs the
   client with `--after-restart`, which dumps `stats`, `list-tubes`,
   `stats-tube`, then walks `peek-ready` / `peek-buried` / `peek-delayed`
   printing `stats-job` for each recovered job and deleting it, and finally
   checks that the next job id continues after the highest id in the log.

Both phases go into one transcript (separated by a marker line), so job
ids are renumbered consistently across the restart. The recovery rules
being checked are in docs/COMPAT.md, section "Binlog (`-b`)".

Binlog directories are removed when the run ends.

Requirements: `python3` (a venv with greenstalk is created automatically)
and, for the Go client, a Go toolchain (the module is fetched on first
build).

Environment overrides:

| Variable | Default | Meaning |
|---|---|---|
| `BSTK_REF_BIN` | `.ref/beanstalkd/beanstalkd` | reference binary |
| `BSTK_RS_BIN` | built from source | `beanstalkd-rs` binary |
| `SMOKE_CLIENTS` | `python go` | clients to run |
| `SMOKE_VENV` | `clients/python/.venv` | Python virtualenv location |
| `SMOKE_TIMEOUT` | `120` | per-client watchdog, seconds |
| `SMOKE_OUT` | a temp dir | where raw and normalized transcripts go |
| `SMOKE_BINLOG` | `0` | `1`: run both servers with `-b` and mask D8 fields |
| `SMOKE_RESTART` | `0` | `1`: binlog mode plus kill/restart phase (implies `SMOKE_BINLOG=1`) |
| `SMOKE_SERVER_ARGS` | empty | extra arguments for both servers, split on whitespace (e.g. `-f0`, `-F`, `-s 100000`) |

A single client can also be run by hand against any server:

```sh
python3 clients/python/smoke.py 127.0.0.1:11300
(cd clients/go && go run . 127.0.0.1:11300)
```

(`--leave-jobs` / `--after-restart` go before the address; see above.)

Both expect a fresh server: the final `stats` assertions (total jobs, all
queues empty) assume nothing else has used it.
