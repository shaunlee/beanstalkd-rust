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

A single client can also be run by hand against any server:

```sh
python3 clients/python/smoke.py 127.0.0.1:11300
(cd clients/go && go run . 127.0.0.1:11300)
```

Both expect a fresh server: the final `stats` assertions (total jobs, all
queues empty) assume nothing else has used it.
