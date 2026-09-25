# Real-client smoke tests

These tests drive `beanstalkd-rs` with unmodified, third-party beanstalkd
client libraries and check that it behaves exactly like the reference C
beanstalkd.

| Client | Library | Script |
|---|---|---|
| Python | [greenstalk](https://pypi.org/project/greenstalk/) | `python/smoke.py` |
| Go | [go-beanstalk](https://github.com/beanstalkd/go-beanstalk) v0.2.0 | `go/main.go` |

Both also run over TLS and mTLS against `beanstalkd-rs` (see "TLS and
mTLS modes"). `python/checks.py` holds the checks no client library can
drive (token authentication, mTLS rejection, HTTP endpoints), and
`mkcerts.sh` generates throwaway certificates for them and for the
benchmarks.

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

### TLS and mTLS modes

```sh
SMOKE_TLS=1 clients/run-smoke.sh
SMOKE_MTLS=1 clients/run-smoke.sh
SMOKE_TLS=1 SMOKE_BINLOG=1 clients/run-smoke.sh
SMOKE_MTLS=1 SMOKE_RESTART=1 clients/run-smoke.sh
```

`clients/mkcerts.sh` generates a throwaway CA, a server certificate for
`localhost` / `127.0.0.1`, a client certificate, and a client certificate
from an unrelated CA (ECDSA P-256, valid 2 days) under `SMOKE_OUT/certs`.
`beanstalkd-rs` is then started with a generated `--config` holding a
single TLS listener on the fresh port (`auth = "none"`, or `auth = "mtls"`
with `client_ca` for `SMOKE_MTLS=1`), plus `-b` in binlog modes. The
reference keeps running in plaintext: the clients connect to it as usual,
and to `beanstalkd-rs` over TLS, and the normalized transcripts must still
be identical (the protocol is unchanged over TLS).

The client libraries are unmodified; the smoke clients only build the
connection themselves when `SMOKE_TLS_CA` is set (with `SMOKE_TLS_CERT` /
`SMOKE_TLS_KEY` for a client certificate):

- Python: `greenstalk.Client` accepts an already connected socket, so the
  client passes `ssl.create_default_context(cafile=CA).wrap_socket(...)`
  (after `load_cert_chain` for mTLS);
- Go: `beanstalk.NewConn` accepts any `io.ReadWriteCloser`, so the client
  passes a `crypto/tls` connection (`tls.DialWithDialer`, `RootCAs` from
  the CA, `Certificates` for mTLS).

Both verify the server certificate against the generated CA and the name
`127.0.0.1`. The runner's readiness probe completes a TLS handshake too:
`beanstalkd-rs` counts a TLS connection in `total-connections` only once
its handshake has completed, while the reference counts the probe's plain
TCP connection.

`SMOKE_MTLS=1` also runs `checks.py mtls-reject` against a fresh mTLS
server: a client without a certificate and a client with a certificate
from an untrusted CA must both be rejected. With TLS 1.3 the client side
of the handshake completes before the server has checked the client
certificate, so each case sends `stats` and requires a TLS alert
(`certificate required` / `unknown CA`), a reset or EOF, never a reply.

### Token authentication (`SMOKE_TOKEN=1`)

```sh
SMOKE_TOKEN=1 SMOKE_CLIENTS= clients/run-smoke.sh
```

Starts `beanstalkd-rs` with one `auth = "token"` TLS listener and a random
token, and runs `checks.py token` (raw TLS socket): `auth <wrong>` gets
`UNAUTHORIZED` and a close; `auth <wrong>` pipelined with `stats` in one
write gets exactly one `UNAUTHORIZED` and a close; `put` (with body),
`stats` and `list-tubes` before `auth` each get `UNAUTHORIZED` and a close;
`quit` closes silently; a client that sends nothing is closed without a
reply after the auth timeout (the generated config sets `[auth] timeout =
"1s"`). An authenticated connection then checks that none
of this reached the engine (`cmd-stats` 1, `cmd-put` 0, `cmd-list-tubes`
0, `total-jobs` 0, `current-tubes` 1, `total-connections` 1), runs `use` /
`put` / `reserve-with-timeout` / `delete` / `stats-tube`, and a wrong
second `auth` on the authenticated connection gets `UNAUTHORIZED` and a
close. No real client library sends `auth`, hence the raw socket.

### HTTP endpoints (`SMOKE_HTTP=1`)

```sh
SMOKE_HTTP=1 SMOKE_CLIENTS= clients/run-smoke.sh
SMOKE_HTTP=1 SMOKE_MTLS=1 SMOKE_BINLOG=1 clients/run-smoke.sh
```

Starts `beanstalkd-rs` with `[http]` on a free port (with
`snapshot_min_interval = "0s"`, so that no cached snapshot predates the
`stats` it is compared with) and the listener of the
current mode (plaintext, TLS or mTLS; with `-b` in binlog modes), and runs
`checks.py http`: over the protocol it builds a known state (two tubes;
urgent, ready, delayed, buried and reserved jobs; a worker blocked in
`reserve`; a paused tube), runs `stats-tube` for four tubes and `stats`
last, and, with those connections still open, fetches with curl:

- `/healthz` (200 `ok`) and `/readyz` (200 `ready`), an unknown path (404)
  and a `POST /metrics` (405);
- `/admin`: every `stats` / `stats-tube` field must equal its JSON value;
- `/metrics`: every numeric field must equal its Prometheus sample
  (`current-jobs-X` as `beanstalkd_current_jobs{state="X"}`, `cmd-X` as
  `beanstalkd_commands_total{cmd="X"}`, per-tube series, binlog fields, ...).

Only the volatile fields are skipped (`pid`, `uptime`, `rusage-*`,
`pause-time-left`). `/readyz` returning 503 during binlog replay is
covered by the server's integration tests, not here.

### Mode matrix

All of these pass (`python` and `go` transcripts identical, extra checks
green):

| Command | Checks |
|---|---|
| `clients/run-smoke.sh` | transcripts |
| `SMOKE_TLS=1` | transcripts over TLS |
| `SMOKE_MTLS=1` | transcripts over mTLS, mtls-reject |
| `SMOKE_TLS=1 SMOKE_BINLOG=1 SMOKE_HTTP=1` | transcripts over TLS with `-b`, http |
| `SMOKE_TLS=1 SMOKE_RESTART=1` | kill / restart over TLS |
| `SMOKE_MTLS=1 SMOKE_RESTART=1 SMOKE_HTTP=1 SMOKE_TOKEN=1` | kill / restart over mTLS, mtls-reject, token, http (with `-b`) |
| `SMOKE_MTLS=1 SMOKE_RESTART=1 SMOKE_SERVER_ARGS=-F` | kill / restart over mTLS with `-F` |
| `SMOKE_TOKEN=1 SMOKE_HTTP=1 SMOKE_CLIENTS=` | token, http (plaintext listener) |
| `SMOKE_BINLOG=1`, `SMOKE_RESTART=1` | unchanged plaintext modes |

Generated certificates and configuration files are removed when the run
ends.

Requirements: `python3` (a venv with greenstalk is created automatically),
`openssl` (TLS modes), `curl` (`SMOKE_HTTP=1`) and, for the Go client, a Go
toolchain (the module is fetched on first build).

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
| `SMOKE_TLS` | `0` | `1`: `beanstalkd-rs` behind one TLS listener, clients over TLS |
| `SMOKE_MTLS` | `0` | `1`: like `SMOKE_TLS` with client certificates, plus the rejection checks (implies `SMOKE_TLS=1`) |
| `SMOKE_TOKEN` | `0` | `1`: token authentication checks |
| `SMOKE_HTTP` | `0` | `1`: `/healthz`, `/readyz`, `/metrics`, `/admin` checks |

A single client can also be run by hand against any server:

```sh
python3 clients/python/smoke.py 127.0.0.1:11300
(cd clients/go && go run . 127.0.0.1:11300)
```

(`--leave-jobs` / `--after-restart` go before the address; see above.
Set `SMOKE_TLS_CA` (and `SMOKE_TLS_CERT` / `SMOKE_TLS_KEY`) to connect over
TLS.) The extra checks run by hand too:

```sh
clients/mkcerts.sh /tmp/certs
python3 clients/python/checks.py token --ca /tmp/certs/ca.pem --token TOKEN 127.0.0.1:11301
python3 clients/python/checks.py mtls-reject --ca /tmp/certs/ca.pem \
  --rogue-cert /tmp/certs/rogue-client.pem --rogue-key /tmp/certs/rogue-client.key 127.0.0.1:11302
python3 clients/python/checks.py http --http 127.0.0.1:9180 127.0.0.1:11300
```

Both expect a fresh server: the final `stats` assertions (total jobs, all
queues empty) assume nothing else has used it.
