#!/usr/bin/env python3
"""TLS, authentication and HTTP endpoint checks for beanstalkd-rs, over raw
(TLS) sockets. Driven by clients/run-smoke.sh (SMOKE_MTLS, SMOKE_TOKEN,
SMOKE_HTTP).

Usage:
    checks.py mtls-reject --ca CA --rogue-cert PEM --rogue-key KEY HOST:PORT
    checks.py token --ca CA --token TOKEN HOST:PORT
    checks.py http --http HOST:PORT [--ca CA [--cert PEM --key KEY]] HOST:PORT

mtls-reject (a listener with auth = "mtls"): a client without a
certificate, and a client whose certificate is signed by an untrusted CA,
must both fail the handshake. With TLS 1.3 the client side of the handshake
completes before the server has checked the client certificate, so each
case sends `stats` and requires a TLS alert, reset or EOF, never a reply.

token (a listener with auth = "token", fresh server): before any
successful `auth`,
  - `auth <wrong>` gets UNAUTHORIZED and the connection is closed;
  - `auth <wrong>` pipelined with `stats` in one write gets exactly one
    UNAUTHORIZED, then the close;
  - `put` (with its body) and `stats` each get UNAUTHORIZED and a close;
  - `quit` closes silently;
  - a client that sends nothing is closed, without a reply, after the
    auth timeout (the runner sets auth.timeout = "1s");
then an authenticated connection checks that none of the above reached the
engine (cmd-stats, cmd-put, total-jobs, current-tubes), runs a short
use / put / reserve / delete / stats-tube flow, and a second `auth` with a
wrong token on the authenticated connection gets UNAUTHORIZED and a close.
The token is never printed.

http (a fresh server with [http]; the protocol listener at HOST:PORT is
TLS when --ca is given): builds a known state over the protocol (two tubes;
urgent, ready, delayed, buried and reserved jobs; a worker blocked in
`reserve`; a paused tube), then runs `stats` and `stats-tube` and, while
those connections stay open, fetches /healthz, /readyz, /metrics and /admin
with curl. Every `stats` / `stats-tube` field must equal its /admin JSON
value and its /metrics sample (except the volatile ones: pid, uptime,
rusage, pause-time-left). Also checks 404 for an unknown path and 405 for a
POST.

Prints one line per check; exits non-zero on the first failure.
"""

from __future__ import annotations

import argparse
import json
import socket
import ssl
import subprocess
import sys
import time

TIMEOUT = 10


def out(line: str) -> None:
    print(line, flush=True)


class Conn:
    def __init__(self, ctx: ssl.SSLContext | None, host: str, port: int) -> None:
        raw = socket.create_connection((host, port), timeout=TIMEOUT)
        self.sock = raw if ctx is None else ctx.wrap_socket(raw, server_hostname=host)
        self.buf = b""

    def send(self, data: bytes) -> None:
        self.sock.sendall(data)

    def _fill(self) -> bool:
        chunk = self.sock.recv(65536)
        if not chunk:
            return False
        self.buf += chunk
        return True

    def line(self) -> bytes | None:
        """One reply line without CRLF, or None on EOF."""
        while b"\r\n" not in self.buf:
            if not self._fill():
                return None
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def body(self, n: int) -> bytes:
        while len(self.buf) < n + 2:
            if not self._fill():
                raise AssertionError("EOF inside a reply body")
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def call(self, cmd: bytes) -> bytes | None:
        self.send(cmd + b"\r\n")
        return self.line()

    def stats(self, cmd: bytes = b"stats") -> dict[str, str]:
        reply = self.call(cmd)
        if reply is None or not reply.startswith(b"OK "):
            raise AssertionError(f"{cmd!r}: got {reply!r}")
        yaml = self.body(int(reply.split()[1])).decode()
        return dict(l.split(": ", 1) for l in yaml.splitlines() if ": " in l)

    def expect_closed(self, what: str) -> None:
        """No further bytes: the server has closed the connection."""
        try:
            extra = self.sock.recv(65536)
        except (ConnectionResetError, ssl.SSLError):
            extra = b""
        if extra or self.buf:
            raise AssertionError(f"{what}: extra bytes after the reply: {self.buf + extra!r}")

    def close(self) -> None:
        self.sock.close()


def expect(label: str, got: object, want: object) -> None:
    if got != want:
        raise AssertionError(f"{label}: got {got!r}, want {want!r}")
    out(f"ok: {label}")


def context(ca: str, cert: str | None = None, key: str | None = None) -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=ca)
    if cert:
        ctx.load_cert_chain(cert, key)
    return ctx


def mtls_reject(args: argparse.Namespace, host: str, port: int) -> None:
    cases = [
        ("no client certificate", context(args.ca)),
        ("client certificate from an untrusted CA", context(args.ca, args.rogue_cert, args.rogue_key)),
    ]
    for label, ctx in cases:
        try:
            c = Conn(ctx, host, port)
            c.send(b"stats\r\n")
            reply = c.line()
        except (ssl.SSLError, ConnectionResetError, BrokenPipeError) as e:
            out(f"ok: {label}: rejected ({type(e).__name__}: {getattr(e, 'reason', None) or e})")
            continue
        if reply is not None:
            raise AssertionError(f"{label}: server replied {reply!r}")
        out(f"ok: {label}: rejected (connection closed without a reply)")


def token(args: argparse.Namespace, host: str, port: int) -> None:
    ctx = context(args.ca)
    wrong = b"not-" + args.token.encode()

    def fresh() -> Conn:
        return Conn(ctx, host, port)

    c = fresh()
    expect("wrong token -> UNAUTHORIZED", c.call(b"auth " + wrong), b"UNAUTHORIZED")
    c.expect_closed("wrong token")
    out("ok: wrong token -> closed")
    c.close()

    c = fresh()
    c.send(b"auth " + wrong + b"\r\nstats\r\n")
    expect("pipelined wrong auth + stats -> one UNAUTHORIZED", c.line(), b"UNAUTHORIZED")
    c.expect_closed("pipelined wrong auth")
    out("ok: pipelined wrong auth + stats -> closed")
    c.close()

    for label, data in [
        ("put before auth", b"put 0 0 60 5\r\nhello\r\n"),
        ("stats before auth", b"stats\r\n"),
        ("list-tubes before auth", b"list-tubes\r\n"),
    ]:
        c = fresh()
        c.send(data)
        expect(f"{label} -> UNAUTHORIZED", c.line(), b"UNAUTHORIZED")
        c.expect_closed(label)
        out(f"ok: {label} -> closed")
        c.close()

    # The runner configures auth.timeout = "1s": a client that completes
    # the handshake but never sends `auth` is closed without a reply.
    c = fresh()
    t0 = time.monotonic()
    expect("silent client -> closed without a reply", c.line(), None)
    waited = time.monotonic() - t0
    if not 0.5 <= waited <= 5:
        raise AssertionError(f"silent client closed after {waited:.2f}s, want about 1s")
    out(f"ok: silent client closed after the auth timeout ({waited:.1f}s)")
    c.close()

    c = fresh()
    c.send(b"quit\r\n")
    expect("quit before auth -> closed silently", c.line(), None)
    c.close()

    c = fresh()
    expect("auth <token> -> AUTHENTICATED", c.call(b"auth " + args.token.encode()), b"AUTHENTICATED")
    st = c.stats()
    # Only this `stats` counts: nothing sent before authentication reached
    # the engine.
    for k, want in [("cmd-stats", "1"), ("cmd-put", "0"), ("cmd-list-tubes", "0"),
                    ("total-jobs", "0"), ("current-tubes", "1")]:
        expect(f"after rejected clients: {k}", st.get(k), want)
    # Rejected connections are never registered with the engine.
    expect("after rejected clients: total-connections", st.get("total-connections"), "1")

    expect("use", c.call(b"use smoke-auth"), b"USING smoke-auth")
    expect("watch", c.call(b"watch smoke-auth"), b"WATCHING 2")
    put = c.call(b"put 10 0 60 5\r\nhello")
    if put is None or not put.startswith(b"INSERTED "):
        raise AssertionError(f"put: got {put!r}")
    jid = put.split()[1]
    out("ok: put -> INSERTED")
    res = c.call(b"reserve-with-timeout 0")
    expect("reserve", res, b"RESERVED " + jid + b" 5")
    expect("reserved body", c.body(5), b"hello")
    expect("delete", c.call(b"delete " + jid), b"DELETED")
    ts = c.stats(b"stats-tube smoke-auth")
    expect("stats-tube total-jobs", ts.get("total-jobs"), "1")
    expect("stats-tube current-jobs-ready", ts.get("current-jobs-ready"), "0")
    expect("second auth with the right token", c.call(b"auth " + args.token.encode()), b"AUTHENTICATED")
    expect("second auth with a wrong token -> UNAUTHORIZED", c.call(b"auth " + wrong), b"UNAUTHORIZED")
    c.expect_closed("second auth with a wrong token")
    out("ok: second auth with a wrong token -> closed")
    c.close()


def curl(url: str, *extra: str) -> tuple[int, str]:
    """(HTTP status, body) of `curl url`."""
    r = subprocess.run(
        ["curl", "-sS", "--max-time", str(TIMEOUT), "-o", "-", "-w", "\n%{http_code}", *extra, url],
        capture_output=True, text=True, check=True,
    )
    body, _, code = r.stdout.rpartition("\n")
    return int(code), body


def parse_metrics(text: str) -> dict[str, float]:
    """Prometheus text format -> {'name{labels}': value}."""
    samples: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        key, _, value = line.rpartition(" ")
        samples[key] = float(value)
    return samples


# Server `stats` fields that /metrics does not export or that change
# between two reads.
VOLATILE = {"pid", "uptime", "rusage-utime", "rusage-stime", "rusage-maxrss", "pause-time-left"}
NOT_A_METRIC = {"version", "id", "hostname", "os", "platform", "draining", "name"}
SERVER_METRIC = {
    "job-timeouts": "beanstalkd_job_timeouts_total",
    "total-jobs": "beanstalkd_jobs_total",
    "max-job-size": "beanstalkd_max_job_size_bytes",
    "total-connections": "beanstalkd_connections_total",
    "binlog-records-migrated": "beanstalkd_binlog_records_migrated_total",
    "binlog-records-written": "beanstalkd_binlog_records_written_total",
    "binlog-max-size": "beanstalkd_binlog_max_size_bytes",
}


def server_metric(k: str) -> str:
    if k.startswith("current-jobs-"):
        return f'beanstalkd_current_jobs{{state="{k.removeprefix("current-jobs-")}"}}'
    if k.startswith("cmd-"):
        return f'beanstalkd_commands_total{{cmd="{k.removeprefix("cmd-")}"}}'
    return SERVER_METRIC.get(k) or "beanstalkd_" + k.replace("-", "_")


def tube_metric(tube: str, k: str) -> str:
    if k.startswith("current-jobs-"):
        return f'beanstalkd_tube_current_jobs{{tube="{tube}",state="{k.removeprefix("current-jobs-")}"}}'
    if k.startswith("cmd-"):
        return f'beanstalkd_tube_commands_total{{tube="{tube}",cmd="{k.removeprefix("cmd-")}"}}'
    if k == "total-jobs":
        return f'beanstalkd_tube_jobs_total{{tube="{tube}"}}'
    if k == "pause":
        return f'beanstalkd_tube_pause_seconds{{tube="{tube}"}}'
    return f'beanstalkd_tube_{k.replace("-", "_")}{{tube="{tube}"}}'


def same(a: str, b: object) -> bool:
    """A `stats` YAML value (quoted when it is a string) vs a JSON or
    Prometheus value."""
    if len(a) >= 2 and a[0] == a[-1] == '"':
        a = a[1:-1]
    if isinstance(b, bool):
        return a == str(b).lower()
    try:
        return float(a) == float(b)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return a == str(b)


def compare(scope: str, stats: dict[str, str], admin: dict[str, object],
            metrics: dict[str, float], metric_name) -> int:
    n = 0
    for k, v in stats.items():
        if k in VOLATILE:
            continue
        if k not in admin:
            raise AssertionError(f"{scope}: {k} missing from /admin")
        if not same(v, admin[k]):
            raise AssertionError(f"{scope}: {k}: stats {v!r}, /admin {admin[k]!r}")
        n += 1
        if k in NOT_A_METRIC:
            continue
        m = metric_name(k)
        if m not in metrics:
            raise AssertionError(f"{scope}: {k}: no /metrics sample {m}")
        if not same(v, metrics[m]):
            raise AssertionError(f"{scope}: {k}: stats {v!r}, /metrics {m} {metrics[m]!r}")
        n += 1
    return n


def http(args: argparse.Namespace, host: str, port: int) -> None:
    ctx = context(args.ca, args.cert, args.key) if args.ca else None
    base = f"http://{args.http}"

    def conn() -> Conn:
        return Conn(ctx, host, port)

    def ok(c: Conn, cmd: bytes, prefix: bytes) -> bytes:
        reply = c.call(cmd)
        if reply is None or not reply.startswith(prefix):
            raise AssertionError(f"{cmd!r}: got {reply!r}")
        return reply

    prod = conn()
    ok(prod, b"use smoke-http-a", b"USING")
    ok(prod, b"put 10 0 60 3\r\nurg", b"INSERTED")          # urgent
    ok(prod, b"put 2000 0 60 5\r\nready", b"INSERTED")      # ready
    ok(prod, b"put 100 3600 60 7\r\ndelayed", b"INSERTED")  # delayed
    ok(prod, b"put 100 0 60 6\r\nburied", b"INSERTED")
    ok(prod, b"put 100 0 60 8\r\nreserved", b"INSERTED")
    ok(prod, b"use smoke-http-b", b"USING")
    ok(prod, b"put 100 0 60 1\r\nb", b"INSERTED")
    ok(prod, b"pause-tube smoke-http-b 3600", b"PAUSED")
    work = conn()
    ok(work, b"watch smoke-http-a", b"WATCHING 2")
    ok(work, b"ignore default", b"WATCHING 1")
    # Reserve three jobs in priority order (the urgent one, then the two
    # pri-100 jobs in id order), release the urgent one and bury the next.
    ids = []
    for _ in range(3):
        r = ok(work, b"reserve-with-timeout 0", b"RESERVED")
        work.body(int(r.split()[2]))
        ids.append(r.split()[1])
    ok(work, b"release " + ids[0] + b" 10 0", b"RELEASED")
    ok(work, b"bury " + ids[1] + b" 100", b"BURIED")
    # ids[2] stays reserved by `work`.
    waiter = conn()
    ok(waiter, b"watch smoke-http-empty", b"WATCHING 2")
    ok(waiter, b"ignore default", b"WATCHING 1")
    waiter.send(b"reserve\r\n")  # blocks: current-waiting 1

    mon = conn()
    # current-waiting is only visible once the blocked reserve arrived.
    for _ in range(100):
        st = mon.stats()
        if st.get("current-waiting") == "1":
            break
    expect("a worker is waiting", st.get("current-waiting"), "1")
    tubes = {t: mon.stats(b"stats-tube " + t.encode()) for t in
             ("default", "smoke-http-a", "smoke-http-b", "smoke-http-empty")}
    # The last protocol command before the HTTP reads, so that every
    # counter (cmd-stats-tube included) is final.
    st = mon.stats()

    code, body = curl(base + "/healthz")
    expect("/healthz", (code, body), (200, "ok"))
    code, body = curl(base + "/readyz")
    expect("/readyz", (code, body), (200, "ready"))
    code, body = curl(base + "/nope")
    expect("unknown path -> 404", code, 404)
    code, body = curl(base + "/metrics", "-X", "POST")
    expect("POST /metrics -> 405", code, 405)

    code, text = curl(base + "/metrics")
    expect("/metrics status", code, 200)
    metrics = parse_metrics(text)
    code, text = curl(base + "/admin")
    expect("/admin status", code, 200)
    admin = json.loads(text)
    n = compare("stats", st, admin["server"], metrics, server_metric)
    out(f"ok: stats == /admin == /metrics ({n} values)")
    by_name = {t["name"]: t for t in admin["tubes"]}
    for t, ts in tubes.items():
        n = compare(f"stats-tube {t}", ts, by_name[t], metrics, lambda k, t=t: tube_metric(t, k))
        out(f"ok: stats-tube {t} == /admin == /metrics ({n} values)")
    expect("tube sample count", len(by_name), int(st["current-tubes"]))
    for k, want in [("current-jobs-urgent", "2"), ("current-jobs-ready", "3"),
                    ("current-jobs-reserved", "1"), ("current-jobs-delayed", "1"),
                    ("current-jobs-buried", "1"), ("current-waiting", "1")]:
        expect(f"state {k}", st.get(k), want)
    expect("paused tube", tubes["smoke-http-b"].get("pause"), "3600")
    for c in (prod, work, waiter, mon):
        c.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("check", choices=["mtls-reject", "token", "http"])
    p.add_argument("addr", help="HOST:PORT")
    p.add_argument("--ca")
    p.add_argument("--cert")
    p.add_argument("--key")
    p.add_argument("--http", help="HTTP listener HOST:PORT")
    p.add_argument("--rogue-cert")
    p.add_argument("--rogue-key")
    p.add_argument("--token")
    args = p.parse_args()
    host, port_s = args.addr.rsplit(":", 1)
    try:
        if args.check == "http":
            if not args.http:
                p.error("http needs --http")
            http(args, host, int(port_s))
        elif not args.ca:
            p.error(f"{args.check} needs --ca")
        elif args.check == "mtls-reject":
            if not (args.rogue_cert and args.rogue_key):
                p.error("mtls-reject needs --rogue-cert and --rogue-key")
            mtls_reject(args, host, int(port_s))
        else:
            if not args.token:
                p.error("token needs --token")
            token(args, host, int(port_s))
    except AssertionError as e:
        print(f"ASSERTION FAILED: {e}", file=sys.stderr)
        return 1
    out("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
