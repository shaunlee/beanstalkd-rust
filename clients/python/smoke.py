#!/usr/bin/env python3
"""Real-client smoke test using the greenstalk client library.

Usage: smoke.py [--leave-jobs | --after-restart | --kill-point] HOST:PORT

Runs a full protocol flow against a (fresh) beanstalkd-compatible server,
asserting on every result, and prints a transcript to stdout.

Binlog restart test (driven by clients/run-smoke.sh with SMOKE_RESTART=1):
    --leave-jobs      after the full flow, leaves jobs in known states in
                      tube smoke-keep (ready, delayed, buried, kicked, and
                      two jobs held reserved), prints HOLDING and then
                      blocks until stdin is closed, so the runner can kill
                      the server while the jobs are still reserved.
    --after-restart   run against the restarted server (same binlog dir):
                      dumps and checks the recovered state, then drains it.

Cluster leader-kill test (driven by clients/run-smoke.sh with
SMOKE_CLUSTER_KILL=1):
    --kill-point      after the full flow, a worker holds a reservation and
                      a second worker waits in reserve-with-timeout; the
                      client prints KILL_POINT and blocks until stdin is
                      closed (the runner kills the cluster leader
                      meanwhile), then checks that both connections and the
                      reservation survived and that the waiter is served.

The raw
transcript contains volatile values (pids, job ids, uptime, ...); the
runner (clients/run-smoke.sh) pipes it through clients/normalize.py
before diffing the reference against beanstalkd-rs.

Transcript line formats (consumed by normalize.py):
    <step>: <result>                    one line per client call
    [<scope>] <key>: <value>            one line per stats field, in the
                                        order the server sent them
Job ids are always printed as `job=<n>` so they can be normalized.

TLS (driven by clients/run-smoke.sh with SMOKE_TLS=1 / SMOKE_MTLS=1): when
SMOKE_TLS_CA is set, every connection is a TLS connection that verifies the
server certificate against that CA bundle (server name: the host part of
HOST:PORT); SMOKE_TLS_CERT / SMOKE_TLS_KEY add a client certificate (mTLS).
The library is unmodified: greenstalk.Client accepts a connected socket,
and ssl.SSLSocket is one.
"""

from __future__ import annotations

import os
import socket
import ssl
import sys
import time
from typing import Any, Callable

import greenstalk

# A hung server must fail the run, not hang it.
socket.setdefaulttimeout(10)

TUBE_A = "smoke-a"
TUBE_B = "smoke-b"
TUBE_C = "smoke-c"
TUBE_KEEP = "smoke-keep"
TUBE_KILL = "smoke-kill"


def tls_context() -> ssl.SSLContext | None:
    ca = os.environ.get("SMOKE_TLS_CA")
    if not ca:
        return None
    ctx = ssl.create_default_context(cafile=ca)
    cert = os.environ.get("SMOKE_TLS_CERT")
    if cert:
        ctx.load_cert_chain(cert, os.environ.get("SMOKE_TLS_KEY"))
    return ctx


TLS = tls_context()


def connect(addr: tuple[str, int], **kw: Any) -> greenstalk.Client:
    """A greenstalk client over plain TCP, or over TLS when configured."""
    if TLS is None:
        return greenstalk.Client(addr, **kw)
    sock = socket.create_connection(addr)
    return greenstalk.Client(TLS.wrap_socket(sock, server_hostname=addr[0]), **kw)


def out(line: str) -> None:
    print(line, flush=True)


def job(j: greenstalk.Job | int) -> str:
    return f"job={j if isinstance(j, int) else j.id}"


def jb(j: greenstalk.Job) -> str:
    return f"{job(j)} body={j.body!r}"


def err_name(fn: Callable[[], Any]) -> str:
    """Calls `fn`, which must raise a greenstalk error; returns its name."""
    try:
        res = fn()
    except greenstalk.Error as e:
        return type(e).__name__
    raise AssertionError(f"expected a greenstalk error, got {res!r}")


def dump(scope: str, stats: dict[str, Any]) -> None:
    for k, v in stats.items():
        out(f"[{scope}] {k}: {v}")


def expect(label: str, got: Any, want: Any) -> None:
    if got != want:
        raise AssertionError(f"{label}: got {got!r}, want {want!r}")


def main() -> int:
    args = sys.argv[1:]
    mode = args.pop(0) if args and args[0].startswith("--") else ""
    if mode not in ("", "--leave-jobs", "--after-restart", "--kill-point") or len(args) != 1 or ":" not in args[0]:
        print(__doc__, file=sys.stderr)
        return 2
    host, port_s = args[0].rsplit(":", 1)
    addr = (host, int(port_s))
    if mode == "--after-restart":
        return after_restart(addr)
    return full_flow(addr, leave_jobs=mode == "--leave-jobs", kill_point=mode == "--kill-point")


def full_flow(addr: tuple[str, int], leave_jobs: bool, kill_point: bool = False) -> int:
    # --- tubes: use / watch / ignore / list -----------------------------
    prod = connect(addr, use=TUBE_A, watch=TUBE_A)
    work = connect(addr, use="default", watch=[TUBE_A, TUBE_B])
    out(f"producer using: {prod.using()}")
    out(f"producer watching: {prod.watching()}")
    out(f"worker watching: {sorted(work.watching())}")
    expect("producer using", prod.using(), TUBE_A)
    expect("worker watching", sorted(work.watching()), [TUBE_A, TUBE_B])
    n = work.watch(TUBE_C)
    out(f"worker watch {TUBE_C}: {n}")
    expect("watch count", n, 3)
    n = work.ignore(TUBE_C)
    out(f"worker ignore {TUBE_C}: {n}")
    expect("ignore count", n, 2)
    e = err_name(lambda: prod.ignore(TUBE_A))
    out(f"producer ignore last watched tube: {e}")
    expect("ignore last", e, "NotIgnoredError")
    tubes = sorted(prod.tubes())
    out(f"list-tubes: {tubes}")
    expect("list-tubes", tubes, ["default", TUBE_A, TUBE_B])

    # --- empty-queue errors ---------------------------------------------
    e = err_name(lambda: work.reserve(timeout=0))
    out(f"reserve-with-timeout 0 on empty: {e}")
    expect("reserve empty", e, "TimedOutError")
    for name, fn in [
        ("peek-ready", prod.peek_ready),
        ("peek-delayed", prod.peek_delayed),
        ("peek-buried", prod.peek_buried),
    ]:
        e = err_name(fn)
        out(f"{name} on empty: {e}")
        expect(name, e, "NotFoundError")

    # --- put with priorities; reserve returns the most urgent first ------
    ids = {}
    for body, pri in [("low", 2000), ("urgent", 10), ("mid", 500), ("mid2", 500)]:
        ids[body] = prod.put(body, priority=pri, ttr=60)
        out(f"put {body} pri={pri}: {job(ids[body])}")
    prod.use(TUBE_B)
    ids["b1"] = prod.put("b1", priority=100, ttr=60)
    out(f"put b1 into {TUBE_B}: {job(ids['b1'])}")

    j = prod.peek_ready()
    out(f"peek-ready ({TUBE_B}): {jb(j)}")
    expect("peek-ready b", j.id, ids["b1"])
    prod.use(TUBE_A)
    j = prod.peek_ready()
    out(f"peek-ready ({TUBE_A}): {jb(j)}")
    expect("peek-ready a", j.id, ids["urgent"])
    j = prod.peek(ids["mid"])
    out(f"peek mid: {jb(j)}")
    expect("peek body", j.body, "mid")
    e = err_name(lambda: prod.peek(999999))
    out(f"peek missing: {e}")
    expect("peek missing", e, "NotFoundError")

    dump(f"stats-tube {TUBE_A}", prod.stats_tube(TUBE_A))
    dump(f"stats-tube {TUBE_B}", prod.stats_tube(TUBE_B))

    order = []
    for i in range(5):
        # The first one is a plain `reserve` (a job is known to be ready);
        # the rest use `reserve-with-timeout`.
        j = work.reserve() if i == 0 else work.reserve(timeout=1)
        order.append(j.body)
        out(f"{'reserve' if i == 0 else 'reserve-with-timeout 1'}: {jb(j)}")
        if j.body == "mid":
            dump("stats-job mid", work.stats_job(j))
            work.touch(j)
            out(f"touch {job(j)}: ok")
            # Release with a delay: the job becomes delayed.
            work.release(j, priority=500, delay=2)
            out(f"release {job(j)} delay=2: ok")
        elif j.body == "mid2":
            work.bury(j, priority=7)
            out(f"bury {job(j)} pri=7: ok")
        elif j.body == "low":
            work.release(j, priority=3000)
            out(f"release {job(j)} no delay: ok")
        else:
            work.delete(j)
            out(f"delete {job(j)}: ok")
    expect("reserve order", order, ["urgent", "b1", "mid", "mid2", "low"])
    e = err_name(lambda: work.delete(ids["urgent"]))
    out(f"delete already-deleted: {e}")
    expect("double delete", e, "NotFoundError")

    j = prod.peek_delayed()
    out(f"peek-delayed: {jb(j)}")
    expect("peek-delayed", j.id, ids["mid"])
    j = prod.peek_buried()
    out(f"peek-buried: {jb(j)}")
    expect("peek-buried", j.id, ids["mid2"])
    dump("stats-job mid2", prod.stats_job(ids["mid2"]))
    dump(f"stats-tube {TUBE_A}", prod.stats_tube(TUBE_A))

    # kick: buried jobs are kicked first (only buried ones while any exist).
    n = prod.kick(10)
    out(f"kick 10: {n}")
    expect("kick buried", n, 1)
    # The delayed job ("mid", delay 2s) comes back on its own; reserve with
    # a generous timeout. "mid2" (pri 7 after bury) is now the most urgent.
    j = work.reserve(timeout=3)
    out(f"reserve: {jb(j)}")
    expect("reserve kicked", j.body, "mid2")
    work.delete(j)
    out(f"delete {job(j)}: ok")
    # reserve-job takes a specific ready job regardless of priority.
    j = work.reserve_job(ids["low"])
    out(f"reserve-job low: {jb(j)}")
    work.delete(j)
    out(f"delete {job(j)}: ok")
    e = err_name(lambda: work.reserve_job(ids["low"]))
    out(f"reserve-job deleted: {e}")
    expect("reserve-job deleted", e, "NotFoundError")
    j = work.reserve(timeout=5)
    out(f"reserve: {jb(j)}")
    expect("reserve after delay", j.body, "mid")
    # Bury it again and bring it back with kick-job.
    work.bury(j)
    out(f"bury {job(j)}: ok")
    prod.kick_job(j)
    out(f"kick-job {job(j)}: ok")
    e = err_name(lambda: prod.kick_job(999999))
    out(f"kick-job missing: {e}")
    expect("kick-job missing", e, "NotFoundError")

    # Delayed job kicked by `kick` (no buried jobs present).
    ids["later"] = prod.put("later", priority=1, delay=100, ttr=60)
    out(f"put later delay=100: {job(ids['later'])}")
    dump("stats-job later", prod.stats_job(ids["later"]))
    n = prod.kick(5)
    out(f"kick 5 (delayed): {n}")
    expect("kick delayed", n, 1)

    # --- reserve with timeout ------------------------------------------------
    for want in ["later", "mid"]:
        j = work.reserve(timeout=1)
        out(f"reserve: {jb(j)}")
        expect("drain", j.body, want)
        work.delete(j)
        out(f"delete {job(j)}: ok")
    e = err_name(lambda: work.reserve(timeout=1))
    out(f"reserve-with-timeout 1 on empty: {e}")
    expect("reserve timeout", e, "TimedOutError")

    # --- pause-tube ---------------------------------------------------------
    prod.use(TUBE_B)
    ids["p"] = prod.put("paused", ttr=60)
    out(f"put paused into {TUBE_B}: {job(ids['p'])}")
    prod.pause_tube(TUBE_B, 1)
    out(f"pause-tube {TUBE_B} 1: ok")
    st = prod.stats_tube(TUBE_B)
    dump(f"stats-tube {TUBE_B}", st)
    expect("pause", st["pause"], 1)
    e = err_name(lambda: work.reserve(timeout=0))
    out(f"reserve-with-timeout 0 while paused: {e}")
    expect("paused reserve", e, "TimedOutError")
    j = work.reserve(timeout=3)
    out(f"reserve after pause expiry: {jb(j)}")
    expect("after pause", j.id, ids["p"])
    work.delete(j)
    out(f"delete {job(j)}: ok")
    e = err_name(lambda: prod.pause_tube("no-such-tube", 1))
    out(f"pause-tube missing: {e}")
    expect("pause missing", e, "NotFoundError")

    # --- final stats -----------------------------------------------------------
    st = prod.stats()
    dump("stats", st)
    for k in [
        "current-jobs-ready",
        "current-jobs-reserved",
        "current-jobs-delayed",
        "current-jobs-buried",
    ]:
        expect(k, st[k], 0)
    expect("total-jobs", st["total-jobs"], 7)

    if leave_jobs:
        return leave_jobs_and_hold(addr, prod)
    if kill_point:
        hold_across_kill(addr, prod, work)

    work.close()
    prod.close()
    out("DONE")
    return 0


def leave_jobs_and_hold(addr: tuple[str, int], prod: greenstalk.Client) -> int:
    """Leaves jobs in known journaled states, then holds two reservations
    until stdin is closed (the runner kills the server meanwhile)."""
    prod.use(TUBE_KEEP)
    hold = connect(addr, use=TUBE_KEEP, watch=TUBE_KEEP)

    def put(body: str, pri: int, delay: int = 0) -> int:
        i = prod.put(body, priority=pri, delay=delay, ttr=60)
        out(f"put {body} pri={pri} delay={delay}: {job(i)}")
        return i

    put("keep-ready", 100)
    put("keep-delayed", 200, delay=3600)
    # Put a before b but bury b first: live FIFO order is b, a; after a
    # restart buried jobs are replayed in order of their first record.
    a = put("keep-buried-a", 300)
    b = put("keep-buried-b", 300)
    for i, pri in [(b, 41), (a, 42)]:
        hold.bury(hold.reserve_job(i), priority=pri)
        out(f"reserve-job + bury {job(i)} pri={pri}: ok")
    j = prod.peek_buried()
    out(f"peek-buried: {jb(j)}")
    expect("live buried fifo", j.id, b)
    i = put("keep-kicked", 400)
    hold.bury(hold.reserve_job(i), priority=400)
    prod.kick_job(i)
    out(f"reserve-job + bury + kick-job {job(i)}: ok")
    i = put("keep-released", 500)
    hold.release(hold.reserve_job(i), priority=501, delay=7200)
    out(f"reserve-job + release {job(i)} pri=501 delay=7200: ok")
    # Reserved at crash; the last journaled record is the put.
    i = put("keep-reserved", 600)
    hold.reserve_job(i)
    out(f"reserve-job {job(i)} (held): ok")
    # Reserved at crash; the last journaled record is a release with a 1s
    # delay, which will have expired by the time the server restarts.
    i = put("keep-reserved-expired", 700)
    hold.release(hold.reserve_job(i), priority=701, delay=1)
    hold.reserve_job(i)
    out(f"reserve-job + release delay=1 + reserve-job {job(i)} (held): ok")
    # Deleted: leaves only a delete record, but its id still counts.
    i = put("keep-deleted", 800)
    prod.delete(i)
    out(f"delete {job(i)}: ok")
    dump(f"stats-tube {TUBE_KEEP}", prod.stats_tube(TUBE_KEEP))
    time.sleep(1.2)  # let the 1s release delay expire
    out("HOLDING")
    # Keep both connections (and the reservations) open until the runner
    # has killed the server and closes our stdin. No `quit`: the server is
    # gone by then.
    sys.stdin.read()
    return 0


def hold_across_kill(addr: tuple[str, int], prod: greenstalk.Client, work: greenstalk.Client) -> None:
    """Holds a reservation and a waiting reserve across the point where the
    runner kills the cluster leader (KILL_POINT, until stdin is closed),
    then checks both survived."""
    import threading

    prod.use(TUBE_KILL)
    n = work.watch(TUBE_KILL)
    out(f"worker watch {TUBE_KILL}: {n}")
    held = prod.put("held", priority=5, ttr=120)
    out(f"put held into {TUBE_KILL}: {job(held)}")
    j = work.reserve(timeout=1)
    out(f"reserve: {jb(j)}")
    expect("reserve held", j.id, held)

    # The waiter blocks in reserve-with-timeout on the (now empty) tube,
    # across the kill; its socket timeout must outlast that.
    socket.setdefaulttimeout(90)
    waiter = connect(addr, use=TUBE_KILL, watch=TUBE_KILL)
    socket.setdefaulttimeout(10)
    got: dict[str, Any] = {}

    def wait() -> None:
        try:
            got["job"] = waiter.reserve(timeout=60)
        except Exception as e:  # reported by the main thread
            got["err"] = e

    t = threading.Thread(target=wait)
    t.start()
    # Make sure the reserve is actually waiting before the kill. A fixed
    # pause and one check, not a poll: the number of stats-tube calls
    # shows in cmd-stats-tube.
    time.sleep(0.5)
    st = prod.stats_tube(TUBE_KILL)
    dump(f"stats-tube {TUBE_KILL}", st)
    expect("waiting before the kill", st["current-waiting"], 1)
    out("KILL_POINT")
    sys.stdin.read()
    out("resumed")

    # Same connections, same reservation.
    dump("stats-job held", work.stats_job(held))
    work.touch(j)
    out(f"touch {job(held)}: ok")
    dump(f"stats-tube {TUBE_KILL}", prod.stats_tube(TUBE_KILL))
    work.release(j, priority=6)
    out(f"release {job(held)}: ok")
    t.join(70)
    if "err" in got or "job" not in got:
        raise AssertionError(f"waiter: {got.get('err', 'no job')!r}")
    j = got["job"]
    out(f"waiter reserved: {jb(j)}")
    expect("waiter job", j.id, held)
    dump("stats-job held", waiter.stats_job(held))
    waiter.delete(j)
    out(f"delete {job(j)}: ok")
    waiter.close()
    st = prod.stats()
    dump("stats", st)
    expect("total-jobs", st["total-jobs"], 8)


def after_restart(addr: tuple[str, int]) -> int:
    """Dumps and checks the state recovered from the binlog, then drains it."""
    out("--- after restart ---")
    c = connect(addr, use=TUBE_KEEP, watch=TUBE_KEEP)
    st = c.stats()
    dump("stats", st)
    for k, want in [
        ("current-jobs-ready", 4),
        ("current-jobs-reserved", 0),
        ("current-jobs-delayed", 2),
        ("current-jobs-buried", 2),
        ("total-jobs", 0),
    ]:
        expect(k, st[k], want)
    out(f"list-tubes: {c.tubes()}")
    expect("list-tubes", sorted(c.tubes()), ["default", TUBE_KEEP])
    dump(f"stats-tube {TUBE_KEEP}", c.stats_tube(TUBE_KEEP))

    seen: list[int] = []
    for state, peek, want in [
        ("ready", c.peek_ready, ["keep-ready", "keep-kicked", "keep-reserved", "keep-reserved-expired"]),
        ("buried", c.peek_buried, ["keep-buried-a", "keep-buried-b"]),
        ("delayed", c.peek_delayed, ["keep-delayed", "keep-released"]),
    ]:
        got = []
        while True:
            try:
                j = peek()
            except greenstalk.NotFoundError:
                out(f"peek-{state}: NotFoundError")
                break
            out(f"peek-{state}: {jb(j)}")
            dump(f"stats-job {j.body}", c.stats_job(j))
            c.delete(j)
            out(f"delete {job(j)}: ok")
            got.append(j.body)
            seen.append(j.id)
        expect(f"recovered {state} jobs", got, want)

    # Ids continue after the highest id in any record (the deleted job's).
    i = c.put("after-restart", ttr=60)
    out(f"put after-restart: {job(i)}")
    out(f"next-id minus highest recovered id: {i - max(seen)}")
    expect("next id", i - max(seen), 2)
    c.delete(i)
    out(f"delete {job(i)}: ok")
    st = c.stats()
    dump("stats", st)
    expect("total-jobs", st["total-jobs"], 1)
    c.close()
    out("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
