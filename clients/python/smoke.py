#!/usr/bin/env python3
"""Real-client smoke test using the greenstalk client library.

Usage: smoke.py HOST:PORT

Runs a full protocol flow against a (fresh) beanstalkd-compatible server,
asserting on every result, and prints a transcript to stdout. The raw
transcript contains volatile values (pids, job ids, uptime, ...); the
runner (clients/run-smoke.sh) pipes it through clients/normalize.py
before diffing the reference against beanstalkd-rs.

Transcript line formats (consumed by normalize.py):
    <step>: <result>                    one line per client call
    [<scope>] <key>: <value>            one line per stats field, in the
                                        order the server sent them
Job ids are always printed as `job=<n>` so they can be normalized.
"""

from __future__ import annotations

import socket
import sys
import time
from typing import Any, Callable

import greenstalk

# A hung server must fail the run, not hang it.
socket.setdefaulttimeout(10)

TUBE_A = "smoke-a"
TUBE_B = "smoke-b"
TUBE_C = "smoke-c"


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
    if len(sys.argv) != 2 or ":" not in sys.argv[1]:
        print(__doc__, file=sys.stderr)
        return 2
    host, port_s = sys.argv[1].rsplit(":", 1)
    addr = (host, int(port_s))

    # --- tubes: use / watch / ignore / list -----------------------------
    prod = greenstalk.Client(addr, use=TUBE_A, watch=TUBE_A)
    work = greenstalk.Client(addr, use="default", watch=[TUBE_A, TUBE_B])
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

    work.close()
    prod.close()
    out("DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
