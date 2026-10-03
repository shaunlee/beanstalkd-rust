#!/usr/bin/env python3
"""Snapshot memory benchmark (P4-T5c): peak memory of cluster nodes while
they build, send, receive / install and restart from a Raft snapshot.
See docs/BENCH.md "P4-T5c" for the method and results.

  snapshot_mem.py --bin PATH --scratch DIR [--small N] [--large N] [--reps R]

Per repetition: starts a fresh 3-node cluster on loopback (plaintext,
`snapshot_every` above the entries the load creates), puts `--small`
16-byte jobs and `--large` 1 KiB jobs through the leader, then
 (a) sends single commands until every node has built its first snapshot,
 (b)+(c) kills a follower, wipes its data directory and restarts it, so
     the leader streams it the snapshot and it installs it,
 (d) kills the other follower and restarts it from its own snapshot.
A sampler thread reads each node's physical footprint (macOS
`proc_pid_rusage`, which counts compressed pages, unlike `ps` RSS) every
few milliseconds; for processes started during a phase the kernel's
lifetime maximum is used too. "State size" is the postcard payload of the
snapshot (the `payload_len` header field of the leader's `.snap` file).

Only paths under --scratch are removed.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from footprint import Proto  # noqa: E402

HOST = "127.0.0.1"
# macOS malloc keeps freed large blocks dirty for reuse ("Malloc Large
# (empty)" in vmmap), which hides the next operation's peak inside the
# previous one's leftovers; `MallocLargeCache=0` turns that off so each
# phase's peak is its own (--large-cache keeps the default, which shows
# what a process retains after a snapshot).
LARGE_CACHE = False
MIB = 1 << 20

_libproc = ctypes.CDLL("/usr/lib/libproc.dylib")


class RusageV4(ctypes.Structure):
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [
        (n, ctypes.c_uint64)
        for n in (
            "user_time system_time pkg_idle_wkups interrupt_wkups pageins wired_size "
            "resident_size phys_footprint proc_start_abstime proc_exit_abstime "
            "child_user_time child_system_time child_pkg_idle_wkups child_interrupt_wkups "
            "child_pageins child_elapsed_abstime diskio_bytesread diskio_byteswritten "
            "qos_default qos_maintenance qos_background qos_utility qos_legacy "
            "qos_user_initiated qos_user_interactive billed_system_time serviced_system_time "
            "logical_writes lifetime_max_phys_footprint instructions cycles billed_energy "
            "serviced_energy interval_max_phys_footprint runnable_time"
        ).split()
    ]


def rusage(pid: int) -> RusageV4 | None:
    ri = RusageV4()
    if _libproc.proc_pid_rusage(pid, 4, ctypes.byref(ri)) != 0:
        return None
    return ri


def free_port() -> int:
    s = socket.socket()
    s.bind((HOST, 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Sampler:
    """Polls the footprint of a set of pids; `window()` returns the peak per
    pid since the last call."""

    def __init__(self, period: float = 0.003):
        self.period = period
        self.pids: dict[str, int] = {}
        self.peak: dict[str, int] = {}
        self.lock = threading.Lock()
        self.stop = False
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()

    def _run(self):
        while not self.stop:
            with self.lock:
                for name, pid in list(self.pids.items()):
                    ri = rusage(pid)
                    if ri is not None:
                        self.peak[name] = max(self.peak.get(name, 0), ri.phys_footprint)
            time.sleep(self.period)

    def track(self, name: str, pid: int | None):
        with self.lock:
            if pid is None:
                self.pids.pop(name, None)
            else:
                self.pids[name] = pid
            self.peak.pop(name, None)

    def window(self) -> dict[str, int]:
        with self.lock:
            out = dict(self.peak)
            self.peak = {}
        return out


class Node:
    def __init__(self, nid: int, root: Path, binary: Path):
        self.id = nid
        self.binary = binary
        self.client = free_port()
        self.http = free_port()
        self.cluster = free_port()
        self.data = root / f"data{nid}"
        self.config = root / f"node{nid}.toml"
        self.log = root / f"node{nid}.log"
        self.proc: subprocess.Popen | None = None

    def write_config(self, nodes: list["Node"], snapshot_every: int):
        peers = "".join(
            f'[[cluster.peer]]\nid = {n.id}\naddr = "{HOST}:{n.cluster}"\n' for n in nodes
        )
        self.config.write_text(
            f'[[listener]]\naddr = "{HOST}:{self.client}"\n'
            f'[http]\naddr = "{HOST}:{self.http}"\n'
            f'[log]\nlevel = "info"\n'
            f"[cluster]\nnode_id = {self.id}\nlisten = \"{HOST}:{self.cluster}\"\n"
            f'data_dir = "{self.data}"\nsnapshot_every = {snapshot_every}\n'
            f"insecure_plaintext = true\n\n{peers}"
        )

    def start(self, init: bool):
        args = [str(self.binary), "--config", str(self.config)]
        if init:
            args.append("--cluster-init")
        log = open(self.log, "ab")
        env = dict(os.environ)
        if not LARGE_CACHE:
            env["MallocLargeCache"] = "0"
        self.proc = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, cwd="/", env=env)

    def kill(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    def admin(self) -> dict | None:
        try:
            with urllib.request.urlopen(f"http://{HOST}:{self.http}/admin", timeout=2) as r:
                return json.loads(r.read())
        except Exception:
            return None

    def cl(self) -> dict:
        """The `cluster` section of `/admin`, or {} while unavailable."""
        return (self.admin() or {}).get("cluster", {})

    def footprint(self) -> int:
        ri = rusage(self.proc.pid)
        return ri.phys_footprint if ri else 0

    def lifetime_max(self) -> int:
        ri = rusage(self.proc.pid)
        return ri.lifetime_max_phys_footprint if ri else 0


def wait_for(pred, timeout: float, what: str, period: float = 0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(period)
    raise TimeoutError(what)


def settle(node: Node, tol: float = 0.01, gap: float = 0.3, timeout: float = 20) -> int:
    """Footprint once two reads `gap` apart agree within `tol`."""
    prev = node.footprint()
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(gap)
        cur = node.footprint()
        if abs(cur - prev) <= tol * max(prev, 1):
            return cur
        prev = cur
    return prev


def payload_len(data_dir: Path) -> int:
    snaps = sorted((data_dir / "snapshot").glob("*.snap"))
    with open(snaps[-1], "rb") as f:
        hdr = f.read(32)
    return struct.unpack_from("<Q", hdr, 24)[0]


def load(port: int, n: int, size: int, rng: random.Random, conns: int = 32):
    """Puts `n` jobs over `conns` parallel connections (one connection
    gets one log entry per command; parallel ones share entries)."""
    seeds = [rng.random() for _ in range(conns)]
    parts = [n // conns + (1 if i < n % conns else 0) for i in range(conns)]
    errs = []

    def one(k, seed):
        try:
            load_one(port, k, size, random.Random(seed))
        except Exception as e:  # noqa: BLE001
            errs.append(e)

    ts = [threading.Thread(target=one, args=(k, s)) for k, s in zip(parts, seeds)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    if errs:
        raise errs[0]


def load_one(port: int, n: int, size: int, rng: random.Random):
    p = Proto(HOST, port, timeout=120)
    pool = rng.randbytes(size * 4096 + 1)
    batch = 500
    done = 0
    while done < n:
        k = min(batch, n - done)
        header = f"put 0 0 120 {size}\r\n".encode()
        buf = bytearray()
        for i in range(k):
            off = rng.randrange(0, len(pool) - size)
            buf += header + pool[off : off + size] + b"\r\n"
        p.send(bytes(buf))
        for _ in range(k):
            line = p._readline()
            if not line.startswith(b"INSERTED "):
                raise RuntimeError(f"put failed: {line!r}")
        done += k
    p.close()


def mib(b: int) -> float:
    return round(b / MIB, 1)


def run_rep(args, rep: int) -> dict:
    root = Path(args.scratch) / f"{Path(args.bin).name}-rep{rep}"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    nodes = [Node(i, root, Path(args.bin)) for i in (1, 2, 3)]
    for n in nodes:
        n.write_config(nodes, args.snapshot_every)
    sampler = Sampler()
    res: dict = {"bin": Path(args.bin).name, "rep": rep, "large_cache": LARGE_CACHE}
    try:
        for n in nodes:
            n.start(init=True)

        def leader():
            for n in nodes:
                a = n.admin()
                if a and a["cluster"]["role"] == "leader" and a["cluster"]["ready"]:
                    return n
            return None

        lead = wait_for(leader, 30, "no leader")
        wait_for(lambda: all(n.cl().get("ready") for n in nodes), 30, "not ready")
        empty = {n.id: settle(n) for n in nodes}
        rng = random.Random(rep)
        t0 = time.time()
        load(lead.client, args.small, 16, rng)
        load(lead.client, args.large, 1024, rng)
        res["load_s"] = round(time.time() - t0, 1)
        applied = wait_for(
            lambda: (lambda a: a and a["cluster"]["applied_index"])(lead.admin()), 10, "applied"
        )
        if any(n.cl().get("snapshot_index") is not None for n in nodes):
            raise RuntimeError("a snapshot was taken during the load; raise --snapshot-every")
        # Restart every node with a snapshot threshold just above the log,
        # so (a) starts from a settled process holding only the state.
        snapshot_every = applied + args.trigger
        for n in nodes:
            n.kill()
            n.write_config(nodes, snapshot_every)
            n.start(init=False)
        lead = wait_for(leader, 120, "no leader after restart")
        applied = wait_for(
            lambda: (lambda a: a and a["cluster"]["applied_index"])(lead.admin()), 10, "applied"
        )
        wait_for(
            lambda: all((n.cl().get("applied_index") or 0) >= applied for n in nodes),
            30,
            "followers behind",
        )
        base = {n.id: settle(n) for n in nodes}
        res["live_state_mib"] = mib(base[lead.id] - empty[lead.id])
        res["base_mib"] = mib(base[lead.id])
        res["entries_after_load"] = applied

        # (a) build: single commands until every node has a snapshot.
        p = Proto(HOST, lead.client, timeout=30)
        sampler.window()
        for n in nodes:
            sampler.track(f"n{n.id}", n.proc.pid)
        need = snapshot_every - applied + 5
        t0 = time.time()
        for _ in range(max(need, 0)):
            p.use("default")
        t_cmds = time.time()
        wait_for(
            lambda: all(n.cl().get("snapshot_index") is not None for n in nodes),
            120,
            "no snapshot",
            period=0.005,
        )
        res["build_s"] = round(time.time() - t_cmds, 2)
        time.sleep(0.5)
        peaks = sampler.window()
        res["state_mib"] = mib(payload_len(lead.data))
        res["build_extra_mib"] = max(mib(peaks[f"n{n.id}"] - base[n.id]) for n in nodes)
        # Lock and build times from the nodes' own log line (P4-T5c and
        # later; earlier builds do not log them).
        locked, total = [], []
        for n in nodes:
            for line in n.log.read_text(errors="replace").splitlines():
                m = re.search(r"built snapshot .*locked for ([0-9.]+) ms, ([0-9.]+) ms in all", line)
                if m:
                    locked.append(float(m.group(1)))
                    total.append(float(m.group(2)))
        if locked:
            res["lock_ms"] = max(locked)
            res["build_ms"] = max(total)
        after = {n.id: settle(n) for n in nodes}
        res["after_build_mib"] = max(mib(after[n.id] - base[n.id]) for n in nodes)

        # (b)+(c): wipe a follower; the leader streams the snapshot.
        fol = [n for n in nodes if n is not lead]
        wipe, restart = fol[0], fol[1]
        wipe.kill()
        shutil.rmtree(wipe.data)
        lead_base = settle(lead)
        sampler.track("lead", lead.proc.pid)
        sampler.window()
        t0 = time.time()
        wipe.start(init=False)
        sampler.track("wiped", wipe.proc.pid)
        target = lead.admin()["cluster"]["snapshot_index"]
        wait_for(
            lambda: (lambda a: a and a["cluster"]["ready"] and (a["cluster"]["applied_index"] or 0) >= target)(
                wipe.admin()
            ),
            300,
            "wiped node never caught up",
            period=0.005,
        )
        res["install_s"] = round(time.time() - t0, 2)
        time.sleep(0.5)
        peaks = sampler.window()
        res["send_extra_mib"] = mib(peaks["lead"] - lead_base)
        final = settle(wipe)
        wpeak = max(peaks.get("wiped", 0), wipe.lifetime_max())
        res["install_peak_mib"] = mib(wpeak)
        res["install_final_mib"] = mib(final)
        # The allocator keeps freed pages, so the settled footprint after
        # the install is no reference; the node's own footprint holding
        # the same state before the wipe is.
        res["install_extra_mib"] = mib(wpeak - base[wipe.id])
        sampler.track("wiped", None)
        sampler.track("lead", None)

        # (d): restart the other follower from its own snapshot.
        before = settle(restart)
        restart.kill()
        t0 = time.time()
        restart.start(init=False)
        sampler.track("restart", restart.proc.pid)
        wait_for(
            lambda: (lambda a: a and a["cluster"]["ready"])(restart.admin()),
            300,
            "restarted node never ready",
            period=0.005,
        )
        res["restart_s"] = round(time.time() - t0, 2)
        time.sleep(0.5)
        final = settle(restart)
        rpeak = max(sampler.window().get("restart", 0), restart.lifetime_max())
        res["restart_before_mib"] = mib(before)
        res["restart_peak_mib"] = mib(rpeak)
        res["restart_final_mib"] = mib(final)
        res["restart_extra_mib"] = mib(rpeak - base[restart.id])
    finally:
        sampler.stop = True
        for n in nodes:
            n.kill()
    if not args.keep:
        shutil.rmtree(root)
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True)
    ap.add_argument("--scratch", required=True)
    ap.add_argument("--small", type=int, default=1_000_000)
    ap.add_argument("--large", type=int, default=100_000)
    ap.add_argument("--snapshot-every", type=int, default=1_000_000, help="during the load")
    ap.add_argument("--trigger", type=int, default=2_000, help="entries after the load until the snapshot")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--large-cache", action="store_true", help="keep macOS malloc's large-block cache")
    args = ap.parse_args()
    global LARGE_CACHE
    LARGE_CACHE = args.large_cache
    if not os.path.isabs(args.scratch):
        sys.exit("--scratch must be absolute")
    for rep in range(1, args.reps + 1):
        print(json.dumps(run_rep(args, rep)), flush=True)


if __name__ == "__main__":
    main()
