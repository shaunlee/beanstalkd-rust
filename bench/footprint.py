#!/usr/bin/env python3
"""Footprint benchmarks (P4-T4): memory per job and binlog bytes per
operation, beanstalkd-rs vs the reference beanstalkd. See docs/PLAN.md
section 7.5 and docs/BENCH.md "P4-T4: footprint" for the method and
results.

Two subcommands:

  footprint.py mem --rs-bin PATH --ref-bin PATH --out CSV
      Starts each server fresh (no binlog), puts N jobs of a given body
      size into one tube or spread over 1000 tubes, samples RSS before,
      after the puts and after deleting every job, and repeats. Emits one
      CSV row per (server, body_size, n, tubes, rep).

  footprint.py binlog --rs-bin PATH --ref-bin PATH --out CSV
      Starts each server fresh with `-b <fresh dir>`, runs a deterministic
      churn or mixed workload for a fixed number of cycles, and reads the
      process's own I/O counters (via bench/pidrusage, a tiny helper around
      macOS's proc_pid_rusage) before and after. Emits one CSV row per
      (server, workload, body_size, seg_size).

Both subcommands bind to 127.0.0.1 and use a scratch directory (--scratch)
for binlog directories; only paths under --scratch are ever removed.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

HOST = "127.0.0.1"


class Proto:
    """A raw beanstalkd connection with pipelined batch helpers.

    Every batch method writes all requests in one `send`, then parses
    replies in order: safe because a single connection's commands are
    processed strictly in the order the server reads them (COMPAT: no
    reordering within a connection), and every reply's length is
    self-describing (either one line, or one line plus a byte count).
    """

    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._buf = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def _fill(self):
        chunk = self.sock.recv(1 << 20)
        if not chunk:
            raise ConnectionError("beanstalkd connection closed")
        self._buf += chunk

    def _readline(self) -> bytes:
        while b"\r\n" not in self._buf:
            self._fill()
        line, self._buf = self._buf.split(b"\r\n", 1)
        return line

    def _readexact(self, n: int) -> bytes:
        while len(self._buf) < n:
            self._fill()
        data, self._buf = self._buf[:n], self._buf[n:]
        return data

    def send(self, data: bytes):
        self.sock.sendall(data)

    def use(self, tube: str):
        self.send(f"use {tube}\r\n".encode())
        line = self._readline()
        assert line == f"USING {tube}".encode(), line

    def watch(self, tube: str):
        self.send(f"watch {tube}\r\n".encode())
        line = self._readline()
        assert line.startswith(b"WATCHING"), line

    def stats(self) -> dict:
        self.send(b"stats\r\n")
        line = self._readline()
        assert line.startswith(b"OK "), line
        n = int(line.split()[1])
        body = self._readexact(n + 2)[:-2]
        out = {}
        for raw in body.decode().splitlines():
            if not raw or raw == "---":
                continue
            k, _, v = raw.partition(":")
            out[k.strip()] = v.strip()
        return out

    def batch_put(self, n: int, pri: int, delay: int, ttr: int, body: bytes) -> list[int]:
        header = f"put {pri} {delay} {ttr} {len(body)}\r\n".encode()
        self.send((header + body + b"\r\n") * n)
        ids = []
        for _ in range(n):
            line = self._readline()
            if not line.startswith(b"INSERTED "):
                raise RuntimeError(f"put failed: {line!r}")
            ids.append(int(line.split()[1]))
        return ids

    def batch_reserve(self, n: int) -> list[int]:
        self.send(b"reserve\r\n" * n)
        ids = []
        for _ in range(n):
            line = self._readline()
            if not line.startswith(b"RESERVED "):
                raise RuntimeError(f"reserve failed: {line!r}")
            _, sid, slen = line.split()
            ids.append(int(sid))
            self._readexact(int(slen) + 2)  # body + trailing CRLF
        return ids

    def batch_delete(self, ids: list[int]):
        self.send(b"".join(f"delete {i}\r\n".encode() for i in ids))
        for i in ids:
            line = self._readline()
            if line != b"DELETED":
                raise RuntimeError(f"delete {i} failed: {line!r}")

    def batch_bury(self, ids: list[int], pri: int):
        self.send(b"".join(f"bury {i} {pri}\r\n".encode() for i in ids))
        for i in ids:
            line = self._readline()
            if line != b"BURIED":
                raise RuntimeError(f"bury {i} failed: {line!r}")

    def batch_release(self, ids: list[int], pri: int, delay: int):
        self.send(b"".join(f"release {i} {pri} {delay}\r\n".encode() for i in ids))
        for i in ids:
            line = self._readline()
            if line != b"RELEASED":
                raise RuntimeError(f"release {i} failed: {line!r}")

    def batch_kick_job(self, ids: list[int]):
        self.send(b"".join(f"kick-job {i}\r\n".encode() for i in ids))
        for i in ids:
            line = self._readline()
            if line != b"KICKED":
                raise RuntimeError(f"kick-job {i} failed: {line!r}")


@dataclass
class Server:
    name: str  # "ref" or "rs"
    binary: Path
    port: int
    extra_args: list[str] = field(default_factory=list)
    proc: subprocess.Popen | None = None

    def start(self, log_path: Path):
        args = [str(self.binary), "-l", HOST, "-p", str(self.port), *self.extra_args]
        log = open(log_path, "wb")
        self.proc = subprocess.Popen(
            args, stdout=log, stderr=subprocess.STDOUT, cwd="/"
        )
        self._wait_ready()

    def _wait_ready(self, timeout: float = 15.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"{self.name} exited early (status {self.proc.returncode})"
                )
            try:
                s = socket.create_connection((HOST, self.port), timeout=0.2)
                s.close()
                return
            except OSError:
                time.sleep(0.05)
        raise TimeoutError(f"{self.name} did not open {HOST}:{self.port} in time")

    def stop(self):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self.proc = None

    @property
    def pid(self) -> int:
        assert self.proc is not None
        return self.proc.pid


_used_ports: set[int] = set()


def free_port() -> int:
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((HOST, 0))
        port = s.getsockname()[1]
        s.close()
        if port not in _used_ports:
            _used_ports.add(port)
            return port


def rss_kib(pid: int) -> float | None:
    try:
        out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
    except subprocess.CalledProcessError:
        return None
    out = out.strip()
    return float(out) if out else None


def settle_rss(pid: int, tries: int = 12, interval: float = 0.3) -> float:
    """Polls RSS until two consecutive reads differ by <1%, or gives up."""
    prev = rss_kib(pid)
    for _ in range(tries):
        time.sleep(interval)
        cur = rss_kib(pid)
        if prev is not None and cur is not None and prev > 0:
            if abs(cur - prev) / prev < 0.01:
                return cur
        prev = cur
    return prev if prev is not None else 0.0


def loadavg1() -> float:
    try:
        return float(os.getloadavg()[0])
    except OSError:
        return -1.0


def pidrusage(helper: Path, pid: int) -> dict:
    out = subprocess.check_output([str(helper), str(pid)], text=True)
    d = {}
    for kv in out.split():
        k, v = kv.split("=")
        d[k] = int(v)
    return d


def random_body(pool: bytes, size: int, rng: random.Random) -> bytes:
    """A slice of a pre-generated random pool, so bodies are incompressible
    (unlike `b'x' * n`, which the allocator or a compressed page could
    fold to nothing and understate RSS)."""
    if size >= len(pool):
        return pool[:size].ljust(size, b"\0")
    off = rng.randrange(0, len(pool) - size)
    return pool[off : off + size]


def load_jobs(port: int, n: int, tubes: int, body: bytes, conns: int) -> None:
    """Puts n jobs (body content varies per job so RSS isn't understated by
    a single shared allocation), split evenly over `tubes` tubes (1 tube
    means every job goes to the same one), using `conns` connections in
    parallel via native threads (Python's GIL is not a bottleneck here:
    the connections spend almost all their time in blocking socket
    syscalls)."""
    import threading

    conns = max(1, min(conns, tubes if tubes > 1 else conns))
    errors: list[Exception] = []

    def worker(idx: int):
        try:
            p = Proto(HOST, port)
            batch = 2000
            rng = random.Random(idx)
            pool = os.urandom(1 << 20)
            # tubes == 1: one shared tube, so the split is across
            # connections. tubes > 1: the split is across tubes instead
            # (each worker owns a disjoint slice of them), because the
            # obvious alternative -- round-robin one batch per tube -- only
            # ever reaches as many tubes as there are batches, which is far
            # fewer than `tubes` once batches are larger than tubes/conns.
            if tubes == 1:
                jobs_by_tube = [(0, n // conns + (1 if idx < n % conns else 0))]
            else:
                jobs_by_tube = [
                    (t, n // tubes + (1 if t < n % tubes else 0))
                    for t in range(idx, tubes, conns)
                ]
            for tube_idx, remaining in jobs_by_tube:
                if tubes > 1:
                    p.use(f"footprint-mem-{tube_idx}")
                while remaining > 0:
                    this = min(batch, remaining)
                    # A fixed body would let the allocator/compressor
                    # collapse identical pages; vary the content per job.
                    bodies = [random_body(pool, len(body), rng) for _ in range(this)]
                    out = b"".join(
                        f"put 1024 0 60 {len(b)}\r\n".encode() + b + b"\r\n" for b in bodies
                    )
                    p.send(out)
                    for _ in range(this):
                        line = p._readline()
                        if not line.startswith(b"INSERTED "):
                            raise RuntimeError(f"put failed: {line!r}")
                    remaining -= this
            p.close()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(conns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


def delete_all(port: int, n: int, conns: int) -> None:
    import threading

    errors: list[Exception] = []

    def worker(idx: int):
        try:
            p = Proto(HOST, port)
            lo = 1 + idx * (n // conns) + min(idx, n % conns)
            share = n // conns + (1 if idx < n % conns else 0)
            ids = list(range(lo, lo + share))
            batch = 4000
            for i in range(0, len(ids), batch):
                p.batch_delete(ids[i : i + batch])
            p.close()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(conns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


def run_mem(args):
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not out_path.exists()
    f = open(out_path, "a", newline="")
    w = csv.writer(f)
    if new_file:
        w.writerow(
            [
                "ts",
                "server",
                "body_size",
                "n",
                "tubes",
                "rep",
                "rss_baseline_kib",
                "rss_loaded_kib",
                "rss_after_delete_kib",
                "bytes_per_job",
                "loadavg1",
            ]
        )
    f.flush()

    servers = [("ref", Path(args.ref_bin)), ("rs", Path(args.rs_bin))]
    log_dir = Path(args.scratch) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    for rep in range(1, args.reps + 1):
        for body_size in args.bodies:
            for n in args.ns:
                for tubes in args.tubes_variants:
                    for name, binary in servers:
                        port = free_port()
                        srv = Server(name, binary, port)
                        log_path = log_dir / f"mem-{name}-{body_size}-{n}-{tubes}-{rep}.log"
                        srv.start(log_path)
                        try:
                            baseline = settle_rss(srv.pid)
                            body = os.urandom(body_size)
                            conns = min(16, max(1, tubes if tubes > 1 else 8))
                            load_jobs(port, n, tubes, body, conns)
                            check = Proto(HOST, port)
                            st = check.stats()
                            check.close()
                            got = int(st.get("current-jobs-ready", -1))
                            if got != n:
                                raise RuntimeError(
                                    f"{name}: expected {n} ready jobs, stats says {got}"
                                )
                            want_tubes = tubes + 1 if tubes > 1 else 1  # +1: "default"
                            got_tubes = int(st.get("current-tubes", -1))
                            if got_tubes != want_tubes:
                                raise RuntimeError(
                                    f"{name}: expected {want_tubes} tubes, stats says {got_tubes}"
                                )
                            loaded = settle_rss(srv.pid)
                            delete_all(port, n, conns)
                            check = Proto(HOST, port)
                            st = check.stats()
                            check.close()
                            got = int(st.get("current-jobs-ready", -1))
                            if got != 0:
                                raise RuntimeError(
                                    f"{name}: expected 0 ready jobs after delete, stats says {got}"
                                )
                            after_delete = settle_rss(srv.pid)
                        finally:
                            srv.stop()
                        bpj = (loaded - baseline) * 1024.0 / n
                        w.writerow(
                            [
                                time.strftime("%Y-%m-%dT%H:%M:%S"),
                                name,
                                body_size,
                                n,
                                tubes,
                                rep,
                                f"{baseline:.0f}",
                                f"{loaded:.0f}",
                                f"{after_delete:.0f}",
                                f"{bpj:.2f}",
                                loadavg1(),
                            ]
                        )
                        f.flush()
                        print(
                            f"[mem] rep={rep} server={name} body={body_size} n={n} "
                            f"tubes={tubes} baseline={baseline:.0f}KiB loaded={loaded:.0f}KiB "
                            f"after_delete={after_delete:.0f}KiB bytes/job={bpj:.1f}",
                            file=sys.stderr,
                        )
    f.close()


def run_binlog_workload(port: int, cycles: int, body: bytes, workload: str, bury_forever_every: int):
    """Runs `cycles` deterministic put/reserve/... cycles on one connection
    against a freshly started, freshly binlogged server, using a single
    tube so job ids are exactly 1..cycles in put order (a fresh server
    always starts numbering at 1). One job per `bury_forever_every` cycles
    is put and buried but never revisited, so the binlog keeps some live
    records across many segment rotations -- otherwise a small `-s` only
    exercises file turnover, never an actual compaction move of live data
    forward (COMPAT "Binlog" item 7).

    Journaled records per fully-cycled job:
      churn: put, delete                                   (2)
      mixed: put, bury, kick, release(delay>0), kick, delete  (6)
    A "buried forever" job only ever gets: put, bury (2).
    """
    p = Proto(HOST, port)
    tube = "wal-bench"
    p.use(tube)
    p.watch(tube)

    chunk = 200
    done = 0
    while done < cycles:
        this = min(chunk, cycles - done)
        ids = p.batch_put(this, 1024, 0, 60, body)
        keep_buried = set()
        if bury_forever_every > 0:
            for i, jid in enumerate(ids):
                if (done + i) % bury_forever_every == bury_forever_every - 1:
                    keep_buried.add(jid)
        if workload == "churn":
            p.batch_reserve(this)
            p.batch_delete([i for i in ids if i not in keep_buried])
            if keep_buried:
                p.batch_bury(list(keep_buried), 1024)
        elif workload == "mixed":
            p.batch_reserve(this)
            p.batch_bury(ids, 1024)
            revive1 = [i for i in ids if i not in keep_buried]
            if revive1:
                p.batch_kick_job(revive1)
                p.batch_reserve(len(revive1))
                p.batch_release(revive1, 1024, 1)
                p.batch_kick_job(revive1)
                p.batch_reserve(len(revive1))
                p.batch_delete(revive1)
        else:
            raise ValueError(workload)
        done += this
    p.close()


def run_binlog(args):
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not out_path.exists()
    f = open(out_path, "a", newline="")
    w = csv.writer(f)
    if new_file:
        w.writerow(
            [
                "ts",
                "server",
                "workload",
                "body_size",
                "seg_size",
                "cycles",
                "records_written_stats",
                "logical_writes_bytes",
                "diskio_written_bytes",
                "bytes_per_op_logical",
                "bytes_per_op_diskio",
                "bytes_per_record_logical",
                "loadavg1",
            ]
        )
    f.flush()

    servers = [("ref", Path(args.ref_bin)), ("rs", Path(args.rs_bin))]
    log_dir = Path(args.scratch) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    helper = Path(args.pidrusage)

    for workload in args.workloads:
        for body_size in args.bodies:
            for seg_size in args.seg_sizes:
                for name, binary in servers:
                    port = free_port()
                    bl_dir = Path(args.scratch) / f"bl-{name}-{workload}-{body_size}-{seg_size}"
                    if bl_dir.exists():
                        import shutil

                        shutil.rmtree(bl_dir)
                    bl_dir.mkdir(parents=True)
                    srv = Server(
                        name, binary, port, extra_args=["-b", str(bl_dir), "-s", str(seg_size)]
                    )
                    log_path = log_dir / f"bl-{name}-{workload}-{body_size}-{seg_size}.log"
                    srv.start(log_path)
                    try:
                        time.sleep(0.2)
                        before = pidrusage(helper, srv.pid)
                        body = os.urandom(body_size)
                        run_binlog_workload(
                            port, args.cycles, body, workload, args.bury_forever_every
                        )
                        # Let any deferred writeback happen so diskio_written
                        # (a cross-check only) catches up; logical_writes is
                        # already exact right after the syscalls return.
                        time.sleep(1.0)
                        after = pidrusage(helper, srv.pid)
                        check = Proto(HOST, port)
                        st = check.stats()
                        check.close()
                        records_written = int(st.get("binlog-records-written", -1))
                    finally:
                        srv.stop()
                    dlogical = after["logical_writes"] - before["logical_writes"]
                    ddisk = after["diskio_written"] - before["diskio_written"]
                    bpo_l = dlogical / args.cycles
                    bpo_d = ddisk / args.cycles
                    bpr_l = dlogical / records_written if records_written > 0 else float("nan")
                    w.writerow(
                        [
                            time.strftime("%Y-%m-%dT%H:%M:%S"),
                            name,
                            workload,
                            body_size,
                            seg_size,
                            args.cycles,
                            records_written,
                            dlogical,
                            ddisk,
                            f"{bpo_l:.2f}",
                            f"{bpo_d:.2f}",
                            f"{bpr_l:.2f}",
                            loadavg1(),
                        ]
                    )
                    f.flush()
                    print(
                        f"[binlog] server={name} workload={workload} body={body_size} "
                        f"seg={seg_size} records={records_written} "
                        f"logical={dlogical} diskio={ddisk} bytes/op(logical)={bpo_l:.1f}",
                        file=sys.stderr,
                    )
    f.close()


def median(xs: list[float]) -> float:
    xs = sorted(xs)
    n = len(xs)
    mid = n // 2
    return xs[mid] if n % 2 else (xs[mid - 1] + xs[mid]) / 2


def run_summarize_mem(args):
    rows = list(csv.DictReader(open(args.csv)))
    keys = sorted(
        {(r["body_size"], r["n"], r["tubes"]) for r in rows},
        key=lambda k: (int(k[0]), int(k[1]), int(k[2])),
    )
    print("| body | n | tubes | ref bytes/job | rs bytes/job | ratio | ref after-delete | rs after-delete |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for body, n, tubes in keys:
        by_server = {}
        after_delete = {}
        for server in ("ref", "rs"):
            vals = [
                float(r["bytes_per_job"])
                for r in rows
                if r["body_size"] == body
                and r["n"] == n
                and r["tubes"] == tubes
                and r["server"] == server
            ]
            ad = [
                float(r["rss_after_delete_kib"])
                for r in rows
                if r["body_size"] == body
                and r["n"] == n
                and r["tubes"] == tubes
                and r["server"] == server
            ]
            by_server[server] = median(vals) if vals else float("nan")
            after_delete[server] = median(ad) if ad else float("nan")
        ratio = by_server["rs"] / by_server["ref"] if by_server["ref"] else float("nan")
        print(
            f"| {body} | {n} | {tubes} | {by_server['ref']:.1f} | {by_server['rs']:.1f} | "
            f"{ratio:.2f} | {after_delete['ref']:.0f} KiB | {after_delete['rs']:.0f} KiB |"
        )


def run_summarize_binlog(args):
    rows = list(csv.DictReader(open(args.csv)))
    keys = sorted(
        {(r["workload"], r["body_size"], r["seg_size"]) for r in rows},
        key=lambda k: (k[0], int(k[1]), int(k[2])),
    )
    print("| workload | body | seg size | ref bytes/op | rs bytes/op | ratio | ref records | rs records |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for workload, body, seg in keys:
        by_server = {}
        records = {}
        for server in ("ref", "rs"):
            vals = [
                float(r["bytes_per_op_logical"])
                for r in rows
                if r["workload"] == workload
                and r["body_size"] == body
                and r["seg_size"] == seg
                and r["server"] == server
            ]
            recs = [
                int(r["records_written_stats"])
                for r in rows
                if r["workload"] == workload
                and r["body_size"] == body
                and r["seg_size"] == seg
                and r["server"] == server
            ]
            by_server[server] = median(vals) if vals else float("nan")
            records[server] = median(recs) if recs else float("nan")
        ratio = by_server["rs"] / by_server["ref"] if by_server["ref"] else float("nan")
        print(
            f"| {workload} | {body} | {seg} | {by_server['ref']:.1f} | {by_server['rs']:.1f} | "
            f"{ratio:.2f} | {records['ref']:.0f} | {records['rs']:.0f} |"
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--rs-bin", required=True)
    common.add_argument("--ref-bin", required=True)
    common.add_argument("--scratch", required=True, help="scratch dir for binlog dirs/logs")
    common.add_argument("--out", required=True, help="CSV output path (appended)")

    m = sub.add_parser("mem", parents=[common])
    m.add_argument("--bodies", type=int, nargs="+", default=[16, 1024])
    m.add_argument("--ns", type=int, nargs="+", default=[100_000, 1_000_000])
    m.add_argument("--tubes-variants", type=int, nargs="+", default=[1, 1000])
    m.add_argument("--reps", type=int, default=3)
    m.set_defaults(func=run_mem)

    b = sub.add_parser("binlog", parents=[common])
    b.add_argument("--pidrusage", required=True, help="path to the compiled pidrusage helper")
    b.add_argument("--workloads", nargs="+", default=["churn", "mixed"])
    b.add_argument("--bodies", type=int, nargs="+", default=[16, 1024])
    b.add_argument("--seg-sizes", type=int, nargs="+", default=[10 << 20, 64 << 10])
    b.add_argument("--cycles", type=int, default=200_000)
    b.add_argument(
        "--bury-forever-every",
        type=int,
        default=50,
        help="1/N jobs is buried and never revisited, to keep live records across segments (0 disables)",
    )
    b.set_defaults(func=run_binlog)

    sm = sub.add_parser("summarize-mem", help="print a medians/ratio markdown table from a mem CSV")
    sm.add_argument("--csv", required=True)
    sm.set_defaults(func=run_summarize_mem, no_scratch=True)

    sb = sub.add_parser("summarize-binlog", help="print a medians/ratio markdown table from a binlog CSV")
    sb.add_argument("--csv", required=True)
    sb.set_defaults(func=run_summarize_binlog, no_scratch=True)

    args = ap.parse_args()
    if not getattr(args, "no_scratch", False):
        Path(args.scratch).mkdir(parents=True, exist_ok=True)
    args.func(args)


if __name__ == "__main__":
    main()
