#!/usr/bin/env python3
"""Rolling restart and membership change of a 3-node cluster under load.

Standard library only. Runs the release binary and the real
`beanstalkd-rs cluster` commands, following docs/OPERATIONS.md sections 5.3,
5.8, 5.9 and 7.3 step by step:

  1. bootstrap three TLS nodes on loopback (scripts/mkcluster-certs.sh);
  2. start a producer / consumer load: every producer puts uniquely numbered
     jobs, every consumer reserves and deletes them, both reconnect to
     another node whenever a connection closes;
  3. restart every node in turn (followers first, the leader last), each
     time waiting for /readyz and for `cluster status` to show every member
     reachable and none rejoining;
  4. under the same load: add node 4 as a learner, start it, wait until it is
     ready, promote it, remove node 1 and stop it;
  5. verify the final membership, stop the producers, let the consumers drain
     the tube, and check the history.

Load checker (at-least-once, docs/COMPAT.md and DESIGN.md: a job whose reply
was lost may be committed anyway, and a reservation whose connection closed
is redelivered):

  - every acknowledged put (INSERTED) was deleted: confirmed (DELETED), or by
    a delete whose reply was lost, and the tube is empty at the end;
  - no body was confirmed deleted twice (that would be a duplicated put);
  - no deleted body was unknown (neither acknowledged nor a put whose reply
    was lost);
  - redeliveries (a body reserved more than once) and puts committed without
    a reply are counted and reported, not failures.

  rolling-restart.py [--bin PATH] [--work DIR] [--base-port N] [--load-secs N]
                     [--no-tls] [--keep]

Exit status 0 when every check passed. The work directory (default: a new
temporary directory) holds the configs, data and logs; it is removed on
success unless --keep is given.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TUBE = "rr-load"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Cluster:
    def __init__(self, binary, work, base, tls):
        self.bin = binary
        self.work = work
        self.base = base
        self.tls = tls
        self.procs = {}

    def client_port(self, i):
        return self.base + 100 + i

    def cluster_port(self, i):
        return self.base + 200 + i

    def http_port(self, i):
        return self.base + 300 + i

    def node_config(self, i, seeds):
        lines = [
            "[[listener]]",
            f'addr = "127.0.0.1:{self.client_port(i)}"',
            "",
            "[cluster]",
            f"node_id = {i}",
            f'listen = "127.0.0.1:{self.cluster_port(i)}"',
            f'data_dir = "node{i}"',
            "",
        ]
        if self.tls:
            lines += [
                "[cluster.tls]",
                f'cert = "tls/node{i}.pem"',
                f'key = "tls/node{i}.key"',
                'ca = "tls/cluster-ca.pem"',
                "",
            ]
        else:
            lines += ["insecure_plaintext = true", ""]
        lines += ["[http]", f'addr = "127.0.0.1:{self.http_port(i)}"', "", "[log]", 'level = "info"', ""]
        for p in seeds:
            lines += ["[[cluster.peer]]", f"id = {p}", f'addr = "127.0.0.1:{self.cluster_port(p)}"', ""]
        with open(os.path.join(self.work, f"node{i}.toml"), "w") as f:
            f.write("\n".join(lines))

    def start(self, i, *args):
        cfg = os.path.join(self.work, f"node{i}.toml")
        out = open(os.path.join(self.work, f"node{i}.log"), "ab")
        self.procs[i] = subprocess.Popen(
            [self.bin, "--config", cfg, *args], stdout=out, stderr=out, cwd=self.work
        )

    def stop(self, i):
        p = self.procs.pop(i)
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
            raise RuntimeError(f"node {i} did not stop on SIGTERM")

    def kill_all(self):
        for p in self.procs.values():
            p.kill()
            p.wait()
        self.procs.clear()

    def ready(self, i):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.http_port(i)}/readyz", timeout=2):
                return True
        except Exception:
            return False

    def wait_ready(self, i, timeout=60):
        end = time.monotonic() + timeout
        while not self.ready(i):
            if self.procs[i].poll() is not None:
                raise RuntimeError(f"node {i} exited with {self.procs[i].returncode}")
            if time.monotonic() > end:
                raise RuntimeError(f"node {i} not ready after {timeout}s")
            time.sleep(0.2)

    def bcl(self, *args, nodes=(1, 2, 3), check=True):
        """`beanstalkd-rs cluster` as the operator (docs/OPERATIONS.md 5.9, `bcl`)."""
        cmd = [self.bin, "cluster"]
        for n in nodes:
            cmd += ["--node", f"127.0.0.1:{self.cluster_port(n)}"]
        if self.tls:
            t = os.path.join(self.work, "tls")
            cmd += ["--ca", f"{t}/cluster-ca.pem", "--cert", f"{t}/admin.pem", "--key", f"{t}/admin.key"]
        else:
            cmd += ["--insecure-plaintext"]
        cmd += list(args)
        r = subprocess.run(cmd, capture_output=True, text=True)
        if check and r.returncode != 0:
            raise RuntimeError(f"{' '.join(args)} exited {r.returncode}: {r.stdout}{r.stderr}")
        return r

    def status(self, nodes=(1, 2, 3)):
        r = self.bcl("status", "--json", nodes=nodes, check=False)
        return json.loads(r.stdout) if r.returncode == 0 else None

    def settled(self, nodes=(1, 2, 3)):
        """Every member answers its status and none is rejoining (section 5.9, `settled`)."""
        s = self.status(nodes)
        return bool(s) and all(n.get("reachable") and not n.get("rejoining") for n in s["nodes"])

    def wait_settled(self, nodes=(1, 2, 3), timeout=60):
        end = time.monotonic() + timeout
        while not self.settled(nodes):
            if time.monotonic() > end:
                raise RuntimeError("cluster did not settle: " + json.dumps(self.status(nodes)))
            time.sleep(0.5)


class Conn:
    """A minimal beanstalk client; any socket error raises OSError."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.settimeout(15)
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            raise OSError("closed")
        self.buf += chunk

    def line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line.decode()

    def body(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def cmd(self, text, payload=None):
        out = text.encode() + b"\r\n"
        if payload is not None:
            out += payload + b"\r\n"
        self.sock.sendall(out)
        return self.line()


class History:
    def __init__(self):
        self.lock = threading.Lock()
        self.acked = set()  # bodies whose put was answered INSERTED
        self.maybe = set()  # bodies whose put reply was lost or not INSERTED
        self.reserved = {}  # body -> number of reservations
        self.deleted = {}  # body -> confirmed DELETED replies
        self.attempted = set()  # bodies for which a delete was sent
        self.errors = []
        self.stop_producing = threading.Event()
        self.stop_consuming = threading.Event()
        self.puts = 0
        self.reconnects = 0

    def note(self, what, body=None):
        with self.lock:
            if what == "ack":
                self.acked.add(body)
                self.puts += 1
            elif what == "maybe":
                self.maybe.add(body)
            elif what == "reserve":
                self.reserved[body] = self.reserved.get(body, 0) + 1
            elif what == "attempt":
                self.attempted.add(body)
            elif what == "deleted":
                self.deleted[body] = self.deleted.get(body, 0) + 1
            elif what == "reconnect":
                self.reconnects += 1


def connect(ports, hist, use_tube=True):
    """Connect to the first node that answers; try the others in turn."""
    while True:
        for p in ports():
            try:
                c = Conn(p)
                if use_tube:
                    if c.cmd(f"use {TUBE}") != f"USING {TUBE}":
                        raise OSError("use")
                return c
            except OSError:
                continue
        time.sleep(0.2)
        if hist.stop_consuming.is_set():
            return None


def producer(pid, ports, hist):
    n = 0
    c = None
    while not hist.stop_producing.is_set():
        if c is None:
            c = connect(ports, hist)
            if c is None:
                return
            hist.note("reconnect")
        body = f"p{pid}-{n}"
        n += 1
        try:
            reply = c.cmd(f"put 0 0 60 {len(body)}", body.encode())
        except OSError:
            hist.note("maybe", body)
            c.close()
            c = None
            continue
        if reply.startswith("INSERTED "):
            hist.note("ack", body)
        else:
            # Refused (for example while the node has no leader): not inserted.
            time.sleep(0.05)
            if reply.startswith("INTERNAL_ERROR") or reply.startswith("OUT_OF_MEMORY"):
                hist.note("maybe", body)
            if reply.startswith("UNKNOWN") or reply.startswith("BAD_FORMAT"):
                hist.errors.append(f"put reply {reply}")
    if c:
        c.close()


def consumer(cid, ports, hist):
    c = None
    while not hist.stop_consuming.is_set():
        if c is None:
            c = connect(ports, hist, use_tube=False)
            if c is None:
                return
            try:
                if not c.cmd(f"watch {TUBE}").startswith("WATCHING"):
                    raise OSError("watch")
                if not c.cmd("ignore default").startswith("WATCHING"):
                    raise OSError("ignore")
            except OSError:
                c.close()
                c = None
                continue
            hist.note("reconnect")
        try:
            reply = c.cmd("reserve-with-timeout 1")
            if reply.startswith("RESERVED "):
                _, jid, n = reply.split()
                body = c.body(int(n)).decode()
                hist.note("reserve", body)
                hist.note("attempt", body)
                r = c.cmd(f"delete {jid}")
                if r == "DELETED":
                    hist.note("deleted", body)
                # NOT_FOUND: the reservation was lost (TTR or a node change);
                # the job is redelivered and deleted later.
            elif reply in ("TIMED_OUT", "DEADLINE_SOON"):
                pass
            else:
                time.sleep(0.05)
        except OSError:
            c.close()
            c = None
    if c:
        c.close()


def tube_pending(ports):
    """Jobs left in the tube, read from any node that answers; None if none does."""
    for p in ports():
        try:
            c = Conn(p)
            c.sock.settimeout(5)
            n = c.cmd(f"stats-tube {TUBE}")
            if not n.startswith("OK "):
                c.close()
                if n.startswith("NOT_FOUND"):
                    return 0
                continue
            data = c.body(int(n.split()[1])).decode()
            c.close()
            st = dict(l.split(": ", 1) for l in data.splitlines() if ": " in l)
            return sum(int(st[k]) for k in ("current-jobs-ready", "current-jobs-reserved",
                                            "current-jobs-delayed", "current-jobs-buried"))
        except (OSError, ValueError, KeyError):
            continue
    return None


def leader_of(cl, nodes):
    s = cl.status(nodes)
    if not s or s.get("leader") is None:
        raise RuntimeError("no leader in status")
    return s["leader"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", default=os.path.join(ROOT, "target", "release", "beanstalkd-rs"))
    ap.add_argument("--work", default=None)
    ap.add_argument("--base-port", type=int, default=21000)
    ap.add_argument("--load-secs", type=float, default=3.0, help="load before and after the changes")
    ap.add_argument("--producers", type=int, default=4)
    ap.add_argument("--consumers", type=int, default=4)
    ap.add_argument("--no-tls", action="store_true", help="plaintext cluster traffic (insecure_plaintext)")
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()

    work = a.work or tempfile.mkdtemp(prefix="bstk-rolling-")
    os.makedirs(work, exist_ok=True)
    work = os.path.realpath(work)
    cl = Cluster(os.path.realpath(a.bin), work, a.base_port, not a.no_tls)
    hist = History()
    members = [1, 2, 3]  # client ports the load may use; edited as the cluster changes
    ports = lambda: [cl.client_port(i) for i in list(members)]
    ok = False
    threads = []
    try:
        # 5.3: certificates, configuration, bootstrap.
        if cl.tls:
            subprocess.run([os.path.join(ROOT, "scripts", "mkcluster-certs.sh"),
                            os.path.join(work, "tls"), "1", "2", "3", "4", "admin"],
                           check=True, capture_output=True)
        for i in (1, 2, 3):
            cl.node_config(i, (1, 2, 3))
        cl.node_config(4, (1, 2, 3))
        for i in (1, 2, 3):
            cl.start(i, "--cluster-init")
        for i in (1, 2, 3):
            cl.wait_ready(i)
        cl.wait_settled()
        log("cluster of 3 ready")

        for k in range(a.producers):
            threads.append(threading.Thread(target=producer, args=(k, ports, hist), daemon=True))
        for k in range(a.consumers):
            threads.append(threading.Thread(target=consumer, args=(k, ports, hist), daemon=True))
        for t in threads:
            t.start()
        time.sleep(a.load_secs)
        log(f"load running: {hist.puts} acknowledged puts")

        # 7.3: followers first, the leader last; each time /readyz, then settled.
        leader = leader_of(cl, (1, 2, 3))
        order = [i for i in (1, 2, 3) if i != leader] + [leader]
        log(f"rolling restart, order {order} (leader {leader})")
        for i in order:
            t0 = time.monotonic()
            cl.stop(i)
            cl.start(i)
            cl.wait_ready(i)
            cl.wait_settled()
            log(f"node {i} restarted and settled in {time.monotonic() - t0:.1f}s, "
                f"{hist.puts} acknowledged puts")
            time.sleep(0.5)

        # 5.9, "Grow from 3 to 5 nodes" (node 4 only), then "Shrink" (remove node 1).
        log("add 4, start, promote")
        cl.bcl("add", "4", f"127.0.0.1:{cl.cluster_port(4)}")
        cl.start(4)
        cl.wait_ready(4)
        members.append(4)
        cl.bcl("promote", "4")
        cl.wait_settled((1, 2, 3, 4))
        st = cl.status((1, 2, 3, 4))
        assert sorted(st["membership"]["voters"]) == [1, 2, 3, 4], st["membership"]
        time.sleep(0.5)
        log("remove 1, stop it")
        cl.bcl("remove", "1", nodes=(1, 2, 3, 4))
        cl.stop(1)
        members.remove(1)
        cl.wait_settled((2, 3, 4))
        st = cl.status((2, 3, 4))
        voters = sorted(st["membership"]["voters"])
        learners = st["membership"].get("learners", [])
        log(f"final membership: voters {voters}, learners {learners}, leader {st['leader']}")
        if voters != [2, 3, 4] or learners:
            raise RuntimeError(f"unexpected final membership: {st['membership']}")
        # /readyz is 503 while a node lags behind the commit index it learned,
        # which happens briefly under this load: poll, do not sample once.
        for i in (2, 3, 4):
            cl.wait_ready(i, timeout=15)

        time.sleep(a.load_secs)
        hist.stop_producing.set()
        for t in threads[: a.producers]:
            t.join(timeout=30)
        log(f"producers stopped: {hist.puts} acknowledged puts; draining")

        end = time.monotonic() + 90
        quiet = 0
        while time.monotonic() < end and quiet < 3:
            left = tube_pending(ports)
            quiet = quiet + 1 if left == 0 else 0
            time.sleep(0.7)
        hist.stop_consuming.set()
        for t in threads[a.producers:]:
            t.join(timeout=30)
        left = tube_pending(ports)

        # The checks.
        problems = list(hist.errors)
        if left != 0:
            problems.append(f"{left} jobs left in the tube")
        lost = sorted(b for b in hist.acked if b not in hist.deleted and b not in hist.attempted)
        if lost:
            problems.append(f"{len(lost)} acknowledged jobs never deleted, e.g. {lost[:5]}")
        dup = sorted(b for b, n in hist.deleted.items() if n > 1)
        if dup:
            problems.append(f"{len(dup)} bodies confirmed deleted twice, e.g. {dup[:5]}")
        unknown = sorted(b for b in hist.deleted if b not in hist.acked and b not in hist.maybe)
        if unknown:
            problems.append(f"{len(unknown)} deleted bodies never put, e.g. {unknown[:5]}")
        redelivered = sum(1 for n in hist.reserved.values() if n > 1)
        committed_unacked = sum(1 for b in hist.maybe if b in hist.reserved)
        unconfirmed = sum(1 for b in hist.acked if b not in hist.deleted)
        log(f"puts acknowledged {len(hist.acked)}, lost-reply puts {len(hist.maybe)} "
            f"(committed anyway: {committed_unacked}), confirmed deletes {len(hist.deleted)}, "
            f"deletes whose reply was lost {unconfirmed}, redelivered {redelivered}, "
            f"reconnects {hist.reconnects}")
        if problems:
            for p in problems:
                log("FAIL: " + p)
            raise RuntimeError("load check failed")
        log("PASS: zero lost jobs, no duplicates beyond at-least-once")
        ok = True
    except Exception as e:
        log(f"FAIL: {type(e).__name__}: {e}")
    finally:
        hist.stop_producing.set()
        hist.stop_consuming.set()
        cl.kill_all()
        if ok and not a.keep and a.work is None:
            shutil.rmtree(work, ignore_errors=True)
        else:
            log(f"work directory kept: {work}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
