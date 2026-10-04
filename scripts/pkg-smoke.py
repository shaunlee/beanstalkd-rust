#!/usr/bin/env python3
"""Packaging smoke test: talks to a running beanstalkd-rs over plain TCP.

Standard library only, so it runs on CI runners, in Debian images and on
macOS without a virtualenv. Used by the release workflow (each built binary),
the docker CI job and the systemd check.

  pkg-smoke.py [--host H] [--port P] [--wait SECONDS] [--version V]
               [--persist-put N | --persist-expect N]

Always: put / reserve / delete one job on tube "pkg-smoke" and check the
reply bytes. --version V: `stats` must report `version: V`.
--persist-put N: leave N ready jobs on tube "pkg-smoke-persist".
--persist-expect N: that tube must hold exactly N ready jobs (after a
restart on the same binlog); they are then deleted.
"""

import argparse
import socket
import sys
import time


class Conn:
    def __init__(self, host, port, wait):
        deadline = time.monotonic() + wait
        while True:
            try:
                self.sock = socket.create_connection((host, port), timeout=10)
                break
            except OSError as e:
                if time.monotonic() >= deadline:
                    sys.exit(f"pkg-smoke: cannot connect to {host}:{port}: {e}")
                time.sleep(0.2)
        self.buf = b""

    def _fill(self):
        chunk = self.sock.recv(65536)
        if not chunk:
            sys.exit("pkg-smoke: server closed the connection")
        self.buf += chunk

    def line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

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


def expect(got, want, what):
    if got != want:
        sys.exit(f"pkg-smoke: {what}: expected {want!r}, got {got!r}")


def yaml_dict(conn, reply, what):
    parts = reply.split()
    if len(parts) != 2 or parts[0] != b"OK":
        sys.exit(f"pkg-smoke: {what}: unexpected reply {reply!r}")
    data = conn.body(int(parts[1])).decode()
    out = {}
    for line in data.splitlines()[1:]:
        key, _, value = line.partition(": ")
        # The reference quotes string values, e.g. version: "1.13".
        out[key] = value.strip('"')
    return out


def reserve_delete(conn, body):
    reply = conn.cmd("reserve-with-timeout 5")
    parts = reply.split()
    if len(parts) != 3 or parts[0] != b"RESERVED":
        sys.exit(f"pkg-smoke: reserve: unexpected reply {reply!r}")
    expect(conn.body(int(parts[2])), body, "reserved body")
    expect(conn.cmd(f"delete {int(parts[1])}"), b"DELETED", "delete")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=11300)
    ap.add_argument("--wait", type=float, default=10.0)
    ap.add_argument("--version")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--persist-put", type=int)
    group.add_argument("--persist-expect", type=int)
    args = ap.parse_args()

    conn = Conn(args.host, args.port, args.wait)

    if args.version is not None:
        stats = yaml_dict(conn, conn.cmd("stats"), "stats")
        expect(stats.get("version"), args.version, "stats version")

    expect(conn.cmd("use pkg-smoke"), b"USING pkg-smoke", "use")
    expect(conn.cmd("watch pkg-smoke"), b"WATCHING 2", "watch")
    expect(conn.cmd("ignore default"), b"WATCHING 1", "ignore")
    body = b"hello from pkg-smoke"
    reply = conn.cmd(f"put 0 0 60 {len(body)}", body)
    if not reply.startswith(b"INSERTED "):
        sys.exit(f"pkg-smoke: put: unexpected reply {reply!r}")
    reserve_delete(conn, body)

    tube = "pkg-smoke-persist"
    if args.persist_put is not None:
        expect(conn.cmd(f"use {tube}"), f"USING {tube}".encode(), "use")
        for i in range(args.persist_put):
            data = f"persist-{i}".encode()
            reply = conn.cmd(f"put 0 0 60 {len(data)}", data)
            if not reply.startswith(b"INSERTED "):
                sys.exit(f"pkg-smoke: persist put: unexpected reply {reply!r}")
    if args.persist_expect is not None:
        reply = conn.cmd(f"stats-tube {tube}")
        if reply == b"NOT_FOUND":
            ready = "0"
        else:
            ready = yaml_dict(conn, reply, "stats-tube").get("current-jobs-ready")
        expect(ready, str(args.persist_expect), f"ready jobs on {tube}")
        expect(conn.cmd(f"watch {tube}"), b"WATCHING 2", "watch")
        expect(conn.cmd("ignore pkg-smoke"), b"WATCHING 1", "ignore")
        for i in range(args.persist_expect):
            reserve_delete(conn, f"persist-{i}".encode())

    conn.sock.sendall(b"quit\r\n")
    print(f"pkg-smoke: ok ({args.host}:{args.port})")


if __name__ == "__main__":
    main()
