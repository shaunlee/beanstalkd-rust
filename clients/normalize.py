#!/usr/bin/env python3
"""Normalizes a smoke-test transcript so two servers' runs can be diffed.

Reads a transcript (see clients/python/smoke.py for the line formats) on
stdin and writes it to stdout with volatile values masked:

* Stats fields that legitimately differ between two server processes
  (same list as tests/compat/src/mask.rs, plus `id` in server-wide stats)
  become `<masked>`.
* Job ids (`job=<n>` and `id: <n>` in stats-job) are renumbered in order of
  first appearance (`job=#1`, `job=#2`, ...). This masks the absolute value
  while still checking that the same job shows up in the same places.
"""

from __future__ import annotations

import re
import sys

# Keep in sync with MASKED_KEYS in tests/compat/src/mask.rs.
MASKED_KEYS = {
    # stats (server-wide)
    "pid",
    "version",
    "rusage-utime",
    "rusage-stime",
    "uptime",
    "hostname",
    "os",
    "platform",
    # stats-job
    "age",
    "time-left",
    # stats-tube
    "pause-time-left",
}
# `id` is the random server id in server-wide stats; in stats-job it is the
# job id, which is renumbered instead.
SERVER_STATS_ONLY_KEYS = {"id"}

STATS_LINE = re.compile(r"^\[(?P<scope>[^\]]+)\] (?P<key>[a-z0-9-]+): (?P<value>.*)$")
JOB_REF = re.compile(r"job=(\d+)")


def main() -> int:
    ids: dict[str, str] = {}

    def label(raw: str) -> str:
        if raw not in ids:
            ids[raw] = f"#{len(ids) + 1}"
        return ids[raw]

    for line in sys.stdin:
        line = line.rstrip("\n")
        m = STATS_LINE.match(line)
        if m:
            scope, key, value = m["scope"], m["key"], m["value"]
            server_stats = scope == "stats"
            if key in MASKED_KEYS or (server_stats and key in SERVER_STATS_ONLY_KEYS):
                value = "<masked>"
            elif key == "id" and scope.startswith("stats-job"):
                value = label(value)
            line = f"[{scope}] {key}: {value}"
        else:
            line = JOB_REF.sub(lambda mm: "job=" + label(mm[1]), line)
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
