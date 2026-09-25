#!/usr/bin/env python3
"""Summarizes bench/run-matrix.sh CSV output as a Markdown table.

Usage: summarize.py results.csv [more.csv ...]

For every (scenario, conns, body size, pipeline, idle conns, delayed
tubes) cell it prints the median
over runs of throughput, server CPU and put/reserve p99 latency for the
reference ("ref") and beanstalkd-rs ("rs"), and the throughput ratio
rs / ref. Cells whose runs spread by more than 20% are flagged with "*".
"""

from __future__ import annotations

import csv
import statistics
import sys
from collections import defaultdict


def med(rows: list[dict[str, str]], key: str) -> float | None:
    vals = [float(r[key]) for r in rows if r.get(key)]
    return statistics.median(vals) if vals else None


def spread(rows: list[dict[str, str]], key: str) -> float:
    vals = [float(r[key]) for r in rows if r.get(key)]
    if len(vals) < 2 or min(vals) == 0:
        return 0.0
    return (max(vals) - min(vals)) / statistics.median(vals)


def fmt(v: float | None, digits: int = 0) -> str:
    return "-" if v is None else f"{v:,.{digits}f}"


def main() -> int:
    cells: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    order: list[tuple] = []
    for path in sys.argv[1:]:
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                key = (
                    r["scenario"],
                    int(r["conns"]),
                    int(r["body_size"]),
                    int(r["pipeline"]),
                    int(r.get("idle_conns") or 0),
                    int(r.get("delayed_tubes") or 0),
                )
                if key not in cells:
                    order.append(key)
                cells[key][r["server"]].append(r)

    scaling = any(k[4] or k[5] for k in order)
    extra_head = " idle conns | delayed tubes |" if scaling else ""
    extra_rule = "---:|---:|" if scaling else ""
    print(
        f"| scenario | conns | body | pipe |{extra_head} ref ops/s | rs ops/s | rs/ref "
        "| ref CPU % | rs CPU % | ref put p99 µs | rs put p99 µs "
        "| ref reserve p99 µs | rs reserve p99 µs |"
    )
    print(f"|---|---:|---:|---:|{extra_rule}---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for key in order:
        ref, rs = cells[key]["ref"], cells[key]["rs"]
        r_ops, s_ops = med(ref, "ops_per_sec"), med(rs, "ops_per_sec")
        ratio = s_ops / r_ops if r_ops and s_ops else None
        noisy = spread(ref, "ops_per_sec") > 0.2 or spread(rs, "ops_per_sec") > 0.2
        flag = "*" if noisy else ""
        scenario, conns, body, pipe, idle, delayed = key
        extra = f" {idle:,} | {delayed:,} |" if scaling else ""
        print(
            f"| {scenario} | {conns} | {body} | {pipe} |{extra} {fmt(r_ops)}{flag} | {fmt(s_ops)}{flag} "
            f"| {fmt(ratio, 2)} | {fmt(med(ref, 'server_cpu_pct'))} | {fmt(med(rs, 'server_cpu_pct'))} "
            f"| {fmt(med(ref, 'put_p99_us'))} | {fmt(med(rs, 'put_p99_us'))} "
            f"| {fmt(med(ref, 'reserve_p99_us'))} | {fmt(med(rs, 'reserve_p99_us'))} |"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
