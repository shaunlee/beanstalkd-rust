#!/usr/bin/env python3
"""Summarizes bench/run-matrix.sh CSV output as a Markdown table.

Usage: summarize.py [--baseline MODE] results.csv [more.csv ...]

For every (server mode, scenario, conns, body size, pipeline, idle conns,
delayed tubes) cell it prints the median
over runs of throughput, server CPU and put/reserve p99 latency for the
reference ("ref") and beanstalkd-rs ("rs"), and the throughput ratio
rs / ref. Cells whose runs spread by more than 20% are flagged with "*".

When the CSV has proxy CPU (TLS modes: the reference behind stunnel), a
"ref proxy CPU %" column shows stunnel's CPU next to the reference's own.
When the CSV has cluster columns (cluster modes: ours only, so the "ref"
columns are empty), "rs nodes CPU %" is the median CPU of all cluster
nodes together (bench/run-matrix.sh: cluster_cpu_pct) and "rs anomalies"
sums over the cell's runs the resent inputs, forward queue rewinds and
leader term changes ("resent/rewinds/terms").
--baseline MODE adds a column with rs ops/s divided by rs ops/s of the same
cell in server mode MODE (e.g. `--baseline none`: TLS cost for ours).
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
    args = sys.argv[1:]
    baseline = None
    if len(args) >= 2 and args[0] == "--baseline":
        baseline, args = args[1], args[2:]
    cells: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    order: list[tuple] = []
    for path in args:
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                key = (
                    r.get("server_mode") or "none",
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

    scaling = any(k[5] or k[6] for k in order)
    modes = any(k[0] != "none" for k in order)
    mode_head = " mode |" if modes else ""
    mode_rule = "---|" if modes else ""
    extra_head = " idle conns | delayed tubes |" if scaling else ""
    extra_rule = "---:|---:|" if scaling else ""
    proxy = any(r.get("proxy_cpu_pct") for c in cells.values() for r in c["ref"])
    proxy_head = " ref proxy CPU % |" if proxy else ""
    proxy_rule = "---:|" if proxy else ""
    clus = any(r.get("cluster_cpu_pct") for c in cells.values() for r in c["rs"])
    clus_head = " rs nodes CPU % | rs anomalies |" if clus else ""
    clus_rule = "---:|---:|" if clus else ""
    base_head = f" rs/rs[{baseline}] |" if baseline else ""
    base_rule = "---:|" if baseline else ""
    print(
        f"|{mode_head} scenario | conns | body | pipe |{extra_head} ref ops/s | rs ops/s | rs/ref |{base_head}"
        f" ref CPU % |{proxy_head} rs CPU % |{clus_head} ref put p99 µs | rs put p99 µs "
        "| ref reserve p99 µs | rs reserve p99 µs |"
    )
    print(
        f"|{mode_rule}---|---:|---:|---:|{extra_rule}---:|---:|---:|{base_rule}---:|{proxy_rule}"
        f"---:|{clus_rule}---:|---:|---:|---:|"
    )
    for key in order:
        ref, rs = cells[key]["ref"], cells[key]["rs"]
        r_ops, s_ops = med(ref, "ops_per_sec"), med(rs, "ops_per_sec")
        ratio = s_ops / r_ops if r_ops and s_ops else None
        noisy = spread(ref, "ops_per_sec") > 0.2 or spread(rs, "ops_per_sec") > 0.2
        flag = "*" if noisy else ""
        mode, scenario, conns, body, pipe, idle, delayed = key
        extra = f" {idle:,} | {delayed:,} |" if scaling else ""
        mode_col = f" {mode} |" if modes else ""
        base_col = ""
        if baseline:
            b_ops = med(cells.get((baseline,) + key[1:], {}).get("rs", []), "ops_per_sec")
            base_col = f" {fmt(s_ops / b_ops if b_ops and s_ops else None, 2)} |"
        proxy_col = f" {fmt(med(ref, 'proxy_cpu_pct'))} |" if proxy else ""
        clus_col = ""
        if clus:
            def total(k: str) -> int:
                return sum(int(r[k]) for r in rs if r.get(k))
            anomalies = "-"
            if any(r.get("cluster_cpu_pct") for r in rs):
                anomalies = f"{total('resent_inputs')}/{total('forward_rewinds')}/{total('term_changes')}"
            clus_col = f" {fmt(med(rs, 'cluster_cpu_pct'))} | {anomalies} |"
        print(
            f"|{mode_col} {scenario} | {conns} | {body} | {pipe} |{extra} {fmt(r_ops)}{flag if r_ops else ''} "
            f"| {fmt(s_ops)}{flag if s_ops else ''} "
            f"| {fmt(ratio, 2)} |{base_col} {fmt(med(ref, 'server_cpu_pct'))} |{proxy_col} {fmt(med(rs, 'server_cpu_pct'))} |{clus_col}"
            f" {fmt(med(ref, 'put_p99_us'))} | {fmt(med(rs, 'put_p99_us'))} "
            f"| {fmt(med(ref, 'reserve_p99_us'))} | {fmt(med(rs, 'reserve_p99_us'))} |"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
