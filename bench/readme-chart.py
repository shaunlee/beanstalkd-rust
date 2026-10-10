#!/usr/bin/env python3
"""Writes the README performance chart (docs/images/perf-{light,dark}.svg)
from the "README numbers" in docs/BENCH.md."""

import os

ROWS = [  # (workload, beanstalkd ops/s, beanstalkd-rs ops/s)
    ("put-reserve-delete, 1 connection", 56176, 53943),
    ("put-reserve-delete, 10 connections", 339283, 468310),
    ("put-reserve-delete, 100 connections", 392713, 516336),
    ("100 connections, 4 KiB bodies", 362247, 463416),
    ("100 connections, 16 pipelined", 490247, 643204),
    ("producers and consumers, 100 connections", 422349, 500891),
    ("binlog (-b), 100 connections", 267391, 594148),
]

THEMES = {
    "light": dict(ref="#2a78d6", rs="#eb6834", ink="#0b0b0b", ink2="#52514e",
                  muted="#6b6a65", grid="#e1e0d9", axis="#c3c2b7"),
    "dark": dict(ref="#3987e5", rs="#d95926", ink="#f0efec", ink2="#c3c2b7",
                 muted="#898781", grid="#2c2c2a", axis="#383835"),
}

W, LABEL_W, PLOT_W, RATIO_W = 820, 300, 400, 70
X0 = LABEL_W + 10
BAR, GAP, GROUP = 14, 2, 46
TOP = 64
MAX = 700_000
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def k(v):
    return f"{v / 1000:.0f}k"


def bar(x, y, w, h, color):
    r = min(4, w)
    return (f'<path d="M{x},{y} h{w - r} a{r},{r} 0 0 1 {r},{r} v{h - 2 * r} '
            f'a{r},{r} 0 0 1 {-r},{r} h{-(w - r)} z" fill="{color}"/>')


def chart(t):
    h = TOP + GROUP * len(ROWS) + 34
    sx = PLOT_W / MAX
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{h}" '
         f'viewBox="0 0 {W} {h}" font-family=\'{FONT}\' role="img" '
         f'aria-label="Operations per second, beanstalkd against beanstalkd-rs">']
    o.append(f'<text x="0" y="18" font-size="14" font-weight="600" fill="{t["ink"]}">'
             f'Operations per second (higher is better)</text>')
    lx = 0
    for name, color in (("beanstalkd", t["ref"]), ("beanstalkd-rs", t["rs"])):
        o.append(f'<rect x="{lx}" y="32" width="12" height="12" rx="2" fill="{color}"/>')
        o.append(f'<text x="{lx + 18}" y="42" font-size="13" fill="{t["ink2"]}">{name}</text>')
        lx += 130
    o.append(f'<text x="{W}" y="42" font-size="12" text-anchor="end" fill="{t["muted"]}">'
             f'beanstalkd-rs / beanstalkd</text>')
    plot_bottom = TOP + GROUP * len(ROWS)
    for v in range(0, MAX + 1, 100_000):
        x = X0 + v * sx
        o.append(f'<line x1="{x:.1f}" y1="{TOP - 4}" x2="{x:.1f}" y2="{plot_bottom}" '
                 f'stroke="{t["grid"] if v else t["axis"]}" stroke-width="1"/>')
        o.append(f'<text x="{x:.1f}" y="{plot_bottom + 16}" font-size="11" text-anchor="middle" '
                 f'fill="{t["muted"]}">{k(v) if v else "0"}</text>')
    for i, (label, ref, rs) in enumerate(ROWS):
        y = TOP + i * GROUP + (GROUP - 2 * BAR - GAP) / 2
        o.append(f'<text x="{LABEL_W}" y="{y + BAR + 5}" font-size="13" text-anchor="end" '
                 f'fill="{t["ink2"]}">{label}</text>')
        for j, (v, color) in enumerate(((ref, t["ref"]), (rs, t["rs"]))):
            by = y + j * (BAR + GAP)
            w = v * sx
            o.append(bar(X0, by, w, BAR, color))
            o.append(f'<text x="{X0 + w + 5:.1f}" y="{by + 11}" font-size="11" '
                     f'fill="{t["ink2"]}">{k(v)}</text>')
        ratio = rs / ref
        o.append(f'<text x="{W}" y="{y + BAR + 5}" font-size="14" text-anchor="end" '
                 f'font-weight="{600 if ratio >= 1 else 400}" fill="{t["ink"]}">{ratio:.2f}×</text>')
    o.append("</svg>")
    return "\n".join(o) + "\n"


out = os.path.join(os.path.dirname(__file__), "..", "docs", "images")
for name, theme in THEMES.items():
    with open(os.path.join(out, f"perf-{name}.svg"), "w") as f:
        f.write(chart(theme))
