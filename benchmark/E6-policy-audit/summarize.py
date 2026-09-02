#!/usr/bin/env python3
"""Derive the plot-ready CSVs of results.org from the raw E6 outputs.

Inputs (under results/): e6.csv (the driver rows), proc.csv (the per-second
CPU/RSS sampler), requests-<mode>-<n>-<p>.log (the apiserver request-audit
log of each grid point). Outputs, one file per (plot, mode) series, columns
fixed so results.org's pgfplots blocks read them directly.
"""

import csv
import json
import os
import re
import statistics
import sys
from collections import defaultdict

RESULTS = sys.argv[1] if len(sys.argv) > 1 else "results"


def rows():
    path = os.path.join(RESULTS, "e6.csv")
    with open(path) as f:
        for row in csv.DictReader(f):
            yield row


def write(name, header, data):
    path = os.path.join(RESULTS, name)
    with open(path, "w") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(data)
    print(f"wrote {path} ({len(data)} rows)")


def quantile(values, q):
    if not values:
        return ""
    values = sorted(values)
    idx = min(len(values) - 1, max(0, int(q * (len(values) - 1) + 0.5)))
    return values[idx]


def main():
    by = defaultdict(list)
    for r in rows():
        by[(r["mode"], r["workload"])].append(r)

    modes = sorted({m for (m, _) in by})

    # w1/w4: latency vs N (one series file per mode/workload/P; the
    # suffixless file aliases the largest P - what results.org plots).
    for wl in ("w1", "w4", "preload"):
        for mode in modes:
            per_np = defaultdict(list)
            for r in by.get((mode, wl), []):
                if r["value_ms"]:
                    per_np[(int(r["n"]), int(r["p"]))].append(float(r["value_ms"]))
            ps = sorted({p for (_, p) in per_np})
            for p in ps:
                data = [
                    [n, round(statistics.median(v), 1), round(quantile(v, 0.95), 1)]
                    for (n, pp), v in sorted(per_np.items())
                    if pp == p
                ]
                if not data:
                    continue
                write(f"e6-{wl}-{mode}-p{p}.csv", ["n", "median_ms", "p95_ms"], data)
                if p == ps[-1]:
                    write(f"e6-{wl}-{mode}.csv", ["n", "median_ms", "p95_ms"], data)

    # w3: detection fraction and mean latency vs violation lifetime.
    for mode in modes:
        per_l = defaultdict(list)
        for r in by.get((mode, "w3"), []):
            per_l[float(r["param"])].append(r)
        data = []
        for lifetime, rs in sorted(per_l.items()):
            detected = [r for r in rs if r["extra"] == "detected"]
            dts = [float(r["value_ms"]) for r in detected if r["value_ms"]]
            data.append([
                lifetime,
                round(len(detected) / len(rs), 3),
                round(statistics.mean(dts), 1) if dts else "",
            ])
        if data:
            write(f"e6-w3-{mode}.csv", ["lifetime_s", "detected", "mean_ms"], data)

    # w5: per-event detection lag (largest N, largest P present).
    for mode in modes:
        rs = by.get((mode, "w5"), [])
        if not rs:
            continue
        n = max(int(r["n"]) for r in rs)
        p = max(int(r["p"]) for r in rs if int(r["n"]) == n)
        data = [
            [int(r["rep"]), round(float(r["value_ms"]), 1)]
            for r in rs
            if int(r["n"]) == n and int(r["p"]) == p and r["value_ms"]
        ]
        if data:
            write(f"e6-w5-{mode}.csv", ["event", "lag_ms"], sorted(data))

    # w2: the idle table - status writes from the driver rows, CPU/RSS from
    # the sampler, API requests from the request log, all sliced to the
    # idle window's epoch markers.
    w2 = []
    for mode in modes:
        for r in by.get((mode, "w2"), []):
            start, end = (int(x) for x in r["extra"].split(":"))
            n, p = int(r["n"]), int(r["p"])
            cpu_s, rss_mb = proc_window(f"{mode}-{n}-{p}", start, end)
            reqs = request_window(mode, n, p, start, end)
            w2.append([mode, n, p, int(r["param"]), int(float(r["value_ms"])),
                       cpu_s, rss_mb, reqs])
    if w2:
        write("e6-w2.csv",
              ["mode", "n", "p", "idle_s", "status_writes", "cpu_s", "rss_mb", "api_requests"],
              sorted(w2))


def proc_window(label, start, end):
    path = os.path.join(RESULTS, "proc.csv")
    if not os.path.exists(path):
        return "", ""
    ticks, rss = [], []
    with open(path) as f:
        for line in f:
            ts, lb, _pid, t, r = line.strip().split(",")
            if lb == label and start <= int(ts) <= end:
                ticks.append(int(t))
                rss.append(int(r))
    if len(ticks) < 2:
        return "", ""
    page_kb = os.sysconf("SC_PAGE_SIZE") // 1024
    return round((ticks[-1] - ticks[0]) / os.sysconf("SC_CLK_TCK"), 2), round(
        statistics.mean(rss) * page_kb / 1024, 1)


def request_window(mode, n, p, start, end):
    """Count SUT requests in the idle window: everything that is neither the
    driver (e6-driver user agent) nor the apiserver's own loopback."""
    path = os.path.join(RESULTS, f"requests-{mode}-{n}-{p}.log")
    if not os.path.exists(path):
        return ""
    count = 0
    with open(path) as f:
        for line in f:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if ev.get("stage") != "ResponseComplete":
                continue
            ts = ev.get("stageTimestamp", "")
            ms = parse_epoch_ms(ts)
            if ms is None or not (start <= ms <= end):
                continue
            ua = ev.get("userAgent", "")
            if ua.startswith("e6-driver") or ua.startswith("kube-apiserver"):
                continue
            count += 1
    return count


EPOCH_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d+)Z")


def parse_epoch_ms(ts):
    import calendar

    m = EPOCH_RE.match(ts)
    if not m:
        return None
    y, mo, d, h, mi, s, frac = m.groups()
    epoch = calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(s)))
    return epoch * 1000 + int(frac[:3].ljust(3, "0"))


if __name__ == "__main__":
    main()
