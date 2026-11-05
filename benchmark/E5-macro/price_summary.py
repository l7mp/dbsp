#!/usr/bin/env python3
"""The price of a change per contender: CPU and API requests per change.

For every grid point mr-<mode>-r<N> of the multiregion sweep, the window from
the "preload:" line (printed once the preload has converged and, for a closed
loop, the loop has been closed) to the end of the last changeset (the
"endpoint-update" line) in the progress log is the measured phase: five
changesets of reps changes each. The sampler's cumulative CPU ticks (results/proc.csv, one row
per second per point label) give the controller's CPU over the window, the
apiserver request log (results/requests-mr-<mode>-r<N>.log) the requests it
issued, every user agent except the driver's and the apiserver's own. Both
are divided by the number of changes. Emits results/E5-price-<mode>.csv:

    regions,changes,window_s,cpu_ms,requests

Usage: price_summary.py <results dir> <progress log>
"""
import csv
import glob
import json
import os
import re
import sys
import time
from collections import defaultdict

LINE = re.compile(r"^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (.*)$")
DONE = re.compile(r"^E5 multiregion (\S+) r=(\d+) (\S+):")
PRELOAD = re.compile(r"^preload: trace=multiregion regions=(\d+) mode=(\S+) in")
EPOCH = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d+)Z")


def local_ms(stamp):
    """A progress-log timestamp (local time of the machine) as epoch ms."""
    return int(time.mktime(time.strptime(stamp, "%Y/%m/%d %H:%M:%S"))) * 1000


def utc_ms(stamp):
    m = EPOCH.match(stamp)
    if not m:
        return None
    import calendar
    y, mo, d, h, mi, s, frac = m.groups()
    return calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(s))) * 1000 + int(frac[:3].ljust(3, "0"))


def windows(progress):
    """(mode, regions) -> (start_ms, end_ms) of the measured phase."""
    start, end = {}, {}
    for line in open(progress):
        m = LINE.match(line.rstrip("\n"))
        if not m:
            continue
        ts, rest = local_ms(m.group(1)), m.group(2)
        p = PRELOAD.match(rest)
        if p:
            start[(p.group(2), int(p.group(1)))] = ts
            continue
        d = DONE.match(rest)
        if d and d.group(3) == "endpoint-update":
            end[(d.group(1), int(d.group(2)))] = ts
    return {k: (start[k], end[k]) for k in end if k in start}


def cpu_seconds(results, label, t0, t1):
    ticks = []
    with open(os.path.join(results, "proc.csv")) as f:
        for line in f:
            ts, lb, _pid, t, _r = line.strip().split(",")
            if lb == label and t0 <= int(ts) <= t1:
                ticks.append(int(t))
    if len(ticks) < 2:
        return None
    return (ticks[-1] - ticks[0]) / os.sysconf("SC_CLK_TCK")


def requests(results, label, t0, t1):
    path = os.path.join(results, f"requests-{label}.log")
    if not os.path.exists(path):
        return None
    n = 0
    with open(path) as f:
        for line in f:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if ev.get("stage") != "ResponseComplete":
                continue
            ua = ev.get("userAgent", "")
            if ua.startswith("bench-driver") or ua.startswith("kube-apiserver"):
                continue
            ms = utc_ms(ev.get("stageTimestamp", ""))
            if ms is not None and t0 <= ms <= t1:
                n += 1
    return n


def main(results, progress):
    reps = {}
    for path in glob.glob(os.path.join(results, "E5-multiregion-*-region-add.csv")):
        mode = os.path.basename(path)[len("E5-multiregion-"):-len("-region-add.csv")]
        with open(path) as f:
            for r in csv.DictReader(f):
                reps[(mode, int(r["regions"]))] = int(r["reps"])
    rows = defaultdict(list)
    for (mode, n), (t0, t1) in sorted(windows(progress).items()):
        if (mode, n) not in reps or "-" in mode:
            continue
        label = f"mr-{mode}-r{n}"
        changes = 5 * reps[(mode, n)]
        cpu = cpu_seconds(results, label, t0, t1)
        req = requests(results, label, t0, t1)
        rows[mode].append([n, changes, (t1 - t0) // 1000,
                           "" if cpu is None else round(1000 * cpu / changes, 2),
                           "" if req is None else round(req / changes, 2)])
        print(f"price mode={mode} regions={n}: window={(t1 - t0) // 1000}s cpu={cpu} s req={req} over {changes} changes")
    for mode, rs in rows.items():
        with open(os.path.join(results, f"E5-price-{mode}.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["regions", "changes", "window_s", "cpu_ms", "requests"])
            w.writerows(sorted(rs))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
