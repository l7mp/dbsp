#!/usr/bin/env python3
"""Join the congestion driver's heal times with the apiserver request logs.

For every row of results/E5-congestion-<mode>.csv, count in the matching
results/requests-cong-<mode>-q<qps>.log the status-subresource PATCH
requests inside the point's [t0, t2] window, split by author: the SUT (the
delta-gateway controller, default client user agent) versus the driver
(user agent "bench-driver", whose patches are the tamperings themselves).
Emits results/E5-congestion.csv with one row per point:

    mode,qps,regions,burst,heal_ms,sut_patches,driver_patches,per_fix

per_fix = sut_patches / burst: 1.0 is exactly-once actuation; anything
above it is duplicate corrections issued while earlier ones were still
queued behind the throttle.
"""

import csv
import glob
import json
import os
import sys


def count_patches(log_path, t0, t2):
    sut = driver = 0
    try:
        with open(log_path) as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if ev.get("verb") != "patch":
                    continue
                if "/status" not in ev.get("requestURI", ""):
                    continue
                ts = ev.get("requestReceivedTimestamp", "")
                if not (t0 <= ts <= t2):
                    continue
                if ev.get("userAgent", "").startswith("bench-driver"):
                    driver += 1
                else:
                    sut += 1
    except OSError:
        return None, None
    return sut, driver


def main(results_dir):
    out_rows = []
    for path in sorted(glob.glob(os.path.join(results_dir, "E5-congestion-*.csv"))):
        if path.endswith("E5-congestion.csv"):
            continue
        with open(path) as f:
            for row in csv.DictReader(f):
                mode, qps = row["mode"], row["qps"]
                log = os.path.join(results_dir, f"requests-cong-{mode}-q{qps}.log")
                sut, driver = count_patches(log, row["t0"], row["t2"])
                burst = int(row["burst"])
                out_rows.append({
                    "mode": mode,
                    "qps": qps,
                    "regions": row["regions"],
                    "burst": burst,
                    "heal_ms": row["heal_ms"],
                    "sut_patches": "" if sut is None else sut,
                    "driver_patches": "" if driver is None else driver,
                    "per_fix": "" if sut is None or burst == 0 else f"{sut / burst:.2f}",
                })
    if not out_rows:
        return
    fields = list(out_rows[0].keys())
    out = os.path.join(results_dir, "E5-congestion.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)
    # Per-mode series for the pgfplots figures, sorted by qps.
    for m in sorted({r["mode"] for r in out_rows}):
        series = sorted((r for r in out_rows if r["mode"] == m), key=lambda r: float(r["qps"]))
        with open(os.path.join(results_dir, f"E5-congestion-{m}-sum.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(series)
    for r in out_rows:
        print(
            f"congestion mode={r['mode']} qps={r['qps']}: heal={r['heal_ms']}ms "
            f"sut_patches={r['sut_patches']} (x{r['per_fix']} per fix)"
        )


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results")
