#!/usr/bin/env python3
"""Time-to-heal distributions of the disturbance benchmark.

Reads results/E5-disturb-heal-<mode>-<variant>.csv (one row per healing
episode: an object observed diverging from the oracle until it matches
again; heal_ms is -1 for an episode still open when the run ended) and
writes results/E5-heal-cdf-<mode>-<variant>.csv, the empirical CDF of the
healing time over ALL episodes (the never-healed ones keep it below one),
plus a table on stdout.

Usage: heal_summary.py <results dir>
"""
import csv
import glob
import os
import re
import sys


def main(results):
    rows = []
    for path in sorted(glob.glob(os.path.join(results, "E5-disturb-heal-*.csv"))):
        m = re.match(r"E5-disturb-heal-(.+)-(burst|l[0-9.]+)\.csv$", os.path.basename(path))
        if not m:
            continue
        mode, variant = m.group(1), m.group(2)
        with open(path) as f:
            heal = [float(r["heal_ms"]) for r in csv.DictReader(f)]
        total = len(heal)
        healed = sorted(h for h in heal if h >= 0)
        if total == 0:
            continue
        out = os.path.join(results, f"E5-heal-cdf-{mode}-{variant}.csv")
        with open(out, "w") as f:
            w = csv.writer(f)
            w.writerow(["t_ms", "frac"])
            w.writerow([0.1, 0.0])
            for i, h in enumerate(healed, 1):
                w.writerow([max(h, 0.1), i / total])
        q = lambda p: healed[min(len(healed) - 1, int(p * len(healed)))] if healed else float("nan")
        rows.append((mode, variant, total, len(healed), total - len(healed), q(0.5), q(0.95), healed[-1] if healed else float("nan")))
    print(f"{'mode':<14} {'variant':<7} {'episodes':>8} {'healed':>6} {'never':>5} {'median_ms':>9} {'p95_ms':>8} {'max_ms':>9}")
    for r in rows:
        print(f"{r[0]:<14} {r[1]:<7} {r[2]:>8} {r[3]:>6} {r[4]:>5} {r[5]:>9.0f} {r[6]:>8.0f} {r[7]:>9.0f}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results")
