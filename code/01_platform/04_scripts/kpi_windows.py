#!/usr/bin/env python3
"""Score the p99 KPI series from a stage-profile run's Prometheus snapshots.

Why this lives in the repo: the p99 <= 50 ms program decides by comparing arms, and a
comparison is only trustworthy with a stated, reproducible scoring rule. The ad-hoc
scorers that lived in /tmp produced numbers that could not be re-derived later
(2026-10-06 audit -> docs/plans/2026-10-06-p99-integrated-program.md, Task 1).

Rule (SLO: worst-subtask p99 <= 50 ms in EVERY 60 s window):

  * one value per snapshot = the LARGEST ``quantile="0.99"`` value across subtasks
    (the SLO is defined on the worst subtask, not the mean);
  * snapshots are ``<run>/main/stages/prom-<epoch>.txt``;
  * windows are 60 s buckets anchored at the FIRST snapshot, and a window is
    ``failing`` when its maximum exceeds the limit.

Usage: kpi_windows.py <run-dir> [more run-dirs...]      # table
       kpi_windows.py --json <run-dir> [...]            # machine readable
"""

import glob
import json
import os
import re
import statistics
import sys

KPIS = ("ingest_to_strategy", "ingest_to_monitor", "tick_to_strategy")
LIMIT_MS = 50.0
WINDOW_S = 60

# One line per (kpi, subtask, quantile); the labels are unordered in the exposition.
LINE = re.compile(
    r"^flink_taskmanager_job_task_operator_compute_latency_(?P<kpi>[a-z_]+)"
    r"\{.*quantile=\"0\.99\".*\}\s+(?P<value>[0-9.eE+-]+)\s*$"
)


def snapshots(run_dir):
    """[(epoch, {kpi: worst_subtask_p99})], ordered by epoch."""
    out = []
    pattern = os.path.join(run_dir, "main", "stages", "prom-*.txt")
    for path in sorted(glob.glob(pattern)):
        epoch = int(re.search(r"prom-(\d+)\.txt$", path).group(1))
        worst = {}
        with open(path, errors="replace") as fh:
            for line in fh:
                match = LINE.match(line)
                if match and match.group("kpi") in KPIS:
                    kpi = match.group("kpi")
                    worst[kpi] = max(worst.get(kpi, 0.0), float(match.group("value")))
        out.append((epoch, worst))
    return sorted(out)


def score(run_dir, limit_ms=LIMIT_MS):
    """{kpi: {n, span_s, median, max, ge_limit, windows_failing, windows_total}}."""
    snaps = snapshots(run_dir)
    if not snaps:
        return {}
    t0 = snaps[0][0]
    result = {}
    for kpi in KPIS:
        values = [worst[kpi] for _, worst in snaps if kpi in worst]
        if not values:
            continue
        buckets = {}
        for epoch, worst in snaps:
            if kpi in worst:
                buckets.setdefault((epoch - t0) // WINDOW_S, []).append(worst[kpi])
        window_max = [max(v) for v in buckets.values()]
        result[kpi] = {
            "n": len(values),
            "span_s": snaps[-1][0] - t0,
            "median": round(statistics.median(values), 1),
            "max": round(max(values), 1),
            "ge_limit": sum(1 for v in values if v >= limit_ms),
            "windows_failing": sum(1 for v in window_max if v > limit_ms),
            "windows_total": len(window_max),
        }
    return result


def main(argv):
    as_json = "--json" in argv
    runs = [a for a in argv if not a.startswith("--")]
    if not runs:
        sys.exit(__doc__)
    scores = {run: score(run) for run in runs}
    if as_json:
        print(json.dumps(scores, indent=2, sort_keys=True))
        return 0
    for run, per_kpi in scores.items():
        if not per_kpi:
            print(f"\n{run}: no prom snapshots")
            continue
        span = next(iter(per_kpi.values()))["span_s"]
        n = next(iter(per_kpi.values()))["n"]
        print(f"\n=== {run}   snapshots={n} span={span}s")
        print(f"{'kpi':22} {'median':>7} {'max':>7} {'>=50':>8} {'failing windows':>17}")
        for kpi in KPIS:
            s = per_kpi.get(kpi)
            if not s:
                print(f"{kpi:22} {'-':>7} {'-':>7} {'-':>8} {'-':>17}")
                continue
            print(
                f"{kpi:22} {s['median']:7.1f} {s['max']:7.1f} "
                f"{s['ge_limit']:3d}/{s['n']:<3d} "
                f"{s['windows_failing']:>8}/{s['windows_total']:<6}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
