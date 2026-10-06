#!/usr/bin/env python3
"""Per-hop DAG latency from Flink's native latency tracking.

Why this lives in the repo: the p99 program's remaining terms are per-hop (the aggregator
hop at +40 ms p99), and hop numbers were previously read from a throwaway /tmp script that
could not be re-run later (2026-10-06 audit -> docs/plans/2026-10-06-p99-integrated-program.md,
Task 1). Tracking must be on when the job is submitted — it is by default:
``metrics.latency.interval: 1000`` in code/01_platform/01_docker/docker-compose.yml and
``-Dmetrics.latency.interval="${LATENCY_TRACKING_MS:-2000}"`` in
code/01_platform/04_scripts/pipeline-lib.sh.

Method: for each prom snapshot keep the worst subtask value per
(source_id, operator_id, quantile) — the straggler view — then take median/max across
snapshots and difference successive DAG stages into per-hop increments. Values are
cumulative from the source, so the hop is a subtraction, not a separate measurement.
``hop_median`` compares the same stage pair across the whole run and is the number to
quote; ``hop_max`` is the gap between independently-taken per-stage maxima (not the same
snapshot), so a negative value there is an artifact, not negative transit — read it as a
loose upper bound only.

Fluss client gauges in the same snapshot (scanner ``fetchLatencyMs`` — the source read leg —
and writer ``sendLatencyMs``) are reported alongside: fetchLatencyMs per table is the number
that tells whether the fetch-window lever (F1) is still effective.

Usage: hop_budget.py <run-dir>/main/stages [more stage dirs...]
       hop_budget.py --json <stage dir> [...]
"""

import collections
import glob
import json
import os
import re
import statistics
import sys
from pathlib import Path

LATENCY_METRIC = (
    "flink_taskmanager_job_task_latency_source_id_operator_id_operator_subtask_index_latency"
)
LABEL = re.compile(r'([A-Za-z_0-9]+)="([^"]*)"')
QUANTILES = ("0.5", "0.99")

# Vertex ids are stable across runs (hashes of the operator chain), so the labels can be
# pinned here. Sinks are listed together: their sparse streams make the markers unreliable
# (2026-10-06: p99 538-777 ms on ~3 rows/s) — read them as an artifact signal, not transit.
ORDER = [
    ("d49b076529ea4f87695ebe831a95f6bf", "1 source (raw-table-1)"),
    ("c608566043058f50e68a5930f2fe450e", "2 fingerprint-dedup"),
    ("60828025aaab0947a851fefb156ff0e6", "3 multi-tf-aggregator"),
    ("082243312e2cb726386581b3d6fb0d38", "4 strategy-host"),
    ("1dec2f1e384e55e052839f99465d1f38", "5 candidates-sink"),
    ("da10c371655c6be845e0b643f488d49d", "5 candidates-current-sink"),
    ("df20787985f46dc40528902f480d9676", "5 candle-features-sink"),
]


def labels(line):
    i, j = line.find("{"), line.rfind("}")
    return dict(LABEL.findall(line[i + 1 : j])) if i >= 0 and j >= 0 else {}


def scan(stages_dir):
    """(per_snapshot worst-subtask values, fetch gauges, send gauges)."""
    per_snapshot = []
    fetch, send = collections.defaultdict(list), collections.defaultdict(list)
    for path in sorted(glob.glob(os.path.join(stages_dir, "prom-*.txt"))):
        worst = collections.defaultdict(float)
        # read_text, not open(): a prom snapshot is ~2 MB of text and this keeps the
        # handle closed (an unclosed handle trips -W error runs and leaks a warning per file).
        for line in Path(path).read_text(errors="replace").splitlines():
            if line.startswith("#"):
                continue
            line = line.strip()
            name = line.split("{", 1)[0] if "{" in line else line.split(" ", 1)[0]
            try:
                value = float(line.rsplit(" ", 1)[1])
            except (IndexError, ValueError):
                continue
            if name.startswith(LATENCY_METRIC):
                if name.endswith("_count"):
                    continue
                lab = labels(line)
                if lab.get("quantile") not in QUANTILES:
                    continue
                key = (lab.get("source_id"), lab.get("operator_id"), lab.get("quantile"))
                worst[key] = max(worst[key], value)
            elif name.endswith("fetchLatencyMs"):
                lab = labels(line)
                fetch[(lab.get("database", "?"), lab.get("table", "?"))].append(value)
            elif name.endswith("sendLatencyMs"):
                send[labels(line).get("operator_name", "?")].append(value)
        per_snapshot.append(worst)
    return per_snapshot, fetch, send


def hop_table(stages_dir):
    """{quantile: {source_id: [{stage, cum_median, cum_max, hop_median, hop_max}]}}."""
    per_snapshot, _, _ = scan(stages_dir)
    series = collections.defaultdict(list)
    for worst in per_snapshot:
        for key, value in worst.items():
            series[key].append(value)
    table = {}
    for quantile in QUANTILES:
        by_source = {}
        for source in sorted({k[0] for k in series if k[2] == quantile}):
            chain, prev_med, prev_max = [], 0.0, 0.0
            for operator, label in ORDER:
                values = series.get((source, operator, quantile))
                if not values:
                    continue
                med, top = statistics.median(values), max(values)
                chain.append(
                    {
                        "stage": label,
                        "cum_median": round(med, 1),
                        "cum_max": round(top, 1),
                        "hop_median": round(med - prev_med, 1),
                        "hop_max": round(top - prev_max, 1),
                    }
                )
                prev_med, prev_max = med, top
            if chain:
                by_source[source] = chain
        table[quantile] = by_source
    return table


def _gauges(gauges):
    return {
        key: {"median": round(statistics.median(v), 1), "max": round(max(v), 1)}
        for key, v in sorted(gauges.items())
    }


def report(stages_dir, as_json=False):
    if not os.path.isdir(stages_dir):
        print(f"!! missing {stages_dir}")
        return None
    table = hop_table(stages_dir)
    _, fetch, send = scan(stages_dir)
    if as_json:
        print(json.dumps(
            {"stages": stages_dir, "hops": table,
             "scanner_fetch_latency_ms": {".".join(k): v for k, v in _gauges(fetch).items()},
             "writer_send_latency_ms": _gauges(send)},
            indent=2, sort_keys=True))
        return table
    print(f"\n=== {stages_dir}")
    for quantile, label in (("0.5", "p50"), ("0.99", "p99")):
        print(f"\n  [{label}] cumulative source -> stage, and the increment that stage adds")
        for source, chain in table[quantile].items():
            print(f"    source {source[:8]}…")
            for row in chain:
                print(f"      {row['stage']:26s} cum med {row['cum_median']:7.1f} "
                      f"max {row['cum_max']:7.1f}  |  hop +{row['hop_median']:6.1f} "
                      f"(worst +{row['hop_max']:7.1f})")
    print("\n  [gauges] Fluss client, worst subtask per snapshot")
    for (db, table_name), stats in _gauges(fetch).items():
        print(f"    scanner fetchLatencyMs {db}.{table_name:22s} "
              f"med {stats['median']:7.1f} max {stats['max']:7.1f}")
    for operator, stats in _gauges(send).items():
        print(f"    writer  sendLatencyMs   {operator[:34]:34s} "
              f"med {stats['median']:7.1f} max {stats['max']:7.1f}")
    return table


def main(argv):
    as_json = "--json" in argv
    dirs = [a for a in argv if not a.startswith("--")]
    if not dirs:
        sys.exit(__doc__)
    for stages_dir in dirs:
        report(stages_dir, as_json)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
