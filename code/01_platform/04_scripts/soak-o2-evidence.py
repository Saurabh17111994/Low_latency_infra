#!/usr/bin/env python3
"""soak-o2-evidence.py — O2 window-evidence leg for soak runs.

Queries OpenObserve's Prometheus API for a fixed multi-TF query set over the
run window [t_start_utc, t_end_utc] and writes one JSONL record per query to
stdout (also saved by the caller as o2-evidence.jsonl). Each record carries
the query, point count, first/last values, and min/max — the raw truth the
plain-English scorecard's O2 section is built from.

Auth: env O2_AUTH_BASIC, else code/01_platform/01_docker/secrets.env (same
convention as o2_ingest.py; never printed). Endpoint:
${O2_URL:-http://localhost:5080}/api/default/prometheus/api/v1/query_range.

Usage:
  soak-o2-evidence.py --start 2026-09-04T12:50:00Z --end 2026-09-04T13:05:00Z \
      --run soak-e2e-20260904-182216 [--step 60s] [--job JOBID]
Exit 0 = all queries answered (even with zero points — emptiness is data);
exit 2 = usage/auth error; exit 1 = O2 unreachable or a query hard-failed.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_SECRETS = os.path.join(_HERE, "..", "01_docker", "secrets.env")

QUERIES = [
    ("session_filtered_post_close",
     "sum(flink_taskmanager_job_task_operator_compute_session_filtered_post_close)"),
    ("session_filtered_pre_open",
     "sum(flink_taskmanager_job_task_operator_compute_session_filtered_pre_open)"),
    ("aggregator_in_per_s",
     'sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="multi_tf_aggregator"})'),
    ("source_in_per_s",
     'sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="Source:_raw_table_1____raw_validation"})'),
    ("new_closed_sink_per_s",
     'sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="candle_closed_sink:_Writer"})'),
    ("new_live_sink_per_s",
     'sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="candle_live_sink:_Writer"})'),
    ("multitf_signal_emitted",
     'sum by (strategy) (flink_taskmanager_job_task_operator_strategy_emitted)'),
    ("multitf_signal_suppressed",
     'sum by (strategy) (flink_taskmanager_job_task_operator_strategy_suppressed)'),
    ("multitf_duplicate_window",
     "max(flink_taskmanager_job_task_operator_compute_candles_multitf_duplicate_window)"),
    ("new_signal_sink_per_s",
     'sum(flink_taskmanager_job_task_numrecordsinpersecond{task_name="strategy_host_candidates_current_sink:_Writer"})'),
]


def _auth_basic() -> str:
    val = os.environ.get("O2_AUTH_BASIC", "")
    if val:
        return val
    path = os.environ.get("O2_SECRETS_FILE", _DEFAULT_SECRETS)
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("O2_AUTH_BASIC="):
                    return line.rstrip("\n").split("=", 1)[1]
    except OSError:
        pass
    return ""


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True, help="window start RFC3339 UTC")
    ap.add_argument("--end", required=True, help="window end RFC3339 UTC")
    ap.add_argument("--run", required=True, help="soak run id (evidence label)")
    ap.add_argument("--step", default="60s")
    ap.add_argument("--job", default="")
    args = ap.parse_args()
    auth = _auth_basic()
    if not auth:
        print("soak-o2-evidence: REFUSED — no O2_AUTH_BASIC (env or "
              f"{_DEFAULT_SECRETS})", file=sys.stderr)
        return 2
    base = os.environ.get("O2_URL", "http://localhost:5080") + "/api/default"
    rc = 0
    for name, q in QUERIES:
        url = (base + "/prometheus/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": q, "start": args.start, "end": args.end, "step": args.step}))
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode())
        except (urllib.error.URLError, OSError) as e:
            print(f"soak-o2-evidence: query {name} FAILED: {e}", file=sys.stderr)
            rc = 1
            continue
        try:
            result = payload.get("data", {}).get("result", [])
            vals = result[0]["values"] if result else []
            floats = [float(v[1]) for v in vals]
            rec = {
                "soak_run": args.run,
                "job_id": args.job,
                "t_start_utc": args.start,
                "t_end_utc": args.end,
                "query_name": name,
                "query": q,
                "n_points": len(vals),
                "first": vals[0][1] if vals else None,
                "last": vals[-1][1] if vals else None,
                "min": min(floats) if floats else None,
                "max": max(floats) if floats else None,
            }
        except (KeyError, IndexError, ValueError, TypeError) as e:
            print(f"soak-o2-evidence: query {name} BAD PAYLOAD: {e}", file=sys.stderr)
            rc = 1
            continue
        print(json.dumps(rec))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
