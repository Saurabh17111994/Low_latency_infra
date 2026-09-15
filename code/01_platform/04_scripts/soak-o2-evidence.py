#!/usr/bin/env python3
"""soak-o2-evidence.py — O2 window-evidence leg for soak runs.

Queries OpenObserve's Prometheus API for a fixed multi-TF query set over the
run window [t_start_utc, t_end_utc] and writes one JSONL record per query to
stdout (also saved by the caller as o2-evidence.jsonl). Each record carries
the query, point count, first/last values, and min/max — the raw truth the
plain-English scorecard's O2 section is built from.

Writes one JSONL record per SERIES (a `sum by (strategy)` query answers with
one series per strategy): each record carries the series labels, point count,
first/last values, and min/max. An answered-but-empty query keeps a single
record with n_points 0, so "zero series" stays distinguishable from "query
never ran".

Auth: env O2_AUTH_BASIC, else code/01_platform/01_docker/secrets.env (same
convention as o2_ingest.py; never printed). Endpoint:
${O2_URL:-http://localhost:5080}/api/default/prometheus/api/v1/query_range.
Plain-http O2_URL on a non-local host is refused (exit 2) because the Basic
Authorization header would go out unencrypted; O2_ALLOW_INSECURE_HTTP=1
downgrades that to a warning.

Usage:
  soak-o2-evidence.py --start 2026-09-04T12:50:00Z --end 2026-09-04T13:05:00Z \
      --run soak-e2e-20260904-182216 [--step 60s] [--job JOBID]
Exit 0 = all queries answered (even with zero points — emptiness is data);
exit 2 = usage/auth/transport error; exit 1 = O2 unreachable, a non-JSON body,
or a query hard-failed.
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
                    # P6-788 asked for rstrip("\r\n"). Note that a text-mode
                    # read already translates CRLF (universal newlines), so the
                    # CR never reached the header and the reported 401 does not
                    # reproduce: this is belt-and-braces in case the read is
                    # ever switched to newline="" or a binary handle.
                    return line.rstrip("\r\n").split("=", 1)[1]
    except OSError:
        pass
    return ""


def _check_transport(base: str) -> str:
    """Basic auth over plain http is only acceptable on this machine (P6-789).

    The default endpoint is a local OpenObserve, where the loopback hop is not
    observable; pointing O2_URL at a remote host over http would put the Basic
    credentials on the wire in the clear.
    """
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme != "http":
        return ""
    host = (parsed.hostname or "").lower()
    if host in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
        return ""
    return (f"O2_URL={base} is plain http on a non-local host ({host}) — the "
            "Basic Authorization header would be sent unencrypted; use https:// "
            "or set O2_ALLOW_INSECURE_HTTP=1 to accept the risk")


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
    root_url = os.environ.get("O2_URL", "http://localhost:5080")
    transport = _check_transport(root_url)
    if transport and os.environ.get("O2_ALLOW_INSECURE_HTTP", "") != "1":
        print(f"soak-o2-evidence: REFUSED — {transport}", file=sys.stderr)
        return 2
    if transport:
        print(f"soak-o2-evidence: WARN — {transport}", file=sys.stderr)
    base = root_url + "/api/default"
    rc = 0
    for name, q in QUERIES:
        url = (base + "/prometheus/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": q, "start": args.start, "end": args.end, "step": args.step}))
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode())
        except (urllib.error.URLError, OSError, ValueError) as e:
            # ValueError covers json.JSONDecodeError and UnicodeDecodeError: an
            # HTML error page or a non-UTF-8 body returned with HTTP 200 used
            # to escape this handler and abort the whole run, skipping every
            # remaining query instead of recording one failure (P6-547).
            print(f"soak-o2-evidence: query {name} FAILED: {e}", file=sys.stderr)
            rc = 1
            continue
        try:
            result = payload.get("data", {}).get("result", [])
            # One record PER SERIES: `sum by (strategy)` returns one series per
            # strategy, and reading only result[0] dropped every other series
            # from the evidence this scorecard is built from (P6-200). A
            # single-series query keeps its historical shape (n_points, first,
            # last) with no series label.
            records = []
            for series in result:
                vals = series.get("values", [])
                floats = [float(v[1]) for v in vals]
                metric = series.get("metric") or {}
                rec = {
                    "soak_run": args.run,
                    "job_id": args.job,
                    "t_start_utc": args.start,
                    "t_end_utc": args.end,
                    "query_name": name,
                    "query": q,
                    "series": metric,
                    "series_labels": ",".join(f"{k}={v}" for k, v in sorted(metric.items())),
                    "n_points": len(vals),
                    "first": vals[0][1] if vals else None,
                    "last": vals[-1][1] if vals else None,
                    "min": min(floats) if floats else None,
                    "max": max(floats) if floats else None,
                }
                records.append(rec)
            if not result:
                # An empty result is data, not a failure: keep one record so
                # the scorecard can see that the query was answered with zero
                # series rather than missing from the file.
                records.append({
                    "soak_run": args.run,
                    "job_id": args.job,
                    "t_start_utc": args.start,
                    "t_end_utc": args.end,
                    "query_name": name,
                    "query": q,
                    "series": {},
                    "series_labels": "",
                    "n_points": 0,
                    "first": None,
                    "last": None,
                    "min": None,
                    "max": None,
                })
        except (KeyError, IndexError, ValueError, TypeError) as e:
            print(f"soak-o2-evidence: query {name} BAD PAYLOAD: {e}", file=sys.stderr)
            rc = 1
            continue
        for rec in records:
            print(json.dumps(rec))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
