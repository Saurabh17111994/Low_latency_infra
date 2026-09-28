#!/usr/bin/env python3
"""Snapshot the signal job's latency histograms from Flink REST (stdout and/or TSV).

Two KPIs share one histogram shape but measure different legs:

* ``ingest_to_monitor`` - ms-exact pipeline latency: ``now - raw.ingest_ts`` at the
  step-2 identity monitor (ingestion accept -> Fluss write -> Flink read -> dedup).
  This is the number to quote against an SLO on the real token feed.
* ``tick_to_strategy`` - end-to-end tick age at the strategy host. On the token feed
  the bridge maps epoch seconds to ms, so the value carries a uniform 0-1000 ms
  quantization on top of the pipeline; use the fake (ms-timestamp) feed as the
  comparable baseline, never this number alone as pipeline latency.

Each histogram is per-subtask with a short sliding window (4096 samples), so query
ONE subtask per request: a single ``get=`` list with too many ids trips Flink's
~4 KB HTTP header limit (404).

Exit codes: 0 = captured at least one metric; 2 = no single running job / vertex or
transport error; 3 = the window was empty for every metric.
"""

import argparse
import json
import statistics as st
import sys
import urllib.request

DEFAULT_JM_URL = "http://localhost:8081"
DEFAULT_JOB_NAME = "signal-job-compute"
QUANTILES = ("median", "p75", "p90", "p95", "p99", "p999")

# (metric label, operator name in the metric id, vertex-name selector)
METRICS = (
    ("ingest_to_monitor", "ingest-latency-monitor",
     lambda name: "ingest-latency-monitor" in name),
    ("tick_to_strategy", "strategy-host",
     lambda name: name.startswith("strategy-host ->")),
)


class ProbeError(Exception):
    """Input/state problem (no single running job, missing vertex)."""


def fetch_json(url, timeout=8):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def running_job(jm_url, job_name):
    overview = fetch_json(f"{jm_url}/jobs/overview")
    running = [j for j in overview.get("jobs", [])
               if j.get("state") == "RUNNING" and j.get("name") == job_name]
    if len(running) != 1:
        raise ProbeError(
            f"expected exactly one RUNNING '{job_name}', found {len(running)}")
    return running[0]["jid"]


def vertex_id(job, selector):
    for vertex in job.get("vertices", []):
        if selector(vertex.get("name", "")):
            return vertex["id"]
    raise ProbeError(f"no vertex matches the built-in {selector} selector")


def collect_rows(jm_url, jid, vid, operator, metric, subtasks):
    """Per-subtask quantile rows for one histogram; empty when the window is empty."""
    rows = {}
    for sub in range(subtasks):
        ids = ",".join(
            f"{sub}.{operator}.compute_latency_{metric}_{q}" for q in QUANTILES)
        data = fetch_json(f"{jm_url}/jobs/{jid}/vertices/{vid}/metrics?get={ids}")
        for entry in data:
            if entry.get("value") is not None:
                # The metric id ends in "<...>_<quantile>": the quantile is after
                # the LAST underscore, not after the last dot.
                rows.setdefault(sub, {})[entry["id"].rsplit("_", 1)[-1]] = \
                    float(entry["value"])
    return rows


def spread(rows, quantile):
    """(median, max) of one quantile across subtasks, or None when absent."""
    values = [r[quantile] for r in rows.values() if quantile in r]
    if not values:
        return None
    return st.median(values), max(values)


def metric_lines(label, rows):
    """The summary lines, in the house format used by the day captures."""
    parts = [f"latency {label} subtasks={len(rows)}"]
    for quantile, key in (("median", "median"), ("p95", "p95"), ("p99", "p99")):
        stats = spread(rows, quantile)
        parts.append(f"{key}_med={stats[0]:.0f}" if stats
                     else f"{key}_med=NA")
        parts.append(f"{key}_max={stats[1]:.0f}" if stats else f"{key}_max=NA")
    return " ".join(parts)


def tsv_lines(label, rows):
    lines = [f"metric={label}"]
    lines.append("subtask\t" + "\t".join(QUANTILES))
    for sub in sorted(rows):
        values = [str(rows[sub].get(q, "")) for q in QUANTILES]
        lines.append(f"{sub}\t" + "\t".join(values))
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jm-url", default=DEFAULT_JM_URL,
                        help=f"Flink JobManager REST base (default {DEFAULT_JM_URL})")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME,
                        help=f"running job name (default {DEFAULT_JOB_NAME})")
    parser.add_argument("--label", default="probe",
                        help="label recorded in the TSV and summary lines")
    parser.add_argument("--subtasks", type=int, default=8,
                        help="subtask count to query (default 8)")
    parser.add_argument("--out", help="TSV output path (all metrics, per-subtask rows)")
    args = parser.parse_args(argv)

    try:
        jid = running_job(args.jm_url, args.job_name)
        job = fetch_json(f"{args.jm_url}/jobs/{jid}")
        output = [f"# label={args.label} job={jid}"]
        captured = False
        for label, operator, selector in METRICS:
            vid = vertex_id(job, selector)
            rows = collect_rows(args.jm_url, jid, vid, operator, label,
                                args.subtasks)
            if rows:
                captured = True
                print(metric_lines(label, rows))
                output += tsv_lines(label, rows)
            else:
                print(f"latency {label} subtasks=0 (empty sliding window; "
                      f"job={jid} vertex={vid})")
        if args.out:
            with open(args.out, "w") as handle:
                handle.write("\n".join(output) + "\n")
        return 0 if captured else 3
    except (ProbeError, OSError, ValueError) as exc:
        print(f"latency_probe: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
