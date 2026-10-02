#!/usr/bin/env python3
"""strategy_live_board.py — what the strategy host is consuming off the live path.

Read-only board from the Flink REST API (no cluster mutation, no job change):
the SignalJob's state, the two live-path operators' throughput/busy/backpressure,
the live-path counters (real registry identifiers only — the parity gate rejects
invented names), and the platform latency histograms, each as p50/p95/p99 of the
worst subtask. Gated metrics that are absent are printed AND recorded
(metric-availability.tsv, stage-capture format) so a flag-off topology cannot
look like a silent zero.

Row VALUES are heap-only by design (DEC-059 closed-only storage: the live path
writes nothing to Fluss), so no external reader can see them. For row content:

    make watch-raw       the raw ticks the host aggregates (source of truth)
    make watch-candles   sealed candles + features the host writes at close
    make watch-signals   fired signals; --full carries the v2 audit with the
                         market snapshot the host handed the strategy at fire

Usage:
    strategy_live_board.py [--url http://localhost:8081] [--watch SECONDS]

Exit codes: 0 board printed (including the "no RUNNING job" state, which prints
the last job states and how to start one); 2 bad arguments or an unreachable
Flink REST endpoint.
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.parse
import urllib.request

DEFAULT_URL = "http://localhost:8081"
JOB_NAME = "signal-job-compute"

# The two live-path operators (see SignalJob.java: name()/uid() wiring).
LIVE_OPERATORS = ("multi-tf-aggregator", "strategy-host")

# Standard Flink metrics, fetched as a sum over subtasks.
RATE_METRICS = ("numRecordsInPerSecond", "numRecordsOutPerSecond")
ABS_METRICS = ("numRecordsIn", "numRecordsOut")
# Time metrics are per-subtask rates; the worst subtask is the honest number.
TIME_METRICS = ("busyTimeMsPerSecond", "backPressuredTimeMsPerSecond")

# Custom live-path counters, exactly as the job graph registers them (the
# registry is pinned by tests/test_compute_identifier_parity.py: an invented
# name fails the gate). The flag column is the rollout flag that registers the
# metric — when such a metric is absent it is printed AND recorded (never a
# silent zero). A missing unconditional one prints "-".
LIVE_COUNTERS = (
    ("compute.candles.live.emitted", "MULTITF_ENABLED"),
    ("compute.candles.emitted", "MULTITF_ENABLED"),
    ("compute.candles.late.dropped", "MULTITF_ENABLED"),
    ("compute.candles.gap.detected", "MULTITF_ENABLED"),
    ("compute.candles.restored_timer_noop", "MULTITF_ENABLED"),
    ("compute.session.filtered.pre_open", "MULTITF_ENABLED"),
    ("compute.session.filtered.post_close", "MULTITF_ENABLED"),
    ("compute.dedup.first", None),
    ("compute.dedup.duplicates", None),
    ("compute.invalid.rows", None),
    ("compute.signal.kv.filtered.noncanonical", None),
    ("compute.strategy.suppressed", "STRATEGY_HOST_ENABLED"),
    ("compute.strategy.failed", "STRATEGY_HOST_ENABLED"),
    ("compute.strategy.dropped.unkeyed", "STRATEGY_HOST_ENABLED"),
    ("compute.strategy.dropped.oversize", "STRATEGY_HOST_ENABLED"),
    ("compute.execution_intent.rejected", "EXECUTION_INTENT_ENABLED"),
)

# Latency histograms (Flink exposes quantiles as <name>_p50 / _p95 / _p99).
# ingest_to_monitor is unconditional; the two intent KPIs exist only when
# EXECUTION_INTENT_ENABLED is on.
LATENCY_METRICS = (
    ("compute.latency.ingest_to_monitor", None),
    ("compute.latency.tick_to_intent", "EXECUTION_INTENT_ENABLED"),
    ("compute.latency.signal_to_intent", "EXECUTION_INTENT_ENABLED"),
)
LATENCY_SUFFIXES = ("_p50", "_p95", "_p99")

GATED_FLAGS = {name: flag for name, flag in LIVE_COUNTERS + LATENCY_METRICS if flag}

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
DEFAULT_AVAILABILITY_PATH = os.path.join(REPO_ROOT, "logs", "live-board",
                                         "metric-availability.tsv")


def fetch_json(url, timeout=5):
    """GET one Flink REST URL. Raises OSError/ValueError on failure."""
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def select_running_job(overview, name=JOB_NAME):
    """The RUNNING job with ``name`` from a /jobs/overview body, else None."""
    for job in (overview or {}).get("jobs", []):
        if job.get("name") == name and job.get("state") == "RUNNING":
            return job
    return None


def recent_jobs(overview, name=JOB_NAME, limit=3):
    """The most recently started jobs with ``name``, newest first."""
    jobs = [j for j in (overview or {}).get("jobs", []) if j.get("name") == name]
    jobs.sort(key=lambda j: j.get("start-time", 0), reverse=True)
    return jobs[:limit]


def find_vertex(vertices, needle):
    """The vertex whose name contains ``needle`` (case-insensitive), else None."""
    needle = needle.lower()
    for vertex in vertices or []:
        if needle in (vertex.get("name") or "").lower():
            return vertex
    return None


def metric_values(base_url, jid, vid, name, timeout=5):
    """Per-subtask values for one metric; [] when the metric is absent."""
    url = ("%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s"
           % (base_url, jid, vid, urllib.parse.quote(name)))
    try:
        data = fetch_json(url, timeout=timeout)
    except (OSError, ValueError):
        return []
    values = []
    for entry in data or []:
        value = entry.get("value")
        if value is None:
            for key in ("sum", "avg", "min", "max"):
                if entry.get(key) is not None:
                    value = entry[key]
                    break
        if value is None or value == "NaN":
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    return values


def fmt(value, digits=1):
    if value is None:
        return "-"
    return ("%%.%df" % digits) % value


def fmt_int(value):
    if value is None:
        return "-"
    return str(int(value))


def record_availability(status, path):
    """Append stage-capture's availability record: one row per requested gated
    name, every sample: ``epoch_ms<TAB>name<TAB>yes|no``. ``path=None`` writes
    nothing (tests, dry runs) — the board text still names every absence."""
    if not path or not status:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    epoch_ms = int(time.time() * 1000)
    with open(path, "a", encoding="utf-8") as fh:
        for name in sorted(status):
            fh.write("%d\t%s\t%s\n" % (epoch_ms, name, "yes" if status[name] else "no"))


def _elapsed(start_ms):
    if not start_ms:
        return "?"
    seconds = max(0, int(time.time() - start_ms / 1000.0))
    return "%dh%02dm" % (seconds // 3600, (seconds % 3600) // 60)


def _table(headers, rows):
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = ["  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
    return lines


def render_no_job(overview):
    """Board when no SignalJob is RUNNING: say so, show the last states, hint."""
    lines = ["strategy-live :: no RUNNING %s" % JOB_NAME]
    for job in recent_jobs(overview):
        lines.append("  %-34s %-9s started %s" % (
            job.get("jid"), job.get("state"),
            dt.datetime.fromtimestamp(job.get("start-time", 0) / 1000.0)
            .strftime("%Y-%m-%d %H:%M:%S")))
    lines.append("  start it with: make day ARGS=\"start\"   (or make rollout-savepoint)")
    return "\n".join(lines)


def render_board(base_url, job, vertices, metrics, availability_path=None):
    """One board: operator table + counters + latency. ``metrics`` is a callable
    ``(vid, name) -> [values]`` so tests can inject a fake; gated metrics are
    recorded through ``record_availability(..., availability_path)``."""
    lines = ["strategy-live :: %s %s for %s (jid %s)" % (
        job.get("name"), job.get("state"), _elapsed(job.get("start-time")),
        job.get("jid"))]

    lines.append("== live-path operators ==")
    headers = ["operator", "par", "in/s", "out/s", "busy%", "bpress%", "records_in",
               "records_out"]
    rows = []
    matched = set()
    for needle in LIVE_OPERATORS:
        vertex = find_vertex(vertices, needle)
        if vertex is None:
            rows.append([needle, "-", "-", "-", "-", "-", "-", "-"])
            continue
        matched.add(vertex.get("id"))
        vid = vertex.get("id")
        rates = {name: metrics(vid, name) for name in RATE_METRICS}
        absolutes = {name: metrics(vid, name) for name in ABS_METRICS}
        times = {name: metrics(vid, name) for name in TIME_METRICS}
        rows.append([
            vertex.get("name", needle),
            str(vertex.get("parallelism", "-")),
            fmt(sum(rates["numRecordsInPerSecond"]) if rates["numRecordsInPerSecond"] else None),
            fmt(sum(rates["numRecordsOutPerSecond"]) if rates["numRecordsOutPerSecond"] else None),
            fmt(max(times["busyTimeMsPerSecond"]) / 10.0 if times["busyTimeMsPerSecond"] else None),
            fmt(max(times["backPressuredTimeMsPerSecond"]) / 10.0
                if times["backPressuredTimeMsPerSecond"] else None),
            fmt_int(sum(absolutes["numRecordsIn"]) if absolutes["numRecordsIn"] else None),
            fmt_int(sum(absolutes["numRecordsOut"]) if absolutes["numRecordsOut"] else None),
        ])
    lines.extend(_table(headers, rows))

    # Fall back to every vertex when the named operators are not found (chained
    # or renamed graphs): show the flow anyway, named.
    if not matched:
        lines.append("  (named operators not found — graph vertices:)")
        for vertex in vertices or []:
            lines.append("    %s par=%s" % (vertex.get("name"), vertex.get("parallelism")))

    lines.append("== live-path counters (sum over all vertices; - = not registered/never fired) ==")
    gated_status = {}
    for name, flag in LIVE_COUNTERS:
        value = None
        for vertex in vertices or []:
            vid = vertex.get("id")
            if vid is None:
                continue
            values = metrics(vid, name)
            if values:
                value = (value or 0.0) + sum(values)
        lines.append("  %-42s %s" % (name, fmt_int(value)))
        if flag:
            gated_status[name] = value is not None

    lines.append("== latency ms (worst subtask over all vertices) ==")
    for base_name, flag in LATENCY_METRICS:
        cells = []
        present = False
        for suffix in LATENCY_SUFFIXES:
            worst = None
            for vertex in vertices or []:
                vid = vertex.get("id")
                if vid is None:
                    continue
                values = metrics(vid, base_name + suffix)
                if values:
                    present = True
                    worst = max(values) if worst is None else max(worst, max(values))
            cells.append(fmt(worst))
        lines.append("  %-38s p50=%s  p95=%s  p99=%s"
                     % (base_name, cells[0], cells[1], cells[2]))
        if flag:
            gated_status[base_name] = present

    absent = [name for name in sorted(gated_status) if not gated_status[name]]
    if absent:
        lines.append("== metric availability (gated, absent this sample) ==")
        for name in absent:
            lines.append("  metric-availability: %s present=no (%s not observed)"
                         % (name, GATED_FLAGS[name]))
        lines.append("  recorded in: %s" % (availability_path or DEFAULT_AVAILABILITY_PATH))
    record_availability(gated_status, availability_path)

    lines.append("note: row values are heap-only (closed-only storage); use"
                 " watch-raw / watch-candles / watch-signals for content")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Strategy-host live-path board from the Flink REST API")
    parser.add_argument("--url", default=DEFAULT_URL,
                        help="Flink JobManager REST base (default %s)" % DEFAULT_URL)
    parser.add_argument("--watch", type=int, default=0, metavar="SECONDS",
                        help="refresh every SECONDS instead of printing once")
    args = parser.parse_args(argv)
    if args.watch < 0:
        parser.error("--watch must be >= 0")

    while True:
        try:
            overview = fetch_json(args.url + "/jobs/overview")
        except (OSError, ValueError) as exc:
            print("strategy-live: cannot reach Flink at %s (%s)" % (args.url, exc),
                  file=sys.stderr)
            return 2
        job = select_running_job(overview)
        if job is None:
            print(render_no_job(overview))
        else:
            jid = job["jid"]
            try:
                detail = fetch_json(args.url + "/jobs/" + jid)
            except (OSError, ValueError) as exc:
                print("strategy-live: cannot read job %s (%s)" % (jid, exc), file=sys.stderr)
                return 2
            vertices = detail.get("vertices", [])
            metrics = lambda vid, name: metric_values(args.url, jid, vid, name)  # noqa: E731
            availability = os.environ.get("STRATEGY_LIVE_BOARD_AVAILABILITY",
                                          DEFAULT_AVAILABILITY_PATH)
            print(render_board(args.url, job, vertices, metrics,
                               availability_path=availability))
        if not args.watch:
            return 0
        time.sleep(args.watch)


if __name__ == "__main__":
    sys.exit(main())
