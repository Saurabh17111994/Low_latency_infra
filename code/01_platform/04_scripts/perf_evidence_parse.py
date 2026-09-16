#!/usr/bin/env python3
"""perf_evidence_parse.py — build one perf-evidence snapshot from raw collector input.

Companion to perf-evidence-collector.sh. The shell script owns the polling loop
(timing, traps, docker/flink/O2 calls); this module owns every decision that
turns raw text into the snapshot JSON and the summary row, so the arithmetic is
testable without a live cluster. Same shape as stage_capture_parse.py.

Reads one JSON job on stdin:

    {"snapshot": 1, "started_at": "2026-09-16T10:00:00Z", "jid": "…",
     "job": {...},            # GET /jobs/<jid>
     "metrics": {...},        # {vertex_id: <raw /subtasks/metrics response>}
     "checkpoints": {...},    # GET /jobs/<jid>/checkpoints
     "docker_stats": "…",     # raw `docker stats --no-stream --format` text
     "o2": {"count":…, "sum":…, "p50":…, "p99":…}}

Writes the snapshot to <snap_dir>/snap-<n>.json (atomically) and prints one
summary row on stdout.

Findings implemented here (P6-136, P6-137, P6-465, P6-466):

  * P6-136 — Flink answers /subtasks/metrics?get=… with ONE aggregate object
    per metric ({"id":"numRecordsIn","min":…,"max":…,"avg":…,"sum":…}), so the
    old code, which treated each entry as one subtask, always wrote
    n_subtasks=1 and used the per-subtask MEAN as the job throughput. Job
    throughput is the SUM; min/max/avg are preserved so skew is visible. The
    per-subtask shape ({"id":"0.numRecordsIn","value":"123"}) is still accepted
    and aggregated here.

  * P6-137 — the collector's header advertises a latency category that never had
    any code behind it. append_latency_ms is an OpenObserve metric (the ingestion
    service's OTLP histogram), NOT a Flink metric, so it arrives from the shell
    as o2.* and is recorded under "latency".

  * P6-465 — docker stats memory arrived as "229.6MiB / 15.46GiB" strings, which
    cannot be compared or summed (MiB vs GiB, and "9" sorts above "15"). Memory
    is normalized to bytes and CPU to a float, and every role is kept rather
    than only the taskmanager and ingestion containers.

  * P6-466 — the header and every row are generated from ONE column definition
    (COLUMNS), so an error row can no longer carry 2 fields under an 8-field
    header. The snapshot is written to a temp file and os.replace()d, so a kill
    mid-write cannot leave a truncated JSON that the next read calls ERROR. The
    timestamp is captured by the caller when the snapshot STARTS, not after the
    slow queries.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

# Container-name fragment -> role. Ordered: first match wins, so the more
# specific "taskmanager" is tested before the generic "flink".
ROLE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("jobmanager", "jm"),
    ("taskmanager", "tm"),
    ("fluss-tablet", "fluss_tablet"),
    ("fluss-coordinator", "fluss_coordinator"),
    ("ingestion", "ingestion"),
)

_MEM_UNITS = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}

# Throughput metrics whose job-level value is the SUM across subtasks.
# Everything else is a per-subtask rate, where avg/max describe the subtasks and
# the sum would be meaningless as a per-second figure.
THROUGHPUT_METRICS = frozenset(
    {"numRecordsInPerSecond", "numRecordsOutPerSecond"}
)

# One request budget for every Flink call. P6-464: `curl -s` with no --max-time
# let a wedged JobManager block the poll loop past INTERVAL, so snapshots
# overlapped and their timestamps stopped describing the sample.
CURL_TIMEOUT_S = 8
SUBPROCESS_TIMEOUT_S = 12

FLINK_METRICS = (
    "busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond,"
    "numRecordsInPerSecond,numRecordsOutPerSecond"
)

def _docker_exec(container: str, url: str) -> Any:
    """GET a Flink REST URL through `docker exec`. None on any failure.

    Every failure mode an operator can hit (daemon down, container gone, JM still
    starting, slow response, non-JSON body) returns None; the caller records it as
    a gap in the snapshot. P6-011: the old code let all of these abort the whole
    capture.
    """
    argv = [
        "docker", "exec", container,
        "curl", "-s", "--max-time", str(CURL_TIMEOUT_S), url,
    ]
    try:
        r = subprocess.run(
            argv, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    try:
        return json.loads(r.stdout)
    except (json.JSONDecodeError, TypeError):
        return None


def find_job_id(container: str, name_pattern: str) -> str | None:
    """The RUNNING job whose name matches name_pattern. None when there is none."""
    over = _docker_exec(container, "http://localhost:8081/jobs/overview")
    jobs = (over or {}).get("jobs") or []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        if str(j.get("state")) != "RUNNING":
            continue
        if name_pattern and name_pattern.lower() not in str(j.get("name", "")).lower():
            continue
        jid = j.get("jid")
        if jid:
            return str(jid)
    return None


def fetch_snapshot_inputs(container: str, jid: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """(job, {vertex_id: metrics}, checkpoints) for one job, vertices in parallel."""
    job = _docker_exec(container, f"http://localhost:8081/jobs/{jid}") or {}
    vertices = [v for v in (job.get("vertices") or []) if isinstance(v, dict)]
    metrics: dict[str, Any] = {}
    if vertices:
        with ThreadPoolExecutor(max_workers=min(8, len(vertices))) as pool:
            futures = {
                v.get("id"): pool.submit(
                    _docker_exec,
                    container,
                    f"http://localhost:8081/jobs/{jid}/vertices/{v.get('id')}"
                    f"/subtasks/metrics?get={FLINK_METRICS}",
                )
                for v in vertices
                if v.get("id")
            }
            for vid, fut in futures.items():
                got = fut.result()
                if isinstance(got, list) and got:
                    metrics[vid] = got
    ckpt = _docker_exec(container, f"http://localhost:8081/jobs/{jid}/checkpoints") or {}
    return job, metrics, ckpt


def docker_stats_text() -> str:
    """Raw `docker stats` output. Empty string when docker is unavailable."""
    try:
        r = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}"],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT_S + 10,
        )
    except (subprocess.TimeoutExpired, OSError):
        return ""
    return r.stdout or ""


def parse_mem(text: str) -> tuple[int | None, int | None]:
    """'229.6MiB / 15.46GiB' -> (used_bytes, limit_bytes). Unparseable -> (None, None)."""
    parts = [p.strip() for p in text.split("/")]
    if len(parts) != 2:
        return None, None

    def one(s: str) -> int | None:
        m = re.fullmatch(r"([0-9.]+)\s*([A-Za-z]+)", s)
        if not m:
            return None
        unit = _MEM_UNITS.get(m.group(2).upper())
        if unit is None:
            return None
        try:
            return int(float(m.group(1)) * unit)
        except ValueError:
            return None

    return one(parts[0]), one(parts[1])


def parse_docker_stats(text: str) -> list[dict[str, Any]]:
    """Parse raw `--format '{{.Name}}|{{.MemUsage}}|{{.CPUPerc}}'` output.

    No grep upstream: a phrase that matches no container used to kill the whole
    capture through `grep`'s exit 1 under pipefail (P6-011). Filtering by role
    happens here instead, where "no match" is an empty list.
    """
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 2:
            continue
        name = parts[0]
        used, limit = parse_mem(parts[1])
        cpu_raw = parts[2].strip() if len(parts) > 2 else ""
        try:
            cpu = float(cpu_raw.rstrip("%"))
        except ValueError:
            cpu = None
        rows.append(
            {
                "name": name,
                "role": next(
                    (role for frag, role in ROLE_PATTERNS if frag in name), "other"
                ),
                "mem_used_bytes": used,
                "mem_limit_bytes": limit,
                "cpu_perc": cpu,
            }
        )
    return rows


def _aggregate_from(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Normalize a /subtasks/metrics response into {metric: min,max,avg,sum,n}."""
    agg: dict[str, dict[str, Any]] = {}

    def slot(name: str) -> dict[str, Any]:
        return agg.setdefault(name, {"min": None, "max": None, "avg": None, "sum": None, "n_subtasks": 0})

    for x in items:
        if not isinstance(x, dict):
            continue
        mid = x.get("id")
        if not isinstance(mid, str) or not mid:
            continue
        # Per-subtask form: "<subtask-index>.<metric>".
        sub = re.fullmatch(r"(\d+)\.(.+)", mid)
        if sub:
            s = slot(sub.group(2))
            try:
                val = float(str(x.get("value", "")))
            except ValueError:
                continue
            s["min"] = val if s["min"] is None else min(s["min"], val)
            s["max"] = val if s["max"] is None else max(s["max"], val)
            s["sum"] = (s["sum"] or 0.0) + val
            s["n_subtasks"] += 1
            continue
        # Aggregate form: the values arrive precomputed, including the
        # cross-subtask "sum" and the subtask count when Flink supplies one.
        s = slot(mid)
        for key in ("min", "max", "avg", "sum"):
            v = x.get(key)
            if isinstance(v, (int, float)):
                s[key] = float(v)
        n = x.get("subtaskCount", x.get("n_subtasks"))
        if isinstance(n, int) and n > 0:
            s["n_subtasks"] = n

    for name, s in agg.items():
        if s["avg"] is None and s["sum"] is not None and s["n_subtasks"]:
            s["avg"] = s["sum"] / s["n_subtasks"]
        for key, v in s.items():
            if key != "n_subtasks" and isinstance(v, float):
                s[key] = round(v, 2)
    return agg


def parse_vertex_metrics(
    job: dict[str, Any], metrics: dict[str, Any]
) -> dict[str, Any]:
    """Per-vertex metric detail, keyed by vertex name."""
    names = {
        v.get("id"): v.get("name", "?")
        for v in job.get("vertices", [])
        if isinstance(v, dict)
    }
    out: dict[str, Any] = {}
    for vid, raw in metrics.items():
        items = raw if isinstance(raw, list) else raw.get("metrics", []) if isinstance(raw, dict) else []
        agg = _aggregate_from(items)
        if agg:
            out[names.get(vid, vid)] = agg
    return out


def job_totals(per_vertex: dict[str, Any]) -> dict[str, Any]:
    """Job-level rollup: throughput summed, busy/backpressure as the worst subtask."""
    totals: dict[str, Any] = {}
    for metric in THROUGHPUT_METRICS:
        s = sum(
            (v[metric]["sum"] or 0.0)
            for v in per_vertex.values()
            if metric in v and v[metric].get("sum") is not None
        )
        if any(metric in v for v in per_vertex.values()):
            totals[metric] = round(s, 2)
    for metric, key in (
        ("busyTimeMsPerSecond", "busy_max_ms_per_s"),
        ("backPressuredTimeMsPerSecond", "backpressured_max_ms_per_s"),
        ("idleTimeMsPerSecond", "idle_max_ms_per_s"),
    ):
        vals = [
            v[metric]["max"]
            for v in per_vertex.values()
            if metric in v and v[metric].get("max") is not None
        ]
        if vals:
            totals[key] = round(max(vals), 2)
    return totals


def build_latency(o2: dict[str, Any] | None) -> dict[str, Any]:
    """P6-137: latency category from OpenObserve, with its provenance recorded.

    `source` says where the number came from; `reason` says why it is missing
    when it is. Keeping both means a reader can tell an idle emitter from a
    broken one without re-running the query by hand.
    """
    o2 = o2 or {}
    count = o2.get("count")
    total = o2.get("sum")
    p99 = o2.get("p99")
    p50 = o2.get("p50")
    mean = None
    if isinstance(count, (int, float)) and count and isinstance(total, (int, float)):
        mean = round(total / count, 2)
    # The header documents "p99 fallback = mean"; say which one was served
    # rather than silently presenting a mean as a p99.
    if p99 is not None:
        source = "histogram_quantile"
    elif mean is not None:
        source = "mean_fallback"
        p99 = mean
    else:
        source = "unavailable"
    reason = o2.get("reason")
    if reason is None and source == "unavailable":
        # An absent O2 block entirely (no base URL, no credential) is a
        # configuration gap, not a stale series.
        reason = "not_configured" if not o2 else "unreachable"
    return {
        "count": count,
        "sum": total,
        "mean_ms": mean,
        "p50_ms": p50,
        "p99_ms": p99,
        # Recorded because the p50/p99 gauges need not exist (they do not in
        # this deployment) — a reader must be able to tell "no quantiles here"
        # from "quantiles were skipped" without re-running the query.
        "quantile_reason": o2.get("quantile_reason", ""),
        "source": source,
        "reason": reason or "",
    }


def _num(v: Any) -> Any:
    return "" if v is None else v


def _role_value(stats: list[dict[str, Any]], role: str, field: str) -> Any:
    for s in stats:
        if s["role"] == role:
            return s.get(field)
    return None


# ONE definition drives both the header and every row, so the two cannot drift.
COLUMNS: tuple[tuple[str, Any], ...] = (
    ("snap", lambda d, n: n),
    ("ts", lambda d, n: d["ts"]),
    ("errors", lambda d, n: d.get("errors", "")),
    ("ckpt_completed", lambda d, n: d["checkpoints"].get("counts", {}).get("completed", "")),
    ("ckpt_latest_size", lambda d, n: d["checkpoints"].get("latest_completed", {}).get("checkpointed_size", "")),
    ("ckpt_duration_ms", lambda d, n: d["checkpoints"].get("latest_completed", {}).get("end_to_end_duration", "")),
    ("jm_mem_mib", lambda d, n: _mib(_role_value(d["docker_stats"], "jm", "mem_used_bytes"))),
    ("tm_mem_mib", lambda d, n: _mib(_role_value(d["docker_stats"], "tm", "mem_used_bytes"))),
    ("fluss_tablet_mem_mib", lambda d, n: _mib(_role_value(d["docker_stats"], "fluss_tablet", "mem_used_bytes"))),
    ("fluss_coord_mem_mib", lambda d, n: _mib(_role_value(d["docker_stats"], "fluss_coordinator", "mem_used_bytes"))),
    ("ingestion_mem_mib", lambda d, n: _mib(_role_value(d["docker_stats"], "ingestion", "mem_used_bytes"))),
    ("tm_cpu_pct", lambda d, n: _num(_role_value(d["docker_stats"], "tm", "cpu_perc"))),
    ("rec_in_per_s", lambda d, n: _num(d["job_totals"].get("numRecordsInPerSecond"))),
    ("rec_out_per_s", lambda d, n: _num(d["job_totals"].get("numRecordsOutPerSecond"))),
    ("busy_max_ms_per_s", lambda d, n: _num(d["job_totals"].get("busy_max_ms_per_s"))),
    ("backpressured_max_ms_per_s", lambda d, n: _num(d["job_totals"].get("backpressured_max_ms_per_s"))),
    ("latency_count", lambda d, n: _num(d["latency"].get("count"))),
    ("latency_mean_ms", lambda d, n: _num(d["latency"].get("mean_ms"))),
    ("latency_p99_ms", lambda d, n: _num(d["latency"].get("p99_ms"))),
    ("latency_source", lambda d, n: d["latency"].get("source", "")),
    ("latency_reason", lambda d, n: d["latency"].get("reason", "")),
    ("latency_quantile_reason", lambda d, n: d["latency"].get("quantile_reason", "")),
)


def _mib(v: Any) -> Any:
    return "" if v is None else round(v / 1024**2, 1)


def header_row() -> str:
    return "\t".join(name for name, _ in COLUMNS)


def summary_row(doc: dict[str, Any], n: int) -> str:
    return "\t".join(str(fn(doc, n)).replace("\t", " ") for _, fn in COLUMNS)


def error_row(n: int, msg: str) -> str:
    """Same field count as header_row() — the P6-466 fix, by construction."""
    vals = {name: "" for name, _ in COLUMNS}
    vals["snap"] = str(n)
    vals["errors"] = f"ERROR {msg}".replace("\t", " ")
    return "\t".join(vals[name] for name, _ in COLUMNS)


def write_snapshot(path: str, doc: dict[str, Any]) -> None:
    """P6-466: temp file + os.replace, so a kill mid-write cannot truncate it."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def build_doc(payload: dict[str, Any]) -> dict[str, Any]:
    job = payload.get("job") or {}
    per_vertex = parse_vertex_metrics(job, payload.get("metrics") or {})
    ckpt_raw = payload.get("checkpoints") or {}
    stats = parse_docker_stats(payload.get("docker_stats") or "")
    doc: dict[str, Any] = {
        "snapshot": payload.get("snapshot"),
        # Captured by the caller when the snapshot STARTED (P6-466) so the stamp
        # describes when the sample was taken, not when the queries finished.
        "ts": payload.get("started_at"),
        "duration_ms": payload.get("duration_ms"),
        "jid": payload.get("jid") or "",
        "errors": payload.get("errors") or "",
        "vertices": [
            {"name": v.get("name"), "par": v.get("parallelism"), "id": v.get("id")}
            for v in job.get("vertices", [])
            if isinstance(v, dict)
        ],
        "per_vertex_metrics": per_vertex,
        "job_totals": job_totals(per_vertex),
        "checkpoints": {
            "counts": ckpt_raw.get("counts", {}),
            "latest_completed": (ckpt_raw.get("latest", {}) or {}).get("completed", {}),
        },
        "docker_stats": stats,
        "latency": build_latency(payload.get("o2")),
    }
    return doc


def read_o2_auth(env_file: str) -> str:
    """Read O2_AUTH_BASIC from an env file. Never prints the value.

    Strips the key prefix rather than splitting on '=' (P6-028): a base64 secret
    ending in '=' or '==' lost its padding to an `-F=` split and every request
    401'd.
    """
    try:
        with open(env_file, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.startswith("O2_AUTH_BASIC="):
                    v = line.split("=", 1)[1]
                    return v.strip().strip("\"'").replace("\r", "")
    except OSError:
        pass
    return ""


def query_o2(base: str, auth_file: str, sql: str) -> tuple[Any, str]:
    """One OpenObserve metric query -> (value|None, reason).

    The credential travels in a curl config file (-K), not argv (P6-029): a
    command line is world-readable via /proc/<pid>/cmdline.

    `reason` separates the ways a value can be absent, because they need
    different operator responses and a bare null cannot tell them apart:
      ""                 — a value was returned
      "unreachable"      — O2 could not be reached (down, bad credential, wrong
                           URL). Fix O2 or the credential.
      "no_fresh_samples" — O2 answered for a series that exists, but nothing
                           landed in the window. append_latency_ms is emitted by
                           the ingestion service over OTLP, so a stopped
                           ingestion container leaves the series stale. Look at
                           the emitter, NOT at O2.
      "no_series"        — the metric does not exist in this O2 at all (HTTP
                           4xx). Nothing to fix; the caller falls back.
    """
    import time
    import urllib.error
    import urllib.request

    now = int(time.time() * 1_000_000)
    body = json.dumps(
        {"query": {"sql": sql, "start_time": now - 3_600_000_000, "end_time": now, "size": 5}}
    ).encode()
    # A curl config file is what -K consumes; the header is the only entry.
    try:
        with open(auth_file, encoding="utf-8") as fh:
            header = fh.read().strip()
    except OSError:
        return None, "unreachable"
    if not header:
        return None, "unreachable"
    token = header.split("Basic", 1)[-1].strip().strip('"')
    req = urllib.request.Request(
        f"{base}/api/default/_search?type=metrics",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Basic {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=CURL_TIMEOUT_S) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as e:
        # A 4xx means O2 is up and answered — the query or the series is the
        # problem, which is a different operator action from an outage. Measured
        # 2026-09-16: O2 returns 400 for a series that does not exist, and
        # append_latency_ms_p50 / _p99 do NOT exist in this deployment (only
        # append_latency_ms, _bucket, _count, _max, _min, _sum), so the
        # documented "p99 fallback = mean" is what always applies here.
        return None, "no_series" if 400 <= e.code < 500 else "unreachable"
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError):
        return None, "unreachable"
    hits = payload.get("hits") or []
    if not hits:
        return None, "no_fresh_samples"
    src = hits[0].get("_source") or {}
    raw = src.get("value", hits[0].get("value"))
    try:
        return float(raw), ""
    except (TypeError, ValueError):
        return None, "no_fresh_samples"


def fetch_o2_latency(base: str, auth_file: str) -> dict[str, Any]:
    """The latency category's inputs (P6-137). All-None when O2 is unreachable.

    The top-level reason describes the COUNT/SUM pair, which is what actually
    carries the latency figure. The p50/p99 gauges are optional by design (the
    header documents a mean fallback), so their absence must not mark the whole
    category unavailable.
    """
    q = 'select value from "{}" order by _timestamp desc limit 1'
    out: dict[str, Any] = {"reason": "", "quantile_reason": ""}
    primary: set[str] = set()
    quantile: set[str] = set()
    for key, metric in (
        ("count", "append_latency_ms_count"),
        ("sum", "append_latency_ms_sum"),
        ("p50", "append_latency_ms_p50"),
        ("p99", "append_latency_ms_p99"),
    ):
        value, reason = query_o2(base, auth_file, q.format(metric))
        out[key] = value
        if reason:
            (quantile if key in ("p50", "p99") else primary).add(reason)
    # "unreachable" wins when both occur: a dead O2 makes the stale-window
    # finding meaningless, and reporting the stronger cause avoids sending an
    # operator to look at the emitter when the collector cannot see O2 at all.
    for bucket, key in ((primary, "reason"), (quantile, "quantile_reason")):
        if "unreachable" in bucket:
            out[key] = "unreachable"
        elif "no_fresh_samples" in bucket:
            out[key] = "no_fresh_samples"
        elif "no_series" in bucket:
            out[key] = "no_series"
    return out


def run_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Fetch everything for one snapshot and return the finished doc."""
    import datetime

    started = payload.get("started_at") or datetime.datetime.now(
        datetime.timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    container = payload.get("jm_container") or "01_docker-flink-jobmanager-1"
    pattern = payload.get("job_name_pattern") or "signal"
    errors: list[str] = []

    jid = payload.get("jid")
    if jid is None:
        jid = find_job_id(container, pattern) or ""
    if not jid:
        errors.append(f"no RUNNING job matching {pattern!r}")

    job: dict[str, Any] = {}
    metrics: dict[str, Any] = {}
    ckpt: dict[str, Any] = {}
    if jid:
        job, metrics, ckpt = fetch_snapshot_inputs(container, jid)
        if not job:
            errors.append(f"job {jid}: no job detail from {container}")

    stats_text = docker_stats_text()
    if not stats_text.strip():
        errors.append("docker stats returned nothing")

    o2 = payload.get("o2")
    if o2 is None and payload.get("o2_base"):
        if payload.get("o2_auth_file"):
            o2 = fetch_o2_latency(payload["o2_base"], payload["o2_auth_file"])
            # Only a problem with a CONFIGURED source is an error. An absent
            # credential is a deliberate choice (the collector is documented to
            # run without O2), and flagging it would make every such capture
            # report itself as damaged. A stale series or a refused request
            # during a bench run is a genuine gap in a promised category.
            if o2.get("reason") in ("unreachable", "no_fresh_samples"):
                errors.append(f"latency: {o2['reason']}")
        else:
            o2 = {"reason": "not_configured"}

    return build_doc(
        {
            "snapshot": payload.get("snapshot"),
            "started_at": started,
            "jid": jid,
            "job": job,
            "metrics": metrics,
            "checkpoints": ckpt,
            "docker_stats": stats_text,
            "o2": o2,
            "errors": "; ".join(errors),
        }
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "--stdin-doc":
        # Test/offline seam: a fully-formed input payload on stdin.
        payload = json.load(sys.stdin)
        doc = build_doc(payload)
    else:
        payload = json.load(sys.stdin)
        doc = run_snapshot(payload)
    out_dir = payload["out_dir"]
    n = payload["snapshot"]
    write_snapshot(f"{out_dir}/snapshots/snap-{n}.json", doc)
    print(summary_row(doc, n))
    return 0


if __name__ == "__main__":
    sys.exit(main())
