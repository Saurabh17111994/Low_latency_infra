#!/usr/bin/env python3
"""fused_timeline.py — one timeline of every observability signal for a run.

2026-09-02 decline hunt: the "fused single timeline" ChatGPT prescribed —
throughput, checkpoint duration+ALIGNMENT, per-op disk latency + queue depth,
RocksDB compaction/flush/SST growth, GC, backpressure — correlated against
time on ONE grid, sourced EXCLUSIVELY from OpenObserve (single source of
truth policy: metrics via PromQL query_range, io-latency + checkpoint events
via O2 SQL over the host_io_latency / flink_checkpoints streams the capture
scripts pushed).

Usage:
  fused_timeline.py --capture <capture-dir> [--start EPOCH] [--end EPOCH]
                    [--step 2] [--out <capture>/stages/fused-timeline.tsv]

Window resolution (first that works):
  1. explicit --start/--end
  2. io-latency.tsv epoch range (probe covers DURATION+30s)
  3. flink-checkpoints.jsonl first/last trigger_timestamp
Exit codes: 0 ok; 3 config (no auth/window); 4 O2 query failed (reason);
5 window found but NO signals returned (nothing to fuse — reason says which).
"""
import argparse
import csv
import datetime as _dt
import io as _io
import json
import os
import sys
import time
import urllib.parse
import urllib.request

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from o2_ingest import _auth_basic  # noqa: E402  (same auth resolution)

O2_URL = os.environ.get("O2_URL", "http://localhost:5080")
DEFAULT_DEVICE = os.environ.get("IO_PROBE_DEVICE", "nvme0n1")


def _o2_get(path, params, auth):
    url = O2_URL + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url, headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _o2_search(sql, start_s, end_s, auth):
    body = json.dumps({
        "query": {"sql": sql,
                  "start_time": int(start_s) * 1_000_000,
                  "end_time": int(end_s) * 1_000_000,
                  "from": 0, "size": 5000}}).encode()
    req = urllib.request.Request(
        O2_URL + "/api/default/_search", data=body, method="POST",
        headers={"Authorization": f"Basic {auth}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _prom_range(query, start_s, end_s, step_s, auth):
    data = _o2_get("/api/default/prometheus/api/v1/query_range",
                   {"query": query, "start": start_s, "end": end_s,
                    "step": step_s}, auth)
    if data.get("status") != "success":
        raise RuntimeError(f"promql failed for {query[:60]!r}: "
                           f"{str(data)[:200]}")
    out = []  # (series_labels, [(epoch, value)])
    for res in data["data"]["result"]:
        vals = [(int(t), float(v)) for t, v in res["values"]]
        out.append((res.get("metric", {}), vals))
    return out


# RocksDB: (column, metric suffix, hot-operator substrings). The operator
# segment in the metric name is matched against these substrings; the dedup
# and forming-bar operators are the per-record-cost hot spots under study.
RDB_SIGNALS = [
    ("rdb_compact", "num_running_compactions", ("fingerprint_dedup", "forming_bar")),
    ("rdb_pending", "compaction_pending", ("fingerprint_dedup", "forming_bar")),
    ("rdb_flush", "num_running_flushes", ("fingerprint_dedup", "forming_bar")),
    ("rdb_sst_gib", "total_sst_files_size", ("fingerprint_dedup", "forming_bar")),
    ("rdb_memtable_gib", "size_all_mem_tables", ("fingerprint_dedup", "forming_bar")),
    ("rdb_pending_cpt_gib", "estimate_pending_compaction_bytes", ("fingerprint_dedup", "forming_bar")),
    ("rdb_keys", "estimate_num_keys", ("fingerprint_dedup", "forming_bar")),
    ("rdb_l0_versions", "num_live_versions", ("fingerprint_dedup", "forming_bar")),
]


def _fetch_rdb_signals(start, end, grid, auth, fused, warnings):
    """Enumerate per-operator rocksdb metric names, query each exactly,
    aggregate max across subtasks/operators client-side, forward-fill."""
    try:
        names = _o2_get(
            "/api/default/prometheus/api/v1/label/__name__/values", {}, auth)
        all_names = names.get("data", [])
    except (urllib.error.URLError, OSError, RuntimeError, ValueError) as e:
        warnings.append(f"rocksdb: name enumeration failed: {str(e)[:100]}")
        return
    for col, suffix, ops in RDB_SIGNALS:
        want = [n for n in all_names
                if n.endswith(f"_rocksdb_{suffix}")
                and any(o in n for o in ops)]
        if not want:
            warnings.append(f"{col}: no per-operator series in index "
                            f"(suffix rocksdb_{suffix}, ops {ops})")
            continue
        by_t = {}
        for n in want:
            try:
                series = _prom_range(n, start - 60, end + 30, 15, auth)
            except (RuntimeError, urllib.error.URLError, OSError) as e:
                warnings.append(f"{col}: query failed for {n[-40:]}: "
                                f"{str(e)[:80]}")
                continue
            for _, vals in series:
                for t, v in vals:
                    by_t[t] = max(by_t.get(t, v), v)
        if not by_t:
            warnings.append(f"{col}: names matched but no points in window")
            continue
        scale = 1073741824.0 if col.endswith("_gib") else 1.0
        last_val = None
        for g in grid:
            cand = [t for t in by_t if t <= g]
            if cand:
                last_val = by_t[max(cand)] / scale
            if last_val is not None:
                fused[g][col] = last_val


def resolve_window(args):
    if args.start and args.end:
        return int(args.start), int(args.end)
    cap = args.capture
    if not cap or not os.path.isdir(cap):
        return None, None
    tsv = os.path.join(cap, "stages", "io-latency.tsv")
    if os.path.isfile(tsv):
        epochs = []
        with open(tsv, encoding="utf-8") as fh:
            for row in csv.DictReader(fh, delimiter="\t"):
                try:
                    epochs.append(int(row["epoch_s"]))
                except (KeyError, ValueError, TypeError):
                    pass
        if epochs:
            return min(epochs), max(epochs) + 2
    jl = os.path.join(cap, "flink-checkpoints.jsonl")
    if os.path.isfile(jl):
        last = None
        with open(jl, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    try:
                        last = json.loads(line)
                    except json.JSONDecodeError:
                        pass
        if last:
            trig = [e.get("trigger_timestamp") for e in last.get("events", [])
                    if isinstance(e.get("trigger_timestamp"), (int, float))]
            if trig:
                # trigger_timestamp is ms; window = last checkpoint - 130s
                end = int(max(trig) / 1000)
                return end - 130, end
    return None, None


# Each fused signal: (column prefix, PromQL, aggregator over series at a
# timestamp: "sum", "max", or None=skip). Grid: step=2s (io probe cadence);
# prom scrape is 15s — values forward-fill onto the grid.
PROM_SIGNALS = [
    # throughput: ALL tasks' records-out summed = pipeline emission; source
    # task specifically is matched by task_name regex (Flink names source
    # tasks "Source: ..." for connectors; ours is a custom source — the sum
    # over numRecordsOut of the FIRST task is unreliable, so emit the sum
    # over all tasks AND per-task max in/out for orientation).
    ("src_out", 'sum(flink_taskmanager_job_task_numrecordsoutpersecond)', "sum"),
    ("records_in", 'sum(flink_taskmanager_job_task_numrecordsinpersecond)', "sum"),
    ("busy_max",
     'max(flink_taskmanager_job_task_busytimemspersecond)', "max"),
    ("bp_max",
     'max(flink_taskmanager_job_task_backpressuredtimemspersecond)', "max"),
    ("gc_rate",
     'sum(rate(flink_taskmanager_status_jvm_gc_time[30s])) * 1000', "sum"),
    ("cp_dur_ms",
     'flink_jobmanager_job_lastcheckpointduration', "max"),
    ("cp_size_mb",
     'flink_jobmanager_job_lastcheckpointsize / 1048576', "max"),
    # RocksDB (2026-09-02 compose toggles). NOTE: Flink's Prometheus
    # reporter folds the OPERATOR NAME into the metric name
    # (..._operator_<op>_rocksdb_<metric>) — an exact-name query matches
    # nothing. Must select by __name__ regex and aggregate with max().
    # RocksDB signals are handled separately (see RDB_SIGNALS): O2 promql
    # does NOT support regex matchers (verified 2026-09-02: exact works,
    # =~ returns 0 series) and Flink folds the operator name into the
    # metric name, so per-operator names are enumerated from the label
    # index and aggregated client-side.
    ("idle_max",
     'max(flink_taskmanager_job_task_idletimemspersecond)', "max"),
    # host (node_exporter): iowait + disk busy fraction + queue depth now
    ("cpu_iowait",
     f'avg(rate(node_cpu_seconds_total{{mode="iowait"}}[30s])) * 100', "sum"),
    ("disk_busy",
     f'rate(node_disk_io_time_seconds_total{{device="{DEFAULT_DEVICE}"}}[30s]) * 100', "max"),
    ("disk_qd_now",
     f'node_disk_io_now{{device="{DEFAULT_DEVICE}"}}', "max"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capture", required=True)
    ap.add_argument("--start", type=int)
    ap.add_argument("--end", type=int)
    ap.add_argument("--step", type=int, default=2)
    ap.add_argument("--out")
    args = ap.parse_args()

    auth = _auth_basic()
    if not auth:
        print("fused_timeline: REFUSED — no O2_AUTH_BASIC (env or "
              "secrets.env); OpenObserve is the single source, cannot "
              "query without it", file=sys.stderr)
        return 3

    start, end = resolve_window(args)
    if not start or not end or end <= start:
        print("fused_timeline: REFUSED — no usable window (need --start/"
              "--end, or stages/io-latency.tsv, or flink-checkpoints.jsonl "
              f"in {args.capture})", file=sys.stderr)
        return 3

    out_path = args.out or os.path.join(
        args.capture, "stages", "fused-timeline.tsv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    warnings = []
    fused = {}   # epoch -> {col: value}

    def touch(t):
        fused.setdefault(t, {})

    # 1) io-latency stream (2s cadence) — the base grid
    io_rows = 0
    r = _o2_search(
        f"SELECT epoch_s, r_await_ms, w_await_ms, aqu_sz, util_pct, "
        f"r_iops, w_iops, psi_io_some_avg10, psi_mem_some_avg10, "
        f"psi_cpu_some_avg10, cpu_mhz_avg, mem_avail_mb, cpu_user_pct, "
        f"cpu_system_pct, cpu_iowait_pct, cpu_idle_pct "
        f"FROM 'host_io_latency' "
        f"WHERE device='{DEFAULT_DEVICE}' "
        f"AND epoch_s >= {start} AND epoch_s <= {end} ORDER BY epoch_s",
        start - 10, end + 10, auth)
    for hit in r.get("hits", []):
        try:
            t = int(hit["epoch_s"])
        except (KeyError, TypeError, ValueError):
            continue
        touch(t)
        fused[t]["io_r_await_ms"] = hit.get("r_await_ms")
        fused[t]["io_w_await_ms"] = hit.get("w_await_ms")
        fused[t]["io_aqu_sz"] = hit.get("aqu_sz")
        fused[t]["io_util_pct"] = hit.get("util_pct")
        fused[t]["io_w_iops"] = hit.get("w_iops")
        for k in ("psi_io_some_avg10", "psi_mem_some_avg10",
                  "psi_cpu_some_avg10", "cpu_mhz_avg", "mem_avail_mb",
                  "cpu_user_pct", "cpu_system_pct", "cpu_iowait_pct",
                  "cpu_idle_pct"):
            fused[t][k] = hit.get(k)
        io_rows += 1
    if not io_rows:
        warnings.append("host_io_latency: 0 rows in window (probe stream "
                        "missing — was io-latency-probe run / pushed?)")

    # 2) checkpoint events (alignment!) — forward-fill onto grid
    cp_rows = 0
    r = _o2_search(
        # NOTE: alignment DURATION / sync_dur / async_dur are NOT exposed
        # top-level by this Flink version's checkpoint REST JSON (verified
        # 2026-09-02: only alignment_buffered exists; per-task sync/async
        # live under tasks{} which history serialization empties). Do not
        # select them — O2 400s on non-existent fields.
        "SELECT trigger_timestamp, id, status, end_to_end_duration, "
        "alignment_buffered, state_size FROM 'flink_checkpoints' "
        f"WHERE trigger_timestamp >= {start * 1000} "
        f"AND trigger_timestamp <= {end * 1000} ORDER BY trigger_timestamp",
        start - 10, end + 10, auth)
    cp_events = []
    for hit in r.get("hits", []):
        try:
            cp_events.append((int(hit["trigger_timestamp"]) // 1000, hit))
        except (KeyError, TypeError, ValueError):
            continue
        cp_rows += 1
    if not cp_rows:
        warnings.append("flink_checkpoints: 0 rows in window (checkpoint "
                        "push missing — capture ran without O2 push?)")

    # 3) prom signals (15s scrape) — forward-fill onto grid
    step = args.step
    grid = list(range(start - (start % step), end + step, step))
    for g in grid:
        touch(g)

    prom_cols = []
    for col, query, agg in PROM_SIGNALS:
        try:
            series = _prom_range(query, start - 60, end + 30, 15, auth)
        except (RuntimeError, urllib.error.URLError, OSError) as e:
            warnings.append(f"{col}: query failed: {str(e)[:120]}")
            continue
        if not series:
            warnings.append(f"{col}: no series (metric absent or window "
                            "outside retention)")
            continue
        prom_cols.append(col)
        # merge all series (usually one) into per-epoch lists, then aggregate
        by_t = {}
        for _, vals in series:
            for t, v in vals:
                by_t.setdefault(t, []).append(v)
        last_val, last_t = None, None
        for g in grid:
            # newest sample at or before g
            cand = [t for t in by_t if t <= g and (t - g) <= 120]
            if cand:
                best = max(cand)
                vals = by_t[best]
                last_val = sum(vals) if agg == "sum" else max(vals)
                last_t = best
            if last_val is not None and last_t is not None:
                fused[g][col] = last_val

    # RocksDB signals (client-side aggregation; see RDB_SIGNALS note)
    _fetch_rdb_signals(start, end, grid, auth, fused, warnings)

    # checkpoint fields onto grid (forward-fill from event stream)
    last_cp = {}
    for g in grid:
        for t, hit in cp_events:
            if t <= g:
                last_cp = hit
        if last_cp:
            fused[g]["cp_e2e_ms"] = last_cp.get("end_to_end_duration")
            fused[g]["cp_align_buf_mb"] = (
                (last_cp.get("alignment_buffered") or 0) / 1048576
                if last_cp.get("alignment_buffered") is not None else None)

    if not fused:
        print("fused_timeline: FAILED — no signals fused at all; "
              f"reasons: {'; '.join(warnings) or 'unknown'}",
              file=sys.stderr)
        return 5

    rdb_cols = [c for c, _, _ in RDB_SIGNALS
                if any(c in row for row in fused.values())]
    cols = (["io_r_await_ms", "io_w_await_ms", "io_aqu_sz", "io_util_pct",
             "io_w_iops", "psi_io_some_avg10", "psi_mem_some_avg10",
             "psi_cpu_some_avg10", "cpu_mhz_avg", "mem_avail_mb",
             "cpu_user_pct", "cpu_system_pct", "cpu_iowait_pct",
             "cpu_idle_pct"] + prom_cols + rdb_cols +
            ["cp_e2e_ms", "cp_align_buf_mb"])
    with open(out_path, "w", newline="") as fh:
        fh.write("epoch\tiso_time\t" + "\t".join(cols) + "\n")
        for g in sorted(fused):
            row = fused[g]
            iso = _dt.datetime.fromtimestamp(
                g, _dt.timezone.utc).strftime("%H:%M:%S")
            fh.write(str(g) + "\t" + iso + "\t" +
                     "\t".join("" if row.get(c) is None
                               else (f"{row[c]:.2f}"
                                     if isinstance(row.get(c), float)
                                     else str(row[c])) for c in cols) + "\n")

    print(f"fused_timeline: {len(fused)} grid points x {len(cols)} cols "
          f"-> {out_path} (io={io_rows} cp={cp_rows})", file=sys.stderr)
    for w in warnings:
        print(f"fused_timeline: WARN — {w}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
