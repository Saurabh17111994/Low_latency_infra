#!/usr/bin/env python3
"""stage_capture_parse.py — offline analysis for stage-capture.sh outputs.

Computes, per operator and per time window, the divergence metrics named in
docs/plans/2026-09-01-stage-throughput-latency-detection-plan.md:

  - in_rate / out_rate (records/s, from consecutive samples of the summed
    counters)
  - busy / backpressured / idle share (ms/s aggregated across subtasks,
    normalized to 0-1 by subtask count)
  - per-operator divergence flag: in_rate vs out_rate vs the source rate

Reads: stages.tsv (+ run-meta.txt, vertex-map.tsv) from one capture dir.
Writes: a summary table to stdout (TSV) and optionally --json.

Usage:
  python3 stage_capture_parse.py logs/tracker-14/stage-capture-XXXX \
      [--window 0:180 180:900 ...] [--json out.json]

Unit-tested by tests/test_stage_capture_parse.py (fixture-based; no live
dependency).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def parse_stages_tsv(path: str | Path) -> list[dict]:
    """Parse stages.tsv into sample rows (epoch, vertex, counters)."""
    rows: list[dict] = []
    with open(path, encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != len(header):
                continue
            row = dict(zip(header, parts))
            for key in ("epoch", "numRecordsIn", "numRecordsOut",
                        "busyMsSum", "backpressuredMsSum", "idleMsSum"):
                try:
                    row[key] = int(row[key]) if row[key] != "" else None
                except (ValueError, KeyError):
                    row[key] = None
            rows.append(row)
    return rows


def _num(value) -> float | None:
    return float(value) if value is not None else None


def operator_series(rows: list[dict]) -> dict[str, list[dict]]:
    """Group rows by operator name, ordered by epoch."""
    series: dict[str, list[dict]] = {}
    for row in rows:
        series.setdefault(row["operator"], []).append(row)
    for samples in series.values():
        samples.sort(key=lambda r: r["epoch"])
    return series


def compute_rates(samples: list[dict]) -> list[dict]:
    """Convert cumulative counters into per-interval rates."""
    out: list[dict] = []
    for prev, cur in zip(samples, samples[1:]):
        dt = cur["epoch"] - prev["epoch"]
        if dt <= 0:
            continue
        in_rate = out_rate = None
        if prev["numRecordsIn"] is not None and cur["numRecordsIn"] is not None:
            in_rate = (cur["numRecordsIn"] - prev["numRecordsIn"]) / dt
        if prev["numRecordsOut"] is not None and cur["numRecordsOut"] is not None:
            out_rate = (cur["numRecordsOut"] - prev["numRecordsOut"]) / dt
        out.append({
            "epoch": cur["epoch"],
            "dt": dt,
            "in_rate": in_rate,
            "out_rate": out_rate,
            "busy": _num(cur["busyMsSum"]),
            "backpressured": _num(cur["backpressuredMsSum"]),
            "idle": _num(cur["idleMsSum"]),
            "in_cum": cur["numRecordsIn"],
            "out_cum": cur["numRecordsOut"],
        })
    return out


def window_stats(rates: list[dict], start: int, end: int) -> dict | None:
    """Average rates over epochs in [start, end)."""
    in_window = [r for r in rates if start <= r["epoch"] < end]
    if not in_window:
        return None
    ins = [r["in_rate"] for r in in_window if r["in_rate"] is not None]
    outs = [r["out_rate"] for r in in_window if r["out_rate"] is not None]
    busy = [r["busy"] for r in in_window if r["busy"] is not None]
    bpress = [r["backpressured"] for r in in_window if r["backpressured"] is not None]
    idle = [r["idle"] for r in in_window if r["idle"] is not None]

    def avg(xs: list[float]) -> float | None:
        return sum(xs) / len(xs) if xs else None

    return {
        "samples": len(in_window),
        "in_rate_avg": avg(ins),
        "out_rate_avg": avg(outs),
        "busy_avg_ms": avg(busy),
        "backpressured_avg_ms": avg(bpress),
        "idle_avg_ms": avg(idle),
    }


def parse_prom_files(capture_dir: str | Path) -> list[dict]:
    """Parse prom-<epoch>.txt scrapes (TM Prometheus) into sample dicts.

    Each scrape line is `metric{labels} value`. We aggregate per
    (epoch, task_name, subtask_index): busy/backpressured/idle ms and
    currentWatermark per split, and latency quantiles per operator.
    """
    import re as _re
    capture_dir = Path(capture_dir)
    samples: list[dict] = []
    for path in sorted(capture_dir.glob("prom-*.txt")):
        try:
            epoch = int(path.stem.split("-", 1)[1])
        except ValueError:
            continue
        busy: dict[tuple[str, str], float] = {}
        bpress: dict[tuple[str, str], float] = {}
        hard: dict[tuple[str, str], float] = {}
        idle: dict[tuple[str, str], float] = {}
        wms: dict[tuple[str, str], float] = {}
        lat: dict[tuple[str, str, str], dict[str, float]] = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            m = _re.match(r'(\w+)\{([^}]*)\}\s+([\d\.eE+-]+)$', line.strip())
            if not m:
                continue
            name, labels_s, val_s = m.groups()
            # strip the reporter scope prefix, e.g.
            # flink_taskmanager_job_task_busyBackPressuredTimeMsPerSecond ->
            # busyBackPressuredTimeMsPerSecond
            if name.startswith("flink_taskmanager_job_task_"):
                name = name[len("flink_taskmanager_job_task_"):]
            labels = dict(_re.findall(r'(\w+)="([^"]*)"', labels_s))
            try:
                val = float(val_s)
            except ValueError:
                continue
            task = labels.get("task_name", "?")
            sub = labels.get("subtask_index", "?")
            key = (task, sub)
            if name == "busyTimeMsPerSecond":
                busy[key] = busy.get(key, 0.0) + val
            elif name == "backPressuredTimeMsPerSecond":
                bpress[key] = bpress.get(key, 0.0) + val
            elif name == "hardBackPressuredTimeMsPerSecond":
                hard[key] = hard.get(key, 0.0) + val
            elif name == "idleTimeMsPerSecond":
                idle[key] = idle.get(key, 0.0) + val
            elif name == "currentWatermark":
                split = labels.get("split", "?")
                k2 = (task, split)
                wms[k2] = max(wms.get(k2, val), val)
            elif name.startswith("latency_source_id"):
                q = labels.get("quantile")
                if q is not None:
                    op = labels.get("operator_subtask_index", "?")
                    lat.setdefault((task, sub, op), {})[q] = val
        samples.append({
            "epoch": epoch,
            "busy": busy, "backpressured": bpress, "hard_backpressured": hard,
            "idle": idle, "watermarks": wms, "latency": lat,
        })
    return samples


def prom_operator_summary(prom_samples: list[dict],
                           windows: list[tuple[int, int]]) -> str:
    """Average busy/bpress/idle per operator per window from prom scrapes."""
    # operator-level average of per-subtask ms-per-second
    agg: dict[tuple[str, int, int], dict[str, list[float]]] = {}
    for s in prom_samples:
        for (start, end) in windows:
            if not (start <= s["epoch"] < end):
                continue
            # combine per subtask: average across subtasks
            subs = set(s["busy"]) | set(s["backpressured"]) | set(s["idle"])
            for (task, sub) in subs:
                key = (task, start, end)
                bucket = agg.setdefault(key, {"busy": [], "bpress": [], "idle": [], "hard": []})
                if (task, sub) in s["busy"]:
                    bucket["busy"].append(s["busy"][(task, sub)])
                if (task, sub) in s["backpressured"]:
                    bucket["bpress"].append(s["backpressured"][(task, sub)])
                if (task, sub) in s["idle"]:
                    bucket["idle"].append(s["idle"][(task, sub)])
                if (task, sub) in s.get("hard_backpressured", {}):
                    bucket["hard"].append(s["hard_backpressured"][(task, sub)])
    lines = ["operator\twindow\tbusy_ms_avg\tbpress_ms_avg\thard_bpress_ms_avg\tidle_ms_avg"]
    for (task, start, end) in sorted(agg, key=lambda k: (k[1], k[0])):
        b = agg[(task, start, end)]
        def _a(xs: list[float]) -> float:
            return sum(xs) / len(xs) if xs else 0.0
        hard = _a(b.get("hard", []))
        lines.append(f"{task}\t{start}-{end}\t{_a(b['busy']):.0f}\t"
                     f"{_a(b['bpress']):.0f}\t{hard:.0f}\t{_a(b['idle']):.0f}")
    return "\n".join(lines)


def latency_report(prom_samples: list[dict],
                   windows: list[tuple[int, int]]) -> str:
    """Per-operator source-to-operator latency percentiles per window.

    Flink latency markers are emitted at the source and histograms are
    recorded at every operator downstream; values are ms since source emit.
    Quantiles are averaged per (operator, window) across subtasks and scrapes.
    MIN_LONG (-9.22e18) watermarks/values are ignored.
    """
    agg: dict[tuple[str, int, int], dict[str, list[float]]] = {}
    for s in prom_samples:
        for (start, end) in windows:
            if not (start <= s["epoch"] < end):
                continue
            for (task, sub, op), quantiles in s["latency"].items():
                for q, val in quantiles.items():
                    if val < -9.0e18:  # MIN watermark sentinel
                        continue
                    bucket = agg.setdefault((task, start, end), {})
                    bucket.setdefault(q, []).append(val)
    def _qname(q: str) -> str:
        # 0.5 -> p50, 0.75 -> p75, 0.98 -> p98, 0.999 -> p999 (avoids the
        # duplicate p99 for 0.98 vs 0.999)
        pct = float(q) * 100
        if abs(pct - round(pct)) < 1e-9:
            return f"p{int(round(pct))}"
        return f"p{pct:.4g}".rstrip("0").rstrip(".")

    all_qs = sorted({q for b in agg.values() for q in b})
    lines = ["operator\twindow\t" + "\t".join(
        f"{_qname(q)}_ms" for q in all_qs)]
    for (task, start, end) in sorted(agg, key=lambda k: (k[1], k[0])):
        b = agg[(task, start, end)]
        cells = []
        for q in all_qs:
            xs = b.get(q, [])
            cells.append(f"{sum(xs) / len(xs):.0f}" if xs else "")
        lines.append(f"{task}\t{start}-{end}\t" + "\t".join(cells))
    return "\n".join(lines)


def watermark_lag_report(prom_samples: list[dict],
                         windows: list[tuple[int, int]]) -> str:
    """Per-source watermark (max across splits) per window, in epoch-ms.

    Lag vs wall clock must be computed by the caller with the capture's
    started_epoch (watermarks are event-time epoch-ms values).
    """
    agg: dict[tuple[str, int, int], list[float]] = {}
    for s in prom_samples:
        for (start, end) in windows:
            if not (start <= s["epoch"] < end):
                continue
            for (task, split), wm in s["watermarks"].items():
                if wm < -9.0e18:
                    continue
                agg.setdefault((task, start, end), []).append(wm)
    lines = ["source\twindow\twatermark_max_ms\twatermark_avg_ms\tsamples"]
    for (task, start, end) in sorted(agg, key=lambda k: (k[1], k[0])):
        xs = agg[(task, start, end)]
        lines.append(f"{task}\t{start}-{end}\t{max(xs):.0f}\t"
                     f"{sum(xs) / len(xs):.0f}\t{len(xs)}")
    return "\n".join(lines)


def divergence_report(capture_dir: str | Path,
                      windows: list[tuple[int, int]]) -> str:
    """Build the per-operator, per-window TSV summary with divergence flags."""
    capture_dir = Path(capture_dir)
    rows = parse_stages_tsv(capture_dir / "stages.tsv")
    series = operator_series(rows)

    # Source operator = the raw-table source (feeds the whole chain).
    source_op = None
    for name in series:
        if "raw_table" in name or "raw-table" in name:
            source_op = name
            break

    lines = [
        "operator\twindow\tin_rate_avg\tout_rate_avg\tbusy_ms\tbpress_ms\tidle_ms\tflag"
    ]
    for name, samples in sorted(series.items()):
        rates = compute_rates(samples)
        for (start, end) in windows:
            stats = window_stats(rates, start, end)
            if stats is None:
                continue
            flag = ""
            if (stats["in_rate_avg"] is not None
                    and stats["out_rate_avg"] is not None):
                if stats["in_rate_avg"] > 0:
                    ratio = stats["out_rate_avg"] / stats["in_rate_avg"]
                    if ratio < 0.5:
                        flag = "OUT_THROTTLED"
            if source_op and name == source_op:
                flag = (flag + "+SOURCE") if flag else "SOURCE"
            lines.append(
                f"{name}\t{start}-{end}\t"
                f"{stats['in_rate_avg'] or 0:.0f}\t{stats['out_rate_avg'] or 0:.0f}\t"
                f"{stats['busy_avg_ms'] or 0:.0f}\t"
                f"{stats['backpressured_avg_ms'] or 0:.0f}\t"
                f"{stats['idle_avg_ms'] or 0:.0f}\t{flag}"
            )
    return "\n".join(lines)


def _parse_epoch_tsv(path: Path, ncols: int) -> list[list[str]]:
    """Parse a B2 hook TSV (epoch_ms first, header row skipped)."""
    rows: list[list[str]] = []
    if not path.exists():
        return rows
    with open(path, encoding="utf-8") as f:
        next(f, None)  # header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == ncols:
                rows.append(parts)
    return rows


def b2_read_lag_report(capture_dir: str | Path,
                       windows: list[tuple[int, int]]) -> str:
    """B2 CP3->CP4 read lag (plan Stage B2): per-window delta differential.

    log-end comes from read-lag.tsv (FlussReadLagProbe, one sample per tick);
    source consumed comes from stages.tsv (source operator numRecordsIn). Both
    are cumulative, but their ZERO POINTS differ: log-end counts records since
    table creation; the Flink counter counts since job start. For a purged
    table both start ~0 at the same moment, BUT the capture begins only after
    warm-up (RUNNING + alignment), so the absolute difference at the first
    sample already contains the pre-capture backlog and is meaningless.

    The valid steady-state metric is the DELTA differential: per window,
    (log_end_end - log_end_start) - (consumed_end - consumed_start) — zero
    when CP3 append and CP4 consume are in sync, positive when the table
    grows faster than the job consumes (a REAL read lag), negative when the
    job drains faster than the feed appends (not a lag). Reported as the
    p50/p95 of per-sample deltas plus the window net.
    """
    capture_dir = Path(capture_dir)
    probe_rows = _parse_epoch_tsv(capture_dir / "read-lag.tsv", 5)
    if not probe_rows:
        return "CP3->CP4 read lag: read-lag.tsv absent (FLUSS_PROBE_CP not set for this capture)"
    # logend: list of (epoch_s, sum); consumed: list of (epoch_s, counter).
    logend: list[tuple[float, float]] = []
    for r in probe_rows:
        try:
            logend.append((float(r[0]) / 1000.0, float(r[4])))
        except ValueError:
            continue
    logend.sort()
    consumed: list[tuple[float, float]] = []
    for row in parse_stages_tsv(capture_dir / "stages.tsv"):
        name = row.get("operator", "")
        if "raw_table" not in name and "raw-table" not in name:
            continue
        if row.get("numRecordsIn") is None:
            continue
        consumed.append((float(row["epoch"]), float(row["numRecordsIn"])))
    consumed.sort()
    if not logend or not consumed:
        return ("CP3->CP4 read lag: no data (read-lag.tsv %s, source series %s)"
                % ("empty" if not logend else "ok",
                   "missing" if not consumed else "ok"))

    def _interp(series: list[tuple[float, float]], t: float) -> float | None:
        """Piecewise-linear interpolation; None outside the series range."""
        if t < series[0][0] or t > series[-1][0]:
            return None
        for i in range(len(series) - 1):
            x0, y0 = series[i]
            x1, y1 = series[i + 1]
            if x0 <= t <= x1:
                if x1 == x0:
                    return y0
                return y0 + (t - x0) * (y1 - y0) / (x1 - x0)
        return series[-1][1]

    # Per consecutive probe-sample pair: append delta vs consume delta.
    out: dict[tuple[int, int], list[str]] = {}
    for i in range(1, len(logend)):
        t0, le0 = logend[i - 1]
        t1, le1 = logend[i]
        c0 = _interp(consumed, t0)
        c1 = _interp(consumed, t1)
        if c0 is None or c1 is None or t1 <= t0:
            continue
        for (start, end) in windows:
            if start <= t0 < end:
                d_le = le1 - le0
                d_c = c1 - c0
                out.setdefault((start, end), []).append(
                    f"{d_le:.0f}\t{d_c:.0f}\t{d_le - d_c:.0f}")
                break
    lines = ["window\tlast_append_delta\tlast_consume_delta\t"
             "net_lag_p50_records\tnet_lag_p99_records\twindow_net_records"]
    for (start, end) in sorted(windows):
        rows = out.get((start, end), [])
        if not rows:
            lines.append(f"{start}-{end}\t\t\t\t\t")
            continue
        nets = sorted(float(r.split("\t")[2]) for r in rows)
        p50 = nets[len(nets) // 2]
        p99 = nets[min(len(nets) - 1, int(len(nets) * 0.99))]
        window_net = sum(float(r.split("\t")[2]) for r in rows)
        lines.append(f"{start}-{end}\t{rows[-1].split(chr(9))[0]}\t"
                     f"{rows[-1].split(chr(9))[1]}\t{p50:.0f}\t{p99:.0f}\t{window_net:.0f}")
    return "\n".join(lines)


def b2_consumer_read_report(capture_dir: str | Path,
                            windows: list[tuple[int, int]]) -> str:
    """B2 CP9->CP10 consumer-read (plan Stage B2): per-window p50/p95 of
    (probe wallclock read - row last_event_ts) = commit->readable staleness.

    output_ts is the pipeline's EVENT-time stamp (synthetic feed clock, may
    run ahead of wall); last_event_ts is the newest visible event carried in
    the row. A consumer reading now sees events up to last_event_ts, so
    (now - last_event_ts) is the honest visibility staleness.
    """
    capture_dir = Path(capture_dir)
    rows = _parse_epoch_tsv(capture_dir / "consumer-read.tsv", 6)
    if not rows:
        return "CP9->CP10 consumer read: consumer-read.tsv absent (FLUSS_PROBE_CP not set for this capture)"
    # (epoch_ms, token, window_start, output_ts, last_event_ts, staleness_ms)
    samples: dict[tuple[int, int], list[float]] = {}
    for r in rows:
        try:
            epoch_ms = float(r[0])
            lag = float(r[5])
        except (ValueError, IndexError):
            continue
        for (start, end) in windows:
            if start <= epoch_ms / 1000.0 < end:
                samples.setdefault((start, end), []).append(lag)
                break
    lines = ["window\tcp9cp10_p50_ms\tcp9cp10_p95_ms\tcp9cp10_p99_ms\tsamples"]
    for (start, end) in sorted(windows):
        xs = sorted(samples.get((start, end), []))
        if not xs:
            lines.append(f"{start}-{end}\t\t\t\t0")
            continue
        p50 = xs[len(xs) // 2]
        p95 = xs[min(len(xs) - 1, int(len(xs) * 0.95))]
        p99 = xs[min(len(xs) - 1, int(len(xs) * 0.99))]
        lines.append(f"{start}-{end}\t{p50:.0f}\t{p95:.0f}\t{p99:.0f}\t{len(xs)}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture_dir", help="stage-capture output directory")
    parser.add_argument("--window", action="append", default=None,
                        help="start:end epoch pair (repeatable)")
    parser.add_argument("--json", default=None, help="optional JSON output path")
    args = parser.parse_args(argv)

    windows: list[tuple[int, int]] = []
    if args.window:
        for w in args.window:
            start_s, end_s = w.split(":", 1)
            windows.append((int(start_s), int(end_s)))
    else:
        # Default: whole capture as one window (epochs are absolute; caller
        # usually passes explicit windows from run-meta started_epoch).
        rows = parse_stages_tsv(Path(args.capture_dir) / "stages.tsv")
        if rows:
            epochs = [r["epoch"] for r in rows]
            windows = [(min(epochs), max(epochs) + 1)]

    report = divergence_report(args.capture_dir, windows)
    print(report)

    # B2 hook reports (plan Stage B2; absent files print an explicit note).
    print()
    print(b2_read_lag_report(args.capture_dir, windows))
    print()
    print(b2_consumer_read_report(args.capture_dir, windows))

    prom = parse_prom_files(args.capture_dir)
    if prom:
        print()
        print(prom_operator_summary(prom, windows))
        print()
        print(latency_report(prom, windows))
        print()
        print(watermark_lag_report(prom, windows))

    if args.json:
        rows = parse_stages_tsv(Path(args.capture_dir) / "stages.tsv")
        payload = {
            "capture_dir": str(args.capture_dir),
            "windows": [list(w) for w in windows],
            "operators": {},
        }
        for name, samples in operator_series(rows).items():
            payload["operators"][name] = compute_rates(samples)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
