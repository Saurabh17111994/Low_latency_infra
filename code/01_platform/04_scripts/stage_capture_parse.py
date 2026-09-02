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
