#!/usr/bin/env python3
"""Greenfield stage profiler — pure report core.

One test, one command, one run, one report (see README.md in this folder).
Every line here is new code: no existing file is imported, sourced, or edited.

This module is the offline half of the profiler — the half a unit test can pin:

* the S1..S9 stage registry (which hop is measured, from which native source);
* percentiles with the same nearest-rank semantics the existing analyzer uses,
  so "p90" means the same thing in every report;
* sample summaries for both row samples (computed here) and Java/Flink
  histogram exports (already computed by the emitter — never re-percentiled);
* the fail-closed smoke presence gate, so an empty leg can never read as PASS;
* Markdown/TSV rendering of the final profile.

The live half (`stage-profile.sh`) brings the pipeline up, samples every native
source into an evidence directory, feeds the counts to the presence gate, and
renders the report. Nothing on the data path is modified by any of this.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

PLACEHOLDER = "—"

# ── percentiles ─────────────────────────────────────────────────────────────


def pct(values: Sequence[float], p: float) -> float:
    """Nearest-rank percentile, same semantics as the existing analyzer's pct().

    Sorted sample, index ``round(p/100 * (N-1))`` clamped to ``[0, N-1]`` — no
    interpolation. Kept identical on purpose: a report that mixes this method
    with another one fabricates differences that are not in the data.
    """
    xs = sorted(values)
    if not xs:
        raise ValueError("percentile of an empty sample")
    if not 0 < p <= 100:
        raise ValueError(f"p out of range: {p}")
    k = max(0, min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1)))))
    return xs[k]


@dataclass(frozen=True)
class Summary:
    """A latency sample in the columns the report shows.

    Any percentile may be absent: Java histograms export p50/p90/p99 (no p95),
    Flink summaries export p50/p95/p99 (no p90), and an absent value must print
    as the placeholder, never as 0.
    """

    n: int
    p50: float | None
    p90: float | None  # absent for Flink summaries
    p95: float | None  # absent for Java histograms
    p99: float | None

    @classmethod
    def of(cls, values: Sequence[float], *, with_p95: bool = True) -> "Summary":
        """Summarize a raw row sample (percentiles computed here)."""
        xs = list(values)
        return cls(
            n=len(xs),
            p50=pct(xs, 50),
            p90=pct(xs, 90),
            p95=pct(xs, 95) if with_p95 else None,
            p99=pct(xs, 99),
        )

    @classmethod
    def exported(
        cls,
        *,
        n: int,
        p50: float,
        p90: float,
        p99: float,
        p95: float | None = None,
    ) -> "Summary":
        """Wrap percentiles a source already computed (Java histogram, tracker)."""
        return cls(n=n, p50=p50, p90=p90, p95=p95, p99=p99)


def fmt_num(value: float | None, digits: int = 1) -> str:
    """Format a number, or the honest placeholder when the source is absent."""
    if value is None:
        return PLACEHOLDER
    return f"{value:.{digits}f}"


# ── stage registry ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Stage:
    sid: str
    boundary: str
    latency: str
    throughput: str


STAGES: tuple[Stage, ...] = (
    Stage(
        "S1",
        "broker -> Java accept",
        "raw_table_1 rows: ingest_ts - event_time (bounded sample)",
        "bridge tick counts + tick.throughput delta",
    ),
    Stage(
        "S2",
        "Go bridge internal (recv -> emit)",
        "stage.ipc_latency (Go emit - Go recv)",
        "bridge emitted tick rate",
    ),
    Stage(
        "S3",
        "Java decode / batch / route",
        "stage.decode_latency, stage.batching_latency, stage.routing_latency",
        "tick.throughput delta (per container)",
    ),
    Stage(
        "S4",
        "Java -> Fluss append ack",
        "stage.fluss_submit_latency, stage.fluss_ack_latency, append.latency.ms",
        "append.latency.ms count delta",
    ),
    Stage(
        "S5",
        "ingestion end-to-end (event time -> ack)",
        "stage.end_to_end_latency",
        "append count delta",
    ),
    Stage(
        "S6",
        "raw_table_1 -> Flink post-dedup",
        "compute.latency.ingest_to_monitor (TM Prom summary quantiles)",
        "raw source records delta",
    ),
    Stage(
        "S7",
        "Flink per-operator",
        "tracker latency percentiles per operator (TM Prom)",
        "numRecordsIn/Out delta per operator",
    ),
    Stage(
        "S8",
        "Flink -> feature tables (window close -> readable)",
        "first-seen minus window end on candle_live / candle_closed polls",
        "sink records delta + feature row counts",
    ),
    Stage(
        "S9",
        "whole path (event time -> feature row)",
        "last-operator tracker latency (Flink latency tracking)",
        "raw rows/s vs feature rows/s",
    ),
)


def registry_table() -> str:
    """The registry as a Markdown table (used by the `stages` subcommand and docs)."""
    lines = [
        "| step | boundary | latency source | throughput source |",
        "|---|---|---|---|",
    ]
    for stage in STAGES:
        lines.append(
            f"| {stage.sid} | {stage.boundary} | {stage.latency} | {stage.throughput} |"
        )
    return "\n".join(lines) + "\n"


# ── Java OTLP stage metrics (java.out) ───────────────────────────────────────
#
# OtlpMetricsEmitter flushes one JSON payload per ~10 s window; each endpoint
# container logs it as a line ending in `otlp-metrics-payload: {...}`. The
# histogram payload carries the emitter's own p50/p90/p99 attributes (there is
# no p95), so those are consumed as-is and never re-percentiled. Aggregation
# across windows is reported as: total samples = sum of counts, and the median
# across windows for each percentile (labeled in the report).

_OTLP_RE = re.compile(r"otlp-metrics-payload: (\{.*\})\s*$")

# Which java.out metric satisfies which presence-gate key.
JAVA_STAGE_PRESENCE: dict[str, str] = {
    "stage.ipc_latency": "S2.ipc",
    "stage.decode_latency": "S3.decode",
    "stage.batching_latency": "S3.batching",
    "stage.routing_latency": "S3.routing",
    "stage.fluss_submit_latency": "S4.fluss_submit",
    "stage.fluss_ack_latency": "S4.fluss_ack",
    "append.latency.ms": "S4.append",
    "stage.end_to_end_latency": "S5.e2e",
}


@dataclass(frozen=True)
class JavaWindow:
    """One flush window of one metric, exactly as the emitter reported it."""

    epoch_ms: int
    metric: str
    kind: str  # "histogram" | "sum" | "gauge"
    count: int | None = None
    p50: float | None = None
    p90: float | None = None
    p99: float | None = None
    value: float | None = None


@dataclass(frozen=True)
class JavaStageStat:
    metric: str
    windows: int
    n: int  # samples in the run: the exported count is cumulative, so take the
    #         last window (restart-tolerant: negative steps reset the base)
    p50: float  # median across windows of the window's p50
    p90: float
    p99: float
    worst_p99: float  # the worst window p99 (labeled separately, never mixed)


@dataclass(frozen=True)
class CounterStat:
    metric: str
    windows: int
    first: float
    last: float
    rate_per_s: float | None


def _attr_number(attr: Mapping) -> float | None:
    value = attr.get("value", {})
    for key in ("intValue", "doubleValue", "asInt", "asDouble"):
        if key in value:
            return float(value[key])
    return None


def _point_number(point: Mapping) -> float | None:
    for key in ("asInt", "asDouble"):
        if key in point:
            return float(point[key])
    return None


def parse_java_out(text: str) -> list[JavaWindow]:
    """Every histogram/sum/gauge window a java.out log carries.

    Malformed payload lines are skipped, not guessed at — a stage with zero
    parsed samples must fail the presence gate, never print a zero.
    """
    out: list[JavaWindow] = []
    for line in text.splitlines():
        m = _OTLP_RE.search(line)
        if not m:
            continue
        try:
            payload = json.loads(m.group(1))
            metrics = payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
        except (json.JSONDecodeError, KeyError, IndexError):
            continue
        for metric in metrics:
            name = metric.get("name", "")
            if "histogram" in metric:
                for point in metric["histogram"].get("dataPoints", []):
                    attrs = {
                        a.get("key"): _attr_number(a)
                        for a in point.get("attributes", [])
                    }
                    out.append(
                        JavaWindow(
                            epoch_ms=int(point["timeUnixNano"]) // 1_000_000,
                            metric=name,
                            kind="histogram",
                            count=int(point.get("count", 0)),
                            p50=attrs.get("p50"),
                            p90=attrs.get("p90"),
                            p99=attrs.get("p99"),
                        )
                    )
            elif "sum" in metric or "gauge" in metric:
                family = metric.get("sum") or metric.get("gauge") or {}
                for point in family.get("dataPoints", []):
                    out.append(
                        JavaWindow(
                            epoch_ms=int(point["timeUnixNano"]) // 1_000_000,
                            metric=name,
                            kind="sum" if "sum" in metric else "gauge",
                            value=_point_number(point),
                        )
                    )
    return out


def java_windows_to_stats(
    windows: Sequence[JavaWindow],
) -> tuple[dict[str, JavaStageStat], dict[str, CounterStat]]:
    """Split parsed windows into histogram stage stats and counter stats."""
    by_metric: dict[str, list[JavaWindow]] = {}
    for window in windows:
        by_metric.setdefault(window.metric, []).append(window)

    stages: dict[str, JavaStageStat] = {}
    counters: dict[str, CounterStat] = {}
    for metric, ws in by_metric.items():
        hist = [w for w in ws if w.kind == "histogram"]
        if hist:
            for percentile in ("p50", "p90", "p99"):
                values = [getattr(w, percentile) for w in hist if getattr(w, percentile) is not None]
                if len(values) != len(hist):
                    raise ValueError(f"{metric}: a histogram window is missing {percentile}")
            ordered = sorted(hist, key=lambda w: w.epoch_ms)
            counts = [w.count or 0 for w in ordered]
            total = counts[0] + sum(b - a for a, b in zip(counts, counts[1:]) if b > a)
            stages[metric] = JavaStageStat(
                metric=metric,
                windows=len(ordered),
                n=total,
                p50=pct([w.p50 for w in hist], 50),
                p90=pct([w.p90 for w in hist], 50),
                p99=pct([w.p99 for w in hist], 50),
                worst_p99=max(w.p99 for w in hist),
            )
            continue
        series = sorted(
            (w for w in ws if w.value is not None), key=lambda w: w.epoch_ms
        )
        if not series:
            continue
        first, last = series[0].value, series[-1].value
        dt_s = (series[-1].epoch_ms - series[0].epoch_ms) / 1000.0
        rate = (last - first) / dt_s if dt_s > 0 else None
        counters[metric] = CounterStat(
            metric=metric,
            windows=len(series),
            first=first,
            last=last,
            rate_per_s=rate,
        )
    return stages, counters


def java_presence_counts(stages: Mapping[str, JavaStageStat]) -> dict[str, int]:
    """Presence-gate counts from parsed Java histograms (samples per stage)."""
    return {
        JAVA_STAGE_PRESENCE[metric]: stat.n
        for metric, stat in stages.items()
        if metric in JAVA_STAGE_PRESENCE and stat.n > 0
    }


def render_java_stats(
    stages: Mapping[str, JavaStageStat], counters: Mapping[str, CounterStat]
) -> str:
    lines = ["metric\twindows\tn\tp50\tp90\tp99\tworst_p99"]
    for name in sorted(stages):
        s = stages[name]
        lines.append(
            f"{name}\t{s.windows}\t{s.n}\t{fmt_num(s.p50)}\t{fmt_num(s.p90)}"
            f"\t{fmt_num(s.p99)}\t{fmt_num(s.worst_p99)}"
        )
    if counters:
        lines.append("counter\twindows\tfirst\tlast\trate_per_s")
        for name in sorted(counters):
            c = counters[name]
            lines.append(
                f"{name}\t{c.windows}\t{fmt_num(c.first, 0)}\t{fmt_num(c.last, 0)}"
                f"\t{fmt_num(c.rate_per_s, 1)}"
            )
    return "\n".join(lines) + "\n"


# ── report rows ─────────────────────────────────────────────────────────────

REPORT_COLUMNS = (
    "step",
    "boundary",
    "throughput",
    "p50",
    "p90",
    "p95",
    "p99",
    "n",
    "source",
)


@dataclass(frozen=True)
class ProfileRow:
    sid: str
    boundary: str
    throughput: str
    summary: Summary | None = None
    source: str = ""


def _cells(row: ProfileRow) -> list[str]:
    s = row.summary
    return [
        row.sid,
        row.boundary,
        row.throughput,
        fmt_num(s.p50) if s else PLACEHOLDER,
        fmt_num(s.p90) if s else PLACEHOLDER,
        fmt_num(s.p95) if s else PLACEHOLDER,
        fmt_num(s.p99) if s else PLACEHOLDER,
        str(s.n) if s else "0",
        row.source,
    ]


def render_markdown(rows: Sequence[ProfileRow], *, title: str = "Stage profile") -> str:
    lines = [
        f"# {title}",
        "",
        "| " + " | ".join(REPORT_COLUMNS) + " |",
        "|" + "|".join("---" for _ in REPORT_COLUMNS) + "|",
    ]
    lines += ["| " + " | ".join(_cells(row)) + " |" for row in rows]
    return "\n".join(lines) + "\n"


def render_tsv(rows: Sequence[ProfileRow]) -> str:
    lines = ["\t".join(REPORT_COLUMNS)]
    for row in rows:
        lines.append("\t".join("-" if c == PLACEHOLDER else c for c in _cells(row)))
    return "\n".join(lines) + "\n"


# ── smoke presence gate ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class PresenceRule:
    key: str
    min_samples: int
    what: str


# Thresholds are deliberately 1 (presence, not volume). The first full-universe
# run is record-only; volume budgets are pinned from its baseline afterwards.
DEFAULT_PRESENCE_RULES: tuple[PresenceRule, ...] = (
    PresenceRule("S1.raw_sample", 1, "raw rows read for ingest_ts - event_time"),
    PresenceRule("S2.ipc", 1, "stage.ipc_latency samples"),
    PresenceRule("S3.decode", 1, "stage.decode_latency samples"),
    PresenceRule("S3.batching", 1, "stage.batching_latency samples"),
    PresenceRule("S3.routing", 1, "stage.routing_latency samples"),
    PresenceRule("S4.fluss_submit", 1, "stage.fluss_submit_latency samples"),
    PresenceRule("S4.fluss_ack", 1, "stage.fluss_ack_latency samples"),
    PresenceRule("S4.append", 1, "append.latency.ms samples"),
    PresenceRule("S5.e2e", 1, "stage.end_to_end_latency samples"),
    PresenceRule("S6.monitor", 1, "compute.latency.ingest_to_monitor samples"),
    PresenceRule("S7.tracker", 1, "per-operator tracker latency samples"),
    PresenceRule("S8.feature_read", 1, "candle window reads"),
    PresenceRule("S9.last_sink", 1, "last-operator tracker latency samples"),
)


def presence_failures(
    observed: Mapping[str, int],
    rules: Sequence[PresenceRule] = DEFAULT_PRESENCE_RULES,
) -> list[str]:
    """Refuse a run whose smoke phase produced no samples for a stage.

    Returns one human-readable line per failed rule; empty list means present.
    Extra observation keys are ignored — the gate only judges what it declared.
    """
    failures: list[str] = []
    for rule in rules:
        got = int(observed.get(rule.key, 0))
        if got < rule.min_samples:
            failures.append(
                f"{rule.key}: {got} < {rule.min_samples} samples ({rule.what})"
            )
    return failures


# ── evidence readers (phase dir -> report) ──────────────────────────────────
#
# The orchestrator writes one directory per phase (smoke, main):
#   capture/j1-{0..N}/java.out   Java OTLP logs, one per ingestion container
#   capture/raw-sample.jsonl     FlussPrefixReader rows for the S1 sample
#   capture/ticks/*.txt          per-container tick counts (teardown evidence)
#   stages/                      stage-capture.sh OUT_DIR: prom snapshots, read
#                                lag, candle reads, custom REST, run-meta.txt
# These readers are pure: give them text, get numbers. The live wiring lives in
# stage-profile.sh.

_PROM_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.*)\}\s+([0-9eE.+-]+)\s*$")
_PROM_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')

TRACKER_FAMILY = (
    "flink_taskmanager_job_task_latency_source_id_operator_id_"
    "operator_subtask_index_latency"
)
MONITOR_FAMILY = "flink_taskmanager_job_task_operator_compute_latency_ingest_to_monitor"


@dataclass(frozen=True)
class PromSample:
    metric: str
    labels: tuple[tuple[str, str], ...]
    value: float

    def label(self, key: str, default: str = "") -> str:
        return dict(self.labels).get(key, default)


def parse_prom(text: str) -> list[PromSample]:
    out: list[PromSample] = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _PROM_LINE.match(line)
        if not m:
            continue
        try:
            value = float(m.group(3))
        except ValueError:
            continue
        out.append(
            PromSample(
                metric=m.group(1),
                labels=tuple(_PROM_LABEL.findall(m.group(2))),
                value=value,
            )
        )
    return out


def max_quantiles(
    samples: Sequence[PromSample], family: str, *, job_id: str = ""
) -> dict[str, float]:
    """quantile -> max across tasks/subtasks, for one Flink summary family."""
    out: dict[str, float] = {}
    for s in samples:
        if s.metric != family:
            continue
        if job_id and s.label("job_id") not in ("", job_id):
            continue
        quantile = s.label("quantile")
        if not quantile:
            continue
        out[quantile] = max(out.get(quantile, float("-inf")), s.value)
    return out


def tracker_by_task(
    samples: Sequence[PromSample], *, job_id: str = ""
) -> dict[str, dict[str, float]]:
    """task_name -> {quantile: max across subtasks} for the tracker family."""
    table: dict[str, dict[str, float]] = {}
    for s in samples:
        if s.metric != TRACKER_FAMILY:
            continue
        if job_id and s.label("job_id") not in ("", job_id):
            continue
        task = s.label("task_name")
        quantile = s.label("quantile")
        if not task or not quantile:
            continue
        table.setdefault(task, {})[quantile] = max(
            table.get(task, {}).get(quantile, float("-inf")), s.value
        )
    return table


def tracker_counts_by_task(
    samples: Sequence[PromSample], *, job_id: str = ""
) -> dict[str, float]:
    out: dict[str, float] = {}
    for s in samples:
        if s.metric != TRACKER_FAMILY + "_count":
            continue
        if job_id and s.label("job_id") not in ("", job_id):
            continue
        task = s.label("task_name")
        if task:
            out[task] = out.get(task, 0.0) + s.value
    return out


def summary_from_quantiles(
    quantiles: Mapping[str, float], *, n: int
) -> "Summary":
    """Flink summary labels -> report Summary (p50/p95/p99; p90 is not exported)."""
    return Summary.exported(
        n=n,
        p50=quantiles.get("0.5"),
        p90=None,
        p95=quantiles.get("0.95"),
        p99=quantiles.get("0.99"),
    )


def parse_raw_sample(text: str) -> list[float]:
    """`ingest_ts - event_time` from FlussPrefixReader JSON rows (ns/µs ignored)."""
    lags: list[float] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("__END__"):
            continue
        try:
            row = json.loads(line)
            lags.append(int(row["ingest_ts"]) - int(row["event_time"]))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
    return lags


def raw_append_rate(read_lag_tsv: str) -> float | None:
    """raw_table_1 rows/s from FlussReadLagProbe's log-end sum (first vs last poll)."""
    rows: list[tuple[int, float]] = []
    for line in read_lag_tsv.splitlines():
        if not line.strip() or line.startswith("epoch_ms"):
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        try:
            rows.append((int(parts[0]), float(parts[4])))
        except ValueError:
            continue
    if len(rows) < 2:
        return None
    dt_s = (rows[-1][0] - rows[0][0]) / 1000.0
    return (rows[-1][1] - rows[0][1]) / dt_s if dt_s > 0 else None


def source_records_rate(stages_tsv: str) -> float | None:
    """Source-operator output rows/s from stage-capture's per-poll Flink counters."""
    per_epoch: dict[int, float] = {}
    for line in stages_tsv.splitlines():
        if not line.strip() or line.startswith("epoch"):
            continue
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        try:
            epoch = int(parts[0])
            operator = parts[2]
            records_out = float(parts[4] or 0)
        except (ValueError, IndexError):
            continue
        if not operator.lower().startswith("source"):
            continue
        per_epoch[epoch] = per_epoch.get(epoch, 0.0) + records_out
    if len(per_epoch) < 2:
        return None
    epochs = sorted(per_epoch)
    dt_s = epochs[-1] - epochs[0]
    return (per_epoch[epochs[-1]] - per_epoch[epochs[0]]) / dt_s if dt_s > 0 else None


def close_to_visible(tsv_text: str) -> list[float]:
    """`window_end -> first sighting` from closed-read.tsv (candle_closed).

    FlussKvProbe rewrites the latest window on every poll; only the first
    sighting at or after the window end counts, so this is a lower-bound
    measurement at the capture poll interval.
    """
    first: dict[tuple[str, str], tuple[int, int]] = {}
    for line in tsv_text.splitlines():
        if not line.strip() or line.startswith("epoch_ms"):
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        try:
            seen, token, window_start, window_end = (
                int(parts[0]),
                parts[1],
                parts[2],
                int(parts[3]),
            )
        except ValueError:
            continue
        if seen < window_end:
            continue
        key = (token, window_start)
        if key not in first or seen < first[key][0]:
            first[key] = (seen, window_end)
    return [seen - window_end for seen, window_end in first.values()]


def _read(path: Path) -> str:
    try:
        return path.read_text(errors="replace")
    except OSError:
        return ""


def _run_job_id(stages_dir: Path) -> str:
    for line in _read(stages_dir / "run-meta.txt").splitlines():
        if line.startswith("job_id="):
            return line.split("=", 1)[1].strip()
    return ""


def java_containers(
    capture_dir: Path,
) -> tuple[dict[str, dict[str, JavaStageStat]], dict[str, dict[str, CounterStat]]]:
    per: dict[str, dict[str, JavaStageStat]] = {}
    counters: dict[str, dict[str, CounterStat]] = {}
    for path in sorted(capture_dir.glob("j1-*/java.out")):
        stages, cnt = java_windows_to_stats(parse_java_out(_read(path)))
        per[path.parent.name] = stages
        counters[path.parent.name] = cnt
    return per, counters


def _phase_prom(stages_dir: Path) -> list[PromSample]:
    files = sorted(stages_dir.glob("prom-*.txt"))
    return parse_prom(_read(files[-1])) if files else []


def _phase_closed_lags(stages_dir: Path) -> list[float]:
    return close_to_visible(_read(stages_dir / "closed-read.tsv"))


FEATURE_SINK_KEYS = ("candle", "candidates", "signal")


def feature_sinks(
    tracks: Mapping[str, Mapping[str, float]],
) -> dict[str, Mapping[str, float]]:
    """Tracker tasks that write the feature tables (candles/signals), not intents."""
    return {
        task: quantiles
        for task, quantiles in tracks.items()
        if "sink" in task.lower() and any(k in task.lower() for k in FEATURE_SINK_KEYS)
    }


def collect_counts(phase: Path) -> dict[str, int]:
    """Presence counts per gate key, from exactly the sources the report shows."""
    counts: dict[str, int] = {}
    stages_dir = phase / "stages"
    capture_dir = phase / "capture"
    job_id = _run_job_id(stages_dir)

    per, _ = java_containers(capture_dir)
    for stats in per.values():
        for key, value in java_presence_counts(stats).items():
            counts[key] = counts.get(key, 0) + value

    raw_lags = parse_raw_sample(_read(capture_dir / "raw-sample.jsonl"))
    if raw_lags:
        counts["S1.raw_sample"] = len(raw_lags)

    samples = _phase_prom(stages_dir)
    if samples:
        if max_quantiles(samples, MONITOR_FAMILY, job_id=job_id):
            counts["S6.monitor"] = int(
                sum(s.value for s in samples if s.metric == MONITOR_FAMILY + "_count")
            )
        tracks = tracker_by_task(samples, job_id=job_id)
        if tracks:
            track_counts = tracker_counts_by_task(samples, job_id=job_id)
            counts["S7.tracker"] = int(sum(track_counts.values()) or 1)
            sinks = feature_sinks(tracks)
            if sinks:
                counts["S9.last_sink"] = int(
                    sum(n for t, n in track_counts.items() if t in sinks) or 1
                )

    closed = _phase_closed_lags(stages_dir)
    if closed:
        counts["S8.feature_read"] = len(closed)
    return counts


def _worst_java(
    per: Mapping[str, Mapping[str, JavaStageStat]], metrics: Sequence[str]
) -> tuple[str, str, JavaStageStat] | None:
    best: tuple[str, str, JavaStageStat] | None = None
    for container, stats in per.items():
        for metric in metrics:
            stat = stats.get(metric)
            if stat is None:
                continue
            if best is None or (stat.p99 or -1) > (best[2].p99 or -1):
                best = (container, metric, stat)
    return best


def _java_row(
    sid: str, boundary: str, throughput: str, pick: tuple[str, str, JavaStageStat] | None
) -> ProfileRow:
    if pick is None:
        return ProfileRow(sid, boundary, throughput, None, source="no Java samples")
    container, metric, stat = pick
    return ProfileRow(
        sid,
        boundary,
        throughput,
        Summary.exported(n=stat.n, p50=stat.p50, p90=stat.p90, p95=None, p99=stat.p99),
        source=f"{metric} @ {container} (median of {stat.windows} windows)",
    )


def _java_detail_table(per: Mapping[str, Mapping[str, JavaStageStat]]) -> str:
    lines = [
        "## Java stages by container",
        "",
        "| metric | container | windows | n | p50 | p90 | p99 |",
        "|---|---|---|---|---|---|---|",
    ]
    for container in sorted(per):
        for metric in sorted(per[container]):
            s = per[container][metric]
            lines.append(
                f"| {metric} | {container} | {s.windows} | {s.n} | "
                f"{fmt_num(s.p50)} | {fmt_num(s.p90)} | {fmt_num(s.p99)} |"
            )
    return "\n".join(lines) + "\n"


def _tracker_detail_table(
    tracks: Mapping[str, Mapping[str, float]], counts: Mapping[str, float]
) -> str:
    lines = [
        "## Flink tracker by operator",
        "",
        "| operator | p50 | p95 | p99 | count |",
        "|---|---|---|---|---|",
    ]
    for task in sorted(tracks):
        q = tracks[task]
        lines.append(
            f"| {task} | {fmt_num(q.get('0.5'))} | {fmt_num(q.get('0.95'))} | "
            f"{fmt_num(q.get('0.99'))} | {int(counts.get(task, 0))} |"
        )
    return "\n".join(lines) + "\n"


def build_report(phase: Path) -> tuple[list[ProfileRow], str, dict[str, int]]:
    """Phase evidence dir -> (step rows, detail sections, presence counts)."""
    stages_dir = phase / "stages"
    capture_dir = phase / "capture"
    per, counters = java_containers(capture_dir)
    job_id = _run_job_id(stages_dir)
    counts = collect_counts(phase)

    tick_rate = sum(
        (c["tick.throughput"].rate_per_s or 0.0)
        for c in counters.values()
        if "tick.throughput" in c
    )
    tick_text = (
        f"{tick_rate:,.0f} ticks/s ({len(per)} containers)"
        if tick_rate
        else PLACEHOLDER
    )
    raw_rate = raw_append_rate(_read(stages_dir / "read-lag.tsv"))
    raw_text = f"{raw_rate:,.0f} rows/s" if raw_rate else PLACEHOLDER

    rows: list[ProfileRow] = []

    raw_lags = parse_raw_sample(_read(capture_dir / "raw-sample.jsonl"))
    rows.append(
        ProfileRow(
            "S1",
            STAGES[0].boundary,
            raw_text,
            Summary.of(raw_lags) if raw_lags else None,
            source=(f"raw sample n={len(raw_lags)}" if raw_lags else "raw sample absent"),
        )
    )
    rows.append(
        _java_row(
            "S2", STAGES[1].boundary, tick_text,
            _worst_java(per, ["stage.ipc_latency"]),
        )
    )
    rows.append(
        _java_row(
            "S3", STAGES[2].boundary, tick_text,
            _worst_java(per, ["stage.decode_latency", "stage.batching_latency",
                              "stage.routing_latency"]),
        )
    )
    rows.append(
        _java_row(
            "S4", STAGES[3].boundary, raw_text,
            _worst_java(per, ["stage.fluss_submit_latency", "stage.fluss_ack_latency",
                              "append.latency.ms"]),
        )
    )
    rows.append(
        _java_row(
            "S5", STAGES[4].boundary, raw_text,
            _worst_java(per, ["stage.end_to_end_latency"]),
        )
    )

    samples = _phase_prom(stages_dir)
    monitor = max_quantiles(samples, MONITOR_FAMILY, job_id=job_id)
    monitor_count = int(
        sum(s.value for s in samples if s.metric == MONITOR_FAMILY + "_count")
    )
    monitor_text = source_records_rate(_read(stages_dir / "stages.tsv"))
    rows.append(
        ProfileRow(
            "S6",
            STAGES[5].boundary,
            f"{monitor_text:,.0f} rows/s" if monitor_text else PLACEHOLDER,
            summary_from_quantiles(monitor, n=monitor_count) if monitor else None,
            source="compute.latency.ingest_to_monitor (TM Prom, latest snapshot)",
        )
    )

    tracks = tracker_by_task(samples, job_id=job_id)
    track_counts = tracker_counts_by_task(samples, job_id=job_id)
    if tracks:
        worst_task = max(tracks, key=lambda t: tracks[t].get("0.99", -1))
        rows.append(
            ProfileRow(
                "S7",
                STAGES[6].boundary,
                PLACEHOLDER,
                summary_from_quantiles(
                    tracks[worst_task], n=int(track_counts.get(worst_task, 0))
                ),
                source=f"slowest operator: {worst_task} (tracker, latest snapshot)",
            )
        )
    else:
        rows.append(ProfileRow("S7", STAGES[6].boundary, PLACEHOLDER, None,
                               source="tracker absent"))

    closed_lags = _phase_closed_lags(stages_dir)
    closed_text = _read(stages_dir / "closed-read.tsv")
    closed_epochs = [
        int(line.split("\t", 1)[0])
        for line in closed_text.splitlines()[1:]
        if line.strip() and line.split("\t", 1)[0].isdigit()
    ]
    feature_rate = None
    if closed_lags and len(closed_epochs) > 1:
        span_s = (max(closed_epochs) - min(closed_epochs)) / 1000.0
        feature_rate = len(closed_lags) / span_s if span_s > 0 else None
    rows.append(
        ProfileRow(
            "S8",
            STAGES[7].boundary,
            f"{feature_rate:,.1f} windows/s (sampled)" if feature_rate else PLACEHOLDER,
            Summary.of(closed_lags) if closed_lags else None,
            source="candle_closed first sighting minus window end (probe samples)",
        )
    )

    sinks = feature_sinks(tracks)
    if sinks:
        worst_sink = max(sinks, key=lambda t: sinks[t].get("0.99", -1))
        rows.append(
            ProfileRow(
                "S9",
                STAGES[8].boundary,
                f"raw {raw_text} vs feature {PLACEHOLDER if not feature_rate else f'{feature_rate:,.1f}/s'}",
                summary_from_quantiles(
                    sinks[worst_sink], n=int(track_counts.get(worst_sink, 0))
                ),
                source=f"sink tracker: {worst_sink} (event time -> feature row)",
            )
        )
    else:
        rows.append(ProfileRow("S9", STAGES[8].boundary, PLACEHOLDER, None,
                               source="sink tracker absent"))

    details = _java_detail_table(per)
    if tracks:
        details += "\n" + _tracker_detail_table(tracks, track_counts)
    return rows, details, counts


# ── CLI (offline) ───────────────────────────────────────────────────────────


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="stage_profiler.py",
        description="Greenfield stage profiler — pure report core (offline).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("stages", help="print the S1..S9 registry")
    java = sub.add_parser("java", help="summarize one java.out OTLP log")
    java.add_argument("path", help="path to a j1-*/java.out file")
    presence = sub.add_parser("presence", help="run the smoke presence gate on a phase dir")
    presence.add_argument("--phase", required=True, help="phase evidence directory")
    report = sub.add_parser("report", help="render profile.md/profile.tsv from a phase dir")
    report.add_argument("--phase", required=True, help="phase evidence directory")
    report.add_argument("--out", required=True, help="directory for profile.md / profile.tsv")
    report.add_argument("--title", default="Stage profile")
    args = parser.parse_args(argv)

    if args.cmd == "stages":
        sys.stdout.write(registry_table())
    elif args.cmd == "java":
        windows = parse_java_out(Path(args.path).read_text(errors="replace"))
        stages, counters = java_windows_to_stats(windows)
        sys.stdout.write(render_java_stats(stages, counters))
    elif args.cmd == "presence":
        phase = Path(args.phase)
        counts = collect_counts(phase)
        failures = presence_failures(counts)
        payload = {"phase": str(phase), "counts": counts, "failures": failures}
        sys.stdout.write(json.dumps(payload, indent=2) + "\n")
        return 1 if failures else 0
    elif args.cmd == "report":
        phase = Path(args.phase)
        rows, details, counts = build_report(phase)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "profile.md").write_text(
            render_markdown(rows, title=args.title) + "\n" + details
        )
        (out / "profile.tsv").write_text(render_tsv(rows))
        (out / "presence.json").write_text(
            json.dumps({"counts": counts, "failures": presence_failures(counts)}, indent=2)
            + "\n"
        )
        sys.stdout.write(render_markdown(rows, title=args.title))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
