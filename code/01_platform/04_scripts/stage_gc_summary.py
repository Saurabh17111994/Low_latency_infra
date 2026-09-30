#!/usr/bin/env python3
"""GC/safepoint summariser for stage-profile rounds (W3-i, CHG-451).

The residual-stall attribution (docs/plans/2026-09-30-w3-w4-design-note.md)
needs every JVM stop >= 15 ms with a wall-clock (IST) timestamp, so a round's
KPI spike windows can be lined up against the TM and ingestion-writer pauses.
This script scans a phase dir for the ``gc.log`` files that ``stage-profile.sh``
pulls into the evidence (``stages/tm-gc.log``,
``capture/ticks/sp-ingestion-*.gc.log``), parses the ``-Xlog:gc*,safepoint``
lines and writes ``<phase>/stages/gc-summary.txt``.

Usage: ``stage_gc_summary.py <phase-dir> [<phase-dir> ...]``
Exit code is always 0 unless called with bad arguments: instrumentation must
never fail a measurement round.
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

MIN_MS = 15.0
TOP_N = 8
IST = timezone(timedelta(hours=5, minutes=30))

_TS = re.compile(r"\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)[+-]\d{4}\]")
_PAUSE = re.compile(r"([\d.]+)ms\s*$")
_SAFE_AT = re.compile(r"At safepoint:\s*(\d+)\s*ns")
_SAFE_TOTAL = re.compile(r"Total:\s*(\d+)\s*ns")


@dataclass
class Event:
    ts: datetime | None
    ms: float
    kind: str

    def ist_str(self) -> str:
        if self.ts is None:
            return "--:--:--"
        return self.ts.astimezone(IST).strftime("%H:%M:%S.%f")[:-3]


def parse_gc(path: Path) -> list[Event]:
    """Extract GC pauses / safepoints from one ``-Xlog`` file."""
    events: list[Event] = []
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return events
    for line in lines:
        if "ms" not in line and "ns" not in line:
            continue
        ts: datetime | None = None
        m = _TS.search(line)
        if m:
            try:
                ts = datetime.strptime(
                    m.group(1), "%Y-%m-%dT%H:%M:%S.%f"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                ts = None
        candidates: list[tuple[float, str]] = []
        # Only stop-the-world "Pause ..." lines are stop-worthy; the per-phase
        # detail lines ("GC(n) Phase 1: ... 3.123ms") also end in "<n>ms" but
        # do not stop the JVM (validated against the 2026-09-30 TM log).
        if "Pause" in line:
            pause = _PAUSE.search(line.rstrip())
            if pause:
                candidates.append((float(pause.group(1)), "gc-pause"))
        at = _SAFE_AT.search(line)
        if at:
            total = _SAFE_TOTAL.search(line)
            ms = max(int(at.group(1)), int(total.group(1)) if total else 0) / 1e6
            candidates.append((ms, "safepoint"))
        if candidates:
            ms, kind = max(candidates)
            events.append(Event(ts, ms, kind))
    return events


def _gc_files(phase_dir: Path) -> list[Path]:
    found: set[Path] = set()
    for pattern in ("**/*gc*.log", "**/*.gc.log"):
        found.update(phase_dir.glob(pattern))
    return sorted(found)


def summarize(phase_dir: Path, min_ms: float = MIN_MS) -> Path:
    """Write ``<phase>/stages/gc-summary.txt``; returns its path."""
    out_path = phase_dir / "stages" / "gc-summary.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    files = _gc_files(phase_dir)

    lines = [
        f"GC/safepoint summary — {phase_dir}",
        f"floor: {min_ms:.1f} ms; times IST; sources: TM + ingestion JVM logs",
        "",
    ]
    overall: tuple[float, str] | None = None
    total = 0
    for f in files:
        events = [e for e in parse_gc(f) if e.ms >= min_ms]
        rel = f.relative_to(phase_dir)
        if not events:
            lines.append(f"  {rel}: 0 events >= floor")
            continue
        total += len(events)
        emax = max(events, key=lambda e: e.ms)
        lines.append(f"  {rel}: {len(events)} events >= floor, max {emax.ms:.1f} ms")
        for e in sorted(events, key=lambda e: -e.ms)[:TOP_N]:
            lines.append(f"     {e.ist_str()}  {e.kind:9s} {e.ms:7.1f} ms")
        if overall is None or emax.ms > overall[0]:
            overall = (emax.ms, str(rel))

    lines.append("")
    if overall is None:
        lines.append("no gc logs with events >= floor found")
    else:
        lines.append(
            f"overall max {overall[0]:.1f} ms ({overall[1]}); "
            f"files={len(files)}; events={total}"
        )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("phase_dirs", nargs="+", type=Path)
    ap.add_argument("--min-ms", type=float, default=MIN_MS)
    args = ap.parse_args(argv)
    for d in args.phase_dirs:
        if not d.is_dir():
            print(f"gc-summary: skip (not a dir): {d}", file=sys.stderr)
            continue
        out = summarize(d, args.min_ms)
        print(f"gc-summary: wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
