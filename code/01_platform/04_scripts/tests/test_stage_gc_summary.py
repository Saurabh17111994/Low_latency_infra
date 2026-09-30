"""Behaviour test for ``stage_gc_summary.py`` (gate step 3, CHG-451/W3-i).

The summariser turns the raw JVM ``gc.log`` files (TM + per-ingestion
containers, pulled into a phase dir by ``stage-profile.sh``) into a compact
evidence file: every GC pause / safepoint >= 15 ms with its IST timestamp, so a
later round can line outliers (KPI spike windows) up against JVM stops.

The fixtures below are real line shapes from the TM log:
  * a safepoint line with ``At safepoint`` / ``Total`` in ns;
  * a G1 pause line ending in ``<ms>ms``;
  * noise lines that must be ignored.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "code" / "01_platform" / "04_scripts"
sys.path.insert(0, str(SCRIPTS))

import stage_gc_summary as summary  # noqa: E402


SAFEPOINT_BIG = (
    "[2026-09-30T05:55:34.252+0000][1157.824s][info][safepoint   ] "
    'Safepoint "G1CollectForAllocation", Time since last: 91426715086 ns, '
    "Reaching safepoint: 49821 ns, Cleanup: 8804 ns, "
    "At safepoint: 25412212 ns, Total: 25470837 ns"
)
PAUSE_BIG = (
    "[2026-09-30T05:42:32.299+0000][42.1s][info][gc] GC(578) "
    "Pause Young (Mixed) (G1 Evacuation Pause) 1183M->100M(2202M) 35.9ms"
)
PAUSE_SMALL = (
    "[2026-09-30T05:42:33.367+0000][42.2s][info][gc] GC(579) "
    "Pause Young (Normal) (G1 Evacuation Pause) 1100M->90M(2202M) 5.4ms"
)
NOISE = "[2026-09-30T05:42:33.367+0000][42.2s][info][gc,heap] Heap region size: 1M"
PHASE_LINE = (
    "[2026-09-30T05:42:24.100+0000][42.1s][info][gc,phases] "
    "GC(577) Phase 1: 3.123ms"
)


def _write_log(tmp_path: Path, name: str, lines: list[str]) -> Path:
    p = tmp_path / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def test_parser_extracts_pauses_and_safepoints_with_ist_times(tmp_path: Path) -> None:
    log = _write_log(
        tmp_path, "tm-gc.log", [NOISE, PHASE_LINE, SAFEPOINT_BIG, PAUSE_BIG, PAUSE_SMALL]
    )
    events = summary.parse_gc(log)
    big = [e for e in events if e.ms >= 15.0]
    assert len(big) == 2, f"expected the 25.4 ms safepoint + 35.9 ms pause, got {events}"
    # The per-phase detail line ends in "ms" but must not be a stop event.
    assert all("3.123" not in str(e.ms) for e in events), events
    kinds = sorted(e.kind for e in big)
    assert kinds == ["gc-pause", "safepoint"]
    # IST rendering: the 05:42:32 UTC pause is 11:12:32 IST.
    pause = [e for e in big if e.kind == "gc-pause"][0]
    assert pause.ist_str().startswith("11:12:32"), pause.ist_str()
    assert round(max(e.ms for e in events), 1) == 35.9


def test_summarize_writes_evidence_file_and_reports_max(tmp_path: Path) -> None:
    _write_log(tmp_path, "stages/tm-gc.log", [SAFEPOINT_BIG, PAUSE_BIG])
    _write_log(tmp_path, "capture/ticks/sp-ingestion-0.gc.log", [PAUSE_SMALL])
    out = summary.summarize(tmp_path)
    body = out.read_text(encoding="utf-8")
    assert "overall max 35.9 ms" in body
    assert "tm-gc.log" in body
    # The 5.4 ms pause is below the 15 ms floor and must not be listed.
    assert "5.4" not in body
    assert "11:12:32" in body


def test_summarize_with_no_gc_logs_is_a_noop_not_a_failure(tmp_path: Path) -> None:
    out = summary.summarize(tmp_path)
    assert out.exists()
    assert "no gc logs" in out.read_text(encoding="utf-8").lower()


def test_cli_runs_and_exits_zero(tmp_path: Path) -> None:
    _write_log(tmp_path, "stages/tm-gc.log", [PAUSE_BIG])
    rc = subprocess.run(
        [sys.executable, str(SCRIPTS / "stage_gc_summary.py"), str(tmp_path)],
        capture_output=True, text=True,
    )
    assert rc.returncode == 0, rc.stderr
    assert (tmp_path / "stages" / "gc-summary.txt").exists()
