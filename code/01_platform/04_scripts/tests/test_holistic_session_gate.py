#!/usr/bin/env python3
"""CHG-304 — a green smoke phase must not be printed over a pipeline that aggregated nothing.

Found on the 2026-09-23 17:44 run: the smoke phase printed "SMOKE PASS" while
`metrics-final.txt` recorded `multi-tf-aggregator | ... | 0` and every candle sink
`0 | 0`. `make holistic` had only ever been run outside 09:15-15:30 IST, where
TimeframeBucket drops every tick before any state is touched — so no candle, no
preview and no late drop can exist, and the smoke inject gate's late half could
never move (the injected 90s-late frames look like the token's first tick, and a
fresh slot takes no lateness branch). The 0.9.1 run on 2026-09-19 has the same
shape (aggregator out=0, sinks 0/0, late counter flat across all 8 subtasks), so
this is not a version difference.

These tests pin the three parts of the fix: the session decision and its
boundaries, the smoke output gate that now rejects an aggregation-free smoke, and
the wiring order that makes the bypass reach the submitted job at all.
"""
from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
HARNESS = REPO / "code/01_platform/04_scripts/holistic-measure.sh"
MARKER = "# ---------------------------------------------------------------- phases"

# The definitions region is loaded verbatim; only the lib it sources is stubbed,
# because nothing under test calls a pipeline_* function.
STUB_LIB = "pipeline_log() { :; }\n"


class SmokeSessionGate(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        scripts = self.tmp / "code/01_platform/04_scripts"
        scripts.mkdir(parents=True)
        (scripts / "pipeline-lib.sh").write_text(STUB_LIB)
        self.scripts = scripts
        self.defs = HARNESS.read_text().split(MARKER, 1)[0]

    def run_driver(self, driver: str) -> subprocess.CompletedProcess:
        path = self.scripts / "driver.sh"
        path.write_text(self.defs + "\n" + driver + "\n")
        return subprocess.run(["bash", str(path)], capture_output=True,
                              text=True, timeout=60)

    # ---- 1. session boundary: 09:15:00.000 inclusive, 15:30:00.000 exclusive
    def test_session_boundary(self) -> None:
        cases = {0: False, 554: False, 555: True, 929: True, 930: False, 1439: False}
        driver = "\n".join(
            f'session_is_open {m} && echo "{m}=open" || echo "{m}=closed"'
            for m in sorted(cases))
        r = self.run_driver(driver)
        self.assertEqual(r.returncode, 0, r.stderr)
        for minutes, want in cases.items():
            with self.subTest(minutes=minutes):
                self.assertIn(f"{minutes}={'open' if want else 'closed'}", r.stdout)

    # ---- 2. the smoke output gate
    def _snapshot(self, aggregator_out: str) -> Path:
        d = self.tmp / "phase"
        d.mkdir(exist_ok=True)
        (d / "metrics-final.txt").write_text(
            "STATE RUNNING\n"
            "Source: raw-table-1 -> raw-validation | 2256982 | 2256982\n"
            "fingerprint-dedup -> ingest-latency-monitor | 2256982 | 2256782\n"
            f"multi-tf-aggregator | 2256782 | {aggregator_out}\n"
            "candle-live-sink: Writer | 0 | 0\n")
        return d

    def _gate(self, path: Path, phase_s: int) -> subprocess.CompletedProcess:
        return self.run_driver(f'smoke_produced_output "{path}" {phase_s}; echo "GATE_RC=$?"')

    def test_zero_aggregation_fails_a_long_smoke_and_names_the_cause(self) -> None:
        r = self._gate(self._snapshot("0"), 180)
        self.assertIn("GATE_RC=1", r.stdout)
        self.assertIn("aggregated nothing", r.stderr)
        self.assertIn("session filter", r.stderr)

    def test_an_emitting_aggregator_passes(self) -> None:
        r = self._gate(self._snapshot("47420"), 180)
        self.assertIn("GATE_RC=0", r.stdout)
        self.assertIn("emitted 47420 records", r.stdout)

    def test_a_short_phase_is_skipped_not_failed(self) -> None:
        r = self._gate(self._snapshot("0"), 60)
        self.assertIn("GATE_RC=0", r.stdout)
        self.assertIn("SKIPPED", r.stdout)

    def test_a_missing_snapshot_is_unavailable_not_passed_by_assertion(self) -> None:
        d = self.tmp / "empty"
        d.mkdir()
        r = self._gate(d, 180)
        self.assertIn("GATE_RC=0", r.stdout)
        self.assertIn("UNAVAILABLE", r.stdout)

    # ---- 3. wiring: the bypass must be decided before the job is submitted,
    #         and the gate must actually run against the smoke phase.
    def test_bypass_is_exported_before_the_job_is_submitted(self) -> None:
        src = HARNESS.read_text()
        self.assertLess(
            src.index("export MULTITF_SESSION_BYPASS="),
            src.index("pipeline_submit_job ||"),
            "the bypass must be exported before the job is submitted")

    def test_driver_calls_the_gate_and_records_the_session(self) -> None:
        src = HARNESS.read_text()
        self.assertIn('smoke_produced_output "$PHASE_OUT/smoke" "$SMOKE_S"', src)
        self.assertIn('> "$PHASE_OUT/session.txt"', src)


if __name__ == "__main__":
    unittest.main()
