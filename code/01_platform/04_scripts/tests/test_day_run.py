#!/usr/bin/env python3
"""Daily single-command runner tests (CHG-324, plan P3-3).

Two layers:
  * pure decisions -- universe contract, posture guard, SignalJob singleton
    rules, and the I1-I9 evaluator over synthetic facts (no I/O);
  * command flow -- ``cmd_start``/``cmd_status``/``cmd_stop`` with a fake
    Runner/Collector, asserting the native calls made and the exit codes.

Auto-discovered by gate step 3 (``test_*.py``, pytest from the repo root).
"""

import dataclasses
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "code" / "01_platform" / "04_scripts" / "day_run.py"
SPEC = importlib.util.spec_from_file_location("day_run", SCRIPT)
day_run = importlib.util.module_from_spec(SPEC)
sys.modules["day_run"] = day_run  # dataclasses resolves cls.__module__ via sys.modules
SPEC.loader.exec_module(day_run)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def now_ms() -> int:
    return int(time.time() * 1000)


def make_facts(**overrides) -> day_run.Facts:
    expected = ["fluss-coordinator", "fluss-tablet", "flink-jobmanager",
                "flink-taskmanager", "ingestion", "execution-bridge",
                "execution-gateway", "nautilus"]
    services = {s: {"state": "running", "health": ""} for s in expected}
    facts = day_run.Facts(
        expected=expected,
        services=services,
        jobs=[{"id": "j1", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}],
        checkpoints={"j1": {"latest_completed_ms": now_ms() - 3000}},
        fluss={
            "raw": {"ok": True, "log_end": 1000, "delta": 250},
            "candles": {"ok": True, "log_end": 500, "delta": 30},
            "signals": {"ok": True, "log_end": 10, "delta": 1},
            "window_s": 20,
        },
        log_errors=[],
        state={"latest_savepoint": "file:///checkpoints/savepoints/savepoint-a",
               "latest_checkpoint": "file:///checkpoints/j1/chk-6"},
        container_env={
            "execution-gateway": {"EXECUTION_ENABLED": "false"},
            "execution-bridge": {"EXECUTION_BRIDGE_MODE": "disabled"},
            "nautilus": {"EXECUTION_ENABLED": "false"},
        },
        effective_tokens=2433,
        universe={"mode": "full", "tokens": 2433, "connections": 3},
        session={"open": True, "label": "OPEN (09:21 IST)"},
        notes=["nautilus_halted"],
    )
    for key, value in overrides.items():
        setattr(facts, key, value)
    return facts


class FakeRunner(day_run.Runner):
    """Records commands; returns canned stdout per argv prefix."""

    def __init__(self, handler=None, staleness=None):
        super().__init__(out=io.StringIO())
        self.calls = []
        self.handler = handler or (lambda argv, env: "")
        self.staleness = list(staleness or [(False, "all FRESH")])

    def image_staleness(self):
        return self._next(self.staleness)

    @staticmethod
    def _next(seq):
        return seq.pop(0) if len(seq) > 1 else seq[0]

    def run(self, argv, env=None, check=True, capture=True, timeout=1800):
        self.calls.append({"argv": [str(a) for a in argv], "env": dict(env or {})})
        result = self.handler([str(a) for a in argv], dict(env or {}))
        rc, out = 0, ""
        if isinstance(result, tuple):
            rc, out = result
        else:
            out = result or ""
        if rc and check:
            raise subprocess.CalledProcessError(rc, argv, output=out)
        return out

    def targets(self) -> list:
        """make targets invoked (argv: make -C ROOT <target>)."""
        found = []
        for call in self.calls:
            if "make" in call["argv"]:
                index = call["argv"].index("make")
                if len(call["argv"]) > index + 3:
                    found.append(call["argv"][index + 3])
        return found

    def has(self, needle: str) -> bool:
        return any(needle in " ".join(call["argv"]) for call in self.calls)


class FakeCollector:
    """Scripted collector: sequences are consumed per call, last value sticks."""

    def __init__(self, facts=None, jobs=None, state=None, checkpoints=None):
        self.facts = list(facts or [make_facts()])
        self.jobs_seq = list(jobs or [[{"id": "j1", "name": day_run.SIGNAL_JOB_NAME,
                                        "state": "RUNNING"}]])
        self.state = state if state is not None else {
            "latest_savepoint": "file:///checkpoints/savepoints/savepoint-a",
            "latest_checkpoint": "file:///checkpoints/j1/chk-6",
        }
        self.checkpoints_map = checkpoints if checkpoints is not None else {
            "j1": {"latest_completed_ms": now_ms() - 3000}}

    def _next(self, seq):
        return seq.pop(0) if len(seq) > 1 else seq[0]

    def collect(self, **kwargs):
        return self._next(self.facts)

    def jobs(self):
        return self._next(self.jobs_seq)

    def state_paths(self):
        return self.state

    def checkpoints(self, jobs):
        return self.checkpoints_map


# --------------------------------------------------------------------------
# pure decisions
# --------------------------------------------------------------------------


class UniverseTests(unittest.TestCase):
    def test_full_dev_contract(self):
        universe = day_run.resolve_universe("full", "dev", {"full": 2433, "approved": 1024})
        self.assertEqual(universe.connections, 3)
        self.assertTrue(universe.approval)
        self.assertEqual(universe.tokens, 2433)

    def test_approved_contract(self):
        universe = day_run.resolve_universe("approved", "dev", {"full": 2433, "approved": 1024})
        self.assertEqual((universe.connections, universe.approval, universe.tokens),
                         (1, False, 1024))

    def test_full_refused_in_production(self):
        with self.assertRaises(day_run.Refusal) as ctx:
            day_run.resolve_universe("full", "production", {"full": 2433, "approved": 1024})
        self.assertIn("dev-only", str(ctx.exception))

    def test_capacity_refused_with_numbers(self):
        with self.assertRaises(day_run.Refusal) as ctx:
            day_run.resolve_universe("full", "dev", {"full": 4000, "approved": 1024})
        self.assertIn("4000", str(ctx.exception))
        self.assertIn("3072", str(ctx.exception))

    def test_unknown_mode_refused(self):
        with self.assertRaises(day_run.Refusal):
            day_run.resolve_universe("everything", "dev", {"full": 1, "approved": 1})


class PostureTests(unittest.TestCase):
    def test_clean(self):
        self.assertEqual(day_run.posture_violations({}), [])
        self.assertEqual(day_run.posture_violations(
            {"EXECUTION_ENABLED": "false", "EXECUTION_BRIDGE_MODE": "disabled"}), [])

    def test_live_flags(self):
        bad = day_run.posture_violations({"EXECUTION_ENABLED": "true",
                                          "EXECUTION_BRIDGE_MODE": "live"})
        self.assertEqual(len(bad), 2)


class CheckpointPayloadTests(unittest.TestCase):
    """Flink /jobs/<id>/checkpoints reports latest_ack_timestamp/trigger_timestamp
    (not end_time) -- the 2026-09-26 dry run hit exactly this shape."""

    def test_flink_ack_timestamp_shape(self):
        payload = {"latest": {"completed": {
            "id": 38, "latest_ack_timestamp": 1790422325613,
            "trigger_timestamp": 1790422325494}}}
        self.assertEqual(day_run._completed_checkpoint_ms(payload), 1790422325613)

    def test_end_time_fallback(self):
        self.assertEqual(
            day_run._completed_checkpoint_ms({"latest": {"completed": {"end_time": 123}}}), 123)

    def test_no_completed(self):
        self.assertIsNone(day_run._completed_checkpoint_ms({"latest": {}}))


class SignalJobDecisionTests(unittest.TestCase):
    def test_keep(self):
        decision = day_run.decide_signaljob(
            [{"id": "j1", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}], {}, False)
        self.assertEqual(decision.action, "keep")

    def test_two_jobs_red(self):
        decision = day_run.decide_signaljob(
            [{"id": "a", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"},
             {"id": "b", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}], {}, False)
        self.assertEqual(decision.action, "red")
        self.assertIn("split-brain", decision.reason)

    def test_restore_prefers_savepoint(self):
        decision = day_run.decide_signaljob([], {
            "latest_savepoint": "sp", "latest_checkpoint": "ck"}, False)
        self.assertEqual((decision.action, decision.path), ("restore", "sp"))

    def test_restore_from_checkpoint(self):
        decision = day_run.decide_signaljob([], {"latest_checkpoint": "ck"}, False)
        self.assertEqual((decision.action, decision.path), ("restore", "ck"))

    def test_fresh_requires_flag(self):
        self.assertEqual(day_run.decide_signaljob([], {}, False).action, "refuse")
        self.assertEqual(day_run.decide_signaljob([], {}, True).action, "fresh")


class EvaluateTests(unittest.TestCase):
    def test_green_in_session(self):
        checks = day_run.evaluate(make_facts())
        self.assertTrue(all(c.ok and not c.pending for c in checks), checks)
        self.assertIn("GREEN 9/9", day_run.render(checks, make_facts()))

    def test_off_session_pending_exit_zero(self):
        facts = make_facts(session={"open": False, "label": "CLOSED (18:00 IST)"},
                           fluss={"raw": {"ok": True, "log_end": 1},
                                  "candles": {"ok": True, "log_end": 1},
                                  "signals": {"ok": True, "log_end": 1},
                                  "window_s": 20})
        checks = day_run.evaluate(facts)
        pending = [c for c in checks if c.pending]
        self.assertEqual([c.ident for c in pending], ["I3", "I4"])
        self.assertIn("PENDING", day_run.render(checks, facts))

    def test_no_running_job_red(self):
        checks = day_run.evaluate(make_facts(jobs=[]))
        i5 = next(c for c in checks if c.ident == "I5")
        self.assertFalse(i5.ok)
        self.assertIn("no running SignalJob", i5.detail)

    def test_two_jobs_red(self):
        checks = day_run.evaluate(make_facts(
            jobs=[{"id": "a", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"},
                  {"id": "b", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}]))
        i5 = next(c for c in checks if c.ident == "I5")
        self.assertFalse(i5.ok)
        self.assertIn("split-brain", i5.detail)

    def test_stale_checkpoint_red(self):
        stale = now_ms() - 10 * day_run.CHECKPOINT_INTERVAL_MS
        checks = day_run.evaluate(make_facts(
            checkpoints={"j1": {"latest_completed_ms": stale}}))
        i5 = next(c for c in checks if c.ident == "I5")
        self.assertFalse(i5.ok)

    def test_live_flag_red(self):
        facts = make_facts(container_env={
            "execution-gateway": {"EXECUTION_ENABLED": "true"},
            "execution-bridge": {"EXECUTION_BRIDGE_MODE": "disabled"},
            "nautilus": {"EXECUTION_ENABLED": "false"},
        })
        checks = day_run.evaluate(facts)
        self.assertFalse(next(c for c in checks if c.ident == "I6").ok)
        self.assertFalse(next(c for c in checks if c.ident == "I7").ok)

    def test_manifest_mismatch_red(self):
        checks = day_run.evaluate(make_facts(effective_tokens=1024))
        i7 = next(c for c in checks if c.ident == "I7")
        self.assertFalse(i7.ok)
        self.assertIn("does not match", i7.detail)

    def test_error_budget_red(self):
        checks = day_run.evaluate(make_facts(log_errors=["ingestion: FATAL - x"]))
        self.assertFalse(next(c for c in checks if c.ident == "I8").ok)

    def test_no_restorable_state_red(self):
        checks = day_run.evaluate(make_facts(
            state={"latest_savepoint": None, "latest_checkpoint": None}))
        self.assertFalse(next(c for c in checks if c.ident == "I9").ok)


# --------------------------------------------------------------------------
# command flow
# --------------------------------------------------------------------------


class CommandFlowTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        patcher = mock.patch.dict(os.environ, {
            "DAY_EVIDENCE_DIR": os.path.join(self._tmp.name, "evidence"),
            "ALLOW_FRESH": "0",
        })
        patcher.start()
        self.addCleanup(patcher.stop)
        # readiness polling is a real 5s sleep; unit tests assert logic, not wall time
        sleep_patcher = mock.patch.object(day_run.time, "sleep", lambda *_: None)
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

    def test_start_keeps_running_job(self):
        runner = FakeRunner()
        collector = FakeCollector()
        rc = day_run.main(["start"], runner=runner,
                          collector_factory=lambda r: collector)
        self.assertEqual(rc, 0)
        self.assertIn("up", runner.targets())
        self.assertFalse(runner.has("rollout-savepoint"))
        self.assertFalse(runner.has("COMPUTE_SUBMIT_SIGNAL=1"))
        # phase 2 activates the execution profile
        self.assertTrue(any(call["env"].get("COMPOSE_PROFILES") == "execution-t3"
                            for call in runner.calls if "make" in call["argv"]))

    def test_start_ready_wait_default_covers_cold_start(self):
        # F1 (2026-09-28): the off-hours cold-start drill measured 22 min 56 s of
        # tablet recovery (candle_live/candle_closed KV changelog replay) and the
        # 900s ceiling went RED while recovery was still progressing. The wait is
        # state-based (it exits as soon as the stack is ready), so the ceiling is
        # a cold-start budget, not a target.
        seen = []
        with mock.patch.object(
                day_run, "wait_ready",
                side_effect=lambda c, u, r, timeout_s: seen.append(timeout_s)):
            rc = day_run.main(["start"], runner=FakeRunner(),
                              collector_factory=lambda r: FakeCollector())
        self.assertEqual(rc, 0)
        self.assertEqual([3600], seen,
                         "the default ready-wait ceiling must cover a measured "
                         "cold-start recovery (~23 min) with margin")

    def test_start_ready_wait_honours_env_override(self):
        seen = []
        with mock.patch.dict(os.environ, {"DAY_READY_TIMEOUT_S": "1234"}), \
                mock.patch.object(
                    day_run, "wait_ready",
                    side_effect=lambda c, u, r, timeout_s: seen.append(timeout_s)):
            rc = day_run.main(["start"], runner=FakeRunner(),
                              collector_factory=lambda r: FakeCollector())
        self.assertEqual(rc, 0)
        self.assertEqual([1234], seen, "DAY_READY_TIMEOUT_S must override the default")

    def test_start_restores_from_state(self):
        runner = FakeRunner()
        collector = FakeCollector(
            jobs=[[], [{"id": "a", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}]],
            state={"latest_savepoint": None,
                   "latest_checkpoint": "file:///checkpoints/j1/chk-6"},
            checkpoints={"a": {"latest_completed_ms": now_ms() - 1000}},
        )
        rc = day_run.main(["start"], runner=runner,
                          collector_factory=lambda r: collector)
        self.assertEqual(rc, 0)
        self.assertTrue(runner.has("rollout-savepoint"))
        self.assertTrue(runner.has("RECOVERY_PATH=file:///checkpoints/j1/chk-6"))

    def test_start_refuses_when_no_state_and_no_flag(self):
        runner = FakeRunner()
        collector = FakeCollector(jobs=[[]], state={})
        rc = day_run.main(["start"], runner=runner,
                          collector_factory=lambda r: collector)
        self.assertEqual(rc, day_run.EXIT_REFUSED)
        self.assertFalse(runner.has("rollout-savepoint"))
        self.assertFalse(runner.has("COMPUTE_SUBMIT_SIGNAL=1"))

    def test_start_fresh_when_allow_fresh(self):
        with mock.patch.dict(os.environ, {"ALLOW_FRESH": "1"}):
            runner = FakeRunner()
            collector = FakeCollector(
                jobs=[[], [{"id": "a", "name": day_run.SIGNAL_JOB_NAME,
                            "state": "RUNNING"}]],
                state={}, checkpoints={"a": {"latest_completed_ms": now_ms() - 1000}})
            rc = day_run.main(["start"], runner=runner,
                              collector_factory=lambda r: collector)
        self.assertEqual(rc, 0)
        self.assertTrue(any(call["env"].get("COMPUTE_SUBMIT_SIGNAL") == "1"
                            for call in runner.calls if "make" in call["argv"]))

    def test_start_refuses_stale_images(self):
        runner = FakeRunner(staleness=[(True, "STALE 01_docker-ingestion:latest")])
        rc = day_run.main(["start"], runner=runner,
                          collector_factory=lambda r: FakeCollector())
        self.assertEqual(rc, day_run.EXIT_REFUSED)
        self.assertNotIn("up", runner.targets())

    def test_start_rebuilds_when_asked(self):
        with mock.patch.dict(os.environ, {"REBUILD": "1"}):
            runner = FakeRunner(staleness=[(True, "STALE x"), (False, "all FRESH")])
            rc = day_run.main(["start"], runner=runner,
                              collector_factory=lambda r: FakeCollector())
        self.assertEqual(rc, 0)
        self.assertIn("images", runner.targets())

    def test_start_refuses_live_flag(self):
        with mock.patch.dict(os.environ, {"EXECUTION_ENABLED": "true"}):
            runner = FakeRunner()
            rc = day_run.main(["start"], runner=runner,
                              collector_factory=lambda r: FakeCollector())
        self.assertEqual(rc, day_run.EXIT_REFUSED)
        self.assertEqual(runner.calls, [])

    def test_status_red_when_job_missing(self):
        runner = FakeRunner()
        facts = make_facts(
            jobs=[],
            state={"latest_savepoint": None, "latest_checkpoint": None})
        rc = day_run.main(["status"], runner=runner,
                          collector_factory=lambda r: FakeCollector(facts=[facts]))
        self.assertEqual(rc, day_run.EXIT_RED)

    def test_stop_calls_down_and_preserves_state(self):
        runner = FakeRunner()
        rc = day_run.main(["stop"], runner=runner,
                          collector_factory=lambda r: FakeCollector())
        self.assertEqual(rc, 0)
        self.assertIn("down", runner.targets())
        self.assertTrue(any(call["env"].get("COMPOSE_PROFILES") == "execution-t3"
                            for call in runner.calls if "make" in call["argv"]))

    def test_evidence_written(self):
        runner = FakeRunner()
        day_run.main(["status"], runner=runner,
                     collector_factory=lambda r: FakeCollector(facts=[make_facts()]))
        board_file = pathlib.Path(os.environ["DAY_EVIDENCE_DIR"]) / "board.txt"
        self.assertTrue(board_file.exists())
        self.assertIn("[day] verdict", board_file.read_text())


class CheckpointSamplingTests(unittest.TestCase):
    """P4-4 finding (2026-09-28): I5 must report the checkpoint age observed at
    sample time. In-session collection spends 20s+ in Fluss probes *after*
    reading the checkpoint; recomputing the age at evaluate time turned a
    healthy ~9s checkpoint age into a false ~60-70s RED."""

    def test_age_sampled_with_the_checkpoint_read(self):
        clock = [1_000_000.0]

        class _Runner:
            def http_json(self, url, timeout=10):
                now_ms = int(clock[0] * 1000)
                return {"counts": {"completed": 1},
                        "latest": {"completed": {
                            "latest_ack_timestamp": now_ms - 5000,
                            "trigger_timestamp": now_ms - 6000}}}

        class _Probe(day_run.Collector):
            def __init__(self):
                super().__init__(runner=_Runner(), window_s=20)

            def services(self):
                return {"fluss-coordinator": {"state": "running"}}

            def expected_services(self):
                return []

            def jobs(self):
                return [{"id": "j1", "name": day_run.SIGNAL_JOB_NAME,
                         "state": "RUNNING"}]

            def state_paths(self):
                return {}

            def container_env(self, services):
                return {}

            def nautilus_halted(self):
                return False

            def log_errors(self, since):
                return []

            def effective_tokens(self, ingestion_running):
                return 2433

            def fluss_log_end(self, table, workdir):
                clock[0] += 10.0  # each probe leg costs wall clock
                return {"ok": True, "log_end": 1000}

        def fake_time():
            return clock[0]

        def fake_sleep(seconds):
            clock[0] += float(seconds)

        with mock.patch.object(day_run.time, "time", fake_time), mock.patch.object(
                day_run.time, "sleep", fake_sleep):
            facts = _Probe().collect(
                samples=True, evidence_dir=pathlib.Path(tempfile.mkdtemp()))
            age = day_run._checkpoint_age_ms(facts)

        # The collection burns 80s of wall clock (20s window + 6 probes x 10s)
        # after the checkpoint read; the age must stay at the ~5s observed.
        self.assertIsNotNone(age)
        self.assertLess(age, 10_000)


if __name__ == "__main__":
    unittest.main()
