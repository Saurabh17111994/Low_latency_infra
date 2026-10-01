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
import stat
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


class EvidenceSecrecyTests(unittest.TestCase):
    """C2 (audit 2026-09-28): day evidence must never carry broker credentials."""

    def test_container_env_returns_only_the_posture_allowlist(self):
        class InspectRunner(day_run.Runner):
            def compose(self, args, env=None, check=True, timeout=300):
                return "cid-1\n"

            def run(self, argv, env=None, check=True, capture=True, timeout=1800):
                return json.dumps([
                    "EXECUTION_ENABLED=false",
                    "EXECUTION_BRIDGE_MODE=disabled",
                    "ARROW_APP_SECRET=super-secret",
                    "ARROW_PASSWORD=hunter2",
                    "ARROW_TOTP_KEY=ABCDEF",
                    "EXECUTION_BRIDGE_TOKEN=tok-123",
                    "PATH=/usr/bin",
                ])

        collector = day_run.Collector(InspectRunner())
        probe = collector.container_env(["execution-bridge"])
        self.assertEqual(probe.values, {"execution-bridge": {
            "EXECUTION_ENABLED": "false",
            "EXECUTION_BRIDGE_MODE": "disabled",
        }})
        self.assertEqual(probe.failures, [])
        self.assertEqual(day_run.POSTURE_ENV_KEYS,
                         ("EXECUTION_ENABLED", "EXECUTION_BRIDGE_MODE"))

    def test_redact_evidence_scrubs_credential_keys_only(self):
        scrubbed = day_run.redact_evidence({
            "container_env": {"ARROW_PASSWORD": "hunter2",
                              "EXECUTION_ENABLED": "false"},
            "effective_tokens": 2433,
            "universe": {"mode": "full", "tokens": 2433},
            "notes": ["nautilus_halted"],
            "nested": [{"API_KEY": "k-1"}, "plain"],
        })
        self.assertEqual(scrubbed["container_env"], {
            "ARROW_PASSWORD": "[REDACTED]", "EXECUTION_ENABLED": "false"})
        self.assertEqual(scrubbed["effective_tokens"], 2433)
        self.assertEqual(scrubbed["universe"]["tokens"], 2433)
        self.assertEqual(scrubbed["notes"], ["nautilus_halted"])
        self.assertEqual(scrubbed["nested"], [{"API_KEY": "[REDACTED]"}, "plain"])

    def test_write_evidence_scrubs_secrets_and_restricts_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            evidence_dir = pathlib.Path(tmp) / "ev"
            with mock.patch.dict(os.environ,
                                 {"DAY_EVIDENCE_DIR": str(evidence_dir)}):
                facts = make_facts(container_env={
                    "execution-bridge": {
                        "EXECUTION_BRIDGE_MODE": "disabled",
                        "ARROW_APP_SECRET": "super-secret",
                        "ARROW_PASSWORD": "hunter2",
                        "ARROW_TOTP_KEY": "ABCDEF",
                        "EXECUTION_BRIDGE_TOKEN": "tok-123",
                    }})
                written = day_run.write_evidence("start", "board text", facts)
            body = (written / "facts.json").read_text()
            for secret in ("super-secret", "hunter2", "ABCDEF", "tok-123"):
                self.assertNotIn(secret, body)
            self.assertIn("[REDACTED]", body)
            self.assertEqual(stat.S_IMODE(written.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((written / "facts.json").stat().st_mode),
                             0o600)
            self.assertEqual(stat.S_IMODE((written / "board.txt").stat().st_mode),
                             0o600)


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

    def test_terminal_failed_job_does_not_block_restore(self):
        # A completed job stays in the JobManager archive (no REST delete;
        # PATCH cancel -> 409), so it must not block the next run's restore.
        decision = day_run.decide_signaljob(
            [{"id": "old", "name": day_run.SIGNAL_JOB_NAME, "state": "FAILED"}],
            {"latest_checkpoint": "ck"}, False)
        self.assertEqual((decision.action, decision.path), ("restore", "ck"))

    def test_terminal_job_does_not_hide_the_running_one(self):
        decision = day_run.decide_signaljob(
            [{"id": "old", "name": day_run.SIGNAL_JOB_NAME, "state": "CANCELED"},
             {"id": "new", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}],
            {}, False)
        self.assertEqual((decision.action, decision.reason), ("keep", "RUNNING id=new"))

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

    # --- M2-5: a failed probe is amber, never green ---

    def test_env_probe_failure_is_amber_not_green(self):
        checks = day_run.evaluate(make_facts(
            env_probe_failures=["execution-gateway: CalledProcessError(1)"]))
        i6 = next(c for c in checks if c.ident == "I6")
        self.assertFalse(i6.ok, "an unread posture env must not pass I6")
        self.assertIn("posture probe failed", i6.detail)

    def test_log_scan_failure_is_amber_not_green(self):
        checks = day_run.evaluate(make_facts(
            log_scan_failed=True,
            notes=["log_scan_failed: log scan failed: docker gone"]))
        i8 = next(c for c in checks if c.ident == "I8")
        self.assertFalse(i8.ok, "an unread error window must not pass I8")
        self.assertIn("unverified", i8.detail)

    def test_manifest_probe_failure_is_amber_not_green(self):
        checks = day_run.evaluate(make_facts(
            manifest_probe_failed=True,
            notes=["manifest_probe_failed: manifest probe failed: docker gone"]))
        i7 = next(c for c in checks if c.ident == "I7")
        self.assertFalse(i7.ok, "an unread manifest window must not pass I7")
        self.assertIn("unverified", i7.detail)


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

    def test_start_restores_with_a_retained_failed_job(self):
        # Regression (2026-10-02 live smoke): the JM keeps the failed job in
        # its archive; start must restore over it instead of refusing.
        runner = FakeRunner()
        collector = FakeCollector(
            jobs=[[{"id": "old", "name": day_run.SIGNAL_JOB_NAME, "state": "FAILED"}],
                  [{"id": "old", "name": day_run.SIGNAL_JOB_NAME, "state": "FAILED"},
                   {"id": "a", "name": day_run.SIGNAL_JOB_NAME, "state": "RUNNING"}]],
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
                return day_run.EnvProbe()

            def nautilus_halted(self):
                return False

            def log_errors(self, since):
                return []

            def effective_tokens(self, ingestion_running):
                return 2433

            def fluss_log_end(self, table):
                clock[0] += 10.0  # each probe leg costs wall clock
                return {"ok": True, "log_end": 1000}

        def fake_time():
            return clock[0]

        def fake_sleep(seconds):
            clock[0] += float(seconds)

        with mock.patch.object(day_run.time, "time", fake_time), mock.patch.object(
                day_run.time, "sleep", fake_sleep):
            facts = _Probe().collect(samples=True)
            age = day_run._checkpoint_age_ms(facts)

        # The collection burns 80s of wall clock (20s window + 6 probes x 10s)
        # after the checkpoint read; the age must stay at the ~5s observed.
        self.assertIsNotNone(age)
        self.assertLess(age, 10_000)


# --------------------------------------------------------------------------
# readiness probe memory safety
# 2026-09-28: the 5s readiness poll ran the full collector, whose log reads
# captured whole `docker compose logs` outputs (4 services x 15 minutes plus
# the entire ingestion log) into strings. During a 36-minute Fluss recovery
# the runner reached 12 GB RSS + 16 GB swap and thrashed the host. The poll
# uses a readiness-only snapshot now, and the log readers stream line by line.
# --------------------------------------------------------------------------


class StreamRunner(day_run.Runner):
    """Runner double: keeps whole-output captures and streamed reads apart."""

    def __init__(self, lines=None, services=(), config_services=()):
        super().__init__(out=io.StringIO())
        self.calls = []
        self.streams = []
        self.lines = dict(lines or {})
        self.services = list(services)
        self.config_services = list(config_services)

    def run(self, argv, env=None, check=True, capture=True, timeout=1800):
        argv = [str(a) for a in argv]
        self.calls.append(argv)
        if "ps" in argv:
            return "\n".join(
                json.dumps({"Service": s, "State": "running",
                            "Health": "", "Name": f"01_docker-{s}-1"})
                for s in self.services)
        if "config" in argv:
            return json.dumps({"services": {
                s: {"restart": "unless-stopped"} for s in self.config_services}})
        return ""

    def http_json(self, url, timeout=10):
        return {"jobs": []}

    def compose_lines(self, args, env=None):
        args = [str(a) for a in args]
        self.streams.append(args)
        for service, lines in self.lines.items():
            if service in args:
                yield from lines


class ProbeContainerTests(unittest.TestCase):
    """C3-2: the daily Fluss probe runs through the ingestion container."""

    def test_fluss_log_end_execs_the_probe_in_the_ingestion_container(self):
        class ProbeRunner(StreamRunner):
            def run(self, argv, env=None, check=True, capture=True, timeout=1800):
                argv = [str(a) for a in argv]
                self.calls.append(argv)
                if "exec" in argv:
                    return ("1790619364842\tdefault\traw_table_1\t3\t12\t1000\n"
                            "1790619364842\tdefault\traw_table_1\t3\t12\t1000\n")
                return ""

        runner = ProbeRunner()
        collector = day_run.Collector(runner)
        result = collector.fluss_log_end("raw_table_1")
        self.assertEqual(result, {"ok": True, "log_end": 1000,
                                  "sampled_at_ms": 1790619364842})
        exec_call = next(call for call in runner.calls if "exec" in call)
        index = exec_call.index("exec")
        self.assertEqual(exec_call[index:index + 9], [
            "exec", "-T", "ingestion", "java",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "-cp", "/app/ingestion.jar:/app/probe", "FlussReadLagProbe",
        ])
        self.assertEqual(exec_call[-3:], ["default", "raw_table_1",
                                          "fluss-coordinator:9123"])

    def test_probe_failure_is_reported_not_guessed(self):
        class BrokenRunner(StreamRunner):
            def run(self, argv, env=None, check=True, capture=True, timeout=1800):
                raise subprocess.CalledProcessError(1, list(argv), output="boom")

        result = day_run.Collector(BrokenRunner()).fluss_log_end("raw_table_1")
        self.assertFalse(result["ok"])
        self.assertIn("boom", result["error"])


class ProbeFailClosedTests(unittest.TestCase):
    """M2-5: a probe that cannot read raises `ProbeUnavailable`; collect records it."""

    def test_log_errors_raises_probe_unavailable(self):
        class BrokenLogs(StreamRunner):
            def compose_lines(self, args, env=None):
                raise OSError("docker gone")

        with self.assertRaises(day_run.ProbeUnavailable):
            day_run.Collector(BrokenLogs()).log_errors("15m")

    def test_effective_tokens_raises_probe_unavailable(self):
        class BrokenLogs(StreamRunner):
            def compose_lines(self, args, env=None):
                raise OSError("docker gone")

        with self.assertRaises(day_run.ProbeUnavailable):
            day_run.Collector(BrokenLogs()).effective_tokens(True)

    def test_collect_records_a_failed_env_probe(self):
        class BrokenInspect(StreamRunner):
            def run(self, argv, env=None, check=True, capture=True, timeout=1800):
                if "inspect" in [str(a) for a in argv]:
                    raise subprocess.CalledProcessError(
                        1, [str(a) for a in argv], output="boom")
                return super().run(argv, env=env, check=check, capture=capture,
                                   timeout=timeout)

        collector = day_run.Collector(BrokenInspect(
            services=["execution-bridge", "execution-gateway", "nautilus"]))
        with mock.patch.object(day_run.Collector, "fluss_log_end",
                               lambda self, table: {"ok": True, "log_end": 7}):
            facts = collector.collect(samples=False)
        self.assertTrue(facts.env_probe_failures,
                        "a failed inspect must land in facts, not vanish")
        self.assertTrue(any("execution-bridge" in f for f in facts.env_probe_failures),
                        facts.env_probe_failures)


class ReadinessCollectTests(unittest.TestCase):
    def _collector(self, **kwargs):
        runner = StreamRunner(**kwargs)
        return day_run.Collector(runner), runner

    def test_readiness_collect_skips_log_reads_and_other_tables(self):
        collector, runner = self._collector(
            services=["fluss-tablet", "ingestion", "flink-jobmanager"],
            config_services=["fluss-tablet", "ingestion", "flink-jobmanager"],
            lines={"fluss-tablet": ["FATAL tablet exploded"]})
        with mock.patch.object(day_run.Collector, "fluss_log_end",
                               lambda self, table: {"ok": True, "log_end": 7}):
            facts = collector.collect(readiness=True)
        self.assertEqual(sorted(facts.fluss), ["raw", "window_s"])
        self.assertFalse(runner.streams)
        self.assertFalse([argv for argv in runner.calls if "logs" in argv])

    def test_full_collect_reads_logs_by_streaming_not_capture(self):
        collector, runner = self._collector(
            services=["fluss-tablet", "ingestion", "flink-jobmanager"],
            config_services=["fluss-tablet", "ingestion", "flink-jobmanager"],
            lines={"fluss-tablet": ["FATAL tablet exploded"],
                   "ingestion": ["manifest loaded (instruments=2433)"]})
        with mock.patch.object(day_run.Collector, "fluss_log_end",
                               lambda self, table: {"ok": True, "log_end": 7}):
            facts = collector.collect(samples=False)
        self.assertEqual(sorted(k for k in facts.fluss if k != "window_s"),
                         ["candles", "raw", "signals"])
        self.assertTrue(runner.streams)
        self.assertFalse([argv for argv in runner.calls if "logs" in argv])
        self.assertIn("FATAL tablet exploded", facts.log_errors)
        self.assertEqual(facts.effective_tokens, 2433)

    def test_log_errors_bounds_hits(self):
        collector, _ = self._collector(
            lines={"fluss-tablet": [f"FATAL boom {i}" for i in range(200)]})
        hits = collector.log_errors("15m")
        self.assertTrue(hits)
        self.assertLessEqual(len(hits), day_run.MAX_LOG_ERROR_HITS)

    def test_effective_tokens_keeps_the_last_manifest_line(self):
        collector, _ = self._collector(
            lines={"ingestion": ["manifest loaded (instruments=100)",
                                 "manifest loaded (instruments=2433)"]})
        self.assertEqual(collector.effective_tokens(True), 2433)
        self.assertIsNone(collector.effective_tokens(False))

    def test_nautilus_halted_detects_the_marker(self):
        halted, _ = self._collector(lines={"nautilus": ["INFO up", "gate HALTED"]})
        self.assertTrue(halted.nautilus_halted())
        quiet, _ = self._collector(lines={"nautilus": ["INFO up"]})
        self.assertFalse(quiet.nautilus_halted())


class WaitReadyProbeTests(unittest.TestCase):
    def test_wait_ready_polls_with_the_readiness_snapshot(self):
        seen = []

        class _C:
            def collect(self, **kwargs):
                seen.append(kwargs)
                return make_facts()

        universe = day_run.Universe(mode="full", tokens=2433, connections=3,
                                    approval=True, manifest=pathlib.Path("/tmp/m.csv"),
                                    deploy_env="dev")
        with mock.patch.object(day_run.time, "sleep", lambda *_: None):
            day_run.wait_ready(_C(), universe, day_run.Runner(out=io.StringIO()),
                               timeout_s=10)
        self.assertEqual(len(seen), 2)  # two consecutive good polls
        self.assertTrue(all(kw.get("readiness") for kw in seen))


# --------------------------------------------------------------------------
# ephemeral-VM profile (A/B) and the EOD stop gate (D), 2026-09-28
# --------------------------------------------------------------------------


class VmProfileTests(unittest.TestCase):
    def test_profile_applies_but_the_shell_wins(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile = pathlib.Path(tmp) / ".env.vm"
            profile.write_text(
                "# ephemeral vm profile\n"
                "ALLOW_FRESH=1\n"
                "EOD_AT=15:45\n"
                'EOD_TABLES="candle_closed"\n')
            with mock.patch.dict(os.environ, {"EOD_AT": "23:30"}, clear=False):
                for key in ("ALLOW_FRESH", "EOD_TABLES"):
                    os.environ.pop(key, None)
                applied = day_run.load_vm_env(profile)
                self.assertEqual(applied, 2)
                self.assertEqual(os.environ["ALLOW_FRESH"], "1")
                self.assertEqual(os.environ["EOD_TABLES"], "candle_closed")
                self.assertEqual(os.environ["EOD_AT"], "23:30",
                                 "an explicit shell value must never be overwritten")

    def test_absent_profile_is_a_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(day_run.load_vm_env(pathlib.Path(tmp) / "absent"), 0)

    def test_main_loads_the_vm_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile = pathlib.Path(tmp) / ".env.vm"
            profile.write_text("DAY_TEST_VM_KEY=applied\n")
            with mock.patch.dict(os.environ, {"DAY_VM_ENV_FILE": str(profile)},
                                 clear=False), \
                    mock.patch.object(day_run, "cmd_status",
                                      lambda runner, factory: 0):
                os.environ.pop("DAY_TEST_VM_KEY", None)
                rc = day_run.main(["status"], runner=day_run.Runner(out=io.StringIO()))
                self.assertEqual(os.environ.get("DAY_TEST_VM_KEY"), "applied")
        self.assertEqual(rc, 0)


class StopEodGateTests(unittest.TestCase):
    """D: on the daily VM `stop` refuses until the day's EOD is confirmed."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.record = pathlib.Path(self._tmp.name) / "eod-last-run"

    def _stop(self, runner, **env):
        patched = {"DAY_EVIDENCE_DIR": os.path.join(self._tmp.name, "evidence")}
        patched.update(env)
        collector = lambda r: FakeCollector(  # noqa: E731 - matches the suite style
            state={"latest_savepoint": None, "latest_checkpoint": None})
        with mock.patch.dict(os.environ, patched, clear=False):
            return day_run.cmd_stop(runner, collector)

    def test_refuses_when_today_has_no_confirmed_archive(self):
        self.record.write_text("2026-09-27T15:45:00+05:30\n")  # yesterday
        runner = FakeRunner()
        rc = self._stop(runner, DAY_STOP_REQUIRE_EOD="1",
                        EOD_LAST_RUN_FILE=str(self.record))
        self.assertEqual(rc, day_run.EXIT_RED)
        self.assertNotIn("down", runner.targets(),
                         "the stack must stay up when the day is not archived")

    def test_refuses_when_no_record_exists_at_all(self):
        runner = FakeRunner()
        rc = self._stop(runner, DAY_STOP_REQUIRE_EOD="1",
                        EOD_LAST_RUN_FILE=str(self.record))
        self.assertEqual(rc, day_run.EXIT_RED)
        self.assertFalse(runner.targets())

    def test_stops_when_today_is_confirmed(self):
        self.record.write_text(day_run.dt.datetime.now(day_run.IST).isoformat() + "\n")
        runner = FakeRunner()
        rc = self._stop(runner, DAY_STOP_REQUIRE_EOD="1",
                        EOD_LAST_RUN_FILE=str(self.record))
        self.assertEqual(rc, day_run.EXIT_OK)
        self.assertIn("down", runner.targets())

    def test_dev_pc_keeps_the_ungated_stop(self):
        runner = FakeRunner()
        rc = self._stop(runner)  # no DAY_STOP_REQUIRE_EOD: unchanged behaviour
        self.assertEqual(rc, day_run.EXIT_OK)
        self.assertIn("down", runner.targets())

    def test_force_override_is_deliberate(self):
        self.record.write_text("2026-09-27T15:45:00+05:30\n")
        runner = FakeRunner()
        rc = self._stop(runner, DAY_STOP_REQUIRE_EOD="1", DAY_STOP_FORCE="1",
                        EOD_LAST_RUN_FILE=str(self.record))
        self.assertEqual(rc, day_run.EXIT_OK)
        self.assertIn("down", runner.targets())


if __name__ == "__main__":
    unittest.main()
