"""Wave 44 — the guards in test-pipeline-lib.sh must BITE (P6-020 … P6-799).

A test-quality fix is only proven by mutation: point the suite at a deliberately
broken copy of the library (or the compose file) and require the matching guard
to fail. Without this, "the guard is per-function now" is a claim about text
rather than about behaviour.

Hermetic: no network, no docker, no repo writes — every mutation happens on a
copy in a temp dir.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
GUARDS = SCRIPTS / "test-pipeline-lib.sh"
REAL_LIB = SCRIPTS / "pipeline-lib.sh"
REPO = SCRIPTS.parents[2]
REAL_COMPOSE = REPO / "code" / "01_platform" / "01_docker" / "docker-compose.yml"
CLEAN = "guards: 134 passed, 0 failed"


def run_guards(lib: Path = REAL_LIB, compose: Path | None = None) -> tuple[int, str]:
    env = dict(os.environ)
    env["PIPELINE_LIB_UNDER_TEST"] = str(lib)
    if compose is not None:
        env["PIPELINE_COMPOSE_UNDER_TEST"] = str(compose)
    proc = subprocess.run(["bash", str(GUARDS)], cwd=str(SCRIPTS), env=env,
                          capture_output=True, text=True, timeout=900)
    return proc.returncode, proc.stdout + proc.stderr


class MutationBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w44-mut-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def mutated_lib(self, old: str, new: str, count: int = 1) -> Path:
        src = REAL_LIB.read_text(encoding="utf-8")
        self.assertEqual(src.count(old), count, f"mutation anchor x{src.count(old)}: {old[:60]!r}")
        dst = self.tmp / "pipeline-lib.sh"
        dst.write_text(src.replace(old, new), encoding="utf-8")
        return dst


class ControlTest(MutationBase):
    def test_the_real_library_and_compose_pass_cleanly(self) -> None:
        rc, out = run_guards()
        self.assertEqual(rc, 0, out[-2000:])
        self.assertIn(CLEAN, out)


class PerFunctionGuardTest(MutationBase):
    """P6-020 (CRITICAL): the guard check must be per-function."""

    def test_removing_the_guard_from_one_function_fails_only_that_function(self) -> None:
        """Only pipeline_start_faketool is doctored; nothing else may go red."""
        src = REAL_LIB.read_text(encoding="utf-8")
        start = src.index("pipeline_start_faketool() {")
        end = src.index("\n}\n", start) + 3
        body = src[start:end]
        self.assertEqual(body.count("pipeline_require_preflight || return 1"), 1,
                         "expected exactly one guard line in pipeline_start_faketool")
        lib = self.tmp / "pipeline-lib.sh"
        lib.write_text(src[:start] + body.replace("pipeline_require_preflight || return 1",
                                                 ": # guard removed by the mutation", 1) + src[end:],
                       encoding="utf-8")
        rc, out = run_guards(lib)
        self.assertNotEqual(rc, 0)
        self.assertIn("FAIL: G19 guard MISSING in pipeline_start_faketool", out)
        # the other four must still be judged on their own bodies
        self.assertIn("PASS: G19 guard wired in pipeline_submit_job", out)
        self.assertNotIn("FAIL: G19 guard MISSING in pipeline_submit_job", out)

    def test_the_delegating_wrapper_is_not_a_false_negative(self) -> None:
        """pipeline_purge_raw_table has no literal guard; it calls the one that does."""
        rc, out = run_guards()
        self.assertEqual(rc, 0)
        self.assertIn("PASS: G19 guard wired in pipeline_purge_raw_table (delegates to", out)


class ReadinessOrderTest(MutationBase):
    """P6-231: readiness must be asserted before preflight commits its OK flag."""

    def test_moving_the_readiness_call_after_the_ok_flag_is_caught(self) -> None:
        line = "  pipeline_wait_for_fluss_ready || return 1\n"
        src_old = self.mutated_lib  # anchor check only; mutation below
        lib = self.tmp / "pipeline-lib.sh"
        src = REAL_LIB.read_text(encoding="utf-8")
        self.assertEqual(src.count(line), 1, "readiness call anchor")
        src = src.replace(line, "", 1)
        ok = "  PIPELINE_PREFLIGHT_OK=1\n"
        self.assertEqual(src.count(ok), 1, "OK-flag anchor")
        lib.write_text(src.replace(ok, ok + line, 1), encoding="utf-8")
        rc, out = run_guards(lib)
        self.assertNotEqual(rc, 0)
        self.assertIn("G17 readiness/OK-flag order wrong", out)


class ContinuationTest(MutationBase):
    """P6-232/P6-233: a comment spliced into a continuation must be caught."""

    def test_a_comment_inside_a_continuation_is_reported(self) -> None:
        lib = self.tmp / "pipeline-lib.sh"
        src = REAL_LIB.read_text(encoding="utf-8")
        start = src.index("pipeline_submit_job() {")
        # the -e env chain: a comment here ends the command silently (bash -n is
        # happy), which is the 2026-08-30 failure class this suite exists for.
        m = re.search(r"^.*-e ALLOW_FULL_REPLAY.*\\\n", src[start:], re.M)
        self.assertIsNotNone(m, "no -e ALLOW_FULL_REPLAY continuation found")
        at = start + m.end()
        lib.write_text(src[:at] + "    # mutation: comment inside a continuation\n" + src[at:],
                       encoding="utf-8")
        rc, out = run_guards(lib)
        self.assertNotEqual(rc, 0)
        self.assertIn("G4 line", out)
        self.assertIn("comment/blank INSIDE a backslash-continued command", out)


class ComposeGuardTest(MutationBase):
    """P6-594 (literal keys) and P6-799 (legal YAML variants)."""

    def write_compose(self, text: str) -> Path:
        dst = self.tmp / "docker-compose.yml"
        dst.write_text(text, encoding="utf-8")
        return dst

    def test_a_typo_d_in_a_rocksdb_key_is_caught(self) -> None:
        src = REAL_COMPOSE.read_text(encoding="utf-8")
        key = "state.backend.rocksdb.metrics.num-running-compactions"
        self.assertIn(key, src, "rocksdb key anchor")
        compose = self.write_compose(src.replace(key, key.replace(".", "X"), 1))
        rc, out = run_guards(REAL_LIB, compose)
        self.assertNotEqual(rc, 0)
        self.assertIn("FAIL: G28d rocksdb metric toggle missing: num-running-compactions", out)

    def test_a_legal_profile_variant_is_not_a_failure(self) -> None:
        src = REAL_COMPOSE.read_text(encoding="utf-8")
        self.assertIn('profiles: ["loadgen"]', src, "profile anchor")
        compose = self.write_compose(src.replace('profiles: ["loadgen"]', "profiles:['loadgen']", 1))
        rc, out = run_guards(REAL_LIB, compose)
        self.assertIn("PASS: G26f loadgen compose service is profile-gated", out)


if __name__ == "__main__":
    unittest.main()
