"""Wave 45 — the EOD controller test must fail loud (P6-070 … P6-366).

Each test either extracts one function from eod-controller-test.sh into a
harness with stubbed tools on PATH, or runs the script itself in a sandbox
whose ROOT is a temp dir. The stubs let a *failing* controller/compile/docker be
simulated, which is the only way to prove a guard bites: the fixed script must
report the failure and exit non-zero, and the un-fixed one would not.

Hermetic: no network, no docker, no live Fluss, no repo writes.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
SCRIPT = SCRIPTS / "eod-controller-test.sh"
STATUS_RC = 2  # what eod_controller.py returns when its classpath is incomplete

# The date the fake controller's `status` day lines carry. The script
# defaults RUN_DATE to *today* in Asia/Kolkata (eod-controller-test.sh:37) and
# then counts `^eod-controller:   day $RUN_DATE` lines to decide whether the
# cycle recorded the day, so a test that runs the whole script must pin the
# date to this value — otherwise it passes only on the day this file was
# written (P6-072/P6-077 failed exactly that way from 2026-09-20 on).
FIXTURE_DAY = "2026-09-19"

FAKE_PYTHON3 = """#!/usr/bin/env bash
# fake python3: serves the two call shapes the script makes (status / run)
printf '%s\\n' "$*" >> "$FAKE_LOG"
case "$*" in
  *" status"*)
    if [ -n "${FAKE_STATUS_FAIL_TIMES:-}" ]; then
      f="$FAKE_LOG.statusfail"; n=$(cat "$f" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$f"
      if [ "$n" -le "$FAKE_STATUS_FAIL_TIMES" ]; then
        printf 'transient: metadata cache unavailable\\n' >&2
        exit "${FAKE_STATUS_RC:-2}"
      fi
    fi
    if [ "${FAKE_STATUS_FAIL:-0}" = "1" ]; then
      printf 'EOD CONTROLLER CLASSPATH INCOMPLETE:\\n  - /nonexistent.jar\\n' >&2
      exit "${FAKE_STATUS_RC:-2}"
    fi
    n="${FAKE_STATUS_DAYS:-0}"
    lines="${FAKE_DAY_LINES:-1}"
    printf 'eod-controller: RESULT=VERIFIED EXIT=0 TABLES=1 DAYS=%s\\n' "$n"
    i=0
    while [ "$i" -lt "$lines" ]; do
      # the date here must equal FIXTURE_DAY (see its comment)
      printf 'eod-controller:   day 2026-09-19 state=%s retry=0 nextRetry=-\\n' "${FAKE_DAY_STATE:-VERIFIED}"
      i=$((i + 1))
    done
    exit "${FAKE_STATUS_EXIT:-0}" ;;   # status may exit 2/3 by design
  *" run"*)
    printf 'eod-controller: RESULT=VERIFIED EXIT=0 TABLES=1 DAYS=0\\n'
    if [ -n "${FAKE_RUN_SLEEP:-}" ]; then sleep "$FAKE_RUN_SLEEP"; fi
    exit "${FAKE_RUN_RC:-0}" ;;
esac
exit 0
"""

FAKE_JAVAC = """#!/usr/bin/env bash
printf 'javac %s\\n' "$*" >> "$FAKE_LOG"
# record the -d target so a test can prove it is a private dir, not /tmp
while [ $# -gt 0 ]; do case "$1" in -d) printf '%s' "$2" > "$FAKE_JAVAC_DIR";; esac; shift; done
if [ "${FAKE_JAVAC_FAIL:-0}" = "1" ]; then
  printf 'error: cannot find symbol\\n  symbol: class DdlText\\n' >&2
  exit 1
fi
exit 0
"""

FAKE_JAVA = """#!/usr/bin/env bash
printf 'java %s\\n' "$*" >> "$FAKE_LOG"
printf '%s\\n' "${FAKE_JAVA_OUT:-PURGED eod_offload_state}"
exit "${FAKE_JAVA_RC:-0}"
"""

FAKE_DOCKER = """#!/usr/bin/env bash
printf 'docker %s\\n' "$*" >> "$FAKE_LOG"
exit 0
"""

FAKE_CURL = """#!/usr/bin/env bash
exit 0
"""

FAKE_PGREP = """#!/usr/bin/env bash
printf 'pgrep %s\\n' "$*" >> "$FAKE_LOG"
printf '%s\\n' "${FAKE_PGREP_PID:?fake pgrep needs FAKE_PGREP_PID}"
exit 0
"""

FAKE_PKILL = """#!/usr/bin/env bash
printf 'pkill %s\\n' "$*" >> "$FAKE_PKILL_MARKER"
exit 0
"""

STUB_LIB = """#!/usr/bin/env bash
# stub pipeline-lib for the wave-45 harness: no docker, no cluster
LIB_FAKETOOL_CONTAINER="pipeline-faketool"
LIB_INGESTION_CONTAINER="pipeline-ingestion"
FAKETOOL_LOG_PID="${STUB_MIRROR_PID:-}"
INGESTION_LOG_PID=""
pipeline_preflight() { return "${STUB_PREFLIGHT_RC:-0}"; }
pipeline_start_faketool() { return 0; }
pipeline_start_ingestion() { return 0; }
pipeline_submit_job() { return 0; }
"""  # noqa: E501


def extract_fn(text: str, name: str) -> str:
    """The function's text, from its column-0 definition to its column-0 brace."""
    m = re.search(rf"^{re.escape(name)}\(\) \{{\n", text, re.M)
    assert m, f"no function definition for {name}"
    end = text.index("\n}\n", m.end())
    return text[m.start():end + 3]


class Sandbox:
    """A temp ROOT shaped like the repo, with fake tools first on PATH."""

    def __init__(self, script_text: str) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="w45-"))
        self.scripts = self.root / "code/01_platform/04_scripts"
        self.scripts.mkdir(parents=True)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.tmpdir = self.root / "tmp"
        self.tmpdir.mkdir()
        self.log = self.root / "calls.log"
        self.g2_counter = self.root / "g2-counter"
        self.log.write_text("")
        (self.scripts / "pipeline-lib.sh").write_text(STUB_LIB)
        self.script = self.scripts / "eod-controller-test.sh"
        self.script.write_text(script_text)
        # prerequisites the script checks before it does cluster work
        (self.root / "code/02_services/01_ingestion/target").mkdir(parents=True)
        (self.root / "code/02_services/01_ingestion/target/cp.txt").write_text("/fake/cp\n")
        (self.root / "ddl.sql").write_text("CREATE TABLE eod_offload_state (id INT);\n")
        for name, body in (("python3", FAKE_PYTHON3), ("javac", FAKE_JAVAC),
                           ("java", FAKE_JAVA), ("docker", FAKE_DOCKER),
                           ("curl", FAKE_CURL), ("pkill", FAKE_PKILL),
                           ("pgrep", FAKE_PGREP)):
            self.write_fake(name, body)

    def write_fake(self, name: str, body: str) -> None:
        p = self.bin / name
        p.write_text(body)
        p.chmod(0o755)

    def env(self, **extra: str) -> dict:
        env = dict(os.environ)
        env["PATH"] = f"{self.bin}:{os.environ['PATH']}"
        env["TMPDIR"] = str(self.tmpdir)
        env["FAKE_LOG"] = str(self.log)
        env["FAKE_PKILL_MARKER"] = str(self.root / "PKILL-CALLED")
        env["FAKE_JAVAC_DIR"] = str(self.root / "javac-dir")
        env["EOD_SMOKE_S"] = "0"     # the script's feed phases must not sleep here
        env["EOD_MAIN_S"] = "0"
        env.update(extra)
        return env

    def run(self, bash: str, timeout: int = 120, **extra: str) -> tuple[int, str]:
        p = subprocess.run(["bash", "-c", bash], cwd=str(self.root), env=self.env(**extra),
                           capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout + p.stderr

    def calls(self) -> str:
        return self.log.read_text()

    def pkill_called(self) -> bool:
        return (self.root / "PKILL-CALLED").exists()

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


class Wave45Base(unittest.TestCase):
    script_text = ""

    @classmethod
    def setUpClass(cls) -> None:
        cls.script_text = SCRIPT.read_text(encoding="utf-8")

    def sandbox(self) -> Sandbox:
        box = Sandbox(self.script_text)
        self.addCleanup(box.cleanup)
        return box

    def harness(self, box: Sandbox, fn_names: list[str], prologue: str, call: str,
                timeout: int = 120, **extra: str) -> tuple[int, str]:
        fns = "\n".join(extract_fn(self.script_text, n) for n in fn_names)
        setup = textwrap.dedent(f"""
            set -uo pipefail
            SCRIPT_DIR="{box.scripts}"
            ROOT="{box.root}"
            EOD="{box.scripts}/eod_controller.py"
            CP_FILE="{box.root}/code/02_services/01_ingestion/target/cp.txt"
            STATE_DDL="{box.root}/ddl.sql"
            LOGDIR="{box.root}/logs"; mkdir -p "$LOGDIR"
            TS=t45; EVIDENCE="$LOGDIR/ev.log"
            RUN_DATE=2026-09-19; EOD_TABLES=candle_closed; EOD_SMOKE_S=0; EOD_MAIN_S=0
            pass=0; fail=0; FIXTURE_DIRS=(); FEED_STATE=""
            ok()   {{ printf 'ok    %s\\n' "$*"; pass=$((pass+1)); }}
            bad()  {{ printf '!! FAIL %s\\n' "$*"; fail=$((fail+1)); }}
            info() {{ printf '      %s\\n' "$*"; }}
            eod()  {{ printf 'eod-stub: %s\\n' "$*"; return "${{STUB_EOD_RC:-0}}"; }}
        """)
        return box.run(setup + prologue + "\n" + fns + "\n" + call, timeout=timeout, **extra)


class DispatchTest(Wave45Base):
    """P6-072/P6-077: an unknown phase must fail, and `main` must actually run."""

    def test_unknown_phase_fails_loud_and_touches_nothing(self) -> None:
        box = self.sandbox()
        rc, out = box.run(f"EOD_TEST_PHASE=bogus bash {box.script}")
        self.assertEqual(rc, 1, out)
        self.assertIn("unknown EOD_TEST_PHASE='bogus'", out)
        self.assertIn("0 passed, 1 failed, rc=1", out)
        # the old code ran zero phases and still purged the live state table
        self.assertNotIn("PURGED", out)
        self.assertNotIn("purge", out.lower().replace("purge_state", ""))

    def test_main_phase_is_dispatched(self) -> None:
        box = self.sandbox()
        rc, out = box.run(f"EOD_TEST_PHASE=main bash {box.script}",
                        RUN_DATE=FIXTURE_DAY)
        self.assertEqual(rc, 0, out)
        self.assertIn("=== main phase:", out)
        self.assertIn("ok    main: EOD cycle verified 1 day-record", out)
        self.assertNotIn("guard tests", out)  # phase=main is not `all`

    def test_smoke_phase_is_dispatched(self) -> None:
        box = self.sandbox()
        rc, out = box.run(f"EOD_TEST_PHASE=smoke bash {box.script}",
                        RUN_DATE=FIXTURE_DAY)
        self.assertEqual(rc, 0, out)
        self.assertIn("=== smoke phase:", out)
        self.assertNotIn("=== main phase:", out)


class FeedCycleTest(Wave45Base):
    """P6-075/P6-070: a failed EOD cycle must return non-zero from feed_and_cycle."""

    def _run(self, **extra: str) -> tuple[int, str, Sandbox]:
        box = self.sandbox()
        prologue = textwrap.dedent("""
            purge_state() { return 0; }
        """)
        rc, out = self.harness(box, ["stop_feed", "feed_and_cycle"], prologue,
                              'feed_and_cycle smoke 0\necho "RC=$?"', **extra)
        return rc, out, box

    def test_good_cycle_returns_zero(self) -> None:
        rc, out, box = self._run()
        self.assertIn("RC=0", out)
        self.assertIn("ok    smoke: EOD cycle verified 1 day-record", out)

    def test_controller_failure_propagates(self) -> None:
        rc, out, box = self._run(STUB_EOD_RC="2")
        self.assertIn("RC=1", out)                      # was RC=0 before the fix
        self.assertIn("!! FAIL smoke: EOD cycle rc=2", out)

    def test_missing_day_record_propagates(self) -> None:
        rc, out, box = self._run(FAKE_DAY_LINES="0")
        self.assertIn("RC=1", out)
        self.assertIn("records=0 (want 1/0)", out)


class TeardownTest(Wave45Base):
    """P6-076/P6-073: teardown kills the recorded PIDs and exact container names."""

    def _run(self) -> tuple[int, str, Sandbox]:
        box = self.sandbox()
        # a real stand-in for the lib's `docker logs -f` mirror process
        mirror = subprocess.Popen(["sleep", "300"])
        self.addCleanup(mirror.kill)
        self.mirror = mirror
        self.mirror_pid = mirror.pid
        prologue = "purge_state() { return 0; }\n"
        rc, out = self.harness(box, ["stop_feed", "feed_and_cycle"], prologue,
                              'feed_and_cycle smoke 0\necho "RC=$?"',
                              STUB_MIRROR_PID=str(mirror.pid))
        return rc, out, box

    def test_recorded_pids_and_exact_names_are_used(self) -> None:
        rc, out, box = self._run()
        self.assertIn("RC=0", out)
        self.assertIn("rm -f pipeline-faketool pipeline-ingestion", box.calls())
        self.assertIn("feed containers removed (pipeline-faketool pipeline-ingestion)", out)
        self.assertFalse(box.pkill_called(), "no pattern-kill may be invoked")

    def test_mirror_process_is_actually_killed(self) -> None:
        rc, out, box = self._run()
        self.assertIn(f"feed log mirrors stopped (pids: {self.mirror_pid}", out)
        self.assertEqual(self.mirror.wait(timeout=5), -15)   # SIGTERM, then reaped

    def test_state_file_is_removed_so_a_trap_cannot_kill_a_recycled_pid(self) -> None:
        rc, out, box = self._run()
        self.assertFalse(list((box.root / "logs").glob("*.feed-state")),
                         "feed state file must be removed by stop_feed")


class PurgeStateTest(Wave45Base):
    """P6-364/P6-360: the grep's status must decide, and P6-363: the fixture
    must be private and a compile error must be surfaced."""

    def _run(self, **extra: str) -> tuple[int, str, Sandbox]:
        box = self.sandbox()
        rc, out = self.harness(box, ["purge_state"], "", 'purge_state\necho "RC=$?"', **extra)
        return rc, out, box

    def test_missing_purged_marker_returns_nonzero(self) -> None:
        rc, out, box = self._run(FAKE_JAVA_OUT="table not found: eod_offload_state")
        self.assertIn("RC=1", out)                 # was RC=0 before the fix

    def test_successful_purge_returns_zero(self) -> None:
        rc, out, box = self._run()
        self.assertIn("RC=0", out)
        self.assertIn("PURGED eod_offload_state", (box.root / "logs/ev.log").read_text())

    def test_fixture_is_private_and_removed(self) -> None:
        rc, out, box = self._run()
        # javac was pointed at a private dir under $TMPDIR, not /tmp
        javac_dir = (box.root / "javac-dir").read_text()
        self.assertTrue(javac_dir.startswith(str(box.tmpdir)), javac_dir)
        self.assertFalse(Path(javac_dir).exists(), "fixture dir must be removed")
        self.assertFalse(Path("/tmp/EodPurge.java").exists())

    def test_compile_failure_is_surfaced(self) -> None:
        rc, out, box = self._run(FAKE_JAVAC_FAIL="1")
        self.assertIn("RC=1", out)
        self.assertIn("javac failed", out)
        self.assertIn("cannot find symbol", out)   # stderr used to be swallowed


class StatusHelperTest(Wave45Base):
    """P6-365/P6-359: a crashed controller must not look like `0`."""

    def _run(self, fn: str, **extra: str) -> tuple[int, str, Sandbox]:
        box = self.sandbox()
        rc, out = self.harness(box, [fn, "eod_status"], "", f'{fn}\necho "RC=$?"', **extra)
        return rc, out, box

    def test_days_on_file_propagates_a_controller_crash(self) -> None:
        rc, out, box = self._run("days_on_file", FAKE_STATUS_FAIL="1")
        self.assertIn("RC=1", out)
        self.assertIn("STATUS_UNUSABLE rc=2", out)   # was `0` before the fix

    def test_days_on_file_still_reports_zero_days(self) -> None:
        rc, out, box = self._run("days_on_file")
        self.assertIn("RC=0", out)
        self.assertIn("0\nRC=0", out)

    def test_verified_days_propagates_a_controller_crash(self) -> None:
        rc, out, box = self._run("verified_days", FAKE_STATUS_FAIL="1")
        self.assertIn("RC=1", out)
        self.assertIn("STATUS_UNUSABLE", out)

    def test_a_pending_work_status_is_usable(self) -> None:
        """Live: `status` exits 3 (PENDING_WORK) whenever unverified days exist —
        a documented code, not a probe failure. Parsing must not depend on it."""
        box = self.sandbox()
        rc, out, box = self._run("days_on_file", FAKE_STATUS_EXIT="3")
        self.assertIn("0\nRC=0", out)

    def test_a_transient_status_failure_is_retried(self) -> None:
        """Live: one status call failed with rc=3, not a documented exit code."""
        box = self.sandbox()
        rc, out, box = self._run("days_on_file", FAKE_STATUS_FAIL_TIMES="1")
        self.assertIn("0\nRC=0", out)

    def test_a_persistent_status_failure_carries_the_diagnostic(self) -> None:
        box = self.sandbox()
        rc, out, box = self._run("days_on_file", FAKE_STATUS_FAIL="1")
        self.assertIn("RC=1", out)
        self.assertIn("STATUS_UNUSABLE rc=2", out)
        self.assertIn("why=", out)                    # the reason is not lost
        self.assertIn("CLASSPATH INCOMPLETE", out)

    def test_verified_days_counts_verified_states(self) -> None:
        rc, out, box = self._run("verified_days", FAKE_DAY_LINES="2")
        self.assertIn("RC=0", out)
        self.assertIn("2\nRC=0", out)


class PollAndWaitTest(Wave45Base):
    """P6-366/P6-361: the lease poll must be observable, and the reap bounded."""

    def test_poll_matches_a_record_in_any_state(self) -> None:
        """Live failure: requiring PENDING|COMMITTED missed a run that reached
        VERIFIED between two polls (each poll is a JVM launch of its own)."""
        box = self.sandbox()
        rc, out = self.harness(box, ["wait_for_day_record"], "",
                              'wait_for_day_record 2026-09-19 5\necho "RC=$?"')
        self.assertIn("RC=0", out)

    def test_poll_times_out_when_no_record_appears(self) -> None:
        box = self.sandbox()
        rc, out = self.harness(box, ["wait_for_day_record"], "",
                              'wait_for_day_record 2026-09-19 2\necho "RC=$?"',
                              FAKE_DAY_LINES="0")
        self.assertIn("RC=1", out)

    def test_poll_times_out_within_its_budget(self) -> None:
        box = self.sandbox()
        rc, out = self.harness(box, ["wait_for_day_record"], "",
                              'time wait_for_day_record 2026-09-19 3\necho "RC=$?"',
                              FAKE_DAY_LINES="0", timeout=60)
        self.assertIn("RC=1", out)
        m = re.search(r"real\s+0m([0-9.]+)s", out)
        self.assertIsNotNone(m, out)
        self.assertLess(float(m.group(1)), 5.0)   # bounded by the 3s budget

    def test_wait_bounded_kills_a_child_that_overstays(self) -> None:
        box = self.sandbox()
        rc, out = self.harness(
            box, ["wait_bounded"], "",
            'sleep 300 & p=$!\n'
            'wait_bounded "$p" 2; echo "RC=$?"\n'
            'if kill -0 "$p" 2>/dev/null; then echo "CHILD-ALIVE"; else echo "CHILD-GONE"; fi',
            timeout=60)
        self.assertIn("CHILD-GONE", out)


class GuardsFencingTest(Wave45Base):
    """P6-366 as it runs live: one attempt can miss a slow cluster, so the check
    retries, and its verdict depends on whether the holder was still running."""

    _PROLOGUE = """G2_COUNTER="@G2COUNTER@"; : > "$G2_COUNTER"
purge_state() { return 0; }
eod() {
  printf 'eod-stub: %s\n' "$*"
  case "$*" in
    *"--offload none"*) return "${G1_RC:-2}" ;;
    *"--lease-ttl "*)
      n=$(cat "$G2_COUNTER" 2>/dev/null || echo 0); n=$((n + 1))
      echo "$n" > "$G2_COUNTER"
      case "$n" in
        1) return "${G2_RC_1:-0}" ;;
        2) return "${G2_RC_2:-0}" ;;
        3) return "${G2_RC_3:-0}" ;;
        *) return "${G2_RC_DEFAULT:-0}" ;;
      esac ;;
    *) return 0 ;;
  esac
}
"""

    def _run_guards(self, **extra: str) -> tuple[int, str, Sandbox]:
        # P1.5: the fencing tests wait out the lease window. The default stays 60s
        # (asserted below); the tests run it at 5s so the same assertion costs seconds.
        extra.setdefault("EOD_LEASE_TTL_S", "5")
        box = self.sandbox()
        mirror = subprocess.Popen(["sleep", "300"])
        self.addCleanup(mirror.kill)
        rc, out = self.harness(
            box,
            ["guards", "wait_for_day_record", "wait_bounded", "eod_status",
             "days_on_file", "verified_days"],
            self._PROLOGUE.replace("@G2COUNTER@", str(box.g2_counter)),
            'guards; echo "GUARDS_RC=$?"',
            FAKE_DAY_STATE="PENDING",                 # verified_days must stay 0
            FAKE_PGREP_PID=str(mirror.pid), **extra)
        return rc, out, box

    def test_a_missed_window_is_retried_and_then_passes(self) -> None:
        rc, out, box = self._run_guards(G2_RC_1="0", G2_RC_2="5")
        self.assertIn("attempt 1: the holder finished before the concurrent run", out)
        # P1.5: the SHIPPED lease window must stay 60s. The fencing tests override
        # EOD_LEASE_TTL_S to run the same assertion faster; this pins the default.
        self.assertIn("EOD_LEASE_TTL_S:-60", SCRIPT.read_text(),
                      "eod-controller-test.sh lease ttl default changed")
        self.assertIn("ok    G-EOD-2 lease fencing: concurrent run refused (rc=5, attempt 2)", out)
        self.assertIn("GUARDS_RC=0", out)

    def test_fencing_not_enforced_while_the_holder_runs_is_a_failure(self) -> None:
        rc, out, box = self._run_guards(G2_RC_1="2", FAKE_RUN_SLEEP="3")
        self.assertIn("!! FAIL G-EOD-2 lease fencing NOT enforced: concurrent rc=2 "
                      "while the holder was still running", out)
        self.assertIn("GUARDS_RC=1", out)

    def test_missing_the_window_three_times_is_reported_as_unproven(self) -> None:
        rc, out, box = self._run_guards(G2_RC_1="0", G2_RC_2="0", G2_RC_3="0")
        self.assertIn("!! FAIL G-EOD-2 unproven: 3 attempts missed the holder's window", out)
        self.assertIn("GUARDS_RC=1", out)



class StaticGuardTest(Wave45Base):
    def test_no_pattern_kill_and_no_fixed_tmp_fixture(self) -> None:
        self.assertNotIn("pkill", self.script_text)
        self.assertNotIn("/tmp/EodPurge", self.script_text)
        self.assertIn("mktemp -d", self.script_text)
        self.assertNotIn("=${PIPESTATUS", self.script_text)   # the old capture
        self.assertIn("local rc=$?", self.script_text)        # now $? + pipefail

    def test_script_is_syntactically_valid(self) -> None:
        p = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(p.returncode, 0, p.stderr)


if __name__ == "__main__":
    unittest.main()
