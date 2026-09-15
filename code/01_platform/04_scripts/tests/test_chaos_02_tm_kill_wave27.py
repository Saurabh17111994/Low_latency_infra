"""Wave-27 regression tests for chaos-02-tm-kill.sh (P6-043..048, 331..333).

The drill is a shell script whose interesting behaviour lives in its functions, so
this suite extracts the top-level function definitions (brace-matched on a lone `}`
in column 0) into a sandbox file, sources it, and replaces every external command it
calls (`docker`, `curl`, `mvn`, `sleep`) with a recording stub.  Nothing here kills a
real container or talks to a real Flink cluster.

Red leg: every assertion marked `# disc` fails against the pre-wave-27 copy of the
script (see the CHG-167 record for the measured counts).
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "chaos" / "chaos-02-tm-kill.sh"

RUNNING_JOB = '{"jobs": [{"jid": "job-a", "state": "RUNNING"}]}'
OTHER_JOB = '{"jobs": [{"jid": "job-z", "state": "RUNNING"}]}'


def extract_functions(text: str) -> str:
    """Return the source of every top-level `name() {` .. `}` definition."""
    lines = text.split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        if re.match(r"^[a-z_][a-z0-9_]*\(\) \{", lines[i]):
            out.append(lines[i])
            i += 1
            if out[-1].rstrip().endswith("}"):  # one-liner: `f() { cmd; }`
                out.append("")
                continue
            while i < len(lines) and lines[i] != "}":
                out.append(lines[i])
                i += 1
            if i < len(lines):
                out.append(lines[i])
                i += 1
            out.append("")
            continue
        i += 1
    return "\n".join(out)


PRELUDE = """\
set -uo pipefail
OUT="__OUT__"
COMPUTE_DIR="$OUT/compute"
FLINK_REST_URL="http://rest:8081"
TM_METRICS_URL="http://tm:9250/metrics"
CHAOS_TM_CONTAINER="${CHAOS_TM_CONTAINER:-}"
CHAOS_TM_RECOVERY_TIMEOUT_S="${CHAOS_TM_RECOVERY_TIMEOUT_S:-1}"
CHAOS_TM_POLL_S="${CHAOS_TM_POLL_S:-1}"
CHAOS_TM_METRICS_REQUIRED="${CHAOS_TM_METRICS_REQUIRED:-1}"
CHAOS_LOGDIR="$OUT/logs"
TM_KILL_TEST_CLASS="SignalJobTaskManagerKillIntegrationTest"
LOGDIR="$OUT/logs"
DOCKER_CALLS="$OUT/docker-calls.txt"
CURL_CALLS="$OUT/curl-calls.txt"
: > "$DOCKER_CALLS"
: > "$CURL_CALLS"
mkdir -p "$LOGDIR" "$COMPUTE_DIR/target/surefire-reports"
source "__FUNCS__"
sleep() { command sleep 0.1; }
mvn() {
  printf 'mvn %s\\n' "$*" >> "$OUT/mvn-calls.txt"
  return "${MVN_RC:-0}"
}
# pop <file>: print and remove the first line (empty once exhausted). Lets a test
# script a per-call response sequence for one endpoint.
pop() {
  local f="$1" line=""
  if [ -s "$f" ]; then
    line="$(sed -n 1p "$f")"
    sed -i '1d' "$f"
  fi
  printf '%s' "$line"
}
docker() {
  printf '%s\\n' "$*" >> "$DOCKER_CALLS"
  case "${1:-}" in
    info)
      [ -n "${DOCKER_INFO_ERR:-}" ] && printf '%s\\n' "$DOCKER_INFO_ERR" >&2
      return "${DOCKER_INFO_RC:-0}" ;;
    ps) cat "$OUT/tms.txt" ;;
    inspect)
      case " ${RUNNING_CONTAINERS:-} " in
        *" ${!#} "*) printf 'true\\n' ;;
        *) printf 'false\\n' ;;
      esac ;;
    kill) return "${DOCKER_KILL_RC:-0}" ;;
    start) return "${DOCKER_START_RC:-0}" ;;
  esac
  return 0
}
curl() {
  local url="${*: -1}"
  printf '%s\\n' "$url" >> "$CURL_CALLS"
  case "$url" in
    */jobs/overview) pop "$OUT/overview-seq.txt" ;;
    */taskmanagers) pop "$OUT/tm-seq.txt" ;;
    *) pop "$OUT/metrics-seq.txt" ;;
  esac
}
"""


class TMKillTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="w27-tmkill-")
        self.tmp = Path(self._tmp)
        self.out = self.tmp / "sandbox"
        self.out.mkdir()
        self.comp = self.out / "compute"
        (self.comp / "target" / "surefire-reports").mkdir(parents=True)
        self.funcs = self.tmp / "funcs.sh"
        self.funcs.write_text(extract_functions(SCRIPT.read_text()))
        check = subprocess.run(["bash", "-n", str(self.funcs)], capture_output=True, text=True)
        assert check.returncode == 0, check.stderr
        self.write("tms.txt", "tm-1 alpha\ntm-2 beta\n")
        self.write("overview-seq.txt", RUNNING_JOB + "\n" + RUNNING_JOB + "\n")
        self.write("tm-seq.txt", '{"taskmanagers": [{"id": "tm-1"}]}\n')
        self.write("metrics-seq.txt", "compute_candles 3\n")

    def tearDown(self) -> None:
        subprocess.run(["rm", "-rf", self._tmp], check=False)

    def write(self, name: str, text: str) -> Path:
        path = self.out / name
        path.write_text(text)
        return path

    def surefire(self, tests: int, skipped: int = 0) -> None:
        report = self.comp / "target" / "surefire-reports" / (
            "TEST-com.example.SignalJobTaskManagerKillIntegrationTest.xml"
        )
        report.write_text(
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<testsuite name="kill" tests="{tests}" skipped="{skipped}" '
            'failures="0" errors="0"/>\n'
        )

    def scenario(self, body: str, env: dict | None = None) -> subprocess.CompletedProcess:
        script = self.tmp / "scenario.sh"
        script.write_text(
            PRELUDE.replace("__OUT__", str(self.out)).replace("__FUNCS__", str(self.funcs))
            + body + "\n"
        )
        return subprocess.run(
            ["bash", str(script)],
            capture_output=True, text=True, timeout=180,
            env={**os.environ, **{k: str(v) for k, v in (env or {}).items()}},
        )

    def docker_calls(self) -> str:
        return (self.out / "docker-calls.txt").read_text()

    def curl_calls(self) -> str:
        return (self.out / "curl-calls.txt").read_text()

    def evidence(self, name: str) -> str:
        return (self.out / "logs" / name).read_text()


class ExtractionTest(TMKillTestCase):
    def test_the_harness_extracts_the_whole_drill(self) -> None:
        funcs = self.funcs.read_text()
        for name in ("leg_a_assert_ran", "run_leg_a", "docker_daemon_ok", "list_tms",
                     "container_running", "wait_same_container", "wait_tm_registered",
                     "jobs_running_ids", "wait_job_restored", "wait_metrics",
                     "run_leg_b", "main", "require_positive_int"):
            self.assertIn(f"{name}() {{", funcs, f"{name} was not extracted")


class LegATest(TMKillTestCase):
    def test_an_executed_test_is_a_pass(self) -> None:
        self.surefire(tests=3)
        r = self.scenario("leg_a_assert_ran; echo \"rc=$?\"\n")
        self.assertIn("rc=0\n", r.stdout, r.stderr)
        self.assertIn("executed 3 test(s)", r.stdout)

    def test_a_run_that_executed_nothing_fails(self) -> None:  # disc (P6-043)
        self.surefire(tests=0)
        r = self.scenario("leg_a_assert_ran; echo \"rc=$?\"\n")
        self.assertIn("rc=1\n", r.stdout, r.stderr)
        self.assertIn("0 tests executed", r.stderr)

    def test_a_fully_skipped_class_fails(self) -> None:  # disc (P6-043)
        self.surefire(tests=2, skipped=2)
        r = self.scenario("leg_a_assert_ran; echo \"rc=$?\"\n")
        self.assertIn("rc=1\n", r.stdout, r.stderr)
        self.assertIn("skipped", r.stderr)

    def test_no_reports_at_all_fails(self) -> None:  # disc (P6-043)
        for stale in (self.comp / "target" / "surefire-reports").glob("*.xml"):
            stale.unlink()
        r = self.scenario("leg_a_assert_ran; echo \"rc=$?\"\n")
        self.assertIn("rc=1\n", r.stdout, r.stderr)

    def test_a_green_mvn_with_no_tests_is_not_a_pass(self) -> None:  # disc (P6-043)
        # -DfailIfNoTests=false makes a mis-scoped -Dtest exit 0: the leg has to
        # check the reports, not the exit status.
        self.surefire(tests=0)
        r = self.scenario("run_leg_a; echo \"leg_a rc=$?\"\n", env={"MVN_RC": 0})
        self.assertIn("leg_a rc=1\n", r.stdout, r.stderr)


class DaemonTest(TMKillTestCase):
    def test_a_reachable_daemon_is_ok(self) -> None:
        r = self.scenario("docker_daemon_ok; echo \"rc=$?\"\n")
        self.assertIn("rc=0\n", r.stdout, r.stderr)

    def test_a_broken_daemon_is_not_a_skip(self) -> None:  # disc (P6-331)
        r = self.scenario(
            "docker_daemon_ok; echo \"rc=$?\"\n",
            env={"DOCKER_INFO_RC": 1, "DOCKER_INFO_ERR": "Cannot connect to the Docker daemon"},
        )
        self.assertIn("rc=1\n", r.stdout, r.stderr)
        self.assertIn("Cannot connect to the Docker daemon", r.stderr)

    def test_leg_b_fails_loudly_when_the_daemon_is_unreachable(self) -> None:  # disc (P6-331)
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"DOCKER_INFO_RC": 1, "DOCKER_INFO_ERR": "permission denied"},
        )
        self.assertIn("leg_b rc=1\n", r.stdout, r.stderr)
        self.assertIn("not a stack that is down", r.stderr)
        self.assertNotIn("kill", self.docker_calls())


class VictimSelectionTest(TMKillTestCase):
    def test_candidates_are_logged_and_the_victim_is_deterministic(self) -> None:  # disc (P6-332)
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=0\n", r.stdout, r.stderr)
        self.assertIn("tm-2 beta", r.stdout)
        self.assertIn("lowest id", r.stdout)
        self.assertIn("kill -s KILL tm-1", self.docker_calls())

    def test_an_unknown_pin_is_a_config_error(self) -> None:  # disc (P6-332)
        r = self.scenario("run_leg_b\n", env={"CHAOS_TM_CONTAINER": "tm-9"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("is not one of the flink-taskmanager containers", r.stderr)

    def test_the_pinned_victim_is_the_one_that_must_return(self) -> None:  # disc (P6-046)
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"CHAOS_TM_CONTAINER": "tm-2", "RUNNING_CONTAINERS": "tm-2"},
        )
        self.assertIn("leg_b rc=0\n", r.stdout, r.stderr)
        self.assertIn("kill -s KILL tm-2", self.docker_calls())

    def test_container_state_follows_the_id_not_the_name(self) -> None:  # disc (P6-046)
        r = self.scenario(
            "container_running tm-1; echo \"tm-1 rc=$?\"\n"
            "container_running tm-2; echo \"tm-2 rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-2"},
        )
        self.assertIn("tm-1 rc=1\n", r.stdout, r.stderr)
        self.assertIn("tm-2 rc=0\n", r.stdout, r.stderr)

    def test_a_victim_that_never_comes_back_fails(self) -> None:  # disc (P6-046)
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"CHAOS_TM_CONTAINER": "tm-2", "RUNNING_CONTAINERS": ""},
        )
        self.assertIn("leg_b rc=1\n", r.stdout, r.stderr)
        self.assertIn("is not Running again", r.stderr)
        self.assertIn("start tm-2", self.docker_calls())


class BaselineTest(TMKillTestCase):
    def test_a_taskmanager_is_not_killed_without_a_workload(self) -> None:  # disc (P6-045)
        self.write("overview-seq.txt", '{"jobs": []}\n')
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=0\n", r.stdout, r.stderr)
        self.assertIn("no RUNNING Flink job", r.stdout)
        self.assertNotIn("kill", self.docker_calls())

    def test_an_unreachable_overview_is_a_failure_not_a_skip(self) -> None:  # disc (P6-045)
        self.write("overview-seq.txt", "\n")
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=1\n", r.stdout, r.stderr)
        self.assertIn("no pre-kill baseline", r.stderr)
        self.assertNotIn("kill", self.docker_calls())

    def test_the_baseline_is_recorded_before_the_kill(self) -> None:  # disc (P6-045)
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=0\n", r.stdout, r.stderr)
        self.assertIn("job-a", self.evidence("jobs-before.txt"))


class RestoreProbeTest(TMKillTestCase):
    def test_a_different_job_is_not_a_restore(self) -> None:  # disc (P6-044)
        self.write("overview-seq.txt", OTHER_JOB + "\n")
        baseline = self.write("baseline.txt", "job-a\n")
        r = self.scenario(
            f"wait_job_restored {baseline}; echo \"rc=$?\"\n"
        )
        self.assertIn("rc=1\n", r.stdout, r.stderr)

    def test_the_same_job_is_the_restore(self) -> None:  # disc (P6-044)
        self.write("overview-seq.txt", RUNNING_JOB + "\n")
        baseline = self.write("baseline.txt", "job-a\n")
        r = self.scenario(f"wait_job_restored {baseline}; echo \"\" ; echo \"rc=$?\"\n")
        self.assertIn("job-a", r.stdout, r.stderr)
        self.assertIn("rc=0\n", r.stdout, r.stderr)

    def test_a_slow_recovery_is_polled_not_failed_on_the_first_shot(self) -> None:  # disc (P6-048)
        # First poll: nothing RUNNING (the JobManager is reconciling). Second: the
        # job is back. A single-shot probe reports a FAIL for a healthy recovery.
        self.write("overview-seq.txt", '{"jobs": []}\n' + RUNNING_JOB + "\n")
        baseline = self.write("baseline.txt", "job-a\n")
        r = self.scenario(f"wait_job_restored {baseline}; echo \"\" ; echo \"rc=$?\"\n")
        self.assertIn("job-a", r.stdout, r.stderr)
        self.assertIn("rc=0\n", r.stdout, r.stderr)

    def test_registration_is_required_not_just_a_running_container(self) -> None:  # disc (P6-047)
        self.write("tm-seq.txt", '{"taskmanagers": []}\n'
                                   '{"taskmanagers": [{"id": "tm-1"}]}\n')
        r = self.scenario("wait_tm_registered; echo \" count=$?\"\n")
        self.assertIn("1 count=0\n", r.stdout, r.stderr)

    def test_a_taskmanager_that_never_registers_fails(self) -> None:  # disc (P6-047)
        self.write("tm-seq.txt", '{"taskmanagers": []}\n'
                                   '{"taskmanagers": []}\n'
                                   '{"taskmanagers": []}\n')
        r = self.scenario("wait_tm_registered; echo \" count=$?\"\n")
        self.assertIn("count=1\n", r.stdout, r.stderr)


class MetricsTest(TMKillTestCase):
    def test_metrics_presence_and_absence(self) -> None:
        present = self.scenario("wait_metrics; echo \"rc=$?\"\n")
        self.assertIn("rc=0\n", present.stdout, present.stderr)
        self.write("metrics-seq.txt", "\n")
        absent = self.scenario("wait_metrics; echo \"rc=$?\"\n")
        self.assertIn("rc=1\n", absent.stdout, absent.stderr)

    def test_missing_metrics_fail_the_leg_by_default(self) -> None:  # disc (P6-333)
        self.write("metrics-seq.txt", "\n")
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=1\n", r.stdout, r.stderr)
        self.assertIn("compute_candles", r.stderr)

    def test_missing_metrics_can_be_downgraded_to_a_warning(self) -> None:  # disc (P6-333)
        self.write("metrics-seq.txt", "\n")
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2", "CHAOS_TM_METRICS_REQUIRED": 0},
        )
        self.assertIn("leg_b rc=0\n", r.stdout, r.stderr)
        self.assertIn("recorded, not asserted", r.stderr)


    def test_leg_b_requires_curl_and_python3(self) -> None:  # disc
        # The recovery probes are curl+python3; a host missing either must fail
        # loudly before the kill instead of reaching a vacuous PASS.
        r = self.scenario(
            'have() { [ "$1" = python3 ] && return 1; command -v "$1" >/dev/null 2>&1; }\n'
            'run_leg_b; echo "leg_b rc=$?"\n',
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=1\n", r.stdout, r.stderr)
        self.assertIn("curl and python3 are required for the recovery probe", r.stderr)
        self.assertNotIn("kill", self.docker_calls())


class HappyPathTest(TMKillTestCase):
    def test_a_killed_taskmanager_comes_back_with_its_job(self) -> None:
        r = self.scenario(
            "run_leg_b; echo \"leg_b rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "tm-1 tm-2"},
        )
        self.assertIn("leg_b rc=0\n", r.stdout, r.stderr)
        self.assertIn("same id as before the kill", r.stdout)
        self.assertIn("kill -s KILL tm-1", self.docker_calls())
        self.assertIn("inspect --format {{.State.Running}} tm-1", self.docker_calls())
        self.assertIn("job-a", self.evidence("job-restored.txt"))
        self.assertIn("taskmanagers", self.curl_calls())


class KnobValidationTest(TMKillTestCase):
    def test_a_numeric_knob_typo_is_a_usage_error(self) -> None:  # disc
        r = self.scenario("main\n", env={"CHAOS_TM_RECOVERY_TIMEOUT_S": "soon"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("is not a positive integer", r.stderr)

    def test_the_metrics_flag_is_validated(self) -> None:  # disc
        r = self.scenario("main\n", env={"CHAOS_TM_METRICS_REQUIRED": "yes"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("must be 0 or 1", r.stderr)


if __name__ == "__main__":
    unittest.main()
