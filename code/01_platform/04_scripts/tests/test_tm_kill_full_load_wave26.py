"""Wave-26 regression tests for tm-kill-full-load.sh (P6-022..024, 251..258, 635..643, 845).

The drill is a shell script whose interesting behaviour lives in its functions, so
this suite extracts the top-level function definitions (brace-matched on a lone
`}` in column 0) into a sandbox file, sources it under `set -uo pipefail`, and
replaces every external command it calls (`docker`, `curl`, `flink_metric_dump`,
`metric_totals`, `sleep_bounded`) with a recording stub.  Nothing here touches the
real Docker daemon or the real Flink cluster.

Red leg: every assertion marked `# disc` below fails against the pre-wave-26 copy
of the script (see the CHG-166 record for the measured counts).
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "tm-kill-full-load.sh"


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
OUT="{out}"
PHASE_OUT="{phase}"
JOB_ID="11111111111111111111111111111111"
CP=""
TIERING_ID=""
PREEXISTING_TIERING_ID=""
TIERING_REQUIRED=0
DRILL_FAILED=0
KILL_EPOCH=0
PRE_KILL_MAX_CP=-1
PRE_CP_MAX_TS=0
RECOVERY_EPOCH=0
AUXILIARY_JOBS=0
RESTARTS_SINCE_KILL=""
FAILED_SINCE_KILL=""
TERMINAL_SINCE_KILL=""
GC_SAMPLES=0
GC_EMPTY_SAMPLES=0
POLL_S=5
DRAIN_TIMEOUT_S=2
DRAIN_STABLE_POLLS=2
REST_TOLERANCE=3
RECOVERY_TIMEOUT_S=30
TIERING_TIMEOUT_S=5
NEW_CP_TIMEOUT_S=10
MIN_PRE_KILL_CHECKPOINTS=1
JOB_NAME_FILTER="streaming-project"
TM_CONTAINER="pipeline-taskmanager"
JM_CONTAINER="pipeline-jobmanager"
TM_CONTAINER_ID="pipeline-taskmanager"
FLINK_REST_URL="http://localhost:8081"
ALERT_CONSUMER_CONTAINER="01_docker-alert-consumer-1"
LIB_FAKETOOL_CONTAINER="pipeline-faketool"
LIB_INGESTION_CONTAINER="pipeline-ingestion"
DOCKER_CALLS="$OUT/docker-calls.txt"
CURL_CALLS="$OUT/curl-calls.txt"
: > "$DOCKER_CALLS"
: > "$CURL_CALLS"
source "{funcs}"
# Test-local overrides: no real sleeping, no real daemon, no real cluster.
sleep_bounded() {{ :; }}
docker() {{
  printf '%s\\n' "$*" >> "$DOCKER_CALLS"
  case "${{1:-}}" in
    inspect)
      case " ${{RUNNING_CONTAINERS:-}} " in
        *" ${{!#}} "*) echo true ;;
        *) echo false ;;
      esac
      return 0 ;;
    rm) return "${{DOCKER_RM_RC:-0}}" ;;
    logs)
      [ -n "${{DOCKER_LOGS_FILE:-}}" ] && cat "$DOCKER_LOGS_FILE"
      return "${{DOCKER_LOGS_RC:-0}}" ;;
    start) return "${{DOCKER_START_RC:-0}}" ;;
    exec)
      [ -n "${{DOCKER_EXEC_OUT:-}}" ] && printf '%s\\n' "$DOCKER_EXEC_OUT"
      return "${{DOCKER_EXEC_RC:-0}}" ;;
  esac
  return 0
}}
curl() {{
  local url="${{*: -1}}"
  printf '%s\\n' "$url" >> "$CURL_CALLS"
  case "$url" in
    */jobs/overview) printf '%s' "${{OVERVIEW_BODY:-}}" ;;
    */jobs/*/checkpoints) printf '%s' "${{CHECKPOINTS_BODY:-}}" ;;
    */jobs/*) printf '%s' "${{JOB_BODY:-}}" ;;
  esac
  return 0
}}
flink_metric_dump() {{ printf '%s\\n' "${{DUMP_BODY:-STATE RUNNING}}"; }}
metric_totals() {{ printf '%s\\n' "${{TOTALS:-100\t50}}"; }}
"""


class DrillTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="w26-drill-")
        self.tmp = Path(self._tmp)
        self.out = self.tmp / "drill"; self.out.mkdir()
        self.phase = self.tmp / "phase"; self.phase.mkdir()
        (self.phase / "findings.txt").touch()
        self.funcs = self.tmp / "funcs.sh"
        self.funcs.write_text(extract_functions(SCRIPT.read_text()))
        check = subprocess.run(["bash", "-n", str(self.funcs)], capture_output=True, text=True)
        assert check.returncode == 0, check.stderr

    def scenario(self, body: str, env: dict | None = None) -> subprocess.CompletedProcess:
        script = self.tmp / "scenario.sh"
        script.write_text(
            PRELUDE.format(out=self.out, phase=self.phase, funcs=self.funcs) + body + "\n"
        )
        return subprocess.run(
            ["bash", str(script)],
            capture_output=True, text=True, timeout=180,
            env={**os.environ, **(env or {})},
        )

    # helpers -------------------------------------------------------------
    def findings(self) -> str:
        return (self.phase / "findings.txt").read_text()

    def docker_calls(self) -> str:
        return (self.out / "docker-calls.txt").read_text()

    def curl_calls(self) -> str:
        return (self.out / "curl-calls.txt").read_text()


class ExtractionTest(DrillTestCase):
    def test_the_harness_extracts_the_whole_drill(self) -> None:
        funcs = self.funcs.read_text()
        for name in ("mark_failure", "fatal", "container_running", "feed_running",
                     "ingestion_running", "run_load_phase", "drill_drain_backlog",
                     "wait_job_recovery", "wait_new_checkpoint", "find_tiering",
                     "wait_tiering_recovery", "cancel_preflight_tiering",
                     "active_tiering_count", "collect_restart_evidence",
                     "assert_restart_contract", "alert_count", "assert_alert_contract",
                     "harvest_live_gc", "assert_gc_evidence", "require_positive_int"):
            self.assertIn(f"{name}() {{", funcs, f"{name} was not extracted")


class PhonyAndKnobTest(DrillTestCase):
    def test_every_knob_with_a_numeric_default_is_validated(self) -> None:
        # P6-635/#252: a knob that feeds arithmetic and is never validated is a
        # typo away from `set -u` aborting (or worse, a silent 0).
        text = SCRIPT.read_text()
        declared = {n for n, v in re.findall(r'^([A-Z][A-Z0-9_]*)="\$\{\1:-([0-9]+)\}"', text, re.M)}
        block = re.search(r"for _arg in \\\n(.*?)\ndone", text, re.S).group(1)
        validated = set(re.findall(r'"([A-Z][A-Z0-9_]*)=\$', block))
        self.assertTrue(declared, "no numeric knobs found - the extractor broke")
        self.assertEqual([], sorted(declared - validated),
                         "numeric knobs missing from the validation loop")


class ContainerLivenessTest(DrillTestCase):
    def test_liveness_helpers_follow_the_containers(self) -> None:  # disc (P6-024)
        r = self.scenario(
            "container_running pipeline-faketool; echo \"faketool rc=$?\"\n"
            "container_running pipeline-ingestion; echo \"ingestion rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "pipeline-faketool"})
        self.assertIn("faketool rc=0", r.stdout)
        self.assertIn("ingestion rc=1", r.stdout)

    def test_feed_running_names_the_feed_container(self) -> None:
        r = self.scenario(
            "feed_running; echo \"feed rc=$?\"\ningestion_running; echo \"ing rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "pipeline-ingestion"})
        self.assertIn("feed rc=1", r.stdout)
        self.assertIn("ing rc=0", r.stdout)


class LoadPhaseTest(DrillTestCase):
    def test_a_dead_feed_container_fails_the_load_phase(self) -> None:  # disc (P6-023)
        r = self.scenario(
            "run_load_phase load 1 || echo \"rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "pipeline-ingestion"})
        self.assertEqual(1, r.returncode, r.stderr)
        self.assertIn("feed container", (self.phase / "FAILURE.txt").read_text())
        self.assertIn("FAIL", (self.phase / "RESULT.txt").read_text())

    def test_a_rest_restart_blip_is_not_a_finding(self) -> None:  # disc (P6-637)
        # One non-RUNNING poll inside a 4-poll window (a REST blip, not a dead
        # job) must not abort the drill after 12 minutes of load.
        r = self.scenario(
            "polls=0\n"
            "curl() { local url=\"${*: -1}\"; case \"$url\" in\n"
            "  */jobs/*/checkpoints) printf '%s' \"$CHECKPOINTS_BODY\" ;;\n"
            "  *) polls=$((polls+1))\n"
            "     if [ \"$polls\" -eq 2 ]; then printf '%s' '{\"state\":\"RESTARTING\"}';\n"
            "     else printf '%s' '{\"state\":\"RUNNING\"}'; fi ;;\n"
            "esac; }\n"
            "run_load_phase load 20 || echo \"rc=$?\"\n",
            env={"RUNNING_CONTAINERS": "pipeline-faketool pipeline-ingestion"})
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertNotIn("FINDING", r.stderr)
        self.assertFalse((self.phase / "FAILURE.txt").exists())

    def test_too_many_rest_restarts_are_a_finding(self) -> None:
        r = self.scenario(
            "curl() { printf '%s' '{\"state\":\"RESTARTING\"}'; }\n"
            "run_load_phase load 20 || true\n",
            env={"RUNNING_CONTAINERS": "pipeline-faketool pipeline-ingestion"})
        self.assertIn("RESTARTING", (self.phase / "FAILURE.txt").read_text())


class DrainTest(DrillTestCase):
    def test_a_stalled_source_with_a_live_sink_does_not_drain(self) -> None:  # disc (P6-253)
        # The source counter freezes while the pipeline keeps writing: that is
        # the replay backlog draining, or a stalled source with the job still
        # accepting down the line. The old rule called three identical source
        # reads "drained" and produced the phantom-tail F4 orphans.
        r = self.scenario(
            "writes=0\n"
            "metric_totals() { writes=$((writes+1)); printf '%s\t%s\n' 100 \"$writes\"; }\n"
            "drill_drain_backlog || true\n"
            "echo \"evidence=$(cat \"$OUT/drain.txt\" | head -1)\"\n",
            env={"DRAIN_TIMEOUT_S": "2"})
        self.assertEqual("drained=false", r.stdout.strip().splitlines()[-1].split("=", 1)[1])
        self.assertIn("did not drain", self.findings())

    def test_a_frozen_or_missing_sink_counter_is_not_drained(self) -> None:  # disc (P6-252/253)
        r = self.scenario(
            "drill_drain_backlog || true\n"
            "echo \"evidence=$(cat \"$OUT/drain.txt\" | head -1)\"\n",
            env={"TOTALS": "100\t-1", "DRAIN_TIMEOUT_S": "2"})
        self.assertIn("drained=false", r.stdout)

    def test_a_missing_metric_is_named_and_not_treated_as_stable(self) -> None:  # disc (P6-252)
        r = self.scenario(
            "drill_drain_backlog || true\n", env={"TOTALS": "-1\t-1", "DRAIN_TIMEOUT_S": "2"})
        self.assertIn("metric missing", r.stdout)
        self.assertIn("drained=false", (self.out / "drain.txt").read_text())

    def test_drain_stops_the_feed_container(self) -> None:  # disc (P6-023)
        # Stopping the load generator is what actually drains the window; the old
        # implementation only sent SIGKILL to a PID pipeline-lib never sets, so
        # the feed kept running and the drain gate was pure fiction.
        r = self.scenario("drill_drain_backlog || true\n")
        self.assertRegex(self.docker_calls(), r"rm -f pipeline-faketool")
        self.assertIn("drained=true", (self.out / "drain.txt").read_text())

    def test_a_failed_feed_stop_fails_the_drill(self) -> None:  # disc (P6-023)
        r = self.scenario("drill_drain_backlog || true\n", env={"DOCKER_RM_RC": "1"})
        self.assertIn("could not stop the feed", self.findings())
        self.assertIn("drained=false", (self.out / "drain.txt").read_text())


class RecoveryTest(DrillTestCase):
    def test_a_new_checkpoint_proves_recovery_without_a_state_flap(self) -> None:  # disc (P6-254)
        # Steady RUNNING: the old code waited for the state to leave RUNNING and
        # then aborted, discarding a perfectly good recovery.
        r = self.scenario(
            "PRE_KILL_MAX_CP=4\n"
            "wait_job_recovery 6 || echo \"rc=$?\"\n"
            "echo \"evidence=$(cat \"$OUT/recovery-evidence.txt\" 2>/dev/null)\"\n",
            env={"CHECKPOINTS_BODY": '{"history":[{"id":5,"status":"COMPLETED","trigger_timestamp":1900000}]}',
                 "JOB_BODY": '{"state":"RUNNING"}'})
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("checkpoint", (self.out / "recovery-evidence.txt").read_text())

    def test_recovery_without_evidence_still_fails(self) -> None:
        r = self.scenario(
            "PRE_KILL_MAX_CP=4\nwait_job_recovery 4 || true\n",
            env={"CHECKPOINTS_BODY": '{"history":[{"id":4,"status":"COMPLETED","trigger_timestamp":900000}]}'})
        self.assertEqual(1, r.returncode, r.stderr)
        self.assertIn("recovery", (self.phase / "FAILURE.txt").read_text())
        self.assertIn("FAIL", (self.phase / "RESULT.txt").read_text())


class CheckpointGateTest(DrillTestCase):
    def test_a_valid_post_kill_checkpoint_is_not_rejected_by_host_clock_skew(self) -> None:  # disc (P6-255)
        # The TaskManager stamps checkpoints with the JM's clock; the old gate
        # compared that stamp against the host's kill time, so an hour of skew
        # rejected every valid checkpoint and aborted the drill.
        r = self.scenario(
            "PRE_CP_MAX_TS=1000000\n"
            "wait_new_checkpoint 4 || echo \"rc=$?\"\n"
            "echo \"evidence=$(cat \"$OUT/post-kill-checkpoint.txt\" 2>/dev/null)\"\n",
            env={"CHECKPOINTS_BODY": '{"history":[{"id":7,"status":"COMPLETED","trigger_timestamp":2000000}]}'})
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("7", (self.out / "post-kill-checkpoint.txt").read_text())

    def test_a_checkpoint_older_than_the_pre_kill_max_is_rejected(self) -> None:
        r = self.scenario(
            "PRE_CP_MAX_TS=9000000\nwait_new_checkpoint 4 || true\n",
            env={"CHECKPOINTS_BODY": '{"history":[{"id":7,"status":"COMPLETED","trigger_timestamp":2000000}]}'})
        self.assertIn("checkpoint", (self.phase / "FAILURE.txt").read_text())


class RestartEvidenceTest(DrillTestCase):
    JID = "11111111111111111111111111111111"

    def _logs(self, *lines: str) -> Path:
        # collect_restart_evidence greps for the drill's own job id, so every
        # transition line has to carry it or the count is legitimately zero.
        f = self.tmp / "tm.log"
        f.write_text("\n".join(f"{self.JID} {line}" for line in lines) + "\n")
        return f

    def test_restart_evidence_asks_for_a_duration_not_an_epoch(self) -> None:  # disc (P6-257)
        log = self._logs(*[f"2026-09-15 INFO line {i}" for i in range(200)],
                         "2026-09-15 INFO TaskManager switched from state RUNNING to RESTARTING.")
        self.scenario("collect_restart_evidence || true\n", env={"DOCKER_LOGS_FILE": str(log)})
        since = re.search(r"--since (\S+)", self.docker_calls())
        self.assertIsNotNone(since, "no --since in the docker logs call")
        self.assertRegex(since.group(1), r"^[0-9]+s$")

    def test_an_empty_log_slice_is_unverified_not_clean(self) -> None:  # disc (P6-256)
        log = self._logs("")
        r = self.scenario(
            "PRE_KILL_MAX_CP=1\ncollect_restart_evidence || echo \"rc=$?\"\n"
            "assert_restart_contract || echo \"contract_rc=$?\"\n",
            env={"DOCKER_LOGS_FILE": str(log)})
        self.assertIn("rc=1", r.stdout)
        self.assertIn("contract_rc=1", r.stdout)
        self.assertIn("unverified", (self.out / "restarts-since-kill.txt").read_text())
        self.assertIn("restart", self.findings())

    def test_a_failing_transition_counts_as_a_restart(self) -> None:  # disc (P6-258)
        # Flink logs the same restart as `RUNNING to FAILING` + `FAILING to
        # RESTARTING`; only matching the first form counted zero restarts.
        log = self._logs(*[f"p{i}" for i in range(200)],
                         "INFO TaskManager switched from state RUNNING to FAILING.",
                         "INFO TaskManager switched from state FAILING to RESTARTING.")
        r = self.scenario(
            "collect_restart_evidence || true\n"
            "echo \"counts=$(grep '^restarts=' \"$OUT/restarts-since-kill.txt\")\"\n",
            env={"DOCKER_LOGS_FILE": str(log)})
        self.assertIn("restarts=1", r.stdout)

    def test_a_kill_induced_restart_passes_the_contract(self) -> None:
        log = self._logs(*[f"p{i}" for i in range(200)],
                         "INFO TaskManager switched from state RUNNING to RESTARTING.")
        r = self.scenario(
            "collect_restart_evidence || true\nassert_restart_contract || echo \"contract_rc=$?\"\n",
            env={"DOCKER_LOGS_FILE": str(log)})
        self.assertNotIn("contract_rc", r.stdout)

    def test_a_terminal_failure_fails_the_contract(self) -> None:
        log = self._logs(*[f"p{i}" for i in range(200)],
                         "INFO TaskManager switched from state RUNNING to FAILED.")
        r = self.scenario(
            "collect_restart_evidence || true\nassert_restart_contract || true\n",
            env={"DOCKER_LOGS_FILE": str(log)})
        self.assertIn("FAILED", (self.phase / "FAILURE.txt").read_text())


class TieringTest(DrillTestCase):
    TWO_LIVE = ('{"jobs":[{"jid":"aaa","name":"Fluss Lake Tiering","state":"RUNNING"},'
                '{"jid":"bbb","name":"Fluss Lake Tiering","state":"RUNNING"}]}')
    ONE_LIVE = ('{"jobs":[{"jid":"aaa","name":"Fluss Lake Tiering","state":"RUNNING"}]}')

    def test_find_tiering_reports_every_live_job(self) -> None:  # disc (P6-636)
        r = self.scenario("find_tiering\n", env={"OVERVIEW_BODY": self.TWO_LIVE})
        self.assertTrue(r.stdout.startswith("2\t"), r.stdout)

    def test_split_brain_tiering_fails_recovery(self) -> None:  # disc (P6-636)
        r = self.scenario(
            "TIERING_REQUIRED=1\nwait_tiering_recovery 5 || true\n",
            env={"OVERVIEW_BODY": self.TWO_LIVE})
        self.assertIn("more than one", self.findings())
        self.assertIn("MULTIPLE", (self.out / "tiering-recovery.txt").read_text())

    def test_recovery_keeps_the_preflight_evidence(self) -> None:  # disc (P6-639)
        r = self.scenario(
            "printf 'before\\t%s\\t%s\\n' aaa 1 > \"$OUT/tiering-recovery.txt\"\n"
            "TIERING_REQUIRED=1\nwait_tiering_recovery 5 || true\n"
            "cat \"$OUT/tiering-recovery.txt\"\n",
            env={"OVERVIEW_BODY": self.ONE_LIVE})
        self.assertIn("before\taaa\t1", r.stdout)
        self.assertIn("PASS", r.stdout)

    def test_a_vanished_preflight_job_is_already_stopped(self) -> None:  # disc (P6-638)
        # The job finishes between the overview and the state probe, so the REST
        # call 404s and prints nothing; aborting a 12-minute drill on that race is
        # wrong.
        r = self.scenario(
            "TIERING_REQUIRED=1\nPREEXISTING_TIERING_ID=aaa\n"
            "cancel_preflight_tiering || echo \"rc=$?\"\n",
            env={"JOB_BODY": ""})
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("already stopped", r.stdout)

    def test_half_dead_tiering_is_cancelled_before_the_purge(self) -> None:
        r = self.scenario(
            "TIERING_REQUIRED=1\nPREEXISTING_TIERING_ID=aaa\n"
            "cancel_preflight_tiering || echo \"rc=$?\"\n"
            "echo \"required=$TIERING_REQUIRED\"\n",
            env={"JOB_BODY": '{"state":"RESTARTING"}', "OVERVIEW_BODY": '{"jobs":[]}'})
        self.assertEqual(0, r.returncode, r.stderr)
        self.assertIn("stopping pre-existing tiering job aaa", r.stdout)
        self.assertRegex(self.docker_calls(), r"exec .*flink cancel aaa")
        self.assertIn("required=1", r.stdout)

    def test_a_terminal_tiering_job_still_requires_recovery(self) -> None:
        src = SCRIPT.read_text()
        # P6-640: the branch that promises a "guarded restart" for a terminal job
        # must also set TIERING_REQUIRED, or the restart never happens.
        block = re.search(
            r'elif \[ "\$_state" = "FAILED" \].*?\n(.*?)\n    fi', src, re.S).group(1)
        self.assertIn("TIERING_REQUIRED=1", block)


class AlertGateTest(DrillTestCase):
    def test_an_unreadable_feed_is_minus_one(self) -> None:  # disc (P6-642)
        r = self.scenario(
            "echo \"count=$(alert_count)\"\n", env={"DOCKER_EXEC_RC": "1"})
        self.assertIn("count=-1", r.stdout)

    def test_no_new_alert_fails_the_drill(self) -> None:  # disc (P6-642)
        r = self.scenario(
            "assert_alert_contract 1 1 || echo \"rc=$?\"\n")
        self.assertIn("rc=1", r.stdout)
        self.assertIn("alert", self.findings())

    def test_an_unreadable_feed_is_unverified_not_clean(self) -> None:  # disc (P6-642)
        r = self.scenario("assert_alert_contract -1 5 || echo \"rc=$?\"\n")
        self.assertIn("rc=1", r.stdout)
        self.assertIn("unverified", self.findings())

    def test_a_new_alert_passes(self) -> None:
        r = self.scenario("assert_alert_contract 1 2 || echo \"rc=$?\"\n")
        self.assertNotIn("rc=", r.stdout)


class GcEvidenceTest(DrillTestCase):
    def test_every_live_sample_gets_its_own_file(self) -> None:  # disc (P6-845)
        r = self.scenario(
            "mkdir -p \"$OUT/gc-live\"\n"
            "harvest_tm_gc_log() { printf '%s\\n' gc-line > \"$1\"; }\n"
            "harvest_live_gc 0\nharvest_live_gc 1\nharvest_live_gc 2\n"
            "ls \"$OUT/gc-live\"\n"
            "echo \"samples=$GC_SAMPLES empty=$GC_EMPTY_SAMPLES\"\n")
        self.assertEqual(3, r.stdout.count("sample-"))
        self.assertIn("samples=3 empty=0", r.stdout)

    def test_no_gc_evidence_at_all_is_a_finding(self) -> None:  # disc (P6-845)
        r = self.scenario(
            "harvest_tm_gc_log() { : > \"$1\"; }\n"
            "harvest_live_gc 0\nharvest_live_gc 1\n"
            "assert_gc_evidence || echo \"rc=$?\"\n")
        self.assertIn("rc=1", r.stdout)
        self.assertIn("GC", self.findings())

    def test_an_empty_final_harvest_is_a_finding(self) -> None:  # disc (P6-845)
        r = self.scenario(
            ": > \"$OUT/tm-gc-final.log\"\n"
            "assert_gc_evidence || echo \"rc=$?\"\n")
        self.assertIn("rc=1", r.stdout)
        self.assertIn("final", (self.out / "gc-evidence.txt").read_text() +
                      self.findings())


class FatalContractTest(DrillTestCase):
    def test_fatal_leaves_the_structured_verdict(self) -> None:  # disc (P6-643)
        r = self.scenario("fatal \"G7 parity mismatch\" || true\n")
        self.assertEqual(1, r.returncode, r.stderr)
        result = (self.phase / "RESULT.txt").read_text()
        self.assertIn("FAIL", result)
        self.assertIn("G7 parity mismatch", result)
        self.assertIn("job_id=", result)


class KnobGuardTest(DrillTestCase):
    def test_require_positive_int_rejects_junk(self) -> None:
        r = self.scenario(
            "require_positive_int POLL_S 5\n"
            "echo \"ok rc=$?\"\n"
            "require_positive_int POLL_S 0 || true\n")
        self.assertIn("ok rc=0", r.stdout)
        self.assertIn("POLL_S", (self.phase / "FAILURE.txt").read_text())


if __name__ == "__main__":
    unittest.main()
