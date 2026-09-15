"""Wave-27 regression tests for chaos-04-vm-loss.sh.

Findings covered: P6-002 (CRITICAL), P6-053..057, P6-340..343, P6-720.

The drill is a shell script whose behaviour lives in its functions, so this suite
extracts the top-level function definitions (brace-matched on a lone `}` in column
0) into a sandbox file, sources it, and replaces every external command it calls
(`docker`, `timeout`, `sleep`) with a recording stub. Nothing here touches a docker
daemon or a swarm.

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

SCRIPT = Path(__file__).resolve().parents[1] / "chaos" / "chaos-04-vm-loss.sh"


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
CHAOS_WORKLOAD_NODE="${CHAOS_WORKLOAD_NODE:-}"
CHAOS_SERVICE="${CHAOS_SERVICE:-}"
CHAOS_VM_OFF_MODE="${CHAOS_VM_OFF_MODE:-drain}"
CHAOS_VM_OFF_CMD="${CHAOS_VM_OFF_CMD:-}"
CHAOS_VM_ON_CMD="${CHAOS_VM_ON_CMD:-}"
CHAOS_ORDER_PROBE_TCP="${CHAOS_ORDER_PROBE_TCP:-}"
CHAOS_NODE_READY_MIN="${CHAOS_NODE_READY_MIN:-2}"
CHAOS_ORDER_HALT_SLO_S="${CHAOS_ORDER_HALT_SLO_S:-1}"
CHAOS_RECOVERY_SLO_S="${CHAOS_RECOVERY_SLO_S:-1}"
CHAOS_NODE_READY_TIMEOUT_S="${CHAOS_NODE_READY_TIMEOUT_S:-1}"
CHAOS_VM_CMD_TIMEOUT_S="${CHAOS_VM_CMD_TIMEOUT_S:-5}"
CHAOS_LOGDIR="$OUT/evidence"
LOGDIR=""
TARGET_NODE_ID=""
TARGET_NODE_HOST=""
RESTORE_NEEDED=false
RESTORE_FAILED=0
ON_ARGV=()
DOCKER_CALLS="$OUT/docker-calls.txt"
TIMEOUT_CALLS="$OUT/timeout-calls.txt"
: > "$DOCKER_CALLS"
: > "$TIMEOUT_CALLS"
mkdir -p "$CHAOS_LOGDIR"
source "__FUNCS__"
# No real cluster and no real sleeping: a short real sleep keeps the deadline
# loops from busy-spinning on a frozen clock.
sleep() { command sleep 0.1; }
timeout() {
  printf '%s\n' "$@" >> "$TIMEOUT_CALLS"
  return "${TIMEOUT_RC:-0}"
}
docker() {
  printf '%s\n' "$*" >> "$DOCKER_CALLS"
  case "${1:-}" in
    info) printf '%s\n' "${DOCKER_LOCAL_STATE:-active}"; return "${DOCKER_INFO_RC:-0}" ;;
    node)
      case "${2:-}" in
        ls)
          case "$*" in
            *".ManagerStatus"*) cat "$OUT/node-rows.txt" ;;
            *) cat "$OUT/node-avail.txt" ;;
          esac ;;
        inspect)
          case "$*" in
            *".Spec.Role"*) printf '%s\n' "${DOCKER_ROLE:-worker}" ;;
            *) printf '%s\n' "${DOCKER_NODE_STATE:-active ready}" ;;
          esac ;;
        update)
          case "$*" in
            *drain*) : > "$OUT/drained.marker" ;;
          esac
          return "${DOCKER_NODE_UPDATE_RC:-0}" ;;
      esac ;;
    service)
      case "${2:-}" in
        ls)
          case "$*" in
            *".Replicas"*) cat "$OUT/service-ls-replicas.txt" ;;
            *) cat "$OUT/service-ls.txt" ;;
          esac ;;
        inspect)
          case "$*" in
            *".Spec.Mode.Replicated.Replicas"*) printf '%s\n' "${DOCKER_DESIRED:-2}" ;;
            *) cat "$OUT/svc-mode-${5:-}" 2>/dev/null || printf '%s\n' "${DOCKER_MODE:-replicated}" ;;
          esac ;;
        ps)
          name="${3:-}"
          if [[ "$*" == *".ID"* ]]; then
            if [ -f "$OUT/drained.marker" ] && [ -f "$OUT/ps-$name.after" ]; then
              cat "$OUT/ps-$name.after"
            else
              cat "$OUT/ps-$name" 2>/dev/null || true
            fi
          else
            cat "$OUT/ps-nodes-$name" 2>/dev/null || true
          fi ;;
      esac ;;
  esac
  return 0
}
"""


class VMLossTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp(prefix="w27-vmloss-")
        self.tmp = Path(self._tmp)
        self.out = self.tmp / "sandbox"
        self.out.mkdir()
        self.funcs = self.tmp / "funcs.sh"
        self.funcs.write_text(extract_functions(SCRIPT.read_text()))
        check = subprocess.run(["bash", "-n", str(self.funcs)], capture_output=True, text=True)
        assert check.returncode == 0, check.stderr
        self.write("node-rows.txt", "m1 mgr1 Active Ready Leader\nw1 node1 Active Ready\n")
        self.write("node-avail.txt", "Active Ready\nActive Ready\n")
        self.write("service-ls.txt", "svc-local\n")
        self.write("service-ls-replicas.txt", "svc-local 2/2\n")
        self.write("svc-mode-svc-local", "replicated\n")
        self.write("ps-svc-local", "t1 node1 Running 5 seconds ago\n")
        self.write("ps-nodes-svc-local", "node1 Running 5 seconds ago\n")
        self.write("ps-svc-local.after", "t1 node1 Shutdown 1 second ago\nt2 node2 Running 0 seconds ago\n")

    def tearDown(self) -> None:
        subprocess.run(["rm", "-rf", self._tmp], check=False)

    def write(self, name: str, text: str) -> Path:
        path = self.out / name
        path.write_text(text)
        return path

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

    def timeout_calls(self) -> str:
        return (self.out / "timeout-calls.txt").read_text()


class ExtractionTest(VMLossTestCase):
    def test_the_harness_extracts_the_whole_drill(self) -> None:
        funcs = self.funcs.read_text()
        for name in ("swarm_active", "count_ready_active", "node_rows", "select_worker",
                     "resolve_node", "mode_is_replicated", "service_replicas",
                     "desired_replicas", "service_running_on_node", "select_service_on_node",
                     "task_rows", "find_rescheduled_task", "probe_order_halt", "run_argv",
                     "wait_node_ready", "restore_node", "on_exit", "main",
                     "require_positive_int"):
            self.assertIn(f"{name}() {{", funcs, f"{name} was not extracted")


class SwarmPreconditionTest(VMLossTestCase):
    def test_swarm_state_comes_from_the_format_not_human_output(self) -> None:  # disc (P6-340)
        r = self.scenario(
            "swarm_active; echo \"active rc=$?\"\n"
            "DOCKER_LOCAL_STATE=inactive swarm_active; echo \"inactive rc=$?\"\n",
            env={"DOCKER_LOCAL_STATE": "active"},
        )
        self.assertIn("active rc=0\n", r.stdout)
        self.assertIn("inactive rc=1\n", r.stdout)
        self.assertIn("info --format {{.Swarm.LocalNodeState}}", self.docker_calls())

    def test_only_active_and_ready_nodes_count(self) -> None:  # disc (P6-341)
        self.write("node-avail.txt", "Active Ready\nDrain Ready\nActive Down\nDown Ready\n")
        r = self.scenario("count_ready_active; echo\n")
        self.assertEqual("1", r.stdout.strip(), r.stderr)

    def test_a_swarm_of_one_ready_node_is_a_skip_not_a_pass(self) -> None:  # disc (P6-341)
        self.write("node-avail.txt", "Active Ready\nDrain Ready\n")
        r = self.scenario("main\n")
        self.assertEqual(3, r.returncode, r.stdout + r.stderr)
        self.assertIn("Active+Ready", r.stderr)
        self.assertNotIn("node update", self.docker_calls())


class NodeSelectionTest(VMLossTestCase):
    def test_node_discovery_never_picks_the_manager(self) -> None:  # disc (P6-002)
        r = self.scenario("select_worker; echo \"rc=$?\"\n")
        self.assertEqual("w1 node1\nrc=0\n", r.stdout, r.stderr)

    def test_a_manager_is_refused_even_without_manager_status(self) -> None:  # disc (P6-002)
        # Defence in depth: the format's ManagerStatus is empty for a node docker
        # reports as a manager, so the role check has to stand on its own.
        self.write("node-rows.txt", "m9 mgr9 Active Ready\n")
        r = self.scenario("select_worker; echo \"rc=$?\"\n", env={"DOCKER_ROLE": "manager"})
        self.assertIn("rc=1\n", r.stdout, r.stderr)

    def test_explicit_targets_are_validated(self) -> None:  # disc (P6-342)
        self.write("node-rows.txt",
                   "m1 mgr1 Active Ready Leader\nw1 node1 Active Ready\nd1 node3 Drain Ready\n")
        manager = self.scenario("resolve_node m1\n")
        self.assertEqual(2, manager.returncode, manager.stdout)
        self.assertIn("refusing", manager.stderr)
        draining = self.scenario("resolve_node d1\n")
        self.assertEqual(2, draining.returncode, draining.stdout)
        self.assertIn("not Active/Ready", draining.stderr)
        missing = self.scenario("resolve_node nope\n")
        self.assertEqual(2, missing.returncode, missing.stdout)
        good = self.scenario("resolve_node node1; echo \"rc=$?\"\n")
        self.assertEqual("w1 node1\nrc=0\n", good.stdout, good.stderr)

    def test_a_refused_target_is_a_config_error_not_a_skip(self) -> None:  # disc (P6-342)
        r = self.scenario("main\n", env={"CHAOS_WORKLOAD_NODE": "m1"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertNotIn("SKIP", r.stderr)


class ServiceSelectionTest(VMLossTestCase):
    def test_only_a_replicated_service_placed_on_the_node_is_eligible(self) -> None:  # disc (P6-053)
        self.write("service-ls.txt", "svc-global\nsvc-elsewhere\nsvc-local\n")
        self.write("svc-mode-svc-global", "global\n")
        self.write("svc-mode-svc-elsewhere", "replicated\n")
        self.write("svc-mode-svc-local", "replicated\n")
        self.write("ps-nodes-svc-global", "node1 Running 5 seconds ago\n")
        self.write("ps-nodes-svc-elsewhere", "node9 Running 5 seconds ago\n")
        self.write("ps-nodes-svc-local", "node1 Running 5 seconds ago\n")
        r = self.scenario("select_service_on_node node1; echo \"rc=$?\"\n")
        self.assertEqual("svc-local\nrc=0\n", r.stdout, r.stderr)

    def test_an_explicit_service_without_a_task_there_is_refused(self) -> None:  # disc (P6-053)
        self.write("service-ls.txt", "svc-elsewhere\n")
        self.write("svc-mode-svc-elsewhere", "replicated\n")
        self.write("ps-nodes-svc-elsewhere", "node9 Running 5 seconds ago\n")
        r = self.scenario("main\n", env={"CHAOS_SERVICE": "svc-elsewhere"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("no Running task on node", r.stderr)

    def test_a_global_service_is_refused(self) -> None:  # disc (P6-053)
        self.write("service-ls.txt", "svc-global\n")
        self.write("svc-mode-svc-global", "global\n")
        r = self.scenario("main\n", env={"CHAOS_SERVICE": "svc-global"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("not replicated", r.stderr)


class RescheduleTest(VMLossTestCase):
    def test_a_task_that_was_already_running_is_not_a_reschedule(self) -> None:  # disc (P6-057)
        baseline = self.write("baseline.txt", "t2 node2 Running 5 seconds ago\n")
        self.write("ps-svc-local", "t2 node2 Running 6 seconds ago\n")
        r = self.scenario(
            f"find_rescheduled_task svc-local node1 {baseline}; echo \"rc=$?\"\n"
        )
        self.assertIn("rc=1\n", r.stdout, r.stderr)

    def test_a_task_still_sitting_on_the_drained_node_is_not_a_reschedule(self) -> None:  # disc (P6-057)
        baseline = self.write("baseline.txt", "t9 node8 Running 5 seconds ago\n")
        self.write("ps-svc-local", "t9 node1 Running 5 seconds ago\n")
        r = self.scenario(
            f"find_rescheduled_task svc-local node1 {baseline}; echo \"rc=$?\"\n"
        )
        self.assertIn("rc=1\n", r.stdout, r.stderr)

    def test_a_new_task_on_a_survivor_is_the_reschedule(self) -> None:  # disc (P6-057)
        baseline = self.write("baseline.txt", "t1 node1 Running 5 seconds ago\n")
        self.write("ps-svc-local", "t1 node1 Shutdown 1 second ago\nt2 node2 Running 0 seconds ago\n")
        r = self.scenario(
            f"find_rescheduled_task svc-local node1 {baseline}; echo \"rc=$?\"\n"
        )
        self.assertEqual("t2 node2\nrc=0\n", r.stdout, r.stderr)

    def test_late_recovery_fails_closed(self) -> None:  # disc (P6-057)
        # The service reports a task but it was already there before the drain:
        # no new task on a survivor, so the drill must not claim a reschedule.
        self.write("ps-svc-local", "t2 node2 Running 5 seconds ago\n")
        self.write("ps-svc-local.after", "t2 node2 Running 5 seconds ago\n")
        r = self.scenario("main\n", env={"CHAOS_SERVICE": "svc-local"})
        self.assertEqual(1, r.returncode, r.stdout + r.stderr)
        self.assertIn("no NEW task Running on a surviving node", r.stderr)
        self.assertIn("active", self.docker_calls(), "the node was not restored after the FAIL")


class OrderHaltProbeTest(VMLossTestCase):
    def test_a_stuck_port_is_a_failure_not_a_fallthrough(self) -> None:  # disc (P6-056)
        # TIMEOUT_RC=0 means the TCP connect kept succeeding: traffic is up.
        r = self.scenario(
            "probe_order_halt; echo \"rc=$?\"\n",
            env={"CHAOS_ORDER_PROBE_TCP": "localhost:8080", "TIMEOUT_RC": 0},
        )
        self.assertIn("rc=1\n", r.stdout, r.stderr)

    def test_a_refused_port_is_a_halt(self) -> None:  # disc (P6-056)
        r = self.scenario(
            "probe_order_halt; echo \"rc=$?\"\n",
            env={"CHAOS_ORDER_PROBE_TCP": "localhost:8080", "TIMEOUT_RC": 1},
        )
        self.assertIn("rc=0\n", r.stdout, r.stderr)

    def test_a_malformed_address_is_a_usage_error(self) -> None:  # disc (P6-056)
        for bad in ("localhost", "localhost:notaport", "hos t:80", "host:99999x"):
            r = self.scenario(
                "probe_order_halt; echo \"rc=$?\"\n",
                env={"CHAOS_ORDER_PROBE_TCP": bad, "TIMEOUT_RC": 1},
            )
            self.assertIn("rc=2\n", r.stdout, f"{bad!r} was accepted: {r.stdout}{r.stderr}")

    def test_the_address_travels_as_argv_not_as_shell_source(self) -> None:  # disc (P6-056)
        r = self.scenario(
            "probe_order_halt; echo \"rc=$?\"\n",
            env={"CHAOS_ORDER_PROBE_TCP": "localhost:8080", "TIMEOUT_RC": 1},
        )
        self.assertIn("rc=0\n", r.stdout, r.stderr)
        recorded = self.timeout_calls()
        self.assertIn("localhost", recorded)
        self.assertNotIn("localhost:8080", recorded, "the address was interpolated into one shell word")


class ArgvExecutionTest(VMLossTestCase):
    def test_the_off_command_runs_without_a_shell(self) -> None:  # disc (P6-055)
        r = self.scenario(
            "run_argv label printf\n",
            env={"CHAOS_VM_OFF_CMD": "", "DOCKER_LOCAL_STATE": "active"},
        )
        self.assertEqual(0, r.returncode, r.stderr)
        r2 = self.scenario(
            "run_argv label echo 'a; touch ${OUT}/pwned' | true; echo \"rc=$?\"\n"
        )
        self.assertFalse((self.out / "pwned").exists(), "the payload was parsed as shell code")
        self.assertIn("rc=0\n", r2.stdout)

    def test_a_semicolon_payload_stays_one_argument(self) -> None:  # disc (P6-055)
        r = self.scenario("run_argv label echo 'x; rm -rf /'\n")
        self.assertEqual(0, r.returncode, r.stderr)
        lines = [ln for ln in self.timeout_calls().splitlines() if "rm -rf" in ln]
        self.assertEqual(1, len(lines), self.timeout_calls())
        self.assertEqual("x; rm -rf /", lines[0], "the payload was split or rewritten")


class RestoreTest(VMLossTestCase):
    def test_restore_verifies_the_node_came_back(self) -> None:  # disc (P6-343)
        r = self.scenario(
            "TARGET_NODE_ID=w1\nRESTORE_NEEDED=true\n"
            "restore_node; echo \"rc=$? needed=$RESTORE_NEEDED failed=$RESTORE_FAILED\"\n"
        )
        self.assertIn("rc=0 needed=false failed=0\n", r.stdout, r.stderr)
        self.assertIn("node update --availability active w1", self.docker_calls())

    def test_a_node_that_stays_drained_is_a_failure(self) -> None:  # disc (P6-343)
        r = self.scenario(
            "TARGET_NODE_ID=w1\nRESTORE_NEEDED=true\n"
            "restore_node; echo \"rc=$? failed=$RESTORE_FAILED\"\n",
            env={"DOCKER_NODE_STATE": "drain ready"},
        )
        self.assertIn("rc=1 failed=1\n", r.stdout, r.stderr)
        self.assertIn("did NOT return to Active+Ready", r.stderr)

    def test_a_failed_restore_turns_a_passing_drill_into_a_failure(self) -> None:  # disc (P6-343)
        # on_exit() is what the EXIT trap runs: a restore that did not land must
        # not leave the exit status at 0.
        r = self.scenario(
            "TARGET_NODE_ID=w1\nRESTORE_NEEDED=true\non_exit\n",
            env={"DOCKER_NODE_STATE": "drain ready"},
        )
        self.assertEqual(1, r.returncode, r.stdout + r.stderr)

    def test_restore_runs_once(self) -> None:  # disc (P6-343)
        r = self.scenario(
            "TARGET_NODE_ID=w1\nRESTORE_NEEDED=true\nrestore_node; restore_node; echo done\n"
        )
        self.assertEqual(1, self.docker_calls().count("node update --availability active w1"),
                         self.docker_calls())
        self.assertIn("done", r.stdout)

    def test_the_trap_is_armed_before_the_node_is_touched(self) -> None:  # disc (P6-343)
        text = SCRIPT.read_text()
        armed = text.index("RESTORE_NEEDED=true\n  trap on_exit EXIT")
        drained = text.index("--availability drain")
        self.assertLess(armed, drained, "the restore trap is armed too late to cover the drain")

    def test_the_trap_expands_the_node_now_not_at_trap_time(self) -> None:  # disc (P6-720)
        text = SCRIPT.read_text()
        self.assertNotIn("trap 'docker node update", text)
        self.assertIn("trap on_exit EXIT", text)


class PoweroffModeTest(VMLossTestCase):
    def test_poweroff_requires_a_rejoin_command(self) -> None:  # disc (P6-054)
        r = self.scenario(
            "main\n",
            env={"CHAOS_VM_OFF_MODE": "poweroff", "CHAOS_VM_OFF_CMD": "ssh host -- poweroff"},
        )
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("CHAOS_VM_ON_CMD", r.stderr)
        self.assertNotIn("node update", self.docker_calls())

    def test_an_unknown_mode_is_a_usage_error(self) -> None:  # disc
        r = self.scenario("main\n", env={"CHAOS_VM_OFF_MODE": "explode"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("unknown CHAOS_VM_OFF_MODE", r.stderr)


class HappyPathTest(VMLossTestCase):
    def test_the_drill_drains_reschedules_and_restores(self) -> None:
        r = self.scenario("main\n", env={"CHAOS_SERVICE": "svc-local"})
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertIn("PASS — VM loss", r.stdout)
        calls = self.docker_calls()
        self.assertIn("node update --availability drain w1", calls)
        self.assertIn("node update --availability active w1", calls)
        evidence = Path(os.environ.get("CHAOS_LOGDIR", str(self.out / "evidence")))
        self.assertTrue((evidence / "task-baseline.txt").is_file())
        self.assertTrue((evidence / "recovery.txt").is_file())
        self.assertIn("rescheduled_task=t2", (evidence / "recovery.txt").read_text())

    def test_an_unprobed_halt_leg_is_named_not_claimed(self) -> None:  # disc (P6-056)
        r = self.scenario("main\n", env={"CHAOS_SERVICE": "svc-local"})
        self.assertEqual(0, r.returncode, r.stdout + r.stderr)
        self.assertIn("NOT probed", r.stderr)
        evidence = self.out / "evidence"
        self.assertIn("not_probed", (evidence / "order-halt.txt").read_text())


class KnobValidationTest(VMLossTestCase):
    def test_a_numeric_knob_typo_is_a_usage_error(self) -> None:  # disc
        r = self.scenario("main\n", env={"CHAOS_RECOVERY_SLO_S": "soon"})
        self.assertEqual(2, r.returncode, r.stdout + r.stderr)
        self.assertIn("is not a positive integer", r.stderr)


if __name__ == "__main__":
    unittest.main()
