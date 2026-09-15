"""Wave-28 regression tests for soak-reconnect-loop.sh (P6-201..205, 548, 549).

Function-level suite: the top-level function definitions are extracted (brace-matched
on a lone `}` in column 0), sourced in a sandbox with the script's own config seams,
and every external command (`ps`, `kill`, `sleep`, `date`, `docker`) is a recording
stub.  `PROC_ROOT` is a fixture tree standing in for /proc and the journal is a real
file, so the readers under test are the real ones.  Nothing here kills a process or
touches the dev stack.

Red leg: every assertion marked `# disc` fails against the pre-wave-28 copy of the
script (see the CHG-168 record for the measured counts).
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "soak-reconnect-loop.sh"

JAVA = "com.trading.ingestion.IngestionService"
BRIDGE = "arrow-bridge"
SRC_WITH_BUDGET = "public class IngestionService {\n  private static final int MAX_BRIDGE_RESTARTS = 1;\n}\n"


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


PRELUDE = r'''
set -uo pipefail
OUT="__OUT__"
LOG_FILE="$OUT/ingestion.json"
OUT_DIR="$OUT/soak"
JAVA_MATCH="__JAVA__"
BRIDGE_MATCH="__BRIDGE__"
INGESTION_SRC="${INGESTION_SRC:-$OUT/IngestionService.java}"
PROC_ROOT="$OUT/proc"
CONTAINER="${CONTAINER:-}"
CYCLES="__CYCLES__"
SETTLE_SEC="__SETTLE__"
LEAK_FD_MARGIN_PCT="${LEAK_FD_MARGIN_PCT:-50}"
LEAK_THREAD_MARGIN="${LEAK_THREAD_MARGIN:-20}"
RESTART_BUDGET="${RESTART_BUDGET:-1}"
PROGRESS_PATTERN="bridge lifecycle event="
PS_OUT="$OUT/ps.txt"
PS_PID_OUT="$OUT/ps-pid.txt"
DOCKER_CALLS="$OUT/docker-calls.txt"
KILL_CALLS="$OUT/kill-calls.txt"
SLEEP_CALLS="$OUT/sleep-calls.txt"
: > "$DOCKER_CALLS"
: > "$KILL_CALLS"
: > "$SLEEP_CALLS"
NS() { if [ -n "$CONTAINER" ]; then docker exec "$CONTAINER" "$@"; else "$@"; fi; }
source "__FUNCS__"
ps() {
  case " $* " in
    *" -p "*) cat "${PS_PID_OUT:-/dev/null}" ;;
    *) cat "${PS_OUT:-/dev/null}" ;;
  esac
}
# kill: records the call, then runs the scenario's transition hook (a restart looks
# like new pids plus a fresh lifecycle event in the journal).
kill() {
  printf '%s\n' "$*" >> "$KILL_CALLS"
  if [ -n "${KILL_HOOK:-}" ]; then OUT="$OUT" PS_OUT="$PS_OUT" LOG_FILE="$LOG_FILE" bash "$KILL_HOOK"; fi
  return 0
}
sleep() {
  printf '%s\n' "$*" >> "$SLEEP_CALLS"
  if [ -n "${SLEEP_HOOK:-}" ]; then OUT="$OUT" PS_OUT="$PS_OUT" LOG_FILE="$LOG_FILE" bash "$SLEEP_HOOK"; fi
}
docker() {
  printf '%s\n' "$*" >> "$DOCKER_CALLS"
  case "${1:-}" in
    exec)
      case " $* " in *" grep -Fc "*) echo "${DOCKER_GREP_OUT:-0}" ;; esac
      ;;
    inspect) return "${DOCKER_INSPECT_RC:-0}" ;;
  esac
}
date() {
  case "${1:-}" in
    +%s)
      local n=0
      [ -f "$OUT/date.n" ] && n="$(cat "$OUT/date.n")"
      echo $((1000 + n))
      echo $((n + 1)) > "$OUT/date.n"
      ;;
    *) echo "2026-09-15 12:00:00" ;;
  esac
}
'''

RESTART_HOOK = r'''
cp "$OUT/ps-after.txt" "$PS_OUT"
printf '%s\n' '{"msg":"bridge lifecycle event=subscription_ack"}' >> "$LOG_FILE"
'''


class Sandbox(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w28rec-")
        self.tmp = Path(self._tmp.name)
        (self.tmp / "soak").mkdir()
        (self.tmp / "proc").mkdir()
        self.funcs = self.tmp / "funcs.sh"
        self.funcs.write_text(extract_functions(SCRIPT.read_text()))
        self.src = self.tmp / "IngestionService.java"
        self.src.write_text(SRC_WITH_BUDGET)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # ── fixture helpers ──────────────────────────────────────────────────────
    def proc_entry(self, pid: int, fds: int, threads: int) -> None:
        (self.tmp / "proc" / str(pid) / "fd").mkdir(parents=True, exist_ok=True)
        for i in range(fds):
            (self.tmp / "proc" / str(pid) / "fd" / str(i)).write_text("")
        (self.tmp / "proc" / str(pid) / "status").write_text(f"Name:\tjava\nThreads:\t{threads}\n")

    def journal(self, lines: int = 1) -> Path:
        p = self.tmp / "ingestion.json"
        p.write_text('{"msg":"bridge lifecycle event=subscription_ack"}\n' * lines)
        return p

    def bridge_row(self, pid: int, ppid: int) -> str:
        return f"{pid} {ppid} /usr/bin/arrow-bridge --feed x"

    def healthy(self, after_fds: int = 10, after_threads: int = 50) -> None:
        """A live ingestion process and its bridge child, plus the post-restart state."""
        self.journal(1)
        (self.tmp / "ps.txt").write_text(
            f"100 500 java -cp x {JAVA}\n555 400 /usr/bin/arrow-bridge --feed x\n"
        )
        (self.tmp / "ps-pid.txt").write_text(self.bridge_row(555, 100) + "\n")
        (self.tmp / "ps-after.txt").write_text(
            f"200 5 java -cp x {JAVA}\n556 3 /usr/bin/arrow-bridge --feed x\n"
        )
        self.proc_entry(100, 10, 50)
        self.proc_entry(555, 6, 4)
        self.proc_entry(200, after_fds, after_threads)
        self.proc_entry(556, 6, 4)

    def hook(self, name: str, body: str) -> str:
        p = self.tmp / name
        p.write_text(body)
        return str(p)

    def run_sbx(
        self,
        body: str,
        cycles: str = "1",
        settle: str = "5",
        env: dict[str, str] | None = None,
        timeout: int = 90,
    ) -> subprocess.CompletedProcess:
        script = self.tmp / "scenario.sh"
        prelude = (
            PRELUDE.replace("__OUT__", str(self.tmp))
            .replace("__JAVA__", JAVA)
            .replace("__BRIDGE__", BRIDGE)
            .replace("__CYCLES__", cycles)
            .replace("__SETTLE__", settle)
            .replace("__FUNCS__", str(self.funcs))
        )
        script.write_text(prelude + "\n" + body + "\n")
        return subprocess.run(
            ["bash", str(script)],
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, **(env or {})},
            cwd=str(self.tmp),
        )

    def result_rows(self) -> list[list[str]]:
        files = sorted((self.tmp / "soak").glob("reconnect-leak-*.tsv"))
        self.assertTrue(files, "no result TSV was written")
        lines = files[-1].read_text().strip().split("\n")
        return [ln.split("\t") for ln in lines]

    def kills(self) -> list[str]:
        txt = (self.tmp / "kill-calls.txt").read_text().strip()
        return txt.split("\n") if txt else []


# ── composition ──────────────────────────────────────────────────────────────
class ExtractionTest(Sandbox):
    def test_the_functions_are_top_level_definitions(self) -> None:  # disc
        r = self.run_sbx(
            'for f in validate_args resolve_restart_budget find_pid count_fds threads_of '
            'count_progress bridge_pid_ok kill_bridge wait_recovery main; '
            'do printf "%s=%s\\n" "$f" "$(type -t "$f")"; done'
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        for line in r.stdout.strip().split("\n"):
            self.assertEqual(line.split("=", 1)[1], "function", line)


# ── P6-549: knob validation ──────────────────────────────────────────────────
class ArgsTest(Sandbox):
    def test_zero_cycles_is_refused(self) -> None:  # disc
        r = self.run_sbx("validate_args; echo ok", cycles="0")
        self.assertEqual(r.returncode, 2)
        self.assertIn("CYCLES must be >= 1", r.stderr)

    def test_a_non_numeric_cycle_count_is_refused(self) -> None:  # disc
        r = self.run_sbx("validate_args; echo ok", cycles="two")
        self.assertEqual(r.returncode, 2)
        self.assertIn("CYCLES='two' is not a non-negative integer", r.stderr)

    def test_zero_settle_is_refused(self) -> None:  # disc
        r = self.run_sbx("validate_args; echo ok", settle="0")
        self.assertEqual(r.returncode, 2)
        self.assertIn("SETTLE_SEC must be >= 1 second", r.stderr)

    def test_a_non_numeric_settle_is_refused(self) -> None:  # disc
        r = self.run_sbx("validate_args; echo ok", settle="8s")
        self.assertEqual(r.returncode, 2)
        self.assertIn("SETTLE_SEC='8s' is not a non-negative integer", r.stderr)

    def test_a_non_numeric_leak_margin_is_refused(self) -> None:  # disc
        r = self.run_sbx(
            "validate_args; echo ok", env={"LEAK_FD_MARGIN_PCT": "half"}
        )
        self.assertEqual(r.returncode, 2)
        self.assertIn("LEAK_FD_MARGIN_PCT='half'", r.stderr)

    def test_cycles_above_the_service_budget_are_refused(self) -> None:  # disc
        r = self.run_sbx("validate_args; echo ok", cycles="2")
        self.assertEqual(r.returncode, 2)
        self.assertIn("restarts the bridge at most 1 time(s)", r.stderr)


# ── P6-548: the restart budget must come from the service source, loudly ─────
class BudgetTest(Sandbox):
    def test_the_budget_is_read_from_the_service_source(self) -> None:  # disc
        self.src.write_text("int MAX_BRIDGE_RESTARTS = 3;\n")
        r = self.run_sbx('resolve_restart_budget; echo "rc=$?"')
        self.assertEqual(r.stdout.strip(), "3\nrc=0")

    def test_a_missing_constant_warns_instead_of_defaulting_silently(self) -> None:  # disc
        self.src.write_text("int SOMETHING_ELSE = 1;\n")
        r = self.run_sbx('resolve_restart_budget; echo "rc=$?"')
        self.assertEqual(r.stdout.strip(), "1\nrc=0")
        self.assertIn("WARN — MAX_BRIDGE_RESTARTS not found in", r.stderr)

    def test_an_unreadable_source_still_yields_a_usable_budget(self) -> None:  # disc
        r = self.run_sbx(
            'resolve_restart_budget; echo "rc=$?"',
            env={"INGESTION_SRC": str(self.tmp / "does-not-exist.java")},
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "1\nrc=0")
        self.assertIn("WARN", r.stderr)


# ── P6-201: newest process, not the oldest ───────────────────────────────────
class FindPidTest(Sandbox):
    def test_the_newest_pid_wins_not_the_oldest(self) -> None:  # disc
        (self.tmp / "ps.txt").write_text(
            f"101 900 java -cp x {JAVA}\n102 10 java -cp x {JAVA}\n"
        )
        r = self.run_sbx(f'find_pid "{JAVA}"')
        self.assertEqual(r.stdout, "102\n", "sort|tail -1 picked the OLDEST process")

    def test_no_match_prints_nothing(self) -> None:
        (self.tmp / "ps.txt").write_text("101 900 java -cp x other.Service\n")
        r = self.run_sbx(f'find_pid "{JAVA}"')
        self.assertEqual(r.stdout, "")


# ── P6-202: the progress count must be taken where the journal lives ─────────
class ProgressTest(Sandbox):
    def test_host_mode_counts_the_real_journal(self) -> None:
        self.journal(3)
        r = self.run_sbx("count_progress")
        self.assertEqual(r.stdout, "3\n")

    def test_container_mode_counts_inside_the_container(self) -> None:  # disc
        self.journal(0)
        r = self.run_sbx(
            "count_progress",
            env={"CONTAINER": "ingestion-c", "DOCKER_GREP_OUT": "7"},
        )
        self.assertEqual(r.stdout, "7\n", "host-path grep cannot see the container journal")
        self.assertIn("exec ingestion-c grep -Fc bridge lifecycle event= ", 
                      (self.tmp / "docker-calls.txt").read_text())


# ── P6-203: verify the SIGKILL target ────────────────────────────────────────
class KillVerifyTest(Sandbox):
    def test_a_verified_bridge_child_is_killed(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx('kill_bridge 555 100; echo "rc=$?"')
        self.assertEqual(r.stdout, "rc=0\n")
        self.assertEqual(self.kills(), ["-9 555"])

    def test_a_pid_that_no_longer_matches_the_bridge_is_refused(self) -> None:  # disc
        self.healthy()
        (self.tmp / "ps-pid.txt").write_text("555 100 /usr/bin/unrelated-daemon\n")
        r = self.run_sbx('kill_bridge 555 100; echo "rc=$?"')
        self.assertEqual(r.stdout, "rc=1\n")
        self.assertIn("refusing to SIGKILL", r.stderr)
        self.assertEqual(self.kills(), [])

    def test_a_pid_whose_parent_is_not_java_is_refused(self) -> None:  # disc
        self.healthy()
        (self.tmp / "ps-pid.txt").write_text(self.bridge_row(555, 777) + "\n")
        r = self.run_sbx('kill_bridge 555 100; echo "rc=$?"')
        self.assertEqual(r.stdout, "rc=1\n")
        self.assertEqual(self.kills(), [])

    def test_a_vanished_pid_is_refused_rather_than_blindly_killed(self) -> None:  # disc
        self.healthy()
        (self.tmp / "ps-pid.txt").write_text("")
        r = self.run_sbx('kill_bridge 555 100; echo "rc=$?"')
        self.assertEqual(r.stdout, "rc=1\n")
        self.assertEqual(self.kills(), [])

    def test_a_missing_bridge_pid_fails_the_cycle(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx('kill_bridge "" 100; echo "rc=$?"')
        self.assertEqual(r.stdout, "rc=1\n")
        self.assertIn("cannot force the disconnect", r.stderr)
        self.assertEqual(self.kills(), [])


# ── P6-204: poll for recovery instead of sleeping a fixed time ───────────────
class RecoveryTest(Sandbox):
    def test_recovery_is_polled_until_the_journal_advances(self) -> None:  # disc
        self.healthy()
        self.journal(1)
        append = self.hook(
            "later.sh",
            'printf \'%s\\n\' \'{"msg":"bridge lifecycle event=subscription_ack"}\' >> "$LOG_FILE"\n',
        )
        r = self.run_sbx(
            'wait_recovery 1; echo "rc=$?"', settle="5", env={"SLEEP_HOOK": append}
        )
        self.assertEqual(r.stdout.strip().split("\n")[-1], "rc=0")
        fields = r.stdout.strip().split("\n")[0].split()
        self.assertEqual(len(fields), 3, r.stdout)
        # No restart hook here: the pids are the pre-existing ones, and the point is that
        # the poll kept going until the journal advanced (t0 was 1).
        self.assertEqual((fields[0], fields[1]), ("100", "555"))
        self.assertGreaterEqual(int(fields[2]), 2)

    def test_the_settle_budget_is_a_deadline_not_a_floor(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx('wait_recovery 1; echo "rc=$?"', settle="2")
        self.assertEqual(r.stdout.strip().split("\n")[-1], "rc=0")
        self.assertEqual(len(r.stdout.strip().split("\n")[0].split()), 3)

    def test_a_recovered_cycle_is_reported_as_recovered(self) -> None:  # disc
        self.healthy()
        hook = self.hook("restart.sh", RESTART_HOOK)
        r = self.run_sbx("main", env={"KILL_HOOK": hook})
        self.assertEqual(r.returncode, 0, r.stderr)
        row = self.result_rows()[1]
        self.assertEqual(row[8], "1", "recovered_ok")
        self.assertEqual(row[7], "1", "progress_delta")

    def test_a_cycle_that_never_recovers_fails_the_run(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx("main", env={"FAIL_JSON": "1"})  # no kill hook: no restart
        self.assertEqual(r.returncode, 1)
        self.assertIn("NOT RECOVERED", r.stdout)
        self.assertEqual(self.result_rows()[1][8], "0")


# ── P6-205: the leak gate needs a healthy PRE-KILL baseline ──────────────────
class LeakBaselineTest(Sandbox):
    def test_the_first_cycle_compares_against_the_pre_kill_baseline(self) -> None:  # disc
        self.healthy(after_fds=20, after_threads=50)  # 20 > 10 * 1.5
        hook = self.hook("restart.sh", RESTART_HOOK)
        r = self.run_sbx("main", env={"KILL_HOOK": hook})
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("LEAK DETECTED", r.stdout)
        row = self.result_rows()[1]
        self.assertEqual(row[1], "10", "java_fds_before must be the pre-kill value")
        self.assertEqual(row[2], "20", "java_fds_after")
        self.assertEqual(row[9], "0", "leak_ok")

    def test_a_thread_leak_is_caught_too(self) -> None:  # disc
        self.healthy(after_fds=10, after_threads=120)  # 120 > 50 + 20
        hook = self.hook("restart.sh", RESTART_HOOK)
        r = self.run_sbx("main", env={"KILL_HOOK": hook})
        self.assertEqual(r.returncode, 1)
        self.assertEqual(self.result_rows()[1][9], "0")

    def test_a_clean_restart_passes_the_gate(self) -> None:  # disc
        self.healthy(after_fds=11, after_threads=52)
        hook = self.hook("restart.sh", RESTART_HOOK)
        r = self.run_sbx("main", env={"KILL_HOOK": hook})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        row = self.result_rows()[1]
        self.assertEqual((row[8], row[9]), ("1", "1"))
        self.assertIn("recovered cleanly with no leak signal", r.stdout)

    def test_an_unreadable_baseline_is_fatal_rather_than_vacuous(self) -> None:  # disc
        self.healthy()
        for i in list((self.tmp / "proc" / "100" / "fd").iterdir()):
            i.unlink()
        r = self.run_sbx("main")
        self.assertEqual(r.returncode, 1)
        self.assertIn("cannot read the Java baseline", r.stderr)


# ── contract: CLI/results shape the caller relies on ─────────────────────────
class ContractTest(Sandbox):
    def test_the_result_columns_stay_stable(self) -> None:  # disc
        self.healthy()
        hook = self.hook("restart.sh", RESTART_HOOK)
        self.run_sbx("main", env={"KILL_HOOK": hook})
        self.assertEqual(
            self.result_rows()[0],
            [
                "cycle",
                "java_fds_before",
                "java_fds_after",
                "bridge_fds_before",
                "bridge_fds_after",
                "java_threads_before",
                "java_threads_after",
                "progress_delta",
                "recovered_ok",
                "leak_ok",
            ],
        )
        self.assertEqual(len(self.result_rows()[1]), 10)

    def test_a_missing_journal_is_fatal(self) -> None:  # disc
        self.healthy()
        (self.tmp / "ingestion.json").unlink()
        r = self.run_sbx("main")
        self.assertEqual(r.returncode, 1)
        self.assertIn("journal not found at", r.stderr)

    def test_a_missing_java_process_is_fatal(self) -> None:  # disc
        self.healthy()
        (self.tmp / "ps.txt").write_text("555 400 /usr/bin/arrow-bridge --feed x\n")
        r = self.run_sbx("main")
        self.assertEqual(r.returncode, 1)
        self.assertIn("Java ingestion service not running", r.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
