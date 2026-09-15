"""Wave-28 regression tests for soak-monitor.sh (P6-018, 196-199, 545, 546).

The monitor is a shell script whose behaviour lives in its functions, so this suite
extracts the top-level function definitions (brace-matched on a lone `}` in column 0)
into a sandbox, sources it, and replaces its external commands (`ps`, `sleep`, `date`,
and a recording `tail`) with stubs.  The journal is a REAL file, so `tail`/`wc -c`/
`stat` are exercised for real; `PROC_ROOT` points at a fixture tree standing in for
/proc.  Nothing here touches the dev stack.

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

SCRIPT = Path(__file__).resolve().parents[1] / "soak-monitor.sh"

JAVA = "com.trading.ingestion.IngestionService"
BRIDGE = "arrow-bridge"


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
PROC_ROOT="$OUT/proc"
DURATION_SEC="__DURATION__"
INTERVAL_SEC="__INTERVAL__"
PS_OUT="$OUT/ps.txt"
TAIL_CALLS="$OUT/tail-calls.txt"
SLEEP_CALLS="$OUT/sleep-calls.txt"
: > "$TAIL_CALLS"
: > "$SLEEP_CALLS"
source "__FUNCS__"
# ps: static fixture list (the monitor always calls `ps -eo pid,etimes,args`).
ps() { cat "${PS_OUT:-/dev/null}"; }
# sleep: records the call and may mutate the journal, standing in for whatever the
# appender does while the monitor is asleep.
sleep() {
  printf '%s\n' "$*" >> "$SLEEP_CALLS"
  if [ -n "${SLEEP_APPEND:-}" ]; then printf '%s\n' "$SLEEP_APPEND" >> "$LOG_FILE"; fi
  if [ -n "${SLEEP_HOOK:-}" ]; then LOG_FILE="$LOG_FILE" PS_OUT="$PS_OUT" bash "$SLEEP_HOOK"; fi
}
# tail: records every invocation (so a test can count journal reads) then delegates.
tail() {
  printf '%s\n' "$*" >> "$TAIL_CALLS"
  command tail "$@"
}
# date: deterministic, and each `date +%s` call advances the clock by one second so
# a duration-bounded run terminates without real waiting.
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


class Sandbox(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w28mon-")
        self.tmp = Path(self._tmp.name)
        (self.tmp / "soak").mkdir()
        (self.tmp / "proc").mkdir()
        self.funcs = self.tmp / "funcs.sh"
        self.funcs.write_text(extract_functions(SCRIPT.read_text()))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # ── fixture helpers ──────────────────────────────────────────────────────
    def proc_entry(self, pid: int, fds: int, threads: int) -> None:
        (self.tmp / "proc" / str(pid) / "fd").mkdir(parents=True, exist_ok=True)
        for i in range(fds):
            (self.tmp / "proc" / str(pid) / "fd" / str(i)).write_text("")
        (self.tmp / "proc" / str(pid) / "status").write_text(f"Name:\tjava\nThreads:\t{threads}\n")

    def ps_fixture(self, rows: list[str]) -> None:
        (self.tmp / "ps.txt").write_text("".join(f"{r}\n" for r in rows))

    def journal(self, size: int = 0) -> Path:
        p = self.tmp / "ingestion.json"
        if not p.exists():
            p.write_text("")
        return p

    def run_sbx(
        self,
        body: str,
        duration: str = "0",
        interval: str = "1",
        env: dict[str, str] | None = None,
        timeout: int = 60,
    ) -> subprocess.CompletedProcess:
        script = self.tmp / "scenario.sh"
        prelude = (
            PRELUDE.replace("__OUT__", str(self.tmp))
            .replace("__JAVA__", JAVA)
            .replace("__BRIDGE__", BRIDGE)
            .replace("__DURATION__", duration)
            .replace("__INTERVAL__", interval)
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

    def tsv_rows(self) -> list[list[str]]:
        files = sorted((self.tmp / "soak").glob("soak-summary-*.tsv"))
        self.assertTrue(files, "no TSV was written")
        lines = files[-1].read_text().strip().split("\n")
        return [ln.split("\t") for ln in lines]

    def healthy(self) -> None:
        self.journal(-1)
        self.ps_fixture([f"100 500 java -cp x {JAVA}", "200 400 /usr/bin/arrow-bridge --feed x"])
        self.proc_entry(100, 12, 60)
        self.proc_entry(200, 8, 12)


# ── composition ──────────────────────────────────────────────────────────────
class ExtractionTest(Sandbox):
    def test_the_functions_are_top_level_definitions(self) -> None:  # disc
        r = self.run_sbx(
            'for f in validate_args find_pid count_fds threads_of journal_size '
            'journal_inode count_window main; do printf "%s=%s\\n" "$f" "$(type -t "$f")"; done'
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        for line in r.stdout.strip().split("\n"):
            self.assertEqual(line.split("=", 1)[1], "function", line)


# ── P6-018: process discovery ────────────────────────────────────────────────
class FindPidTest(Sandbox):
    def test_the_full_command_line_is_matched_not_argv_3(self) -> None:  # disc
        self.ps_fixture(["101 900 java -Xmx1g -cp /tmp/x.jar " + JAVA])
        r = self.run_sbx(f'find_pid "{JAVA}"')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout, "101\n")

    def test_the_newest_process_wins_not_the_oldest(self) -> None:  # disc
        self.ps_fixture(
            [
                "101 900 java -cp x " + JAVA,
                "102 10 java -cp x " + JAVA,
            ]
        )
        r = self.run_sbx(f'find_pid "{JAVA}"')
        self.assertEqual(r.stdout, "102\n", "sort|tail -1 picked the OLDEST process")

    def test_the_monitors_own_pid_is_never_selected(self) -> None:  # disc
        # Probed with "java" (argv[0]): the pre-wave-28 `$3 ~ pat` match selects
        # the monitor's own shell, while the new code excludes SELF via $1.
        r = self.run_sbx(
            f'echo "$$ 5 java -cp x {JAVA}" > "$PS_OUT"; find_pid "java"; echo "rc=$?"'
        )
        self.assertEqual(r.stdout.strip(), "rc=0")

    def test_no_match_prints_nothing_not_a_zero(self) -> None:
        self.ps_fixture(["101 900 java -cp x other.Service"])
        r = self.run_sbx(f'find_pid "{JAVA}"')
        self.assertEqual(r.stdout, "")

    def test_the_bridge_is_selected_by_its_own_pattern(self) -> None:  # disc
        # A stale bridge decoy plus the Java line: the pre-wave-28 oldest-pick
        # returns the stale PID, the new code returns the newest bridge.
        self.ps_fixture(
            [
                "100 500 java -cp x " + JAVA,
                "200 900 /usr/bin/arrow-bridge --feed x",
                "201 10 /usr/bin/arrow-bridge --feed x",
            ]
        )
        r = self.run_sbx(f'find_pid "{BRIDGE}"')
        self.assertEqual(r.stdout, "201\n")


# ── P6-545: argument validation ──────────────────────────────────────────────
class ArgsTest(Sandbox):
    def test_a_non_numeric_duration_is_refused(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx("main", duration="soon")
        self.assertEqual(r.returncode, 1)
        self.assertIn("duration 'soon' must be a non-negative integer", r.stderr)

    def test_a_non_numeric_interval_is_refused(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx("main", interval="5s")
        self.assertEqual(r.returncode, 1)
        self.assertIn("interval '5s' must be a positive integer", r.stderr)

    def test_a_zero_interval_is_refused(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx("main", interval="0")
        self.assertEqual(r.returncode, 1)
        self.assertIn("interval must be >= 1 second", r.stderr)

    def test_no_tsv_is_written_for_refused_arguments(self) -> None:  # disc
        self.healthy()
        r = self.run_sbx("main", interval="0")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(list((self.tmp / "soak").glob("*.tsv")), [])


# ── P6-197: the monitor must be able to observe both subjects ────────────────
class StartupTest(Sandbox):
    def test_a_missing_journal_is_fatal(self) -> None:  # disc
        self.ps_fixture([])
        r = self.run_sbx("main", duration="1")
        self.assertEqual(r.returncode, 1)
        self.assertIn("journal not found at", r.stderr)

    def test_a_missing_java_process_is_fatal(self) -> None:  # disc
        self.journal(-1)
        self.ps_fixture(["200 400 /usr/bin/arrow-bridge --feed"])
        r = self.run_sbx("main", duration="1")
        self.assertEqual(r.returncode, 1)
        self.assertIn("no Java ingestion process matching", r.stderr)

    def test_a_missing_bridge_is_fatal_not_a_zero_column(self) -> None:  # disc
        self.journal(-1)
        self.ps_fixture(["100 500 java -cp x " + JAVA])
        self.proc_entry(100, 12, 60)
        r = self.run_sbx("main", duration="1")
        self.assertEqual(r.returncode, 1)
        self.assertIn("no bridge process matching", r.stderr)
        self.assertEqual(list((self.tmp / "soak").glob("*.tsv")), [])

    def test_a_refused_start_writes_no_evidence(self) -> None:  # disc
        self.journal(-1)
        self.ps_fixture(["100 500 java -cp x " + JAVA])
        r = self.run_sbx("main", duration="1")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(list((self.tmp / "soak").glob("*.tsv")), [])


# ── journal window + /proc readers ───────────────────────────────────────────
class JournalTest(Sandbox):
    def test_only_the_window_is_counted(self) -> None:  # disc
        payload = "bridge lifecycle event=subscription_ack\n" * 3
        p = self.journal(0)
        p.write_text(payload)
        offset = len(payload)
        p.write_text(payload + "bridge lifecycle event=reconnect\n")
        r = self.run_sbx(f'count_window {offset} "$(journal_size)"; echo')
        self.assertEqual(r.stdout, "0 1 0 0\n")

    def test_all_four_patterns_are_counted(self) -> None:  # disc
        p = self.journal(0)
        p.write_text(
            "bridge lifecycle event=subscription_ack\n"
            "bridge lifecycle event=reconnect\n"
            "bridge lifecycle event=heartbeat_failed\n"
            "bridge lifecycle event=feed_stalled\n"
        )
        r = self.run_sbx('count_window 0 "$(journal_size)"; echo')
        self.assertEqual(r.stdout, "1 1 1 1\n")

    def test_an_empty_window_counts_nothing_without_reading_the_journal(self) -> None:  # disc
        self.journal(-1)
        r = self.run_sbx('count_window 0 0; echo; wc -l < "$TAIL_CALLS"')
        self.assertEqual(r.stdout, "0 0 0 0\n0\n")

    def test_fds_are_read_from_the_proc_root(self) -> None:  # disc
        self.proc_entry(4242, 5, 7)
        r = self.run_sbx('printf "fds=%s threads=%s\\n" "$(count_fds 4242)" "$(threads_of 4242)"')
        self.assertEqual(r.stdout, "fds=5 threads=7\n")

    def test_a_dead_pid_reads_zero_rather_than_aborting(self) -> None:
        self.proc_entry(1, 3, 3)
        r = self.run_sbx('printf "fds=%s threads=%s\\n" "$(count_fds 999999)" "$(threads_of 999999)"; echo rc=$?')
        self.assertEqual(r.stdout, "fds=0 threads=0\nrc=0\n")


# ── P6-198 / P6-546: the sampling loop ───────────────────────────────────────
class SamplingTest(Sandbox):
    def test_each_row_reports_that_intervals_events(self) -> None:  # disc
        self.healthy()
        append = '{"msg":"bridge lifecycle event=reconnect"}'
        r = self.run_sbx(
            "main", duration="3", interval="1", env={"SLEEP_APPEND": append}
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        rows = self.tsv_rows()
        self.assertEqual(len(rows), 4, rows)  # header + 3 samples
        deltas = [int(row[6]) for row in rows[1:]]  # reconnects column
        self.assertTrue(all(d >= 0 for d in deltas), f"negative delta in {deltas}")
        self.assertEqual(deltas, [0, 1, 1], deltas)

    def test_the_journal_is_read_once_per_interval(self) -> None:  # disc
        self.healthy()
        self.journal(-1).write_text("bridge lifecycle event=subscription_ack\n")
        append = '{"msg":"bridge lifecycle event=reconnect"}'
        r = self.run_sbx("main", duration="3", interval="1", env={"SLEEP_APPEND": append})
        self.assertEqual(r.returncode, 0, r.stderr)
        reads = len((self.tmp / "tail-calls.txt").read_text().strip().split("\n"))
        # 3 samples; interval 1's window is empty (the startup offset skips the existing
        # backlog), so 2 non-empty windows must cost exactly 2 reads — the pre-wave-28
        # code issued four reads of the same window.
        self.assertEqual(len(self.tsv_rows()), 4)
        self.assertEqual(reads, 2, "P6-546: four reads of the same window per sample")

    def test_the_tsv_columns_stay_stable(self) -> None:  # disc
        self.healthy()
        self.run_sbx("main", duration="1", interval="1")
        header = self.tsv_rows()[0]
        self.assertEqual(
            header,
            [
                "ts",
                "java_fds",
                "bridge_fds",
                "java_threads",
                "bridge_os_threads",
                "sub_acks",
                "reconnects",
                "heartbeat_fails",
                "stalls",
            ],
        )
        self.assertEqual(len(self.tsv_rows()[1]), 9)


# ── P6-199: rotation, truncation, disappearance ──────────────────────────────
class RotationTest(Sandbox):
    def _rotate_hook(self, new_text: str) -> str:
        hook = self.tmp / "rotate.sh"
        hook.write_text(
            f'mv "$LOG_FILE" "$LOG_FILE.1"\nprintf %s "{new_text}" > "$LOG_FILE"\n'
        )
        return str(hook)

    def test_a_rotated_journal_is_counted_from_zero(self) -> None:  # disc
        self.healthy()
        hook = self._rotate_hook("bridge lifecycle event=reconnect\\n")
        r = self.run_sbx("main", duration="3", interval="1", env={"SLEEP_HOOK": hook})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("rotated or truncated", r.stdout)
        rows = self.tsv_rows()
        # Every rotation writes a fresh inode, so each rotated file's single reconnect
        # is counted exactly once: no interval is skipped and none is double counted.
        self.assertEqual([int(row[6]) for row in rows[1:]], [0, 1, 1])

    def test_a_truncated_journal_is_counted_from_zero(self) -> None:  # disc
        p = self.healthy() or self.journal(-1)
        p.write_text("bridge lifecycle event=subscription_ack\n" * 4)
        hook = self.tmp / "trunc.sh"
        hook.write_text(
            'printf %s "bridge lifecycle event=heartbeat_failed\\n" > "$LOG_FILE"\n'
        )
        r = self.run_sbx("main", duration="3", interval="1", env={"SLEEP_HOOK": str(hook)})
        self.assertEqual(r.returncode, 0, r.stderr)
        rows = self.tsv_rows()
        self.assertEqual([int(row[7]) for row in rows[1:]], [0, 1, 0])

    def test_a_vanishing_journal_is_fatal_not_silent(self) -> None:  # disc
        self.healthy()
        hook = self.tmp / "rm.sh"
        hook.write_text('rm -f "$LOG_FILE"\n')
        r = self.run_sbx("main", duration="5", interval="1", env={"SLEEP_HOOK": str(hook)})
        self.assertEqual(r.returncode, 1)
        self.assertIn("journal disappeared at", r.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
