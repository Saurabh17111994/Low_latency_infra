"""Wave-29 regression tests for soak-headroom.sh (P6-193..195, 540..544, 784..787).

The script is driven through its real entry point — `bash soak-headroom.sh <journal>`
with the documented OUT_DIR / CAP_TOKENS / SLOT env seams — because that is exactly
how run-full-suite.sh calls it, so these tests pin the caller contract as well as the
behaviour. The journal is a REAL file, `mktemp` and `sort` are the real binaries, and
only three external commands are shimmed through a PATH prefix:

  date  — pinned clock, so two runs land in the same summary timestamp (P6-784)
  grep  — records journal reads and can append one extra ack after the FIRST read,
          which is what makes the snapshot-consistency probe (P6-193) observable
  sort  — records its arguments, which is how the external nearest-rank selection
          (P6-786) is observed instead of grepping the script text

Nothing here touches the dev stack, and no evidence is written into the repo: OUT_DIR
points at the test's own temporary directory.

Red leg: assertions marked `# disc` fail against the pre-wave-29 copy of the script
(see the CHG-169 record for the measured counts). Unmarked probes are documented as
non-discriminating there.
"""

from __future__ import annotations

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "soak-headroom.sh"

ACK = "2026-09-15 10:00:{sec},001 INFO [bridge] com.trading.ingestion.IngestionService - bridge lifecycle event=subscription_ack slot={slot} state={state} epoch={epoch} assigned={assigned} acknowledged={ack} rejected={rej} reason={reason}"


def ack_line(
    slot: str = "hft-0",
    state: str = "ACTIVE",
    assigned: int = 1024,
    ack: int = 900,
    rej: int = 0,
    epoch: int = 1,
    sec: int = 1,
    reason: str = "ok",
) -> str:
    return ACK.format(
        sec=sec, slot=slot, state=state, epoch=epoch, assigned=assigned, ack=ack, rej=rej, reason=reason
    )


class Sandbox(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="w29hr-")
        self.tmp = Path(self._tmp.name)
        (self.tmp / "out").mkdir()
        (self.tmp / "bin").mkdir()
        self.journal_path = self.tmp / "ingestion.json"
        self.reads = self.tmp / "journal-reads.txt"
        self.sorts = self.tmp / "sort-calls.txt"
        self._write_shims()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # ── shims ────────────────────────────────────────────────────────────────
    def _shim(self, name: str, body: str) -> None:
        p = self.tmp / "bin" / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IEXEC)

    def _write_shims(self) -> None:
        self._shim(
            "date",
            "#!/usr/bin/env bash\n"
            "# Pinned clock: two runs in the same 'second' must still not overwrite.\n"
            "printf '%s\\n' '20260915-120000'\n",
        )
        self._shim(
            "sort",
            "#!/usr/bin/env bash\n"
            'printf "%s\\n" "$*" >> "${SORT_CALLS:-/dev/null}"\n'
            'exec /usr/bin/sort "$@"\n',
        )
        self._shim(
            "grep",
            "#!/usr/bin/env bash\n"
            "# A journal read is any grep whose argv names the journal. Each read is\n"
            "# counted; from the SECOND read onward one extra ack is appended first —\n"
            "# the appender/rotation that used to make one report's sections disagree\n"
            "# (P6-193). A single-read script never triggers it.\n"
            "hit=0\n"
            'for a in "$@"; do [ "$a" = "${LOG_FILE:-}" ] && hit=1; done\n'
            'if [ "$hit" = 1 ]; then\n'
            '  printf "%s\\n" "${LOG_FILE:-}" >> "${READS:-/dev/null}"\n'
            '  c=0; [ -f "${READCOUNT:-/dev/null}" ] && c="$(cat "${READCOUNT:-/dev/null}")"\n'
            "  c=$((c + 1))\n"
            '  printf "%s\\n" "$c" > "${READCOUNT:-/dev/null}"\n'
            '  if [ "$c" -ge 2 ] && [ -n "${EXTRA_ACK:-}" ]; then\n'
            '    printf "%s\\n" "$EXTRA_ACK" >> "${LOG_FILE:-}"\n'
            "  fi\n"
            "fi\n"
            'exec /usr/bin/grep "$@"\n',
        )

    # ── fixtures ─────────────────────────────────────────────────────────────
    def journal(self, lines: list[str]) -> Path:
        self.journal_path.write_text("".join(f"{ln}\n" for ln in lines))
        return self.journal_path

    def run_script(
        self,
        journal: Path | None = None,
        env: dict[str, str] | None = None,
        args: list[str] | None = None,
        timeout: int = 120,
    ) -> subprocess.CompletedProcess:
        journal = journal if journal is not None else self.journal_path
        argv = ["bash", str(SCRIPT), str(journal)] if args is None else ["bash", str(SCRIPT), *args]
        full_env = {
            **os.environ,
            "PATH": f"{self.tmp / 'bin'}:{os.environ['PATH']}",
            "OUT_DIR": str(self.tmp / "out"),
            "LOG_FILE": str(journal),
            "CAP_TOKENS": "1024",
            "READS": str(self.reads),
            "SORT_CALLS": str(self.sorts),
            "READCOUNT": str(self.tmp / "read-count.txt"),
            **(env or {}),
        }
        return subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout, env=full_env, cwd=str(self.tmp)
        )

    # ── assertions helpers ───────────────────────────────────────────────────
    def summaries(self) -> list[Path]:
        return sorted((self.tmp / "out").glob("headroom-summary-*.txt"))

    def stat_line(self, out: str) -> str:
        return next(ln for ln in out.split("\n") if ln.strip().startswith("samples="))

    def section(self, out: str, header: str) -> str:
        """Text between `header` and the next '---' separator."""
        lines = out.split("\n")
        i = lines.index(header)
        j = i + 1
        while j < len(lines) and lines[j] != "---":
            j += 1
        return "\n".join(lines[i + 1 : j])

    def reads_of_journal(self) -> int:
        if not self.reads.exists():
            return 0
        return len(self.reads.read_text().split())


# ── contract: the caller's CLI and evidence shape ────────────────────────────
class ContractTest(Sandbox):
    def test_the_positional_journal_argument_is_still_honoured(self) -> None:
        self.journal([ack_line(ack=900)])
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("headroom: scanning", r.stdout)

    def test_a_missing_journal_is_fatal(self) -> None:
        r = self.run_script(journal=self.tmp / "absent.json")
        self.assertEqual(r.returncode, 1)
        self.assertIn("FATAL: no journal at", r.stderr)

    def test_the_section_headers_stay_stable_for_the_caller(self) -> None:
        self.journal([ack_line(ack=900)])
        r = self.run_script()
        for header in (
            "Subscription acks (ACTIVE/PARTIAL/TERMINAL):",
            "Any partial/terminal/rejected events (tightness evidence):",
        ):
            self.assertIn(header, r.stdout)

    def test_the_summary_lands_in_out_dir(self) -> None:
        self.journal([ack_line(ack=900)])
        self.run_script()
        files = self.summaries()
        self.assertEqual(len(files), 1)
        self.assertIn("headroom: scanning", files[0].read_text())

    def test_an_ack_below_capacity_passes_with_exit_zero(self) -> None:
        self.journal([ack_line(ack=900)])
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("headroom=12.1%", r.stdout)

    def test_an_empty_cap_token_falls_back_to_the_1024_default(self) -> None:
        self.journal([ack_line(ack=900)])
        r = self.run_script(env={"CAP_TOKENS": ""})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("MaxHFTTokensPerConnection=1024", r.stdout)
        self.assertIn("124 spare tokens of 1024", r.stdout)


# ── P6-193: one snapshot feeds every section ─────────────────────────────────
class SnapshotTest(Sandbox):
    def test_the_journal_is_read_once_into_a_snapshot(self) -> None:  # disc
        self.journal([ack_line(sec=1, ack=900), ack_line(sec=2, ack=512)])
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.reads_of_journal(), 1, "the journal was re-scanned per section")

    def test_an_ack_arriving_after_the_read_cannot_change_the_report(self) -> None:  # disc
        self.journal([ack_line(sec=1, ack=900), ack_line(sec=2, ack=512)])
        extra = ack_line(sec=3, slot="hft-9", ack=1024)
        r = self.run_script(env={"EXTRA_ACK": extra})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.reads_of_journal(), 1)
        self.assertNotIn("hft-9", r.stdout, "a later re-read folded the new ack into the report")
        table = self.section(r.stdout, "Subscription acks (ACTIVE/PARTIAL/TERMINAL):")
        self.assertEqual(len([ln for ln in table.split("\n") if ln.strip()]), 2)
        self.assertEqual(self.stat_line(r.stdout).split()[0], "samples=2")


# ── P6-194: tightness evidence and the no-ack case ───────────────────────────
class TightnessTest(Sandbox):
    def test_a_clean_ack_is_not_reported_as_tightness_evidence(self) -> None:  # disc
        self.journal([ack_line(assigned=1024, ack=1024, rej=0)])
        r = self.run_script()
        tight = self.section(r.stdout, "Any partial/terminal/rejected events (tightness evidence):")
        self.assertEqual(tight.strip(), "(none — all acks full & clean)")

    def test_a_partial_ack_with_rejections_is_listed(self) -> None:  # disc
        tight_line = ack_line(state="PARTIAL", assigned=1024, ack=900, rej=124, sec=2)
        self.journal([ack_line(sec=1, assigned=1024, ack=900), tight_line])
        r = self.run_script()
        tight = self.section(r.stdout, "Any partial/terminal/rejected events (tightness evidence):")
        self.assertIn("state=PARTIAL", tight)
        self.assertIn("rejected=124", tight)
        listed = [ln for ln in tight.split("\n") if ln.strip()]
        self.assertEqual(len(listed), 1, f"a clean ack was listed as tightness evidence: {listed}")
        self.assertNotIn("state=ACTIVE", tight)

    def test_zero_acks_is_reported_as_no_data_not_as_all_clean(self) -> None:  # disc
        self.journal(["2026-09-15 10:00:01,001 INFO [main] nothing to see"])
        r = self.run_script()
        tight = self.section(r.stdout, "Any partial/terminal/rejected events (tightness evidence):")
        self.assertIn("no subscription_ack rows", tight)
        self.assertNotIn("all acks full & clean", tight)


# ── P6-195: per-record field reset ───────────────────────────────────────────
class LeakResetTest(Sandbox):
    def test_an_unparseable_ack_does_not_reuse_the_previous_ack_numbers(self) -> None:  # disc
        truncated = (
            "2026-09-15 10:00:02,002 WARN [bridge] com.trading.ingestion.IngestionService - "
            "bridge lifecycle event=subscription_ack slot=hft-0 state=ACTIVE reason=line_truncated"
        )
        self.journal([ack_line(assigned=1024, ack=512), truncated])
        r = self.run_script()
        self.assertEqual(self.stat_line(r.stdout).split()[0], "samples=1", "the ack numbers leaked")


# ── P6-540: CAP_TOKENS validation ────────────────────────────────────────────
class CapValidationTest(Sandbox):
    def test_a_zero_cap_is_refused_before_any_arithmetic(self) -> None:  # disc
        self.journal([ack_line(ack=900)])
        r = self.run_script(env={"CAP_TOKENS": "0"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("CAP_TOKENS must be a positive integer (got '0')", r.stderr)
        self.assertEqual(self.summaries(), [], "evidence was written despite the refusal")

    def test_a_non_numeric_cap_is_refused(self) -> None:  # disc
        self.journal([ack_line(ack=900)])
        r = self.run_script(env={"CAP_TOKENS": "one-thousand"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("CAP_TOKENS must be a positive integer", r.stderr)


# ── P6-541: per-slot grouping and the slot filter ────────────────────────────
class SlotGroupingTest(Sandbox):
    def slots(self) -> list[str]:
        self.journal(
            [
                ack_line(slot="hft-0", sec=1, assigned=1024, ack=1024),
                ack_line(slot="hft-0", sec=2, assigned=1024, ack=900),
                ack_line(slot="hft-1", sec=3, assigned=2048, ack=1500),
            ]
        )
        return self.run_script().stdout.split("\n")

    def test_each_slot_gets_its_own_max_and_avg(self) -> None:  # disc
        out = "\n".join(self.slots())
        self.assertIn("slot=hft-0 samples=2 max=100.0% avg=93.9%", out)
        self.assertIn("slot=hft-1 samples=1 max=73.2% avg=73.2%", out)

    def test_the_ack_table_keeps_the_slot_identity(self) -> None:  # disc
        out = "\n".join(self.slots())
        table = self.section(out, "Subscription acks (ACTIVE/PARTIAL/TERMINAL):")
        self.assertIn("slot=hft-1", table)
        self.assertIn("slot=hft-0", table)

    def test_identical_samples_still_all_count(self) -> None:  # disc
        # Guards both the dedup trap (sort -u would report samples=1 here) and the
        # silent no-input awk (an aggregation with no input file reads stdin and
        # prints nothing at all).
        self.journal([ack_line(slot="hft-0", sec=n, assigned=1024, ack=512) for n in (1, 2, 3)])
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("slot=hft-0 samples=3 max=50.0% avg=50.0%", r.stdout)
        self.assertEqual(self.stat_line(r.stdout).split()[0], "samples=3")

    def test_the_slot_filter_narrows_the_whole_report(self) -> None:  # disc
        self.journal(
            [
                ack_line(slot="hft-0", sec=1, assigned=1024, ack=1024),
                ack_line(slot="hft-1", sec=2, assigned=1024, ack=512),
            ]
        )
        r = self.run_script(env={"SLOT": "hft-1"})
        self.assertEqual(r.returncode, 0, r.stderr)
        table = self.section(r.stdout, "Subscription acks (ACTIVE/PARTIAL/TERMINAL):")
        self.assertIn("slot=hft-1", table)
        self.assertNotIn("slot=hft-0", table)
        self.assertEqual(self.stat_line(r.stdout).split()[0], "samples=1")


# ── P6-542: nearest-rank p99 is a ceiling ────────────────────────────────────
class P99Test(Sandbox):
    def test_p99_takes_the_ceiling_rank_and_not_the_rounded_one(self) -> None:  # disc
        # 68 samples: 0.99 * 68 = 67.32 -> ceil 68 (the max), round 67 (the small value).
        lines = [ack_line(sec=1, assigned=1024, ack=100) for _ in range(67)]
        lines.append(ack_line(sec=2, assigned=1024, ack=1024))
        self.journal(lines)
        r = self.run_script()
        stat = self.stat_line(r.stdout)
        self.assertIn("samples=68", stat)
        self.assertIn("p99=100.0%", stat, "p99 used the rounded rank and understated the tail")


# ── P6-543 / P6-787: the per-connection cap side ─────────────────────────────
class CapOverflowTest(Sandbox):
    def run_overflow(self, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
        # 1500 of 2048 assigned is 73% of the assignment but 146% of the 1024 cap.
        self.journal([ack_line(slot="hft-0", assigned=2048, ack=1500)])
        return self.run_script(env=env)

    def test_exceeding_the_cap_alerts_even_with_assignment_headroom(self) -> None:  # disc
        r = self.run_overflow()
        self.assertIn("AT CAPACITY (146.5% of the 1024-token per-connection cap)", r.stdout)
        self.assertIn("OVER cap by 476 tokens", r.stdout)

    def test_the_spare_token_line_never_goes_negative(self) -> None:  # disc
        r = self.run_overflow()
        self.assertNotIn("-476 spare", r.stdout)
        self.assertNotIn("spare tokens", r.stdout)

    def test_an_expanded_cap_reports_headroom_again(self) -> None:
        r = self.run_overflow(env={"CAP_TOKENS": "2048"})
        self.assertIn("vs per-connection cap: max used=73.2%", r.stdout)
        self.assertIn("headroom=26.8%", r.stdout)


# ── P6-544: the exit status is the alert channel ─────────────────────────────
class ExitStatusTest(Sandbox):
    def test_at_capacity_exits_three(self) -> None:  # disc
        self.journal([ack_line(assigned=1024, ack=1024)])
        r = self.run_script()
        self.assertEqual(r.returncode, 3, "at-capacity was masked by the tee pipeline")
        self.assertIn("headroom: ALERT — at capacity", r.stderr)

    def test_no_ack_rows_exits_four(self) -> None:  # disc
        self.journal(["2026-09-15 10:00:01,001 INFO [main] nothing to see"])
        r = self.run_script()
        self.assertEqual(r.returncode, 4, "no-data was masked by the tee pipeline")
        self.assertIn("headroom: NO DATA", r.stderr)

    def test_the_report_is_complete_on_stdout_even_when_alerting(self) -> None:  # disc
        self.journal([ack_line(assigned=1024, ack=1024)])
        r = self.run_script()
        self.assertEqual(r.returncode, 3)
        self.assertIn("AT CAPACITY (100.0%)", r.stdout)
        self.assertIn("samples=1", r.stdout)
        self.assertIn("(of assignment)", r.stdout)


# ── P6-784: two runs in the same second ──────────────────────────────────────
class UniqueSummaryTest(Sandbox):
    def test_two_runs_in_the_same_second_write_two_summaries(self) -> None:  # disc
        self.journal([ack_line(ack=900)])
        rc1 = self.run_script().returncode
        rc2 = self.run_script().returncode
        self.assertEqual((rc1, rc2), (0, 0))
        self.assertEqual(len(self.summaries()), 2, "the second run overwrote the first's evidence")


# ── P6-785 / P6-786: first-row seeding and external selection ────────────────
class StatsTest(Sandbox):
    def test_the_first_row_seeds_min_even_when_it_is_zero(self) -> None:
        # The pre-fix `used < min_used || min_used==""` fallback also handled this,
        # so this probe passes both sides — it pins the explicit-flag behaviour.
        self.journal([ack_line(sec=1, assigned=1024, ack=0), ack_line(sec=2, assigned=1024, ack=10)])
        r = self.run_script()
        stat = self.stat_line(r.stdout)
        self.assertIn("min=0.0%", stat)
        self.assertIn("max=1.0%", stat)

    def test_p99_is_selected_outside_awk_with_a_numeric_sort(self) -> None:  # disc
        self.journal([ack_line(sec=1, assigned=1024, ack=100), ack_line(sec=2, assigned=1024, ack=900)])
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stderr)
        calls = self.sorts.read_text().split("\n") if self.sorts.exists() else []
        self.assertTrue(
            any(c.startswith("-n ") and "used.vals" in c for c in calls),
            f"no external numeric sort of the sample values: {calls}",
        )


if __name__ == "__main__":
    unittest.main()
