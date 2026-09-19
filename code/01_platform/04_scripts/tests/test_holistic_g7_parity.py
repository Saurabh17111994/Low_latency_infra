"""Unit tests for the G7 parity guards in holistic-analyze.py (CHG-120).

Covers the two changes made for the TM-kill-at-full-load drill:
  1. counter_deltas is reset-aware: a job restart mid-run re-creates
     every counter at 0, so naive last-minus-first goes NEGATIVE and
     every guard built on it mis-fires. A reset must carry the pre-reset
     total forward (Prometheus rate() semantics).
  2. g7c_compare is the extracted zero-loss parity check: every
     fully-closed (token, 15s window) must have a final candle whose
     tick_count and volume equal the raw recount. A window lost to the
     kill (no candle after the startup prefix) IS data loss and must
     fire; the contiguous LATEST-mode startup prefix is tolerated.
"""

import ast
import importlib.util
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

ANALYZE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "holistic-analyze.py")


def load_analyze():
    # hyphenated module name - load via importlib (repo pattern, see
    # test_reconcile_compare.py). Safe: main() only runs under __main__.
    spec = importlib.util.spec_from_file_location("holistic_analyze", ANALYZE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SERIES = 'flink_taskmanager_job_task_operator_compute_dedup_duplicates{subtask="0"}'


def write_tsv(rows):
    fh = tempfile.NamedTemporaryFile(
        "w", suffix=".tsv", delete=False)
    for ts, name, val in rows:
        fh.write(f"{ts} {name} {val}\n")
    fh.close()
    return fh.name


class CounterDeltaTests(unittest.TestCase):
    """counter_deltas reset semantics."""

    def setUp(self):
        self.mod = load_analyze()

    def _deltas(self, rows):
        # P6-623: register the cleanup where the file is created. Registering it in
        # setUp could never fire — _tmp does not exist yet — so every run leaked one
        # NamedTemporaryFile(delete=False) tsv (48 were sitting in /tmp).
        self._tmp = write_tsv(rows)
        self.addCleanup(os.unlink, self._tmp)
        return self.mod.counter_deltas(self._tmp)

    def test_no_reset_is_last_minus_first(self):
        d = self._deltas([(100, SERIES, 10), (115, SERIES, 50)])
        self.assertEqual(d[SERIES], 40.0)

    def test_single_reset_carries_pre_reset_total(self):
        # job restart: counter re-created at 0 mid-run. True counted
        # after the first sample = 40 (pre-kill) + 5 (post-restart) = 45.
        d = self._deltas([(100, SERIES, 10), (115, SERIES, 50),
                          (130, SERIES, 0), (145, SERIES, 5)])
        self.assertEqual(d[SERIES], 45.0)

    def test_multiple_resets(self):
        # 0->30, reset, 0->25, reset, 0->10: total counted = 30+25+10 = 65
        d = self._deltas([(100, SERIES, 0), (110, SERIES, 30),
                          (120, SERIES, 5), (130, SERIES, 25),
                          (140, SERIES, 0), (150, SERIES, 10)])
        self.assertEqual(d[SERIES], 65.0)

    def test_help_and_type_lines_ignored(self):
        # Prometheus HELP/TYPE lines match the name grep but are not
        # samples - their last field is not a float (crashed the first
        # G7 run live); they must be skipped, not parsed.
        d = self._deltas([(100, SERIES, 10), (115, SERIES, 50),
                          (116, "#", "HELP"), (117, "#", "TYPE")])
        self.assertEqual(d[SERIES], 40.0)

    def test_missing_file_is_all_zero(self):
        d = self.mod.counter_deltas("/nonexistent/path.tsv")
        self.assertEqual(d, {})


class G7cCompareTests(unittest.TestCase):
    """g7c_compare zero-loss parity semantics.

    Window math: 15000ms windows. run_start=1000000, run_end=2000000.
    Fully-comparable windows: ws >= 1005000 and ws + 14999 <= 1990000.
    """

    def setUp(self):
        self.mod = load_analyze()
        self.run_start = 1000000
        self.run_end = 2000000
        # three fully-closed windows for token 1, 10 ticks / vol 100 each
        self.win_ticks = {(1, 1005000): 10, (1, 1020000): 10,
                          (1, 1035000): 10}
        self.win_vol = {k: 100 for k in self.win_ticks}
        self.final = {(1, 1005000): (10, 100), (1, 1020000): (10, 100),
                      (1, 1035000): (10, 100)}

    def compare(self, win_ticks=None, win_vol=None, final=None,
                run_start=None, run_end=None, event_horizon=None):
        return self.mod.g7c_compare(
            win_ticks if win_ticks is not None else self.win_ticks,
            win_vol if win_vol is not None else self.win_vol,
            final if final is not None else self.final,
            run_start if run_start is not None else self.run_start,
            run_end if run_end is not None else self.run_end,
            event_horizon=event_horizon)

    def test_clean_pass(self):
        mismatch, compared, skipped = self.compare()
        self.assertEqual(mismatch, [])
        self.assertEqual(compared, 3)
        self.assertEqual(skipped, 0)

    def test_measurement_guard_rejects_unavailable_recount(self):
        message = self.mod.g7c_measurement_guard(False, 3)
        self.assertIn("raw recount unavailable", message)

    def test_measurement_guard_rejects_zero_comparisons(self):
        message = self.mod.g7c_measurement_guard(True, 0)
        self.assertIn("zero fully-closed", message)

    def test_measurement_guard_allows_real_comparison(self):
        self.assertIsNone(self.mod.g7c_measurement_guard(True, 1))

    def test_missing_window_after_first_candle_fires(self):
        # the window a TM kill would eat: raw ticks exist, no final candle
        final = dict(self.final)
        del final[(1, 1020000)]
        mismatch, compared, _ = self.compare(final=final)
        self.assertEqual(len(mismatch), 1)
        self.assertIn("NO final candle", mismatch[0])
        self.assertEqual(compared, 2)

    def test_tick_count_mismatch_fires(self):
        final = dict(self.final)
        final[(1, 1020000)] = (9, 100)  # one tick lost between raw and candle
        mismatch, compared, _ = self.compare(final=final)
        self.assertEqual(len(mismatch), 1)
        self.assertIn("ticks=9", mismatch[0])
        self.assertIn("raw recount ticks=10", mismatch[0])

    def test_volume_mismatch_fires(self):
        final = dict(self.final)
        final[(1, 1020000)] = (10, 99)  # volume drifted
        mismatch, _, _ = self.compare(final=final)
        self.assertEqual(len(mismatch), 1)
        self.assertIn("vol=99", mismatch[0])

    def test_startup_prefix_tolerated(self):
        # LATEST-mode source: first windows legitimately have no candle.
        # A CONTIGUOUS prefix of candle-less windows must not fire.
        win_ticks = {(1, 1005000): 10, (1, 1020000): 10, (1, 1035000): 10}
        win_vol = {k: 100 for k in win_ticks}
        final = {(1, 1035000): (10, 100)}  # candles only from window 3
        mismatch, compared, skipped = self.compare(
            win_ticks=win_ticks, win_vol=win_vol, final=final)
        self.assertEqual(mismatch, [])
        self.assertEqual(skipped, 2)
        self.assertEqual(compared, 1)

    def test_first_present_candle_partial_window_exempt(self):
        # the first present candle's window may be partial (LATEST source
        # entered mid-window) - wrong counts there must NOT fire
        final = dict(self.final)
        final[(1, 1005000)] = (4, 40)
        mismatch, _, _ = self.compare(final=final)
        self.assertEqual(mismatch, [])

    def test_window_after_cutoff_excluded(self):
        # windows not fully closed + drained at run end are not compared
        # (their candle may legitimately still be in flight)
        win_ticks = dict(self.win_ticks)
        win_vol = dict(self.win_vol)
        win_ticks[(1, 1980000)] = 10  # wend 1994999 > cutoff 1990000
        win_vol[(1, 1980000)] = 100
        mismatch, compared, _ = self.compare(
            win_ticks=win_ticks, win_vol=win_vol)
        self.assertEqual(mismatch, [])
        self.assertEqual(compared, 3)

    def test_unclosed_tail_window_not_demanded(self):
        # Drill 20260902-034813 regression: the drain phase stops the feed
        # mid-window, the watermark stalls, and the LAST window can never
        # emit its candle. The event horizon (max event ts) bounds what can
        # be demanded: a window with wend > horizon - 500 never closed.
        # The old wall-clock cutoff fired 1024 phantom mismatches here.
        horizon = 1050000  # feed stopped inside window 1035000
        win_ticks = {(1, 1005000): 10, (1, 1020000): 10, (1, 1035000): 5}
        win_vol = {k: 100 for k in win_ticks}
        final = {(1, 1005000): (10, 100), (1, 1020000): (10, 100)}
        # window 1035000 has raw ticks but NO candle — and no successor
        # event ever advanced the watermark past its end.
        mismatch, compared, _ = self.compare(
            win_ticks=win_ticks, win_vol=win_vol, final=final,
            event_horizon=horizon)
        self.assertEqual(mismatch, [])
        self.assertEqual(compared, 2)

    def test_closed_window_gap_still_fires_with_horizon(self):
        # The horizon must NOT mask real loss: a window that provably
        # closed (wend <= horizon - 500, i.e. successor events flowed)
        # with raw ticks and no candle IS a dropped window and fires.
        horizon = 1100000  # events continued 50s past window 1035000
        win_ticks = {(1, 1005000): 10, (1, 1020000): 10, (1, 1035000): 10}
        win_vol = {k: 100 for k in win_ticks}
        final = {(1, 1005000): (10, 100), (1, 1020000): (10, 100)}
        mismatch, compared, _ = self.compare(
            win_ticks=win_ticks, win_vol=win_vol, final=final,
            event_horizon=horizon)
        self.assertEqual(len(mismatch), 1)
        self.assertIn("NO final candle", mismatch[0])
        self.assertEqual(compared, 2)  # the 2 present candles were compared

    def test_horizon_governs_when_narrower_than_run_end(self):
        # Analysis runs minutes after run end, so wall clock is always
        # past the horizon; the NARROWER bound (horizon) must win.
        horizon = 1040000  # feed stopped before window 1035000 closed
        win_ticks = {(1, 1005000): 10, (1, 1020000): 10, (1, 1035000): 10}
        win_vol = {k: 100 for k in win_ticks}
        final = {(1, 1005000): (10, 100), (1, 1020000): (10, 100)}
        mismatch, compared, _ = self.compare(
            win_ticks=win_ticks, win_vol=win_vol, final=final,
            event_horizon=horizon)
        self.assertEqual(mismatch, [])
        self.assertEqual(compared, 2)

    def test_window_straddling_run_start_excluded(self):
        # a window that opened before run_start has partial raw history
        win_ticks = dict(self.win_ticks)
        win_vol = dict(self.win_vol)
        win_ticks[(1, 996000)] = 10
        win_vol[(1, 996000)] = 100
        mismatch, compared, _ = self.compare(
            win_ticks=win_ticks, win_vol=win_vol)
        self.assertEqual(mismatch, [])
        self.assertEqual(compared, 3)

    def test_startup_prefix_is_independent_per_token(self):
        # A sparse token may produce its first candle later than another
        # token.  Startup skipping must be keyed by token; a global first
        # candle would falsely call token 2's legitimate startup prefix loss.
        win_ticks = {
            (1, 1005000): 10, (1, 1020000): 10,
            (2, 1005000): 10, (2, 1020000): 10,
        }
        win_vol = {key: 100 for key in win_ticks}
        final = {
            (1, 1005000): (10, 100),
            (1, 1020000): (10, 100),
            (2, 1020000): (10, 100),
        }
        mismatch, compared, skipped = self.compare(
            win_ticks=win_ticks, win_vol=win_vol, final=final)
        self.assertEqual(mismatch, [])
        self.assertEqual(compared, 3)
        self.assertEqual(skipped, 1)


class RawReaderGuardTests(unittest.TestCase):
    """The G7 raw reader must track the partitioned v3 table contract."""

    def setUp(self):
        self.mod = load_analyze()

    def test_reader_subscribes_partitions_and_uses_v3_indexes(self):
        source = self.mod.LOG_READ_SRC
        self.assertIn("t.getTableInfo().isPartitioned()", source)
        self.assertIn("admin.listPartitionInfos(tp).get()", source)
        self.assertIn(
            "scanner.subscribe(partition.getPartitionId(), b, 0L)", source)
        # event_day is the new v3 index 0; audit fields therefore shifted by 1.
        for expression in (
                "rw.getString(1)", "rw.getLong(5)", "rw.getLong(8)",
                "rw.getLong(9)", "rw.getLong(12)", "rw.getLong(13)",
                "rw.getString(18)"):
            self.assertIn(expression, source)
        self.assertIn("raw_table_1 audit schema mismatch", source)

    def test_failed_stream_reader_preserves_previous_evidence_and_error(self):
        with tempfile.TemporaryDirectory() as out_dir:
            rows_path = os.path.join(
                out_dir, "latency-raw_table_1.rows.txt")
            err_path = os.path.join(
                out_dir, "latency-raw_table_1.reader.stderr.log")
            with open(rows_path, "w") as fh:
                fh.write("previous evidence\n")

            def run(*_args, **kwargs):
                if kwargs.get("capture_output"):
                    return SimpleNamespace(returncode=0)
                kwargs["stderr"].write("partitioned table subscription failed")
                return SimpleNamespace(returncode=1)

            with mock.patch.object(self.mod.subprocess, "run", side_effect=run):
                result = self.mod.collect_rows_stream(
                    "raw_table_1", "unused", out_dir, run_ms=1, mode="audit")

            self.assertIsNone(result)
            with open(rows_path) as fh:
                self.assertEqual(fh.read(), "previous evidence\n")
            with open(err_path) as fh:
                self.assertIn("partitioned table", fh.read())

    def test_successful_audit_stream_publishes_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            def run(*_args, **kwargs):
                if kwargs.get("capture_output"):
                    return SimpleNamespace(returncode=0)
                kwargs["stdout"].write(
                    "28:14:0\tfingerprint\t123\t1000\t1001\t10\t2\tVALID_TRADE\n")
                return SimpleNamespace(returncode=0)

            with mock.patch.object(self.mod.subprocess, "run", side_effect=run):
                result = self.mod.collect_rows_stream(
                    "raw_table_1", "unused", out_dir, run_ms=1, mode="audit")

            self.assertEqual(
                result, os.path.join(out_dir, "latency-raw_table_1.rows.txt"))
            with open(result) as fh:
                self.assertTrue(fh.read().startswith("28:14:0\t"))

    def test_failed_row_reader_preserves_previous_evidence_and_error(self):
        with tempfile.TemporaryDirectory() as out_dir:
            rows_path = os.path.join(
                out_dir, "latency-feature_candles_15s.rows.txt")
            err_path = os.path.join(
                out_dir, "latency-feature_candles_15s.reader.stderr.log")
            with open(rows_path, "w") as fh:
                fh.write("previous feature evidence\n")

            def run(*_args, **kwargs):
                if _args[0][0] == "javac":
                    return SimpleNamespace(returncode=0, stdout="", stderr="")
                return SimpleNamespace(
                    returncode=1, stdout="", stderr="connection refused")

            with mock.patch.object(self.mod.subprocess, "run", side_effect=run):
                result = self.mod.collect_rows(
                    "feature_candles_15s", "unused", out_dir, run_ms=1)

            self.assertIsNone(result)
            with open(rows_path) as fh:
                self.assertEqual(fh.read(), "previous feature evidence\n")
            with open(err_path) as fh:
                self.assertIn("connection refused", fh.read())

    def test_successful_row_reader_publishes_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            def run(*_args, **kwargs):
                if _args[0][0] == "javac":
                    return SimpleNamespace(returncode=0, stdout="", stderr="")
                return SimpleNamespace(
                    returncode=0, stdout="(1,NSE,2,3,4,5,6,7,8,9,10,11,12,13,14)\n",
                    stderr="")

            with mock.patch.object(self.mod.subprocess, "run", side_effect=run):
                result = self.mod.collect_rows(
                    "feature_candles_15s", "unused", out_dir, run_ms=1)

            self.assertEqual(result, [
                "(1,NSE,2,3,4,5,6,7,8,9,10,11,12,13,14)"])


class F4OrphanCheckTest(unittest.TestCase):
    """f4_orphan_check event-horizon semantics.

    Drill 20260902-030521 regression: 31 phantom orphans — tentatives in
    the last two windows before run end, whose finals were still in the
    kill backlog when the job was cancelled. With the drain phase the
    backlog drains, but the feed stops mid-window: trailing tentatives'
    windows never close and can never settle. They are UNVERIFIABLE, not
    orphans.
    """

    def setUp(self):
        self.mod = load_analyze()

    def check(self, groups, run_end=2000000, horizon=None, grace=30000):
        ts = {p: 1000000 for p in groups}  # all tentatives at t=1000000
        return self.mod.f4_orphan_check(groups, ts, run_end,
                                        event_horizon=horizon, grace_ms=grace)

    def test_settled_group_not_counted(self):
        orphans, lonely, unver = self.check(
            {"a": {"TENTATIVE", "CONFIRM"}, "b": {"TENTATIVE", "CANCEL"}},
            horizon=1100000)
        self.assertEqual(orphans, [])
        self.assertEqual(lonely, 0)
        self.assertEqual(unver, 0)

    def test_trailing_tentative_is_unverifiable_not_orphan(self):
        # tentative at 1000000, window end <= 1015000; horizon 1030000:
        # window closed but not provably closed+settled for a 30s grace
        # (needs horizon >= 1050000) → unverifiable, NOT an orphan.
        orphans, lonely, unver = self.check(
            {"a": {"TENTATIVE"}}, horizon=1030000)
        self.assertEqual(orphans, [])
        self.assertEqual(lonely, 1)
        self.assertEqual(unver, 1)

    def test_tentative_in_never_closed_window_excluded(self):
        # horizon 1010000: the window (end <= 1015000) never closed — no
        # successor event advanced the watermark past its end. Not an orphan.
        orphans, lonely, unver = self.check(
            {"a": {"TENTATIVE"}}, horizon=1010000)
        self.assertEqual(orphans, [])
        self.assertEqual(unver, 1)

    def test_midrun_dropped_settle_still_fires(self):
        # horizon 60s past window end + grace: the window closed, the final
        # was processed (the drain phase guarantees backlog-free
        # processing), and the settle row is STILL missing — a dropped
        # decision. Fires.
        orphans, lonely, unver = self.check(
            {"a": {"TENTATIVE"}}, horizon=1100000)
        self.assertEqual(len(orphans), 1)
        self.assertEqual(unver, 0)

    def test_no_horizon_falls_back_to_wall_clock(self):
        # Pre-drain behavior preserved when no raw recount is available
        # (run_end 2000000, tentative at 1000000 → 1000s past grace).
        orphans, lonely, unver = self.check({"a": {"TENTATIVE"}})
        self.assertEqual(len(orphans), 1)
        self.assertEqual(unver, 0)


class G7bLateSupersetTests(unittest.TestCase):
    """P6-487 — the late-drop counter is a SUPERSET of the injection.

    `compute.candles.late.dropped` counts every late/out-of-order drop the
    multi-TF aggregator makes, so the injected frames and the feed's own
    re-feeds land in the same counter. Equality was never achievable: the
    2026-09-17 run counted the injected 20 exactly on one subtask while
    another subtask carried 114 natural re-feeds into the same window, so the
    old `late_delta != late_in_window` reported "data loss" for a run whose
    injection was counted perfectly.
    """

    def setUp(self):
        self.mod = load_analyze()

    def test_the_measured_surplus_is_not_a_failure(self):
        """Run 9's real numbers: 134 counted, 20 of them injected in-window."""
        failures, notes = self.mod.g7b_verdicts(134, 20, 20, 20)

        self.assertEqual(failures, [],
                         "natural feed re-feeds were reported as data loss")
        self.assertTrue(notes, "the surplus must stay visible in the report")
        self.assertIn("114", notes[0], notes[0])
        self.assertIn("natural", notes[0], notes[0])

    def test_a_shortfall_still_fails(self):
        """Run 8's defect: the counter never moved because the path was absent."""
        failures, _ = self.mod.g7b_verdicts(0, 20, 20, 20)

        self.assertEqual(len(failures), 1, failures)
        self.assertIn("BELOW", failures[0], failures[0])

    def test_injected_rows_missing_from_raw_still_fails(self):
        failures, _ = self.mod.g7b_verdicts(20, 20, 17, 20)

        self.assertEqual(len(failures), 1, failures)
        self.assertIn("raw_table_1", failures[0], failures[0])

    def test_an_exact_cover_reports_no_surplus(self):
        failures, notes = self.mod.g7b_verdicts(20, 20, 20, 20)

        self.assertEqual(failures, [])
        self.assertEqual(notes, [])

    def test_the_equality_comparison_is_gone_and_main_uses_the_helper(self):
        """main() is not unit-testable end to end, so pin the wiring here."""
        src = open(ANALYZE, encoding="utf-8").read()

        self.assertIn("g7b_failures, g7b_notes = g7b_verdicts(", src,
                      "main() no longer calls the extracted G7b helper")
        self.assertIn("failures.extend(g7b_failures)", src,
                      "the helper's verdicts are not reaching the guard list")
        # The superset semantics themselves are pinned by the behavioural tests
        # above (134 vs 20 must pass); this only guards the wiring, so it does
        # not grep for the old expression — a comment or docstring naming it is
        # documentation, not a regression.


def guard_messages(src, listname):
    """String literals passed to <listname>.append(...) anywhere in `src`.

    Read through the AST so the assertions describe behaviour (which channel a
    verdict travels down) instead of line layout, which any reformat would
    invalidate.
    """
    out = []
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "append"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == listname
                and node.args):
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                out.append(arg.value)
    return out


class G7cCandleSourceTests(unittest.TestCase):
    """CHG-194 — the parity proof reads candle_closed, not an empty list.

    feature_candles_15s was retired on 2026-09-05; the analyzer was patched to
    stop reading it but kept `final_rows = []`, so the candle side of G7c was
    structurally empty: every one of a run's ~22.5k windows reported "NO final
    candle" and the harness could never pass. candle_closed (DDL 33) carries
    tick_count + volume per (instrument_token, tf, window_start), so parity is
    measurable without the retired table.
    """

    def setUp(self):
        self.mod = load_analyze()

    @staticmethod
    def _row(token, ws, ticks, vol, width=15000):
        return {"instrument_token": token, "window_start": ws,
                "window_end": ws + width, "tick_count": ticks, "volume": vol}

    def test_a_15s_candle_becomes_a_parity_key(self):
        mapping = self.mod.candle_parity_map([self._row(25, 15000, 150, 9000)])

        self.assertEqual(mapping, {(25, 15000): (150, 9000)})

    def test_other_timeframes_are_not_the_15s_family(self):
        """candle_closed holds six TFs under the same window_start and the raw
        recount groups at 15s, so a 30s row must not be compared against it."""
        mapping = self.mod.candle_parity_map([
            self._row(25, 15000, 150, 9000),
            self._row(25, 15000, 300, 18000, width=30000)])

        self.assertEqual(mapping, {(25, 15000): (150, 9000)},
                         "a second timeframe leaked into the 15s comparison")

    def test_only_other_timeframes_leaves_nothing_to_compare(self):
        """The map goes empty rather than guessing - and the guard, not the map,
        is what turns an empty comparison into a failure."""
        mapping = self.mod.candle_parity_map(
            [self._row(25, 15000, 300, 18000, width=30000)])

        self.assertEqual(mapping, {})

    def test_a_row_missing_a_column_is_skipped_not_crashed(self):
        mapping = self.mod.candle_parity_map([
            {"instrument_token": 25, "window_start": 15000},
            self._row(26, 30000, 150, 1)])

        self.assertEqual(mapping, {(26, 30000): (150, 1)})

    def test_the_candle_side_is_read_by_name_not_by_position(self):
        """The retired parse read f[3]/f[9]/f[10] of a 15-field layout; the
        probe emits a JSON object keyed by column name, so a reorder cannot
        shift the comparison onto the wrong values."""
        src = open(ANALYZE, encoding="utf-8").read()

        for column in ('r["tick_count"]', 'r["volume"]', 'r["window_start"]',
                       'r["window_end"]', 'r["instrument_token"]'):
            self.assertIn(column, src, column)

    def test_a_failed_candle_read_is_a_failure_not_a_pass(self):
        message = self.mod.g7c_measurement_guard(True, 22528, False)

        self.assertIsNotNone(message, "a dead read was accepted as a real one")
        self.assertIn("candle_closed read failed", message)

    def test_a_zero_comparison_is_still_rejected_when_the_read_worked(self):
        message = self.mod.g7c_measurement_guard(True, 0, True)

        self.assertIn("zero fully-closed", message)

    def test_main_reads_the_live_table_and_hands_the_result_to_the_guard(self):
        """Wiring pin: without both halves a dead read would land in the guard
        as `compared == 0` and be reported as a mere startup skip."""
        src = open(ANALYZE, encoding="utf-8").read()

        self.assertIn("read_candle_closed_rows(", src,
                      "main() no longer reads candle_closed")
        self.assertIn(
            "g7c_measurement_guard(raw_read_ok, compared, candle_read_ok)", src,
            "the candle read result never reaches the parity guard")

    def test_the_kv_read_never_falls_back_to_the_log_wildcard(self):
        """candle_closed is a PRIMARY-KEY table: FlussPrefixReader's '*' means
        'no filter' only on the LOG path, and on the KV path it would parse '*'
        as a token, fail, and print __END__ 0 - a silent empty read (P6-082)."""
        src = open(ANALYZE, encoding="utf-8").read()

        self.assertIn('",".join(str(t) for t in sorted(tokens))', src,
                      "the candle read must pass an explicit token list")


class AcceptedUnavailableTests(unittest.TestCase):
    """CHG-194 — "cannot measure" must not be reported as "pipeline is broken".

    Three legs are unmeasurable in this topology: the preview table was retired,
    the live candle pair carries no write timestamp, and Signal_Candidates has
    no writer at all while the strategy host is off. They were appended to
    `failures`, whose non-emptiness is the analyzer's exit code - so the run
    exited 1 no matter how clean the pipeline was, and the harness could not
    pass. They now travel the `unavailable` channel: printed, counted, and
    rc-neutral. The split is fail-closed, so an unmeasurable leg OUTSIDE the
    accepted set still fails.
    """

    def setUp(self):
        self.src = open(ANALYZE, encoding="utf-8").read()

    def test_the_retired_legs_are_reported_as_unavailable(self):
        unavailable = guard_messages(self.src, "unavailable")

        for stem in ("G6: preview latency leg",
                     "G7c: final-candle latency leg",
                     "G6: no preview e2e samples"):
            self.assertTrue(any(m.startswith(stem) for m in unavailable),
                            f"{stem!r} is not on the unavailable channel")

    def test_the_retired_legs_are_no_longer_hard_failures(self):
        hard = guard_messages(self.src, "failures")

        for stem in ("G6: preview latency leg",
                     "G7c: final-candle latency leg",
                     "G6: no preview e2e samples"):
            self.assertFalse(any(m.startswith(stem) for m in hard),
                             f"{stem!r} would still fail every run")

    def test_unavailable_is_printed_and_counted(self):
        self.assertIn("~~ UNAVAILABLE (accepted, not a failure)", self.src)
        self.assertIn("measurement leg(s) unavailable in this ",
                      self.src)

    def test_unavailable_never_exits_nonzero(self):
        """The exit code is driven by `failures` alone."""
        start = self.src.index("if unavailable:")
        block = self.src[start:self.src.index("if failures:", start)]

        self.assertNotIn("sys.exit", block,
                         "an unmeasurable leg must not fail the run")

    def test_the_signal_leg_still_fails_when_the_host_did_run(self):
        """With the strategy host wired, Signal_Candidates HAS a writer, so an
        unreadable table is a real defect and must not be excused."""
        self.assertIn("if strategy_host_enabled():", self.src)
        self.assertIn("failures.append(msg)", self.src,
                      "the host-on path must still fail")
        self.assertIn("STRATEGY_HOST_ENABLED is off", self.src)


class StrategyHostFlagTests(unittest.TestCase):
    """The F4 exemption is decided by the submitted flag, not by guessing."""

    def setUp(self):
        self.mod = load_analyze()

    def test_absent_flag_means_the_host_is_off(self):
        self.assertFalse(self.mod.strategy_host_enabled({}))

    def test_true_is_recognised_in_any_case(self):
        for raw in ("true", "TRUE", " true "):
            self.assertTrue(
                self.mod.strategy_host_enabled({"STRATEGY_HOST_ENABLED": raw}),
                raw)

    def test_anything_else_is_off(self):
        for raw in ("false", "1", "yes", ""):
            self.assertFalse(
                self.mod.strategy_host_enabled({"STRATEGY_HOST_ENABLED": raw}),
                raw)


class CandleReaderGuardTests(unittest.TestCase):
    """The G7c candle reader: a failed read must never look like "no candles".

    An empty candle side and a broken candle reader produce the same comparison
    (`compared == 0`), so the only thing separating "this run lost nothing" from
    "this run proved nothing" is the probe's `__END__` sentinel. These drive the
    reader directly, mirroring RawReaderGuardTests above.
    """

    ROW = ('{"instrument_token":25,"tf":"FIFTEEN_S","window_start":15000,'
           '"window_end":30000,"volume":9000,"tick_count":150}\n')

    def setUp(self):
        self.mod = load_analyze()

    def _read(self, out_dir, stdout_text, rc=0, tokens=(25, 26)):
        """Run the reader with javac/java faked; return (result, calls)."""
        calls = []

        def run(*args, **kwargs):
            calls.append((args, kwargs))
            if kwargs.get("capture_output"):
                return SimpleNamespace(returncode=0)   # the javac step
            kwargs["stdout"].write(stdout_text)        # the java step
            return SimpleNamespace(returncode=rc)

        with mock.patch.object(self.mod.subprocess, "run", side_effect=run):
            result = self.mod.read_candle_closed_rows(
                "unused", out_dir, set(tokens))
        return result, calls

    def test_a_complete_read_returns_parsable_rows(self):
        with tempfile.TemporaryDirectory() as out_dir:
            (rows, ok), _ = self._read(out_dir, self.ROW + "__END__ 1\n")

        self.assertTrue(ok)
        self.assertEqual(len(rows), 1)
        self.assertEqual(self.mod.candle_parity_map(rows),
                         {(25, 15000): (150, 9000)})

    def test_a_read_without_the_sentinel_is_not_a_pass(self):
        """rc=1 with rows on stdout is a FAILED read — the rows are diagnostics,
        and reporting them as the candle side would be the phantom-mismatch bug
        in reverse (a silent 'no candles')."""
        with tempfile.TemporaryDirectory() as out_dir:
            (rows, ok), _ = self._read(out_dir, self.ROW, rc=1)

        self.assertFalse(ok, "a failed read passed as a clean one")
        self.assertIsNotNone(self.mod.g7c_measurement_guard(True, 1, ok))

    def test_the_read_asks_for_explicit_tokens(self):
        """A KV table cannot be read with the LOG wildcard: `*` parses as a token,
        fails, and prints `__END__ 0` (P6-082)."""
        with tempfile.TemporaryDirectory() as out_dir:
            _, calls = self._read(out_dir, self.ROW + "__END__ 1\n")

        # subprocess.run(argv, …) — the first positional is the whole argv list.
        java_argv = [call[0][0] for call in calls
                     if call[0] and call[0][0][0] == "java"][0]
        self.assertIn("25,26", java_argv)
        self.assertNotIn("*", java_argv)

    def test_no_tokens_refuses_before_reading_anything(self):
        """Without the raw recount there is nothing to look up, and an unfiltered
        read would compare the candles of every token that was never measured."""
        with tempfile.TemporaryDirectory() as out_dir:
            (rows, ok), calls = self._read(
                out_dir, self.ROW + "__END__ 1\n", tokens=())

        self.assertFalse(ok)
        self.assertEqual(rows, [])
        # Asserting the refusal alone is not enough: an unfiltered read still
        # ends with __END__ 0, so `ok` stays False for the wrong reason. The
        # probe must never be launched at all.
        self.assertEqual([call for call in calls if call[0][0][0] == "java"],
                         [], "java ran with no token list to look up")


if __name__ == "__main__":
    unittest.main()
