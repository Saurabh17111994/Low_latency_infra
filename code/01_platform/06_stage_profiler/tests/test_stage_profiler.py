#!/usr/bin/env python3
"""Offline tests for the greenfield stage profiler core.

Run from the repository root:

    python3 -m unittest discover -s code/01_platform/06_stage_profiler/tests -v

No network, no docker, no cluster: this pins only the pure half (percentiles,
presence gate, registry, rendering). The live half is exercised by the smoke
phase inside `stage-profile.sh`.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
import unittest

MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "stage_profiler.py"


def _load():
    spec = importlib.util.spec_from_file_location("stage_profiler", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolves `cls.__module__` through sys.modules; register first.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sp = _load()


class PercentileTest(unittest.TestCase):
    """Nearest-rank semantics identical to the existing analyzer's pct()."""

    def test_index_is_round_p_times_n_minus_one(self):
        xs = [10, 20, 30, 40]
        # round(0.50 * 3) = 2 -> 30; round(0.90 * 3) = 3 -> 40; round(0.99 * 3) = 3.
        self.assertEqual(30, sp.pct(xs, 50))
        self.assertEqual(40, sp.pct(xs, 90))
        self.assertEqual(40, sp.pct(xs, 99))
        self.assertEqual(40, sp.pct(xs, 100))

    def test_unsorted_input_is_sorted_first(self):
        self.assertEqual(30, sp.pct([40, 10, 30, 20], 50))

    def test_single_value_is_every_percentile(self):
        for p in (50, 90, 95, 99):
            self.assertEqual(7.5, sp.pct([7.5], p))

    def test_empty_sample_is_refused(self):
        with self.assertRaises(ValueError):
            sp.pct([], 50)

    def test_out_of_range_is_refused(self):
        for bad in (0, -1, 101):
            with self.assertRaises(ValueError):
                sp.pct([1, 2], bad)


class SummaryTest(unittest.TestCase):
    def test_of_computes_all_columns(self):
        summary = sp.Summary.of([1, 2, 3, 4])
        self.assertEqual(4, summary.n)
        self.assertEqual(3, summary.p50)
        self.assertIsNotNone(summary.p95)

    def test_of_can_omit_p95_for_java_histograms(self):
        summary = sp.Summary.of([1, 2, 3, 4], with_p95=False)
        self.assertIsNone(summary.p95)

    def test_exported_wraps_precomputed_percentiles_unchanged(self):
        summary = sp.Summary.exported(n=41386, p50=1.0, p90=4.0, p99=9.0)
        self.assertEqual((41386, 1.0, 4.0, 9.0), (summary.n, summary.p50, summary.p90, summary.p99))
        self.assertIsNone(summary.p95)


class FormatTest(unittest.TestCase):
    def test_absent_is_a_placeholder_not_a_zero(self):
        self.assertEqual(sp.PLACEHOLDER, sp.fmt_num(None))

    def test_number_is_one_decimal(self):
        self.assertEqual("12.4", sp.fmt_num(12.36))


class RegistryTest(unittest.TestCase):
    def test_stages_s1_to_s12_in_order(self):
        self.assertEqual([f"S{i}" for i in range(1, 13)], [s.sid for s in sp.STAGES])

    def test_every_stage_names_both_sources(self):
        for stage in sp.STAGES:
            self.assertTrue(stage.boundary, stage.sid)
            self.assertTrue(stage.latency, stage.sid)
            self.assertTrue(stage.throughput, stage.sid)

    def test_registry_table_covers_every_stage(self):
        table = sp.registry_table()
        self.assertIn("| S1 |", table)
        self.assertIn("| S9 |", table)
        self.assertIn("| S12 |", table)
        self.assertEqual(len(sp.STAGES) + 2, len(table.strip().splitlines()))


class PresenceTest(unittest.TestCase):
    def test_every_rule_fails_on_an_empty_observation(self):
        failures = sp.presence_failures({})
        self.assertEqual(len(sp.DEFAULT_PRESENCE_RULES), len(failures))

    def test_below_minimum_fails_with_the_key_named(self):
        rule = sp.PresenceRule("S4.append", 2, "append samples")
        failures = sp.presence_failures({"S4.append": 1}, rules=[rule])
        self.assertEqual(1, len(failures))
        self.assertIn("S4.append", failures[0])

    def test_exactly_minimum_passes(self):
        rule = sp.PresenceRule("S4.append", 2, "append samples")
        self.assertEqual([], sp.presence_failures({"S4.append": 2}, rules=[rule]))

    def test_extra_observations_are_ignored(self):
        rule = sp.PresenceRule("S4.append", 1, "append samples")
        self.assertEqual(
            [], sp.presence_failures({"S4.append": 5, "unrelated": 0}, rules=[rule])
        )

    def test_default_rules_cover_every_step_prefix(self):
        prefixes = {rule.key.split(".", 1)[0] for rule in sp.DEFAULT_PRESENCE_RULES}
        self.assertEqual({f"S{i}" for i in range(1, 13)}, prefixes)


class ReadabilityMatrixTest(unittest.TestCase):
    """CHG-461: the latency matrix parsers + the per-window 75 ms scoring."""

    def test_live_rows_drop_negative_staleness_not_clamp(self):
        text = (
            "epoch_ms\ttoken\ttf\twindow_start\twindow_end\tlast_event_time\tstaleness_ms\n"
            "1000\t4\tFIFTEEN_S\t900\t15000\t0\t0\n"
            "1001\t4\tFIFTEEN_S\t900\t15000\t0\t-7\n"
            "1002\t7\tONE_M\t0\t60000\t0\t12\n"
            "1003\t7\tBOGUS\t0\t0\t0\t5\n"
        )
        self.assertEqual(
            [("FIFTEEN_S", 900, 0.0), ("ONE_M", 0, 12.0)], sp.parse_liveread(text)
        )

    def test_live_window_stats_score_every_tf_window(self):
        rows = [
            ("FIFTEEN_S", 900, 1.0),
            ("FIFTEEN_S", 900, 2.0),
            ("FIFTEEN_S", 915, 200.0),
            ("FIFTEEN_S", 915, 100.0),
            ("ONE_M", 0, 3.0),
        ]
        stats = sp.live_window_stats(rows)
        # window 915's p99 is over the 75 ms SLO; window 900 passes.
        self.assertEqual((2, 1, 200.0), stats["FIFTEEN_S"])
        self.assertEqual((1, 0, 3.0), stats["ONE_M"])

    def test_s10_repointed_to_informational_latest_sealed_age(self):
        """CHG-489: no forming rows exist, so S10 reports sealed age as info."""
        self.assertIn("latest sealed candle age", sp.STAGES[9].boundary)
        self.assertIn("informational", sp.STAGES[9].boundary)
        detail = sp._readability_detail_tables(
            [("FIFTEEN_S", 0, 5.0)],
            {"FIFTEEN_S": [10.0]},
            {"FIFTEEN_S": [10.0]},
            {},
        )
        self.assertIn("sealed-age samples", detail)
        self.assertIn("carried by the closed + feature legs", detail)
        self.assertIn("sealed-age windows >75 ms (info)", detail)
        self.assertNotIn("live candles", detail)
        self.assertNotIn("live readability", detail)

    def test_closeread_groups_by_tf(self):
        text = (
            "epoch_ms\ttoken\ttf\twindow_start\twindow_end\tlatency_ms\n"
            "1\t4\tFIFTEEN_S\t0\t15000\t40\n"
            "2\t4\tFIFTEEN_S\t0\t15000\t30\n"
            "3\t7\tFIVE_M\t0\t300000\t900\n"
        )
        out = sp.parse_closeread(text)
        self.assertEqual([40.0, 30.0], out["FIFTEEN_S"])
        self.assertEqual([900.0], out["FIVE_M"])

    def test_featureread_expands_feature_ids(self):
        text = (
            "epoch_ms\ttoken\ttf\twindow_start\twindow_end\tfeature_ids\tlatency_ms\n"
            "1\t4\tONE_M\t0\t60000\t0,1\t12\n"
        )
        per_tf, per_feature = sp.parse_featureread(text)
        self.assertEqual([12.0], per_tf["ONE_M"])
        self.assertEqual([12.0], per_feature["last_price"])
        self.assertEqual([12.0], per_feature["sma_close_20"])
        self.assertEqual([], per_feature["rsi_close_14"])

    def test_close_window_stats_counts_over_slo(self):
        stats = sp.close_window_stats({"ONE_M": [10.0, 80.0, 20.0]})
        self.assertEqual((3, 1, 80.0), stats["ONE_M"])

    def test_summary_line_names_worst_tf(self):
        line = sp.summary_line({"ONE_M": (3, 1, 80.0), "FIVE_M": (1, 0, 40.0)})
        self.assertIn("ONE_M", line)
        self.assertIn("80.0", line)
        self.assertIn("> 75 ms", line)


class StateGrowthTest(unittest.TestCase):
    """CHG-461: state-growth series normalization + growth-rate math."""

    def test_rocksdb_dirs_normalize_to_operator_and_sum_subtasks(self):
        text = (
            "epoch_ms\tlayer\tkey\tvalue\n"
            "1000\tflink.rocksdb.bytes\tjob_ab_op_KeyedProcessOperator_x__1_8__uuid_u1\t100\n"
            "1000\tflink.rocksdb.bytes\tjob_ab_op_KeyedProcessOperator_x__2_8__uuid_u2\t300\n"
            "1000\tflink.changelog.bytes\tjob_ab\t50\n"
            "2000\tflink.rocksdb.bytes\tjob_ab_op_KeyedProcessOperator_x__1_8__uuid_u3\t140\n"
            "2000\tflink.rocksdb.bytes\tjob_ab_op_KeyedProcessOperator_x__2_8__uuid_u4\t360\n"
            "2000\tflink.changelog.bytes\tjob_ab\t110\n"
        )
        series = sp.state_growth_series(sp.parse_state_growth(text))
        self.assertEqual(
            [(1000, 400.0), (2000, 500.0)],
            series[("flink.rocksdb.bytes", "KeyedProcessOperator_x")],
        )
        self.assertEqual(
            [(1000, 50.0), (2000, 110.0)], series[("flink.changelog.bytes", "job_ab")]
        )
        rate = sp.growth_rate(series[("flink.changelog.bytes", "job_ab")])
        self.assertEqual((50.0, 110.0), rate[:2])
        self.assertAlmostEqual(3600.0, rate[2])  # 60 B over 1 s = 3600 B/min

    def test_tablet_dir_key_drops_the_table_id_suffix(self):
        rows = sp.parse_state_growth(
            "epoch_ms\tlayer\tkey\tvalue\n"
            "1\tfluss.tablet.bytes\tcandle_closed-20644\t4096\n"
            "1\tfluss.table.rows\tSignal_Candidates-20642\t7\n"
        )
        self.assertEqual(
            [("candle_closed", 4096.0), ("Signal_Candidates", 7.0)],
            [(key, value) for _, _, key, value in rows],
        )

    def test_single_sample_has_no_growth_rate(self):
        self.assertIsNone(sp.growth_rate([(1, 10.0)]))

    def test_parse_state_tf_keeps_unknown_bytes_negative(self):
        text = (
            "epoch_ms\ttable\ttf\trows\tbytes\n"
            "1000\tcandle_closed\tFIFTEEN_S\t100\t5000\n"
            "1000\tcandle_live\tFIFTEEN_S\t42\t-1\n"
        )
        rows = sp.parse_state_tf(text)
        self.assertEqual(2, len(rows))
        self.assertEqual((1000, "candle_live", "FIFTEEN_S", 42.0, -1.0), rows[1])

    def test_tf_state_series_groups_and_rates(self):
        rows = [
            (1000, "candle_closed", "FIFTEEN_S", 100.0, 5000.0),
            (61000, "candle_closed", "FIFTEEN_S", 220.0, 11000.0),
            (1000, "candle_closed", "ONE_M", 50.0, 2500.0),
        ]
        series = sp.tf_state_series(rows)
        rate = sp.tf_state_rate(series[("candle_closed", "FIFTEEN_S")])
        self.assertEqual((100.0, 220.0), rate[:2])
        self.assertAlmostEqual(120.0, rate[2])  # 120 rows over one minute
        self.assertEqual(11000.0, rate[3])
        self.assertIsNone(sp.tf_state_rate([(1, 1.0, -1.0)]))


class RenderTest(unittest.TestCase):
    def _rows(self):
        return [
            sp.ProfileRow(
                "S5",
                "ingestion end-to-end",
                "48660 rows/s",
                sp.Summary.exported(n=100, p50=2.0, p90=5.0, p99=9.0),
                source="j1-0 java.out",
            ),
            sp.ProfileRow("S9", "whole path", "unknown", None, source="absent"),
        ]

    def test_markdown_header_and_rows(self):
        text = sp.render_markdown(self._rows(), title="t")
        self.assertIn("| step | boundary | throughput | p50 | p90 | p95 | p99 | n | source |", text)
        self.assertIn("| S5 | ingestion end-to-end | 48660 rows/s | 2.0 | 5.0 | — | 9.0 | 100 | j1-0 java.out |", text)

    def test_missing_summary_renders_placeholders_and_zero_n(self):
        text = sp.render_markdown(self._rows())
        self.assertIn("| S9 | whole path | unknown | — | — | — | — | 0 | absent |", text)

    def test_tsv_uses_ascii_dash_and_one_row_per_stage(self):
        lines = sp.render_tsv(self._rows()).strip().splitlines()
        self.assertEqual(len(self._rows()) + 1, len(lines))
        self.assertTrue(lines[0].startswith("step\tboundary\tthroughput"))
        self.assertIn("\t-\t", lines[2])  # placeholder is ASCII in TSV


def _hist_payload(name: str, epoch_ns: int, count: int, p50: int, p90: int, p99: int) -> str:
    return json.dumps(
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": name,
                                    "unit": "ms",
                                    "histogram": {
                                        "dataPoints": [
                                            {
                                                "count": count,
                                                "sum": count,
                                                "bucketCounts": [count, 0, 0, 0],
                                                "explicitBounds": [p50, p90, p99],
                                                "attributes": [
                                                    {"key": "p50", "value": {"intValue": p50}},
                                                    {"key": "p90", "value": {"intValue": p90}},
                                                    {"key": "p99", "value": {"intValue": p99}},
                                                ],
                                                "timeUnixNano": str(epoch_ns),
                                            }
                                        ]
                                    },
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )


def _sum_payload(name: str, epoch_ns: int, value: int) -> str:
    return json.dumps(
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": [
                                {
                                    "name": name,
                                    "unit": "ticks",
                                    "sum": {
                                        "isMonotonic": True,
                                        "dataPoints": [
                                            {"asInt": value, "timeUnixNano": str(epoch_ns)}
                                        ],
                                    },
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    )


def _log_line(payload: str) -> str:
    return (
        "12:00:00.000 [otlp-metrics-flush] INFO  "
        "com.trading.ingestion.telemetry.OtlpMetricsEmitter  - "
        f"otlp-metrics-payload: {payload}"
    )


class JavaOutTest(unittest.TestCase):
    """java.out parsing: windows in, per-stage stats out; absent is never zero."""

    def _text(self):
        return "\n".join(
            [
                "some unrelated log line",
                _log_line(_hist_payload("stage.decode_latency", 1_000_000_000_000, 100, 4, 9, 12)),
                _log_line(_hist_payload("stage.decode_latency", 1_010_000_000_000, 300, 6, 9, 15)),
                _log_line(_sum_payload("tick.throughput", 1_000_000_000_000, 1000)),
                _log_line(_sum_payload("tick.throughput", 1_010_000_000_000, 1300)),
                _log_line("{not json"),
            ]
        )

    def test_parses_every_valid_window_and_skips_junk(self):
        windows = sp.parse_java_out(self._text())
        self.assertEqual(4, len(windows))
        kinds = sorted({w.kind for w in windows})
        self.assertEqual(["histogram", "sum"], kinds)

    def test_histogram_stats_take_cumulative_count_and_median_across_windows(self):
        stages, _ = sp.java_windows_to_stats(sp.parse_java_out(self._text()))
        stat = stages["stage.decode_latency"]
        self.assertEqual(2, stat.windows)
        self.assertEqual(300, stat.n)  # cumulative export: last window, not the sum
        self.assertEqual(4, stat.p50)  # nearest-rank median of [4, 6]
        self.assertEqual(9, stat.p90)
        self.assertEqual(12, stat.p99)
        self.assertEqual(15, stat.worst_p99)

    def test_cumulative_count_tolerates_a_mid_run_reset(self):
        text = "\n".join(
            _log_line(_hist_payload("stage.ipc_latency", 1, c, 1, 2, 3))
            for c in (100, 300, 50, 80)
        )
        stages, _ = sp.java_windows_to_stats(sp.parse_java_out(text))
        # 100 + (300-100) + 0 (reset) + (80-50) = 330
        self.assertEqual(330, stages["stage.ipc_latency"].n)

    def test_counter_rate_uses_emitter_timestamps(self):
        _, counters = sp.java_windows_to_stats(sp.parse_java_out(self._text()))
        tick = counters["tick.throughput"]
        self.assertEqual((1000, 1300), (tick.first, tick.last))
        self.assertEqual(30.0, tick.rate_per_s)

    def test_presence_counts_map_to_stage_keys(self):
        stages, _ = sp.java_windows_to_stats(sp.parse_java_out(self._text()))
        self.assertEqual({"S3.decode": 300}, sp.java_presence_counts(stages))

    def test_a_metric_with_windows_but_no_samples_is_absent_from_presence(self):
        stages, _ = sp.java_windows_to_stats(
            sp.parse_java_out(_log_line(_hist_payload("stage.ipc_latency", 1, 0, 0, 0, 0)))
        )
        self.assertEqual({}, sp.java_presence_counts(stages))


class EvidenceReadersTest(unittest.TestCase):
    """The phase-dir readers: raw JSON sample, read-lag rate, closed-window lag."""

    def test_raw_sample_lag_is_ingest_minus_event(self):
        text = '\n'.join([
            '{"instrument_token": 1, "event_time": 1000, "ingest_ts": 1007}',
            "__END__ 1",
            "not json",
        ])
        self.assertEqual([7.0], sp.parse_raw_sample(text))

    def test_raw_sample_skips_rows_missing_stamps(self):
        text = '{"instrument_token": 1}\n{"event_time": 1, "ingest_ts": 2}'
        self.assertEqual([1.0], sp.parse_raw_sample(text))

    def test_raw_append_rate_uses_log_end_sum(self):
        tsv = "epoch_ms\ttable\tpartitions\tbuckets\tlog_end_sum\n"
        tsv += "1000\traw_table_1\t1\t16\t1000\n"
        tsv += "11000\traw_table_1\t1\t16\t11000\n"
        self.assertEqual(1000.0, sp.raw_append_rate(tsv))

    def test_raw_append_rate_needs_two_polls(self):
        self.assertIsNone(sp.raw_append_rate("epoch_ms\ttable\tpartitions\tbuckets\tlog_end_sum\n"))

    def test_source_rate_counts_only_source_operators(self):
        tsv = "epoch\tvertex_id\toperator\tnumRecordsIn\tnumRecordsOut\tbusyMsSum\n"
        tsv += "1\tv\tsource-something\t0\t100\t\n"
        tsv += "1\tv\taggregator\t100\t50\t\n"
        tsv += "11\tv\tsource-something\t0\t1100\t\n"
        tsv += "11\tv\taggregator\t1100\t550\t\n"
        self.assertEqual(100.0, sp.source_records_rate(tsv))

    def test_close_to_visible_keeps_first_sighting_after_window_end(self):
        tsv = "epoch_ms\ttoken\twindow_start\toutput_ts\tlast_event_ts\tread_lag_ms\n"
        tsv += "3000\t7\t0\t4000\t3900\t-100\n"   # before close: skipped
        tsv += "6000\t7\t0\t4000\t3900\t-100\n"   # first after close: 2000
        tsv += "7000\t7\t0\t4000\t3900\t-100\n"   # later: ignored
        tsv += "9000\t7\t5000\t8000\t7900\t-100\n"  # second window: 1000
        self.assertEqual([2000.0, 1000.0], sp.close_to_visible(tsv))


class PromTest(unittest.TestCase):
    """Prometheus summary parsing for the monitor histogram and the tracker."""

    def test_parse_prom_reads_labels_and_value(self):
        text = (
            "# HELP x help\n"
            'flink_taskmanager_job_task_operator_compute_latency_ingest_to_monitor'
            '{job_id="abc",task_name="monitor",quantile="0.99",subtask_index="1",} 9.0\n'
        )
        samples = sp.parse_prom(text)
        self.assertEqual(1, len(samples))
        self.assertEqual("abc", samples[0].label("job_id"))
        self.assertEqual("0.99", samples[0].label("quantile"))
        self.assertEqual(9.0, samples[0].value)

    def test_max_quantiles_takes_max_across_subtasks_and_filters_job(self):
        family = sp.MONITOR_FAMILY
        text = "\n".join([
            f'{family}' + '{job_id="job1",task_name="m",quantile="0.99",} 5.0',
            f'{family}' + '{job_id="job1",task_name="m",quantile="0.99",subtask_index="2",} 9.0',
            f'{family}' + '{job_id="other",task_name="m",quantile="0.99",} 99.0',
        ])
        qs = sp.max_quantiles(sp.parse_prom(text), family, job_id="job1")
        self.assertEqual({"0.99": 9.0}, qs)

    def test_tracker_groups_by_task_and_counts(self):
        family = sp.TRACKER_FAMILY
        text = "\n".join([
            f'{family}' + '{job_id="j",task_name="Sink: candle_live",quantile="0.99",} 12.0',
            f'{family}' + '{job_id="j",task_name="Sink: candle_live",quantile="0.5",} 2.0',
            f'{family + "_count"}' + '{job_id="j",task_name="Sink: candle_live",} 100.0',
        ])
        samples = sp.parse_prom(text)
        tracks = sp.tracker_by_task(samples, job_id="j")
        self.assertEqual({"0.99": 12.0, "0.5": 2.0}, tracks["Sink: candle_live"])
        self.assertEqual({"Sink: candle_live": 100.0}, sp.tracker_counts_by_task(samples, job_id="j"))


SHELL_PATH = pathlib.Path(__file__).resolve().parents[1] / "stage-profile.sh"


class ShellFeedContractTest(unittest.TestCase):
    """FEED=faketool|real contract pinned in the shell orchestrator.

    The shell half cannot run without docker/cluster; these assertions pin the
    static contract the live dry run then exercises (guards, env wiring, the
    slot-vs-container confirmation rule). Read as text on purpose: no shell
    interpreter, no docker, no network.
    """

    def setUp(self):
        self.text = SHELL_PATH.read_text(encoding="utf-8")

    def _real_branch(self):
        start = self.text.index("# ── real broker branch (FEED=real)")
        end = self.text.index("# ── end real broker branch")
        self.assertLess(start, end)
        return self.text[start:end]

    def test_feed_defaults_to_faketool(self):
        self.assertIn('FEED="${FEED:-faketool}"', self.text)

    def test_invalid_feed_fails_before_anything_starts(self):
        self.assertIn("FEED must be 'faketool' or 'real'", self.text)

    def test_real_branch_uses_the_real_datasream_and_stack_credentials(self):
        branch = self._real_branch()
        self.assertIn("-e ARROW_FEED=token", branch)
        self.assertIn("stack_env_value ARROW_APP_ID", branch)
        self.assertIn("stack_env_value ARROW_USER_ID", branch)
        self.assertIn('--env-file "$LIB_SECRETS_FILE"', branch)
        self.assertIn("ARROW_HFT_MULTI_CONNECTION_APPROVED=true", branch)

    def test_real_branch_feeds_the_full_unfiltered_universe(self):
        branch = self._real_branch()
        self.assertIn('cp "$NSE_PATH" "$out/manifest-00.csv"', branch)
        self.assertIn("unfiltered universe", branch)

    def test_real_branch_never_sets_fake_broker_vars(self):
        branch = self._real_branch()
        self.assertNotIn("ARROW_FAKE_BROKER", branch)
        self.assertNotIn("ARROW_HFT_URL", branch)

    def test_real_slots_cover_the_2433_universe(self):
        self.assertIn("slots=$(( (rows + 1023) / 1024 ))", self.text)
        self.assertIn("3 x 1024 slots capacity", self.text)

    def test_real_requires_exactly_one_container(self):
        self.assertIn("FEED=real runs ONE ingestion container", self.text)

    def test_market_hours_guard_blocks_offhours_without_the_escape_hatch(self):
        self.assertIn('ALLOW_OFFHOURS_REAL="${ALLOW_OFFHOURS_REAL:-0}"', self.text)
        self.assertIn("09:15-15:30 IST", self.text)
        self.assertIn("555", self.text)
        self.assertIn("930", self.text)

    def test_bringup_only_stops_after_subscription_confirmation(self):
        self.assertIn('BRINGUP_ONLY="${BRINGUP_ONLY:-0}"', self.text)
        self.assertIn("bring-up-only: PASS", self.text)

    def test_confirmation_waits_for_all_slots_not_one_container_line(self):
        branch = self._real_branch()
        self.assertIn("grep -c 'HFT subscribed'", branch)
        self.assertIn("got_tokens", branch)

    def test_real_branch_uses_brace_form_for_the_single_container_name(self):
        # `$ING_PREFIX0` parses as an unbound variable under `set -u`
        # (caught by the 2026-09-26 dry run); the digit-suffixed name must use
        # the braced form `${ING_PREFIX}0`.
        branch = self._real_branch()
        self.assertIn("${ING_PREFIX}0", branch)
        self.assertNotIn("$ING_PREFIX0", branch)

    def test_fake_path_still_uses_the_exact_per_container_grep(self):
        self.assertIn('grep -qF "HFT subscribed $per_slice"', self.text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
