"""Pin the arithmetic of the two p99 scorers (kpi_windows.py, hop_budget.py).

Both scripts turn prom snapshots into the numbers the p99 program decides on, so a wrong
aggregation would silently mis-rank arms. The fixtures below are hand-computed: every
expected value in this file can be checked on paper.
"""
import importlib.util
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).parents[4]
SCRIPTS = ROOT / "code/01_platform/04_scripts"

LAT = (
    "flink_taskmanager_job_task_latency_source_id_operator_id_operator_subtask_index_latency"
)
SRC = "d49b076529ea4f87695ebe831a95f6bf"
DEDUP = "c608566043058f50e68a5930f2fe450e"
AGG = "60828025aaab0947a851fefb156ff0e6"


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def kpi_line(kpi, value, subtask=0):
    return (
        f"flink_taskmanager_job_task_operator_compute_latency_{kpi}"
        f'{{subtask_index="{subtask}",quantile="0.99",}} {value}\n'
    )


def lat_line(operator, value, source=SRC, quantile="0.5", subtask=0):
    return (
        f'{LAT}{{source_id="{source}",operator_id="{operator}",'
        f'operator_subtask_index="{subtask}",quantile="{quantile}",}} {value}\n'
    )


class KpiWindowsTest(unittest.TestCase):
    """3 snapshots at +0 s, +5 s, +65 s => 2 windows of 60 s anchored at the first sample."""

    def setUp(self):
        self.kpi = load("kpi_windows")
        self.tmp = TemporaryDirectory()
        run = Path(self.tmp.name) / "run"
        stages = run / "main" / "stages"
        stages.mkdir(parents=True)
        first = [
            kpi_line("ingest_to_strategy", 30.0),      # worst-subtask rule: 60 wins
            kpi_line("ingest_to_strategy", 60.0, subtask=1),
            kpi_line("ingest_to_monitor", 40.0),
        ]
        second = [kpi_line("ingest_to_strategy", 20.0)]
        third = [kpi_line("ingest_to_strategy", 10.0), kpi_line("ingest_to_monitor", 55.0)]
        for epoch, lines in ((1791264430, first), (1791264435, second), (1791264495, third)):
            (stages / f"prom-{epoch}.txt").write_text("".join(lines))
        self.run = str(run)

    def tearDown(self):
        self.tmp.cleanup()

    def test_worst_subtask_pick_and_window_aggregation(self):
        scores = self.kpi.score(self.run)
        strategy = scores["ingest_to_strategy"]
        self.assertEqual(strategy["n"], 3)
        self.assertEqual(strategy["span_s"], 65)
        self.assertEqual(strategy["median"], 20.0)   # median(60, 20, 10)
        self.assertEqual(strategy["max"], 60.0)
        self.assertEqual(strategy["ge_limit"], 1)
        self.assertEqual(strategy["windows_total"], 2)
        self.assertEqual(strategy["windows_failing"], 1)  # window 0 max 60, window 1 max 10

    def test_missing_kpi_is_skipped_not_zeroed(self):
        scores = self.kpi.score(self.run)
        self.assertNotIn("tick_to_strategy", scores)
        self.assertEqual(scores["ingest_to_monitor"]["n"], 2)      # absent from snapshot 2
        self.assertEqual(scores["ingest_to_monitor"]["median"], 47.5)
        self.assertEqual(scores["ingest_to_monitor"]["windows_failing"], 1)

    def test_empty_run_scores_nothing(self):
        with TemporaryDirectory() as empty:
            self.assertEqual(self.kpi.score(empty), {})


class HopBudgetTest(unittest.TestCase):
    def setUp(self):
        self.hops = load("hop_budget")
        self.tmp = TemporaryDirectory()
        stages = Path(self.tmp.name) / "stages"
        stages.mkdir(parents=True)
        first = [
            lat_line(SRC, 10.0),
            lat_line(DEDUP, 14.0),
            lat_line(AGG, 26.0),
            lat_line(AGG, 30.0, subtask=1),          # worst subtask: 30
            lat_line(AGG, 60.0, quantile="0.99"),
            "flink_taskmanager_job_task_operator_fluss_client_scanner_client_id_database_table_fetchLatencyMs"
            '{database="default",table="raw_table_1",subtask_index="3",} 2.0\n',
            "flink_taskmanager_job_task_operator_fluss_client_writer_client_id_sendLatencyMs"
            '{operator_name="candle-features-sink:_Writer",} 14.0\n',
        ]
        second = [
            lat_line(SRC, 12.0),
            lat_line(DEDUP, 16.0),
            lat_line(AGG, 40.0),
            lat_line(AGG, 120.0, quantile="0.99"),
        ]
        for epoch, lines in ((1791264430, first), (1791264435, second)):
            (stages / f"prom-{epoch}.txt").write_text("".join(lines))
        self.stages = str(stages)

    def tearDown(self):
        self.tmp.cleanup()

    def test_cumulative_medians_and_per_hop_increments(self):
        table = self.hops.hop_table(self.stages)["0.5"]
        chain = table[SRC]
        self.assertEqual([row["stage"] for row in chain],
                         ["1 source (raw-table-1)", "2 fingerprint-dedup",
                          "3 multi-tf-aggregator"])
        self.assertEqual(chain[0]["cum_median"], 11.0)             # median(10, 12)
        self.assertEqual(chain[1]["cum_median"], 15.0)             # median(14, 16)
        self.assertEqual(chain[1]["hop_median"], 4.0)
        self.assertEqual(chain[2]["cum_median"], 35.0)             # worst subtask: median(30, 40)
        self.assertEqual(chain[2]["cum_max"], 40.0)
        self.assertEqual(chain[2]["hop_median"], 20.0)

    def test_p99_chain_starts_at_the_first_stage_that_reports(self):
        chain = self.hops.hop_table(self.stages)["0.99"][SRC]
        self.assertEqual([row["stage"] for row in chain], ["3 multi-tf-aggregator"])
        self.assertEqual(chain[0]["hop_median"], 90.0)             # median(60, 120) - 0

    def test_client_gauges_are_reported_beside_the_hops(self):
        _, fetch, send = self.hops.scan(self.stages)
        self.assertEqual(fetch[("default", "raw_table_1")], [2.0])
        self.assertEqual(send["candle-features-sink:_Writer"], [14.0])


if __name__ == "__main__":
    unittest.main()
