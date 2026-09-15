#!/usr/bin/env python3
"""Wave 33 — P6-369: `fluss-client-metrics.py` dropped every sample whose
timestamp was not the exact integer on its output grid.

The grid is `range(start - 15, end + 30, 15)` and the old row loop looked the
series up with `series[col].get(g)` — an exact-key match. The timestamps come
from the Prometheus payload, where they are typed by the encoder, not by this
script: a float with a fractional part, or a string, misses every key. The
column then came out silently EMPTY while the run reported success, which is
the worst shape for a diagnosis tool — the operator reads "no data" as "the
metric was flat", not as "the lookup is broken".

The fix normalises keys with `int(float(t))` and forward-fills the grid lookup
(newest sample at or before the grid point), matching `fused_timeline.py`.

Driven in-process against the real module imported from a throwaway tree, with
`o2_get` patched out — no live OpenObserve, no network.
"""
import datetime
import importlib.util
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO = Path(__file__).resolve().parents[4]
METRICS_SRC = REPO / "code/01_platform/04_scripts/fluss-client-metrics.py"

START = 1_000_000
END = 1_000_060
# What the script derives from START/END; spelled out so the expectations below
# are readable and a change to the step is caught here rather than in prod.
GRID = list(range(START - 15, END + 30, 15))


def load_module(path: Path):
    """Import the hyphenated script by path (it is not a valid module name)."""
    spec = importlib.util.spec_from_file_location("w33_fluss_client_metrics", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def responses(values_by_name, names):
    """An `o2_get` stand-in: the label index plus one query_range per series."""
    def fake(path, params, auth):
        if params == {}:                                   # label __name__ values
            return {"data": names}
        return {"status": "success", "data": {"result": [
            {"metric": {"operator_name": "op-a"},
             "values": values_by_name[params["query"]]}]}}
    return fake


class MetricsCase(unittest.TestCase):
    """A throwaway tree with its own script copy and secrets file."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w33m-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        scripts = self.tmp / "code/01_platform/04_scripts"
        scripts.mkdir(parents=True)
        self.script = scripts / "fluss-client-metrics.py"
        shutil.copy2(METRICS_SRC, self.script)
        docker = self.tmp / "code/01_platform/01_docker"
        docker.mkdir(parents=True)
        (docker / "secrets.env").write_text("O2_AUTH_BASIC=user:pass\n")
        self.out = self.tmp / "metrics.tsv"
        self.mod = load_module(self.script)

    def run_main(self, fake_get) -> str:
        argv = ["fluss-client-metrics.py", "--start", str(START),
                "--end", str(END), "--out", str(self.out)]
        with mock.patch.object(self.mod, "o2_get", fake_get), \
                mock.patch.object(sys, "argv", argv):
            self.mod.main()
        return self.out.read_text()

    def column(self, tsv: str, name: str) -> list:
        lines = [l.split("\t") for l in tsv.strip().splitlines()]
        idx = lines[0].index(name)
        return [r[idx] for r in lines[1:]]


class ForwardFillTest(MetricsCase):
    """P6-369 — the lookup helper itself."""

    def test_exact_hit_wins(self):
        self.assertEqual(self.mod._at_or_before({10: 5.0, 20: 6.0}, 20), 6.0)

    def test_between_samples_uses_the_earlier_one(self):
        self.assertEqual(self.mod._at_or_before({10: 5.0, 20: 6.0}, 15), 5.0)

    def test_none_before_the_series_starts(self):
        self.assertIsNone(self.mod._at_or_before({10: 5.0}, 5))

    def test_empty_series_is_none(self):
        self.assertIsNone(self.mod._at_or_before({}, 10))

    def test_does_not_depend_on_insertion_order(self):
        """Keys are read via max(), not by walking until a miss."""
        self.assertEqual(self.mod._at_or_before({30: 7.0, 10: 5.0, 20: 6.0}, 25), 6.0)


class GridPopulationTest(MetricsCase):
    """P6-369 — end to end through `main()`, the shape that actually regressed."""

    NAME = "fluss_client_client_id_requestlatencyms_avg"

    def test_off_grid_float_timestamps_still_fill_the_column(self):
        """The reported defect: fractional timestamps emptied the column.

        Pre-fix, the keys were 1000000.4/1000015.9 while the grid asked for
        1000000/1000015, so `.get(g)` returned None for every row.
        """
        tsv = self.run_main(responses(
            {self.NAME: [[START + 0.4, "12.5"], [START + 15.9, "13.5"]]},
            [self.NAME]))
        # Grid is start-15 .. end+30: row 0 predates the series, then the two
        # samples carry forward. Pre-fix every row was blank, so this assertion
        # is the one that notices the regression.
        self.assertEqual(self.column(tsv, "req_lat_avg"),
                         ["", "12.50", "13.50", "13.50", "13.50", "13.50", "13.50"])

    def test_string_timestamps_are_normalised_too(self):
        tsv = self.run_main(responses(
            {self.NAME: [[str(START), "1.0"], [str(START + 15), "2.0"]]},
            [self.NAME]))
        self.assertEqual(self.column(tsv, "req_lat_avg")[:3],
                         ["", "1.00", "2.00"])

    def test_rows_before_the_first_sample_stay_empty(self):
        """First grid point is start-15; the series begins at start."""
        tsv = self.run_main(responses(
            {self.NAME: [[START, "9.0"]]}, [self.NAME]))
        self.assertEqual(self.column(tsv, "req_lat_avg")[0], "")

    def test_a_series_with_no_samples_is_omitted(self):
        """`by_t` stays empty, so the column never appears — not an all-blank
        column, which would imply the metric exists and is flat."""
        tsv = self.run_main(responses({self.NAME: []}, [self.NAME]))
        self.assertNotIn("req_lat_avg", tsv.splitlines()[0])

    def test_grid_and_header_are_intact(self):
        tsv = self.run_main(responses({self.NAME: [[START, "1.0"]]}, [self.NAME]))
        rows = tsv.strip().splitlines()
        self.assertEqual(rows[0], "epoch_s\tiso_time\treq_lat_avg")
        self.assertEqual([r.split("\t")[0] for r in rows[1:]],
                         [str(g) for g in GRID])
        iso = rows[1].split("\t")[1]
        self.assertEqual(
            iso, datetime.datetime.fromtimestamp(
                GRID[0], datetime.timezone.utc).isoformat())


if __name__ == "__main__":
    unittest.main()
