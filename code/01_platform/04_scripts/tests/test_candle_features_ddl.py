#!/usr/bin/env python3
"""DEC-059 (W-B1): the merged candle+feature table contract.

The merged table supersedes `candle_live` + `candle_closed` + `feature_values`:
it carries candle_closed's column contract, adds `features MAP<INT, DOUBLE>`
(keyed by the append-only registry ids, DEC-057) and `sealed BOOLEAN`, retains
3 d locally, and carries lake options **opt-in** (DEC-060: one selectable
archive entry, never a merged lake table).
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "code/01_platform/04_scripts"))

import ddl_apply  # noqa: E402

DDL_DIR = ROOT / "code/01_platform/02_sql/ddl"


def _columns(ddl_text: str) -> list[tuple[str, str]]:
    """(name, type-and-constraints) per column line, DDL order; comments skipped."""
    after_create = ddl_text.split("CREATE TABLE", 1)[1]
    body = after_create.split("(", 1)[1].split("PRIMARY KEY", 1)[0]
    columns: list[tuple[str, str]] = []
    for raw in body.splitlines():
        line = raw.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            columns.append((parts[0], " ".join(parts[1:])))
    return columns


class CandleFeaturesDdlTest(unittest.TestCase):
    def test_merged_table_carries_the_candle_contract_plus_features_and_sealed(self) -> None:
        merged = (DDL_DIR / "35_candle_features.sql").read_text()

        # Wave C W-C5a (DEC-059): the old reference DDLs (32 candle_live,
        # 33 candle_closed, 34 feature_values) were retired with their tables
        # (dropped on dev in W-C7). The candle contract is pinned as literals
        # here so column drift in the single surviving candle table still fails.
        expected_candle = [
            ("instrument_token", "BIGINT NOT NULL"),
            ("exchange", "STRING"),
            ("symbol", "STRING"),
            ("tf", "STRING NOT NULL"),
            ("window_start", "BIGINT NOT NULL"),
            ("window_end", "BIGINT NOT NULL"),
            ("open_paise", "BIGINT NOT NULL"),
            ("high_paise", "BIGINT NOT NULL"),
            ("low_paise", "BIGINT NOT NULL"),
            ("close_paise", "BIGINT NOT NULL"),
            ("volume", "BIGINT NOT NULL"),
            ("tick_count", "INT NOT NULL"),
            ("last_event_time", "BIGINT NOT NULL"),
            ("last_event_fingerprint", "STRING"),
            ("schema_version", "STRING NOT NULL"),
        ]

        self.assertEqual("candle_features", ddl_apply.parse_table_name(merged))
        self.assertEqual(["instrument_token", "tf", "window_start"],
                         [part.strip()
                          for part in ddl_apply.parse_primary_key(merged).split(",")])
        self.assertEqual(
            expected_candle + [("features", "MAP<INT, DOUBLE>"),
                               ("sealed", "BOOLEAN NOT NULL")],
            _columns(merged))

    def test_merged_table_is_3d_and_lake_opt_in(self) -> None:
        options = ddl_apply.parse_with_options(
            (DDL_DIR / "35_candle_features.sql").read_text())
        self.assertEqual("3d", options.get("table.log.ttl"))
        self.assertEqual("false", options.get("table.datalake.enabled"))
        self.assertEqual("iceberg", options.get("table.datalake.format"))
        self.assertEqual("2", options.get("table.kv.format-version"))
        self.assertEqual("16", options.get("bucket.num"))
        self.assertEqual("instrument_token", options.get("bucket.key"))

    def test_merged_primary_key_is_the_single_writer_key(self) -> None:
        primary_key = ddl_apply.parse_primary_key(
            (DDL_DIR / "35_candle_features.sql").read_text())
        self.assertEqual(["instrument_token", "tf", "window_start"],
                         [part.strip() for part in primary_key.split(",")])


if __name__ == "__main__":
    unittest.main()
