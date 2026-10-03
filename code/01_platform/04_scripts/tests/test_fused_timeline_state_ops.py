"""Drift guard for fused_timeline.py's RocksDB operator filter (CHG-339).

The RDB_SIGNALS operator substrings must name managed-state operators that
exist in the current Signal job. The retired single-timeframe era's operators
(`fingerprint_dedup`, `forming_bar`) hold no managed state (DEC-054; the
2026-09-05 multi-timeframe cutover), so matching them yields silent empty
series and hides the real hot spots. This test pins the current set: the
strategy host (`strategy-host-emitted-ids`) is the only managed-state operator
left.
"""

import os
import sys

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)
import fused_timeline as ft  # noqa: E402

RETIRED_OPERATORS = ("fingerprint_dedup", "forming_bar", "feature_candles_15s")


def test_rdb_signals_do_not_match_retired_state_operators():
    """Retired operators must never re-enter the RocksDB filter."""
    for col, _suffix, ops in ft.RDB_SIGNALS:
        for op in ops:
            assert op not in RETIRED_OPERATORS, (
                f"{col}: retired operator {op!r} in RocksDB filter")


def test_rdb_signals_cover_the_current_managed_state_operator():
    """The strategy host is the only managed-state operator left."""
    ops = {op for _col, _suffix, ops_tuple in ft.RDB_SIGNALS for op in ops_tuple}
    assert "strategy-host" in ops, (
        "strategy-host missing from the RocksDB filter — no current operator "
        "would be matched")


def test_rdb_signals_columns_are_unique_and_complete():
    """One row per fused-timeline column; no silent duplicates."""
    cols = [col for col, _suffix, _ops in ft.RDB_SIGNALS]
    assert len(cols) == len(set(cols)) == 8, f"unexpected column set: {cols}"


def test_rdb_forward_fill_uses_the_bounded_helper():
    """XC-11: the RocksDB path must expire like the Prometheus path (XC-6)."""
    src = open(os.path.join(SCRIPTS, "fused_timeline.py"), encoding="utf-8").read()
    rdb = src.split("def _fetch_rdb_signals", 1)[1].split("\ndef ", 1)[0]
    assert 'forward_fill(grid, by_t, "max")' in rdb, (
        "the RocksDB fill must go through the bounded forward_fill helper")
    assert "last_val" not in rdb, (
        "no carried value may survive an expired sample")
