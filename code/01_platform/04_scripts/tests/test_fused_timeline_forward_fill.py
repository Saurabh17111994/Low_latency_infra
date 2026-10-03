"""XC-6 (G11 verification): fused_timeline forward-fill must expire.

P6-401 bounded the sample age at 120 s but kept carrying the last value, so a
metric that stopped scraping was still forward-filled to the grid's end — the
exact bug the P6-401 comment claims to have fixed. These tests pin the
expiry: a grid point with no sample within the bound stays empty.
"""
import os
import sys

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)
import fused_timeline as ft  # noqa: E402


def test_forward_fill_expires_after_the_bound():
    """A stopped scrape must not be carried past the 120 s bound."""
    by_t = {1000: [5.0]}
    grid = [1000, 1060, 1120, 1180, 1240]
    out = ft.forward_fill(grid, by_t, "max", max_age_s=120)
    assert out == {1000: 5.0, 1060: 5.0, 1120: 5.0}, (
        f"grid points >120s past the last sample must be empty, got {out}")


def test_forward_fill_resumes_when_a_fresh_sample_arrives():
    by_t = {1000: [5.0], 1300: [9.0]}
    out = ft.forward_fill([1120, 1180, 1300, 1330, 1420, 1480],
                          by_t, "max", max_age_s=120)
    assert out == {1120: 5.0, 1300: 9.0, 1330: 9.0, 1420: 9.0}, out


def test_forward_fill_uses_newest_sample_at_or_before_grid_point():
    by_t = {1000: [3.0, 4.0], 1060: [9.0]}
    out = ft.forward_fill([1000, 1030, 1060], by_t, "sum", max_age_s=120)
    assert out[1000] == 7.0   # sum aggregation
    assert out[1030] == 7.0   # newest at or before 1030 is still 1000
    assert out[1060] == 9.0
