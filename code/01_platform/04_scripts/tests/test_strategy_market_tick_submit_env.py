"""Static pin (gate step 3, CHG-505): STRATEGY_MARKET_TICK_ENABLED reaches the job.

WHY THIS EXISTS
---------------
The market-only row emission lives inside the SignalJob (``SignalJobConfig``
reads ``System.getenv()``). Two independent paths must carry the flag or the
feature silently no-ops — the exact failure class as W1's
``FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS`` gap found 2026-09-30:

* the shared compose anchor (jobmanager / taskmanager / compute launcher
  process environment, interpolated from ``.env`` at recreate time), and
* the ``pipeline_submit_job`` exec pass-through (the launcher shell).

A real submit needs a live cluster, so this is a static pin by design. The
host-shell default in the pass-through must match ``SignalJobConfig``'s
(default false).

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* ``pipeline_submit_job`` forwards ``-e STRATEGY_MARKET_TICK_ENABLED=...``.
* The compose anchor carries ``STRATEGY_MARKET_TICK_ENABLED: ${...:-false}``.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
LIB = REPO / "code" / "01_platform" / "04_scripts" / "pipeline-lib.sh"
COMPOSE = REPO / "code" / "01_platform" / "01_docker" / "docker-compose.yml"

SUBMIT_LINE = '-e STRATEGY_MARKET_TICK_ENABLED="${STRATEGY_MARKET_TICK_ENABLED:-false}"'
COMPOSE_LINE = "STRATEGY_MARKET_TICK_ENABLED: ${STRATEGY_MARKET_TICK_ENABLED:-false}"


def test_pipeline_submit_forwards_the_market_tick_flag() -> None:
    lib = LIB.read_text(encoding="utf-8")
    assert SUBMIT_LINE in lib, (
        "pipeline_submit_job no longer forwards STRATEGY_MARKET_TICK_ENABLED — "
        "the live smoke would run with the feature silently off")


def test_compose_anchor_carries_the_market_tick_flag() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    assert COMPOSE_LINE in compose, (
        "the shared JM/TM/compute anchor must carry STRATEGY_MARKET_TICK_ENABLED "
        "so a recreate reads .env instead of interpolating nothing")
