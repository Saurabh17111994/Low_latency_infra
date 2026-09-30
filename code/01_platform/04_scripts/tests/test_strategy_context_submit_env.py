"""Static pin (gate step 3, CHG-445): ``pipeline_submit_job`` forwards the
STRATEGY_CONTEXT_* env keys to the ``flink run`` client.

The context provider is created inside the SignalJob from
``SignalJobConfig.fromEnv()`` — its knobs only exist if the submit path passes
them through ``docker compose exec -e``. The stage profiler and any measurement
run set them in the host shell; without these lines the C1 smoke would run
with the provider silently disabled (the exact failure class as W1's
FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS gap, found 2026-09-30).

A real submit needs a live cluster, so this is a static pin by design.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
LIB = REPO / "code" / "01_platform" / "04_scripts" / "pipeline-lib.sh"

# line-for-line: the value expressions are part of the contract (host shell
# default must match the SignalJobConfig default).
REQUIRED = [
    '-e STRATEGY_CONTEXT_ENABLED="${STRATEGY_CONTEXT_ENABLED:-false}"',
    '-e STRATEGY_CONTEXT_CACHE_BYTES="${STRATEGY_CONTEXT_CACHE_BYTES:-8388608}"',
    '-e STRATEGY_CONTEXT_MAX_INFLIGHT="${STRATEGY_CONTEXT_MAX_INFLIGHT:-32}"',
    '-e STRATEGY_CONTEXT_FETCH_TIMEOUT_MS="${STRATEGY_CONTEXT_FETCH_TIMEOUT_MS:-100}"',
    '-e STRATEGY_CONTEXT_RETRY_COOLDOWN_MS="${STRATEGY_CONTEXT_RETRY_COOLDOWN_MS:-250}"',
]


def test_pipeline_submit_forwards_strategy_context_env() -> None:
    lib = LIB.read_text(encoding="utf-8")
    missing = [line for line in REQUIRED if line not in lib]
    assert not missing, (
        "pipeline_submit_job no longer forwards these STRATEGY_CONTEXT_* "
        f"pass-throughs: {missing}"
    )
