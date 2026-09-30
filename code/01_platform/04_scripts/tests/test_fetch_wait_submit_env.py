"""Static pin (gate step 3, CHG-449/W1): ``pipeline_submit_job`` forwards the
Fluss scanner fetch-wait cap to the ``flink run`` client.

The Fluss source's chunk-accumulation wait is the biggest contributor to the
ingest->monitor latency decomposition (W0 analysis section 2: ``fetchLatencyMs``
reads 20 ms on every raw subtask, every snapshot). The knob only exists for a
run if the submit path passes it through ``docker compose exec -e`` and the
host-shell default matches ``SignalJobConfig``'s default (20 ms); otherwise a
trial that sets ``FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS=2`` silently measures
20 ms -- the same silent-gap failure class as the STRATEGY_CONTEXT_*
pass-throughs (``test_strategy_context_submit_env.py``).

A real submit needs a live cluster, so this is a static pin by design.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
LIB = REPO / "code" / "01_platform" / "04_scripts" / "pipeline-lib.sh"
CONFIG = (
    REPO / "code" / "02_services" / "02_compute" / "src" / "main" / "java"
    / "com" / "trading" / "compute" / "signaljob" / "SignalJobConfig.java"
)

REQUIRED = (
    '-e FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS='
    '"${FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS:-20}"'
)


def test_pipeline_submit_forwards_fetch_wait_knob() -> None:
    lib = LIB.read_text(encoding="utf-8")
    assert REQUIRED in lib, (
        "pipeline_submit_job no longer forwards the Fluss scanner fetch-wait "
        f"knob; expected line: {REQUIRED}"
    )


def test_submit_default_matches_signal_job_config_default() -> None:
    lib = LIB.read_text(encoding="utf-8")
    shell_default = re.search(r"FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS:-(\d+)", lib)
    assert shell_default, "the pass-through line has no numeric default"
    config_src = CONFIG.read_text(encoding="utf-8")
    config_default = re.search(
        r'"FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS",\s*(\d+)L', config_src)
    assert config_default, "SignalJobConfig no longer declares the key"
    assert shell_default.group(1) == config_default.group(1), (
        "the host-shell default and the SignalJobConfig default must match "
        "(a mismatch silently changes the measured knob)"
    )
