#!/usr/bin/env python3
"""M3-4 / DEC-058: the watermark/lateness/idleness defaults are one truth in code AND docs.

WHY THIS EXISTS
---------------
The code default for `WATERMARK_OUT_OF_ORDER_MS` is 500 ms (single-timeline rule
2026-08-30; measured: the 5 s default produced p95≈5.7 s end-to-end, the wait itself),
pinned by `SignalJobConfigTest`. Six doc sites still said 5 s — a deploy reading the docs
would have assumed a tolerance ten times the code's. `ALLOWED_LATENESS_MS` (5 s) is a
different knob and keeps its default.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* The Java default for `WATERMARK_OUT_OF_ORDER_MS` is 500 ms, the test pin still says
  500L, and `ALLOWED_LATENESS_MS`/`SOURCE_IDLE_MS` keep their documented defaults.
* Every named doc site carries the 500 ms shape for the out-of-orderness bound, and none
  carries the stale 5 s shape.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
COMPUTE = ROOT / "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob"
CONFIG = COMPUTE / "SignalJobConfig.java"
CONFIG_TEST = (ROOT / "code/02_services/02_compute/src/test/java/com/trading/compute/"
                      "signaljob/SignalJobConfigTest.java")
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"

DOSSIER = ROOT / "docs/08_implementation/04-signal-job.md"
CONTRACT = ROOT / "docs/04_contracts/03-compute.md"
REQUIREMENTS = ROOT / "docs/02_requirements/02-functional/03-compute.md"
PIPELINE = ROOT / "docs/03_architecture/02-data-pipeline.md"
AUDIT = ROOT / "docs/08_implementation/24-configuration-audit.md"

DOC_SITES = (DOSSIER, CONTRACT, REQUIREMENTS, PIPELINE, AUDIT)
STALE_SHAPES = (
    "out-of-orderness (default 5 s)",
    "out-of-orderness of 5 seconds",
    "out-of-orderness of five seconds",
    "five seconds bounded out-of-orderness",
    "WATERMARK_OUT_OF_ORDER_MS=5000",
    "silently uses 5s",
)


def _longvalue_default(key: str) -> int:
    match = re.search(rf'"{key}",\s*([0-9_]+)L', CONFIG.read_text())
    assert match, f"{key} default not found in SignalJobConfig"
    return int(match.group(1).replace("_", ""))


def _constant(name: str) -> int:
    match = re.search(rf"long {name} = ([0-9_]+)L", CONFIG.read_text())
    assert match, f"{name} not found in SignalJobConfig"
    return int(match.group(1).replace("_", ""))


def test_code_defaults_and_the_test_pin_agree():
    assert _longvalue_default("WATERMARK_OUT_OF_ORDER_MS") == 500
    assert _constant("DEFAULT_ALLOWED_LATENESS_MS") == 5_000
    assert _longvalue_default("SOURCE_IDLE_MS") == 15_000
    assert "assertEquals(500L, cfg.outOfOrderMs());" in CONFIG_TEST.read_text(), \
        "the out-of-orderness pin must stay 500L"


def test_every_doc_site_carries_the_500_ms_bound():
    assert "watermark out-of-orderness (default 500 ms)" in REQUIREMENTS.read_text()
    assert "bounded out-of-orderness of 500 milliseconds" in REQUIREMENTS.read_text()
    assert "bounded out-of-orderness of 500 milliseconds" in CONTRACT.read_text()
    assert "500 milliseconds bounded out-of-orderness" in PIPELINE.read_text()
    assert "`WATERMARK_OUT_OF_ORDER_MS=500`" in AUDIT.read_text()
    dossier = DOSSIER.read_text()
    assert "| `WATERMARK_OUT_OF_ORDER_MS` | Default 500" in dossier
    assert "`WATERMARK_OUT_OF_ORDER_MS=500`, `ALLOWED_LATENESS_MS=5000`" in dossier


def test_no_doc_site_keeps_the_stale_5_s_shape():
    for path in DOC_SITES:
        text = path.read_text()
        for shape in STALE_SHAPES:
            assert shape not in text, f"{path.name} still carries the stale shape {shape!r}"
    # The lateness row legitimately says 5000; the watermark row must not.
    assert "| `WATERMARK_OUT_OF_ORDER_MS` | Default 5000" not in DOSSIER.read_text()


def test_lateness_and_idleness_defaults_stay_documented():
    text = REQUIREMENTS.read_text()
    assert "allowed lateness (default 5 s)" in text
    assert "source idleness (default 15 s)" in text


# ---------------------------------------------------------------------------
# L3-1: the dedup bound is DEDUP_WINDOW_ENTRIES (DEC-054) — the dossier must say so


def _platform_int_constant(name: str) -> int:
    match = re.search(rf"int {name} = ([0-9_]+);", PLATFORM_CONFIG.read_text())
    assert match, f"{name} not found in PlatformConfig"
    return int(match.group(1).replace("_", ""))


def _config_contract_section() -> str:
    """The live configuration table: from its heading to the next level-3 heading."""
    dossier = DOSSIER.read_text()
    start = dossier.index("### Configuration contract")
    rest = dossier[start:]
    return rest[: rest.index("\n### ", 1)]


def test_the_live_config_table_carries_one_dedup_bound_row_equal_to_the_constant():
    bound = _platform_int_constant("DEDUP_WINDOW_ENTRIES")
    assert bound == 200, "the platform dedup bound moved — re-check DEC-054 before editing docs"
    section = _config_contract_section()
    rows = [line for line in section.splitlines() if line.startswith("| `DEDUP_WINDOW_ENTRIES`")]
    assert len(rows) == 1, f"exactly one DEDUP_WINDOW_ENTRIES row belongs in the live table: {rows}"
    assert f"`{bound}`" in rows[0], rows[0]
    ttl_rows = [line for line in section.splitlines() if line.startswith("| `DEDUP_TTL_MS`")]
    assert not ttl_rows, \
        f"DEDUP_TTL_MS is retired (DEC-054); a live config-table row is back: {ttl_rows}"


def test_every_sig_unit_003_line_names_the_dedup_bound():
    for i, line in enumerate(DOSSIER.read_text().splitlines(), 1):
        if "SIG-UNIT-003" in line:
            assert "DEDUP_WINDOW_ENTRIES" in line, (
                f"04-signal-job.md:{i}: SIG-UNIT-003 is the dedup-bound requirement — "
                f"the line must name DEDUP_WINDOW_ENTRIES: {line[:140]}")


def test_the_dossier_banner_covers_every_ttl_mention():
    dossier = DOSSIER.read_text()
    flat = re.sub(r"[\s>]+", " ", dossier)  # markdown wraps lines; compare flattened
    assert ("Every `DEDUP_TTL_MS`, `60 000`/`60000` and `StateTtlConfig` mention in this dossier"
            in flat), "the heap-window banner must mark every TTL/StateTtlConfig mention historical"
    assert "`DEDUP_WINDOW_ENTRIES` = 200" in flat, "the banner must name the live bound"
