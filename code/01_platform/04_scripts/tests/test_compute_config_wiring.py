#!/usr/bin/env python3
"""M3-1: ALLOWED_LATENESS_MS must be one truth, not three hardcoded literals.

WHY THIS EXISTS
---------------
`SignalJobConfig` parsed `ALLOWED_LATENESS_MS` into `config.allowedLatenessMs()`, but
`MultiTimeframeAggregateFunction` hardcoded `5_000L` at its three arithmetic sites
(emitted-map eviction, the late-window gate, the emitted-map bound) and `SignalJob`
never passed the configured value. A deployment that set a different tolerance was
silently ignored — the env key half-existed, and only the default worked.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* The aggregator carries the `allowedLatenessMs` field and the 5-arg canonical
  constructor, and refuses a negative value.
* No bare lateness literal remains in the aggregator — the default is named once,
  in `SignalJobConfig`.
* `SignalJob` passes `config.allowedLatenessMs()` into the constructor.
* `SignalJobConfig` owns `DEFAULT_ALLOWED_LATENESS_MS` and the env parser uses it.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
COMPUTE = ROOT / "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob"
AGG = COMPUTE / "MultiTimeframeAggregateFunction.java"
JOB = COMPUTE / "SignalJob.java"
CONFIG = COMPUTE / "SignalJobConfig.java"


def test_aggregator_has_the_field_and_the_canonical_constructor():
    text = AGG.read_text()
    assert "private final long allowedLatenessMs;" in text, \
        "the aggregator must carry the configured tolerance as a field"
    assert re.search(
        r"public MultiTimeframeAggregateFunction\(long liveSnapshotIntervalMs, boolean sessionBypass,\s*"
        r"boolean signalContextEnabled, boolean emitLiveTick, long allowedLatenessMs,\s*"
        r"boolean marketTickEnabled\)", text), \
        "the 6-arg canonical constructor (CHG-505 adds marketTickEnabled) is missing"
    assert "allowedLatenessMs must be >=0" in text, \
        "a negative tolerance must be refused at construction"


def test_no_bare_lateness_literal_remains_in_the_aggregator():
    text = AGG.read_text()
    assert "5_000L" not in text, (
        "a hardcoded 5_000L lateness literal is back — ALLOWED_LATENESS_MS would be "
        "ignored at that site again")
    assert "SignalJobConfig.DEFAULT_ALLOWED_LATENESS_MS" in text, \
        "the default must be named once, in SignalJobConfig"


def test_job_passes_the_configured_lateness_into_the_constructor():
    text = JOB.read_text()
    match = re.search(
        r"new MultiTimeframeAggregateFunction\([\s\S]{0,400}?config\.allowedLatenessMs\(\)",
        text)
    assert match, "SignalJob must pass config.allowedLatenessMs() to the aggregator"


def test_config_default_constant_is_the_single_source():
    text = CONFIG.read_text()
    assert "public static final long DEFAULT_ALLOWED_LATENESS_MS = 5_000L;" in text, \
        "the public default constant is missing"
    assert re.search(r'"ALLOWED_LATENESS_MS",\s*DEFAULT_ALLOWED_LATENESS_MS\)', text), \
        "the env parser must use the constant, not a second literal"
