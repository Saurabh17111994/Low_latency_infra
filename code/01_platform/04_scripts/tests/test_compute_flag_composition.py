#!/usr/bin/env python3
"""M3-3: STRATEGY_HOST_ENABLED and MULTITF_ENABLED must compose, not silently no-op.

WHY THIS EXISTS
---------------
The strategy-host branch lives inside `if (config.multiTfEnabled())`. A deployment with
`STRATEGY_HOST_ENABLED=true` and `MULTITF_ENABLED=false` parsed fine, wired nothing, and
the only warning about the inert STRATEGIES list was nested inside the disabled branch —
so the operator saw a healthy job that ran no strategies. The same silence hid a
strategies list set with the host off when multi-TF was also off.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* `SignalJobConfig` throws when host-on meets multi-TF-off, naming both flags.
* `SignalJob`'s strategies-set-but-host-off warning is at the method's top level (not
  inside the multi-TF branch) and names both flags.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
COMPUTE = ROOT / "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob"
JOB = COMPUTE / "SignalJob.java"
CONFIG = COMPUTE / "SignalJobConfig.java"

WARNING = "if (!config.strategyHostEnabled() && !config.strategyIds().isEmpty())"


def _multitf_branch_range(text: str) -> tuple[int, int]:
    """The [start, end] index range of `if (config.multiTfEnabled()) { ... }` in SignalJob."""
    start = text.index("if (config.multiTfEnabled()) {")
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return start, index
    raise AssertionError("unbalanced braces around the multiTf branch")


def test_config_refuses_host_on_with_multitf_off():
    text = CONFIG.read_text()
    assert "strategyHostEnabled && !multiTfEnabled" in text, \
        "the host-on/multi-TF-off combination must be refused at config load"
    match = re.search(
        r"strategyHostEnabled && !multiTfEnabled\)[\s\S]{0,600}?throw new IllegalStateException",
        text)
    assert match, "the refusal must be an IllegalStateException"
    block = text[match.start():match.start() + 800]
    assert "STRATEGY_HOST_ENABLED=true" in block and "MULTITF_ENABLED=true" in block, \
        "the message must name both flags and the required pairing"


def test_strategies_warning_is_hoisted_out_of_the_multitf_branch():
    text = JOB.read_text()
    assert "else if (!config.strategyIds().isEmpty())" not in text, \
        "the nested warning is back inside the multi-TF branch"
    _, branch_end = _multitf_branch_range(text)
    warn_at = text.index(WARNING)
    assert warn_at > branch_end, \
        "the strategies warning must sit at the method top level, after the multi-TF branch"
    block = text[warn_at:warn_at + 500]
    assert "STRATEGY_HOST_ENABLED=false" in block, "the warning must name the host flag"
    assert "MULTITF_ENABLED=" in block, "the warning must name the multi-TF flag too"
