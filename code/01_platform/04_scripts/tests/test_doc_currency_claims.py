#!/usr/bin/env python3
"""M7-5: currency claims in live docs must match the tree, or say they are history.

WHY THIS EXISTS
---------------
Three claim classes kept rotting because nothing checked them:

* the gate size — docs quoted 13/13 while `run-monday-gates.sh` ran 19 steps;
* DDL file references — `00-start-here.md` pointed at `03_feature_candles_15s.sql`,
  deleted with the 15 s-candle era;
* snapshot sovereignty — a dated readiness snapshot may keep old numbers only if
  it is explicitly marked historical, so a reader can tell current from history.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* `GATE_TOTAL` in `run-monday-gates.sh` equals the count named by the two docs
  that claim the current gate size, and no gate-shaped `N/N` claim in them
  contradicts it without a same-line history marker.
* Every `ddl/**.sql` reference in a live doc resolves on disk, unless the same
  line marks it deleted/retired/historical/removed/superseded.
* The readiness snapshot heading is banner-marked historical.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
GATE_SCRIPT = ROOT / "code/01_platform/04_scripts/run-monday-gates.sh"
DDL_DIR = ROOT / "code/01_platform/02_sql/ddl"
DOCS = ROOT / "docs"

GATE_DOCS = [DOCS / "08_implementation/00-start-here.md", DOCS / "commands/COMMANDS.md"]
DDL_REF = re.compile(r"ddl/([0-9A-Za-z_./-]+\.sql)")
HISTORY_MARKERS = ("historical", "era", "dated", "13-step", "delete", "retired",
                   "removed", "superseded", "banner")


def gate_total() -> int:
    m = re.search(r"^GATE_TOTAL=(\d+)", GATE_SCRIPT.read_text(), re.MULTILINE)
    assert m, "run-monday-gates.sh must define GATE_TOTAL"
    return int(m.group(1))


def test_the_docs_name_the_current_gate_size():
    total = gate_total()
    start = (DOCS / "08_implementation/00-start-here.md").read_text()
    commands = (DOCS / "commands/COMMANDS.md").read_text()
    assert f"{total} steps" in start, f"00-start-here.md must name the current gate ({total} steps)"
    assert f"{total}/{total} verified" in start, \
        f"00-start-here.md must carry the standing {total}/{total} certificate"
    assert f"**{total} steps**" in commands, f"COMMANDS.md must name the current gate ({total} steps)"


def test_no_unmarked_gate_claim_contradicts_the_current_size():
    total = gate_total()
    for doc in GATE_DOCS:
        for i, line in enumerate(doc.read_text().splitlines(), 1):
            if "make gate" not in line and "GATE RESULT" not in line:
                continue
            claims = re.findall(r"\b(\d{1,2})/\1\b", line)
            if not claims or f"{total}/{total}" in line:
                continue
            low = line.lower()
            assert any(marker in low for marker in HISTORY_MARKERS), (
                f"{doc.relative_to(ROOT)}:{i}: gate claim {claims} contradicts "
                f"GATE_TOTAL={total} without a history marker: {line[:140]}")


def test_every_live_ddl_reference_resolves_or_is_marked_history():
    dead = []
    for doc in sorted(DOCS.rglob("*.md")):
        rel = doc.relative_to(DOCS)
        if "change-records" in rel.parts or "plans" in rel.parts:
            continue  # append-only history / dated rationale
        for i, line in enumerate(doc.read_text().splitlines(), 1):
            for ref in DDL_REF.findall(line):
                if (DDL_DIR / ref).exists():
                    continue
                if any(marker in line.lower() for marker in HISTORY_MARKERS):
                    continue
                dead.append(f"{doc.relative_to(ROOT)}:{i}: ddl/{ref}")
    assert not dead, "live docs reference DDL files that do not exist: " + "; ".join(dead[:5])


def test_the_readiness_snapshot_is_banner_marked_historical():
    start = (DOCS / "08_implementation/00-start-here.md").read_text()
    heading = next(ln for ln in start.splitlines()
                   if ln.startswith("## ") and "readiness" in ln.lower())
    assert re.search(r"historical|snapshot", heading, re.IGNORECASE), (
        "the readiness section must be marked historical/snapshot so its dated numbers "
        f"cannot be read as current: {heading}")


def test_the_live_readiness_plan_carries_its_supersession_banner():
    plan = (DOCS / "plans/2026-08-25-live-readiness-unified-plan.md").read_text()
    head = "\n".join(plan.splitlines()[:24])
    assert re.search(r"supersession banner", head, re.IGNORECASE), (
        "the 2026-08-25 plan must open with its additive supersession banner — without it "
        "the dated Fluss 0.9.1 / 13-step claims read as current")
    assert "1.0.0" in head and "19 steps" in head, (
        "the banner must name the successor era (Fluss 1.0.0, 19-step gate)")


def test_the_vocabulary_plan_records_its_execution():
    plan = (DOCS / "plans/2026-09-23-design-vocabulary-cleanup.md").read_text()
    status = next(ln for ln in plan.splitlines() if ln.startswith("**Status:**"))
    assert "EXECUTED" in status and "Nothing executed yet" not in status, (
        f"the vocabulary cleanup plan status must record its execution: {status}")
