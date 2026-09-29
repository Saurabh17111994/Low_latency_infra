#!/usr/bin/env python3
"""H6-3: LIVE facts-ledger claims must match the tree they describe.

WHY THIS EXISTS
---------------
`docs/ENVIRONMENT.md` is the machine-truth ledger and `env_facts.py check` runs each
row's `Check:` line. But a row's prose can outlive the thing it describes: FACT-009
stayed LIVE saying the Flink image was "unpushed" after the GHCR push, and FACT-001
stayed LIVE saying this PC had no manager after it became the one-node Leader.
Nothing bound a claim to `runtime.lock`, so the ledger kept claims the tree had
outgrown.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* Claim rule: a LIVE row may carry `Claim: VAR=value` lines. `*_IMAGE`/`*_VERSION`
  are compared byte-for-byte with `runtime.lock`; an unregistered variable fails
  closed with "register the source", and "no claims checked" fails so the guard
  cannot go vacuous.
* Stale-pin rule: a LIVE row claiming a pin is "unpushed"/"blocked on push" is
  false while `runtime.lock` already pins a pushed `ghcr.io/...@sha256:` digest.
* Host-state rule: the superseded host-state phrases may not appear in a LIVE row —
  the 2026-09-21 measurement (one node, this host Leader, two local deploys)
  superseded FACT-001/FACT-012, and the successor bodies must carry that anchor.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code/01_platform/04_scripts"
sys.path.insert(0, str(SCRIPTS))
import env_facts  # noqa: E402

LEDGER = ROOT / "docs/ENVIRONMENT.md"
RUNTIME_LOCK = ROOT / "code/01_platform/01_docker/runtime.lock"

CLAIM_RE = re.compile(r"^Claim:\s*([A-Z][A-Z0-9_]*)=(.*)$", re.M)
LOCK_RE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$", re.M)
# A registry manifest digest can only come from a push.
PUSHED_DIGEST_RE = re.compile(r"^ghcr\.io/.*@sha256:[0-9a-f]{64}$")
STALE_PIN_PHRASES = ("unpushed", "blocked on push", "cannot move yet")
STALE_HOST_PHRASES = ("not a manager", "no manager",
                      "no real stack deploy has ever run")


def _live_rows():
    _, rows, errors = env_facts.parse_ledger(LEDGER.read_text())
    assert not errors, f"ledger shape errors: {errors}"
    return [r for r in rows if r["fields"].get("Status") == "LIVE"]


def _row_text(row):
    return "\n".join([row["title"], *row["fields"].values(), *row["body"]])


def _runtime_lock():
    return dict(LOCK_RE.findall(RUNTIME_LOCK.read_text()))


def test_live_claims_match_runtime_lock():
    lock = _runtime_lock()
    checked = 0
    for row in _live_rows():
        for var, value in CLAIM_RE.findall(_row_text(row)):
            assert var.endswith(("_IMAGE", "_VERSION")), (
                f"{row['id']}: Claim variable {var!r} has no registered source — add it "
                "to this test's registry (runtime.lock today) or drop the claim")
            assert var in lock, f"{row['id']}: Claim {var} is not in runtime.lock"
            assert value == lock[var], (
                f"{row['id']}: Claim {var}={value} but runtime.lock says {lock[var]}")
            checked += 1
    assert checked >= 2, (
        f"only {checked} LIVE digest claim(s) checked — the pushed-pin successors must "
        "carry Claim: lines or this guard is vacuous")


def test_no_live_row_claims_an_unpushed_pin():
    lock = _runtime_lock()
    pushed = [v for v, val in lock.items() if PUSHED_DIGEST_RE.match(val)]
    assert pushed, "runtime.lock pins no pushed GHCR digest — update this guard's premise"
    for row in _live_rows():
        text = _row_text(row).lower()
        for phrase in STALE_PIN_PHRASES:
            assert phrase not in text, (
                f"{row['id']} is LIVE and says {phrase!r} while runtime.lock pins pushed "
                f"digests {pushed} — supersede the row (never edit it in place)")


def test_no_live_row_claims_the_superseded_host_state():
    for row in _live_rows():
        text = _row_text(row).lower()
        for phrase in STALE_HOST_PHRASES:
            assert phrase not in text, (
                f"{row['id']} is LIVE and claims {phrase!r}; the 2026-09-21 measurement "
                "(one node, this host Leader, two local stack deploys) superseded it — "
                "append a successor row")


def test_the_2026_09_21_measurement_is_recorded_in_a_live_body():
    bodies = "\n".join("\n".join(r["body"]) for r in _live_rows())
    assert "2026-09-21" in bodies, (
        "no LIVE row body carries the 2026-09-21 host/deploy measurement — the "
        "supersession anchor must stay recorded")
    assert "Leader" in bodies, "the one-node Leader state must be recorded in a LIVE body"
