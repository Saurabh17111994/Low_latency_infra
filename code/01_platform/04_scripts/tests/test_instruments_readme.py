#!/usr/bin/env python3
"""L6-3: the instruments README describes the tree that exists.

WHY THIS EXISTS
---------------
`05_instruments/README.md` described a fixture that no longer exists (a
TEST-ONLY 50-token manifest and an `instruments.csv` file table), named the
removed plural key `ARROW_INSTRUMENT_MANIFESTS` (no reader; P5-012), and linked
two dead paths (`manifests/README.md`, `/docs/09_data_gaps.md`). Nothing
checked README links, keys, or claims, so the wrong contract survived every
audit. The README is contract surface: it is what an operator reads before
dropping a manifest in.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* every markdown link resolves — relative to the README, or to the repo root
  for `/`-absolute links;
* the dead plural key is absent and the live keys are named;
* the fixture/`instruments.csv` claims stay deleted;
* the infra surfaces the README describes still exist (compose `x-manifest` /
  `x-manifest-dir`, the stack's `manifest-nse` Swarm config, the split helper).

The test is auto-discovered by gate step 3 (``test_*.py``), so the README joins
the Monday gate with no gate change.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
README = ROOT / "code/01_platform/05_instruments/README.md"
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
STACK = ROOT / "code/01_platform/01_docker/docker-stack.yml"
DEAD_KEY = "ARROW_INSTRUMENT_MANIFESTS"  # plural — never read by any app
LIVE_KEYS = (
    "INSTRUMENT_MANIFEST_PATH",
    "ARROW_INSTRUMENT_MANIFEST",
    "INSTRUMENT_MANIFEST_MIN_COUNT",
    "ARROW_HFT_CONNECTIONS",
)


def _readme_text() -> str:
    return README.read_text()


def test_every_readme_link_resolves():
    missing = []
    for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", _readme_text()):
        target = target.split("#", 1)[0].strip()
        if not target or "://" in target or target.startswith("mailto:"):
            continue
        path = (ROOT / target.lstrip("/")) if target.startswith("/") else (README.parent / target)
        if not path.exists():
            missing.append(target)
    assert not missing, "dead README link(s): " + ", ".join(missing)


def test_readme_states_the_live_key_contract():
    text = _readme_text()
    assert DEAD_KEY not in text, (
        f"{DEAD_KEY} is dead (no reader; P5-012) — the README must use "
        "ARROW_INSTRUMENT_MANIFEST (singular) and the single-CSV auto-shard path")
    for key in LIVE_KEYS:
        assert key in text, f"README must name the live key {key}"


def test_readme_does_not_claim_a_fixture_manifest():
    lowered = _readme_text().lower()
    for claim in ("test-only", "stub", "instruments.csv"):
        assert claim not in lowered, (
            f"README still claims {claim!r}: the loader refuses a missing/empty "
            "manifest and no fixture CSV lives in this directory")


def test_readme_surfaces_still_exist():
    compose = COMPOSE.read_text()
    stack = STACK.read_text()
    assert "x-manifest:" in compose and "x-manifest-dir:" in compose, (
        "the compose anchors the README describes are gone")
    assert "INSTRUMENT_MANIFEST_HOST_PATH" in compose, (
        "the single-file host env the README describes is gone")
    assert "INSTRUMENT_MANIFESTS_HOST_DIR" in compose, (
        "the host-dir env the README describes is gone")
    assert "manifest-nse:" in stack, "the stack's Swarm manifest config is gone"
    assert (README.parent / "split_manifest.py").exists(), (
        "the README links the split helper — it must exist")
