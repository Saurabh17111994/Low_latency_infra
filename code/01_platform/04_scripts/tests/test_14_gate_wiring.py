"""Guard tests for the Monday gate's own shape.

The gate is the certificate, so its structure is worth pinning: the 19 steps are numbered and never
renumbered, and the checks added for it must read files that exist in a fresh clone. These tests read
the gate script; they run nothing.
"""

from __future__ import annotations

import re
from pathlib import Path

GATE = Path(__file__).resolve().parents[1] / "run-monday-gates.sh"
TEXT = GATE.read_text()
STEP_HEADERS = re.findall(r"=== \[(\d+)/19\]", TEXT)


def test_the_gate_still_has_nineteen_steps_in_order():
    assert STEP_HEADERS == [str(n) for n in range(1, 20)], STEP_HEADERS


def test_the_gate_runs_the_deploy_preflight_against_the_tracked_template():
    assert "deploy_preflight.py" in TEXT
    assert "--env-file \"$SCRIPT_DIR/../01_docker/.env.example\"" in TEXT
    assert "--expect dev" in TEXT
    # It must not reach the lake during a gate run: no credentials, and a gate step that needs the
    # network is a gate step that fails for the wrong reason.
    assert "--check-lake" not in TEXT


def test_the_gate_never_names_an_untracked_environment_file():
    # code/01_platform/01_docker/.env is deliberately not committed; a gate step that reads it would
    # pass on the workstation and fail on a fresh clone (or worse, silently check nothing).
    assert "/01_docker/.env" not in TEXT.replace("/01_docker/.env.example", "")


def test_the_gate_refuses_an_empty_deploy_preflight_pass():
    assert "grep -q '^0 failure(s)$'" in TEXT
