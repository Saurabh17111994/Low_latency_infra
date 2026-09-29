"""Guard tests for the Monday gate's own shape.

The gate is the certificate, so its structure is worth pinning: the 19 steps are numbered and never
renumbered, and the checks added for it must read files that exist in a fresh clone. These tests read
the gate script; they run nothing.
"""

from __future__ import annotations

import re
import subprocess
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


# ── H4-3: a WARN that skips a step must be counted ──────────────────────────


def _skip_warn_offenders():
    """`echo "WARN: …skipping…"` lines that do not record the skip.

    The message may continue on the next line (step 7's base-image WARN did), so the
    judgement window is the line plus its continuation.
    """
    lines = TEXT.splitlines()
    offenders = []
    for i, line in enumerate(lines):
        if 'echo "WARN:' not in line:
            continue
        window = " ".join(lines[i : i + 2])
        if "skipping" not in window:
            continue
        if "warn_skip" in line:
            continue
        if "shellcheck not installed" in window:
            continue  # allow-listed: step 1 still runs bash -n (partial check)
        offenders.append(f"line {i + 1}: {line.strip()}")
    return offenders


def test_every_skip_warn_is_counted_by_warn_skip():
    """H4-3 (P1-4): the verdict subtracts GATE_SKIPS — an uncounted WARN lies.

    Steps 2 and 7 printed their skip WARN with a bare `echo`; `note_skip` ran only in
    the later SKIP branches, so a run that skipped compose-config or the build smoke
    still certified the full step count. The shellcheck WARN is the one allow-listed
    exception: step 1 still runs `bash -n` on every script, so that step is partially
    verified rather than skipped.
    """
    assert "warn_skip() { note_skip" in TEXT, "warn_skip must record the skip via note_skip"
    offenders = _skip_warn_offenders()
    assert offenders == [], f"skip WARNs that do not record the skip: {offenders}"


def test_warn_skip_records_the_step_and_prints_the_warn():
    """Behavioral: lift the real helpers out of the gate and drive them once."""
    note = re.search(r"note_skip\(\) \{([^}]*)\}", TEXT)
    warn = re.search(r"warn_skip\(\) \{([^}]*)\}", TEXT)
    assert note and warn, "note_skip/warn_skip helpers missing from the gate"
    script = (
        'GATE_SKIPS=0; SKIPPED_STEPS=""; SUMMARY=/dev/null\n'
        f"note_skip() {{{note.group(1)}}}\n"
        f"warn_skip() {{{warn.group(1)}}}\n"
        'warn_skip 2 "compose file or env files missing — skipping compose config"\n'
        'warn_skip 7 "docker unavailable — skipping build smoke"\n'
        'echo "skips=$GATE_SKIPS steps=$SKIPPED_STEPS"\n'
    )
    proc = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "WARN: compose file or env files missing" in proc.stdout
    assert "WARN: docker unavailable" in proc.stdout
    assert "skips=2 steps= 2 7" in proc.stdout, proc.stdout
