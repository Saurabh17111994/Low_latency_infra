#!/usr/bin/env python3
"""No variable may be read in a step that does not assign it.

`--steps` and `--sweep` run a subset of the 16 steps, so any value a step reads
must be assigned either in that same step or in the region before step 1 — which
always runs. `set -u` turns a violation into a hard abort that looks like a broken
step: `--steps 2` died on `STATIC_LOG: unbound variable` (assigned in step 1, read
by step 2) and `--steps 8` died on `COMPOSE_FILE: unbound variable` (assigned in
step 2, read by step 8). Both were real, both cost a gate run, and neither is
visible by reading one step.

This walks the gate script, groups every assignment and every `$VAR` read by the
region it sits in (a step body, or the shared region outside all steps), and
requires reads(region) ⊆ assigned(region) ∪ assigned(shared). Loop variables,
positional parameters and `$$` are not variables in this sense and are ignored.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

GATE = Path(__file__).resolve().parents[1] / "run-monday-gates.sh"

STEP = re.compile(r"^if step_active (\d+); then$")
ASSIGN = re.compile(r"^\s*(?:export\s+|local\s+|readonly\s+|declare\s+|typeset\s+)*([A-Z_][A-Z0-9_]*)\+?=")
MAPFILE = re.compile(r"^\s*mapfile\s+(?:-\w+\s+)*([A-Z_][A-Z0-9_]*)")
READ = re.compile(r"\$\{?([A-Z_][A-Z0-9_]*)")
IGNORE = {"@", "#", "?", "!", "*", "-", "0", "1", "2", "$", "PPID", "RANDOM", "LINENO",
          "BASH_SOURCE", "BASH_REMATCH", "PIPESTATUS", "IFS", "_"}
# Supplied by the caller's environment, not by this script. Every read of these
# must carry a `:-`/`:=` default, or a --steps run in a bare shell aborts.
ENV_PROVIDED = {"FLUSS_BOOTSTRAP", "GATE_ALLOW_NO_FLUSS", "GATE_SWEEP_FROM", "GATE_SWEEP_CHILD"}


def regions() -> dict[str, set[str]]:
    """Region label -> identifiers assigned there."""
    out: dict[str, set[str]] = {}
    step = None
    for line in GATE.read_text(encoding="utf-8").split("\n"):
        m = STEP.match(line)
        if m:
            step = m.group(1)
        label = f"step{step}" if step else "shared"
        out.setdefault(label, set())
        for pattern in (ASSIGN, MAPFILE):
            a = pattern.match(line)
            if a and a.group(1) not in IGNORE:
                out[label].add(a.group(1))
    return out


def assignments() -> dict[str, set[str]]:
    return regions()


def reads() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    step = None
    for line in GATE.read_text(encoding="utf-8").split("\n"):
        m = STEP.match(line)
        if m:
            step = m.group(1)
        label = f"step{step}" if step else "shared"
        for var in READ.findall(line):
            if var not in IGNORE:
                out.setdefault(label, set()).add(var)
    return out


class GateVariableScoping(unittest.TestCase):
    def test_no_read_outside_the_assigning_region(self) -> None:
        assigned, read = assignments(), reads()
        shared = assigned.get("shared", set())
        offenders = []
        for label, vars_ in sorted(read.items()):
            missing = sorted(v for v in vars_ if v not in assigned.get(label, set())
                             and v not in shared and v not in ENV_PROVIDED)
            if missing:
                offenders.append(f"{label}: {', '.join(missing)}")
        self.assertEqual(
            offenders, [],
            "a step reads a variable that neither it nor the shared region assigns — "
            "a --steps run that skips the assigning step will abort on set -u:\n  " + "\n  ".join(offenders),
        )

    def test_env_provided_vars_are_read_with_a_default(self) -> None:
        """`--steps 11` in a shell without FLUSS_BOOTSTRAP must not abort."""
        text = GATE.read_text(encoding="utf-8")
        for var in sorted(ENV_PROVIDED):
            hits = [n for n, line in enumerate(text.split("\n"), 1)
                    if re.search(r"\$\{?" + var + r"[^_}]*\}", line)
                    and not re.search(r"\$\{" + var + r":[-=]", line)
                    and var + "=" not in line and "export " + var not in line]
            self.assertEqual(hits, [], f"{var} is read at line(s) {hits} without a :- default")

    def test_shared_region_assigns_every_path_constant(self) -> None:
        shared = assignments()["shared"]
        for name in ("OUT_DIR", "SUMMARY", "SWEEP_FAILED_FILE", "COMPOSE_FILE", "COMPOSE_ENV_DIR"):
            self.assertIn(name, shared, f"{name} must be assigned before the steps")


if __name__ == "__main__":
    unittest.main()
