"""Steps 14/16 must not re-run the live classes that step 9's drill already owns.

Every drill class self-gates on ``FLUSS_BOOTSTRAP`` (``assumeTrue(getenv(...))``),
so a module suite that runs with the gate's bootstrap in the environment executes
them a second time against the live cluster. That is what failed run 4b on
2026-09-18: the second B4 run waited 420 s on the coordinator's table-deletion
backlog, timed out, and its cleanup dropped a scratch DB under a pending write
(628,185 storm lines).

The gate script therefore excludes the drill-owned classes from steps 14 and 16
(``DRILL_OWNED_CLASSES_GATEWAY`` / ``DRILL_OWNED_CLASSES_COMPUTE``), and this test
keeps that list honest: add a drill class to the Makefile without adding it to the
exclusion list and the suite goes red here instead of storming in a gate run.

The common module is deliberately not covered by those exclusions: step 9's java
suite runs it under ``env -u FLUSS_BOOTSTRAP``, so its drill classes self-skip
there and step 9's own drill is the only place they run live.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
GATE_SCRIPT = ROOT / "code/01_platform/04_scripts/run-monday-gates.sh"
MAKEFILE = ROOT / "Makefile"

# module directory -> the gate-script variable that excludes that module's drill classes
MODULES = {
    "02_services/06_execution_gateway": "DRILL_OWNED_CLASSES_GATEWAY",
    "02_services/02_compute": "DRILL_OWNED_CLASSES_COMPUTE",
}


def _drill_live_block(makefile_text: str) -> str:
    """The body of the Makefile's drill-live target, up to the next target."""
    match = re.search(r"\ndrill-live:\n(.*?)(?=\n[a-zA-Z][a-zA-Z0-9_-]*:)", makefile_text, re.S)
    if match is None:  # pragma: no cover - only when the target is renamed away
        raise AssertionError("Makefile has no drill-live target to read the drill list from")
    return match.group(1)


def _drill_classes() -> list[str]:
    """Every class name in the drill-live target's -Dtest lists, in order."""
    classes: list[str] = []
    for block in re.findall(r"-Dtest='([^']+)'", _drill_live_block(MAKEFILE.read_text("utf-8"))):
        classes.extend(name.strip() for name in block.split(",") if name.strip())
    return classes


def _gate_var(source: str, name: str) -> list[str]:
    match = re.search(rf"^{name}='([^']*)'$", source, re.M)
    if match is None:
        raise AssertionError(f"run-monday-gates.sh no longer defines {name}")
    return [name.strip() for name in match.group(1).split(",") if name.strip()]


def _module_of(class_name: str) -> str | None:
    """Which module owns this test class, as a path relative to code/.

    Only steps 14 (gateway) and 16 (compute) run a java suite *with* the gate's
    FLUSS_BOOTSTRAP in the environment; every other module is covered by the java
    suite step, which runs under `env -u FLUSS_BOOTSTRAP`. So a class that belongs
    to any other module is safe by that convention and ``MODULES`` decides what
    this test enforces.
    """
    roots = [ROOT / "code" / "common", *sorted((ROOT / "code" / "02_services").iterdir())]
    for module in roots:
        if list((module / "src" / "test" / "java").rglob(f"{class_name}.java")):
            return str(module.relative_to(ROOT / "code"))
    return None


class GateDrillExclusionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = GATE_SCRIPT.read_text("utf-8")
        self.drill = _drill_classes()

    def test_drill_list_is_not_empty(self) -> None:
        # Guards the parser itself: if it stops matching, the checks below would
        # pass vacuously.
        self.assertGreaterEqual(len(self.drill), 10, f"parsed only {self.drill} from drill-live")

    def test_every_drill_class_of_a_module_suite_is_excluded(self) -> None:
        for class_name in self.drill:
            module = _module_of(class_name)
            if module is None:
                self.fail(
                    f"drill class {class_name} is in the Makefile's drill list but no "
                    f"source file was found under code/ — cannot tell whether a module "
                    f"suite would run it live"
                )
            if module not in MODULES:
                continue  # common module: step 9's java suite runs it with bootstrap unset
            excluded = _gate_var(self.gate, MODULES[module])
            self.assertIn(
                class_name,
                excluded,
                f"{class_name} ({module}) runs live in step 9's drill but is not excluded "
                f"from that module's gate step — add it to {MODULES[module]} in "
                f"run-monday-gates.sh, or it will run a second time against the live "
                f"cluster on top of the drill's table churn",
            )

    def test_module_suite_commands_use_the_exclusion_lists(self) -> None:
        for name in MODULES.values():
            self.assertIn(
                f'"-Dtest=$(surefire_exclude "${name}")"',
                self.gate,
                f"run-monday-gates.sh defines {name} but the step that needs it does not "
                f"pass it to surefire",
            )
        self.assertIn("surefire_exclude() {", self.gate, "surefire_exclude() helper is gone")

    def test_no_dead_exclusions(self) -> None:
        # An excluded class that the drill no longer runs would silently stop being
        # tested anywhere: step 9 would not run it and steps 14/16 would skip it.
        for module, name in MODULES.items():
            for excluded in _gate_var(self.gate, name):
                self.assertIn(
                    excluded,
                    self.drill,
                    f"{excluded} is excluded from {module} but the drill-live target no "
                    f"longer runs it — it would then run in no gate step at all",
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
