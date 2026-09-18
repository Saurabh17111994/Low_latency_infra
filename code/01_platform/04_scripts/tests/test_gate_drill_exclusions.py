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


def _step_blocks(gate: str) -> list[tuple[str, str]]:
    """[(label, text)] split on the gate's own `=== [N/19] … ===` banners."""
    banners = list(re.finditer(r'echo "=== \[(\d+)/19\] ([^"]+) ==="', gate))
    blocks: list[tuple[str, str]] = []
    for index, banner in enumerate(banners):
        end = banners[index + 1].start() if index + 1 < len(banners) else len(gate)
        blocks.append((f"step {banner.group(1)} ({banner.group(2)})", gate[banner.start():end]))
    return blocks


def _joined(block: str) -> str:
    """The block with line continuations folded, so a wrapped mvn call reads as one line."""
    return block.replace("\\\n", " ")


def _mvn_commands(block: str) -> list[str]:
    """Every mvn invocation in the block, including whatever precedes it on the command.

    The guard flags do not all sit on the right of ``mvn``: ``env -u FLUSS_BOOTSTRAP``
    comes before it (the gate's own form in steps 9 and 19), while ``-Dtest=`` comes
    after. So the window starts at the enclosing subshell/``&&`` and ends at the closing
    parenthesis — a parse that started at ``mvn`` would call a guarded step unguarded
    (it did, on the first version of this guard).
    """
    joined = _joined(block)
    commands: list[str] = []
    for match in re.finditer(r"\bmvn\b", joined):
        start = max(joined.rfind("(", 0, match.start()), joined.rfind("&&", 0, match.start())) + 1
        end = joined.find(")", match.start())
        commands.append(joined[start : end if end != -1 else len(joined)])
    return commands


EXPORT_FLUSS_BOOTSTRAP = re.compile(r"^[ \t]*export FLUSS_BOOTSTRAP[ \t]*$", re.M)


def _export_at(gate: str) -> int:
    """Where step 11 exports FLUSS_BOOTSTRAP — the point every later step inherits from.

    Tab-indented in the script, hence the explicit ``[ \\t]`` classes rather than ``\\s``:
    this anchor is what makes the guard below mean "after the export".
    """
    match = EXPORT_FLUSS_BOOTSTRAP.search(gate)
    if match is None:
        raise AssertionError(
            "run-monday-gates.sh no longer exports FLUSS_BOOTSTRAP — the post-export "
            "suite guard checks nothing without that anchor; fix the anchor or the guard"
        )
    return match.start()


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

    def test_the_bootstrap_export_site_is_still_there(self) -> None:
        # Everything below is "after the export". If step 11 stops exporting, the checks
        # would quietly stop covering anything — so the anchor itself is asserted.
        self.assertGreater(_export_at(self.gate), 0)  # raises with why, if it moved

    def test_every_module_suite_after_the_export_is_guarded(self) -> None:
        """Step 11 exports FLUSS_BOOTSTRAP, and every later step inherits it.

        That is deliberate (the DDL smoke needs it), but it means any module suite that
        runs afterwards executes every class self-gating on ``FLUSS_BOOTSTRAP`` against
        the live cluster — the second-run shape that stormed in run 4b (see this file's
        docstring). Each such suite must therefore either unset it (``env -u``) or name
        what it runs (``-Dtest=``, including the drill exclusions). Step 19 did neither.
        """
        export_at = _export_at(self.gate)
        checked = 0
        for label, block in _step_blocks(self.gate):
            if self.gate.index(block) < export_at:
                continue  # runs before the export: nothing has leaked in yet
            for command in _mvn_commands(block):
                if not re.search(r"\btest\b", command):
                    continue  # a non-test mvn goal says nothing about live classes
                checked += 1
                self.assertTrue(
                    "env -u FLUSS_BOOTSTRAP" in command or "-Dtest=" in command,
                    f"{label} runs a module suite with FLUSS_BOOTSTRAP exported from step 11 "
                    f"and no guard:\n  {command.strip()}\n"
                    f"A class gated on the bootstrap would run live here, a second time on "
                    f"top of the drill's own table churn. Add `env -u FLUSS_BOOTSTRAP` or "
                    f"`-Dtest=…` (drill classes: surefire_exclude).",
                )
        # Guards the parser: a rename of the banners would make the loop above vacuous.
        self.assertGreaterEqual(checked, 5, f"only {checked} mvn test command(s) parsed")

    def test_the_mock_arrow_suite_cannot_need_the_bootstrap(self) -> None:
        """Step 19 unsets FLUSS_BOOTSTRAP; that must not silently cut live coverage.

        The suite is offline mock-arrow by design, so nothing in it should gate on the
        bootstrap. If that ever changes, the class belongs in the drill list (and the
        unset has to be reconsidered) rather than quietly skipping under step 19.
        """
        sources = sorted((ROOT / "code/02_services/05_mock_arrow/src/test").rglob("*.java"))
        self.assertTrue(sources, "mock-arrow has no test sources — check this guard's path")
        for path in sources:
            self.assertNotIn(
                "FLUSS_BOOTSTRAP",
                path.read_text("utf-8"),
                f"{path.name} gates on FLUSS_BOOTSTRAP, but step 19 runs with it unset — "
                f"the leg would skip instead of running; add the class to the drill list "
                f"and the step's handling, or drop the unset deliberately",
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
