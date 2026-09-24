"""Step 9's live drills must not use a freshly created table before its buckets have a leader.

Gate step 9 died on 2026-09-24: GateMergeEngineDrillIntegrationTest created a scratch table and
upserted into it, and the upsert's 20 s budget expired at 20.05 s with ZERO server-side lines. A
table that has just been created is not yet writable - the coordinator assigns buckets, the tablet
elects the leader, the client discovers it - and while other tables are being placed or torn down
that wait has been measured at 1-13 s (FlussPlacementAwait's javadoc), so any fixture that budgets
~20 s for its first use loses that race eventually. The same family already killed the CHG-221
fixture (20 s) and gate step 11 scenario 2 (60 s), which is why every live fixture now waits for
the table it just created instead of racing it.

These tests read the drill-live class list straight out of the Makefile, so a new drill class is
covered the moment it is added to step 9.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_gate_drill_exclusions import _drill_classes  # noqa: E402  (sibling helper, same dir)

ROOT = Path(__file__).resolve().parents[4]
TEST_JAVA = ROOT / "code"
GATE_CALL = "FlussPlacementAwait.awaitServing("
SERVING_HELPER = ROOT / "code/common/src/main/java/com/trading/common/schema/fluss/FlussPlacementAwait.java"
SERVING_HELPER_TEST_TREE = (
    ROOT / "code/common/src/test/java/com/trading/common/schema/fluss/FlussPlacementAwait.java"
)

# Every way a drill proves a fresh table is usable before touching it.
WAIT_CALLS = (GATE_CALL, "FlussPlacementAwait.awaitPlacement(", "awaitWritable(")

# Classes that create tables and are legitimately exempt, each with the reason it is safe.
EXEMPT = {
    "CompatFlussDdlParityIntegrationTest": (
        "creates scratch tables to compare DDL and reads only table metadata (getTableInfo); it "
        "never writes or scans a row, so placement is not on its path"
    ),
    "DdlSmokeTwinSweepTest": (
        "skipped by decision (the plan's 100% waiver) and its twins are gated on the tool side by "
        "DdlApplyTool.awaitServing"
    ),
}


def _sources() -> dict[str, Path]:
    """Every live-drill test class name -> its source file."""
    found: dict[str, Path] = {}
    for path in (TEST_JAVA / "common").rglob("*.java"), (TEST_JAVA / "02_services").rglob("*.java"):
        for java in path:
            if "/src/test/java/" in str(java):
                found[java.stem] = java
    return found


class DrillPlacementGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sources = _sources()
        self.drill = _drill_classes()
        self.assertTrue(self.drill, "no drill classes parsed out of the Makefile")

    def test_every_drill_class_that_creates_a_table_waits_for_it(self) -> None:
        ungated: list[str] = []
        for name in self.drill:
            source = self.sources.get(name)
            self.assertIsNotNone(source, f"{name} is in the drill list but has no test source")
            text = source.read_text("utf-8")
            if "createTable(" not in text or name in EXEMPT:
                continue
            if not any(call in text for call in WAIT_CALLS):
                ungated.append(name)
        self.assertEqual(
            ungated,
            [],
            "these drill classes create a table and then use it without waiting for its bucket "
            "leaders, so they fail on the next placement stall (add "
            f"{GATE_CALL}... right after createTable, or exempt them with a reason): {ungated}",
        )

    def test_each_exemption_is_still_needed(self) -> None:
        for name, reason in EXEMPT.items():
            self.assertTrue(reason, f"{name} is exempt without a reason")
            source = self.sources.get(name)
            self.assertIsNotNone(source, f"{name} is exempt but no longer exists")
            self.assertIn("createTable(", source.read_text("utf-8"),
                          f"{name} is exempt from the placement gate but creates no table now")

    def test_every_gate_call_is_the_shared_helper(self) -> None:
        for name in self.drill:
            text = self.sources[name].read_text("utf-8")
            if GATE_CALL in text:
                self.assertIn("com.trading.common.schema.fluss.FlussPlacementAwait",
                              text, f"{name} calls the gate without importing it")

    def test_the_shared_helper_lives_where_every_drill_module_can_see_it(self) -> None:
        # Load-bearing for 2026-09-25: the gateway and compute drills cannot see common's
        # test-jar (no module declares it), so a test-scope helper left them uncompilable.
        self.assertTrue(SERVING_HELPER.exists(), f"{SERVING_HELPER} is missing")
        self.assertFalse(SERVING_HELPER_TEST_TREE.exists(),
                         "the helper moved back into the test tree, where the gateway drills "
                         "cannot import it")
        self.assertIn("public static void awaitServing(", SERVING_HELPER.read_text("utf-8"))

    def test_the_gate_fails_closed_when_no_leader_ever_appears(self) -> None:
        text = SERVING_HELPER.read_text("utf-8")
        self.assertIn("no leader serves every bucket of", text,
                      "the gate must fail loudly when placement never completes, rather than "
                      "returning and letting the caller's own write time out")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
