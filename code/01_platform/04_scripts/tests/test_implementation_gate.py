"""Unit tests for implementation_gate.py — no mvn is ever invoked.

The gate logic (strict ordering, fail-on-red, fail-on-missing) is tested with
an injected fake runner; the real GateRunner's file/contains/tree-contains
primitives are tested against temp files; the real TASKS table is shape- and
path-checked so stale pins fail fast.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import implementation_gate as gate


class FakeRunner:
    """Scripted runner: yields the given (ok, detail) results per check call."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = []  # check types in invocation order

    def check(self, chk):
        self.calls.append(chk["type"])
        return self.results.pop(0)


FAKE_TASKS = [
    {"seq": 1, "title": "t1", "dossier": "d1", "checks": [
        {"type": "run", "cmd": "true", "desc": "c1"},
        {"type": "file", "path": "a", "desc": "c2"},
    ]},
    {"seq": 2, "title": "t2", "dossier": "d2", "checks": [
        {"type": "contains", "path": "b", "needle": "x", "desc": "c3"},
    ]},
    {"seq": 3, "title": "t3", "dossier": "d3", "checks": [
        {"type": "tree-contains", "dir": "d", "needle": "y", "desc": "c4"},
    ]},
]


class OrderingTests(unittest.TestCase):
    def test_all_pass_in_order(self):
        runner = FakeRunner([(True, "ok")] * 4)
        captured = []
        rc = gate.run_gate(FAKE_TASKS, runner, out=captured.append)
        self.assertEqual(rc, 0)
        self.assertEqual(runner.calls, ["run", "file", "contains", "tree-contains"])
        self.assertTrue(any("all 3 tasks pass in order" in line for line in captured))

    def test_stops_at_first_failing_check_and_blocks_downstream(self):
        # task 1 check 2 fails -> task 2 and 3 must never run
        runner = FakeRunner([(True, "ok"), (False, "boom"), (True, "ok")])
        captured = []
        rc = gate.run_gate(FAKE_TASKS, runner, out=captured.append)
        self.assertEqual(rc, 1)
        self.assertEqual(runner.calls, ["run", "file"])  # stopped after c2
        self.assertTrue(any("task 1 FAILED" in line for line in captured))
        self.assertTrue(any("downstream" in line for line in captured))

    def test_missing_evidence_fails_and_blocks(self):
        # missing file = the "missing" case, not just red tests
        runner = FakeRunner([(False, "missing evidence: a")])
        captured = []
        rc = gate.run_gate(FAKE_TASKS, runner, out=captured.append)
        self.assertEqual(rc, 1)
        self.assertEqual(runner.calls, ["run"])
        self.assertTrue(any("missing" in line for line in captured))

    def test_failure_in_last_task_still_fails(self):
        runner = FakeRunner([(True, "ok")] * 3 + [(False, "late failure")])
        captured = []
        rc = gate.run_gate(FAKE_TASKS, runner, out=captured.append)
        self.assertEqual(rc, 1)
        self.assertEqual(runner.calls, ["run", "file", "contains", "tree-contains"])
        self.assertTrue(any("task 3 FAILED" in line for line in captured))

    def test_last_task_failure_does_not_claim_a_downstream_range(self):
        """P6-745: seq 3 of 3 printed "downstream tasks 4..3 BLOCKED"."""
        runner = FakeRunner([(True, "ok")] * 3 + [(False, "late failure")])
        captured = []
        gate.run_gate(FAKE_TASKS, runner, out=captured.append)
        message = "\n".join(captured)
        self.assertNotIn("4..3", message)
        self.assertNotIn("downstream", message)
        self.assertIn("final task", message)


class RealRunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.r = gate.GateRunner(self.tmp.name)

    def test_contains_present_and_absent(self):
        with open(os.path.join(self.tmp.name, "f.txt"), "w", encoding="utf-8") as fh:
            fh.write("alpha beta gamma")
        ok, detail = self.r.contains("f.txt", "beta")
        self.assertTrue(ok)
        ok, detail = self.r.contains("f.txt", "zeta")
        self.assertFalse(ok)

    def test_file_present_and_missing(self):
        with open(os.path.join(self.tmp.name, "a.java"), "w", encoding="utf-8") as fh:
            fh.write("x")
        ok, _ = self.r.file("a.java")
        self.assertTrue(ok)
        ok, detail = self.r.file("nope.java")
        self.assertFalse(ok)
        self.assertIn("missing", detail)

    def test_tree_contains_finds_nested_java(self):
        sub = os.path.join(self.tmp.name, "pkg", "sub")
        os.makedirs(sub)
        with open(os.path.join(sub, "Thing.java"), "w", encoding="utf-8") as fh:
            fh.write("class Thing { BabysitterJob j; }")
        ok, detail = self.r.tree_contains("pkg", "BabysitterJob")
        self.assertTrue(ok)
        ok, _ = self.r.tree_contains("pkg", "DoesNotExist")
        self.assertFalse(ok)
        ok, detail = self.r.tree_contains("absent-dir", "x")
        self.assertFalse(ok)
        self.assertIn("missing", detail)

    def test_unknown_check_type_raises(self):
        with self.assertRaises(ValueError):
            self.r.check({"type": "nonsense"})


class TaskTableTests(unittest.TestCase):
    def test_seven_tasks_in_order(self):
        self.assertEqual([t["seq"] for t in gate.TASKS], [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(len(gate.TASKS), 7)
        for task in gate.TASKS:
            self.assertTrue(task["title"])
            self.assertTrue(task["dossier"])
            self.assertTrue(task["checks"], f"task {task['seq']} has no checks")
            for chk in task["checks"]:
                self.assertIn(chk["type"], ("run", "file", "contains", "tree-contains"))
                self.assertTrue(chk["desc"])

    def test_task_two_pins_the_no_batch_value_not_only_the_name(self):
        """P6-416: a name-only needle stays green if the value drifts."""
        checks = [c for t in gate.TASKS if t["seq"] == 2 for c in t["checks"]]
        value_pins = [
            c for c in checks
            if c["type"] == "contains" and "INGESTION_MAX_BATCH_RECORDS = " in c["needle"]
        ]
        self.assertTrue(value_pins, [c["desc"] for c in checks])
        for pin in value_pins:
            self.assertEqual(pin["needle"].split("= ", 1)[1].strip(), "1")
            ok, detail = gate.GateRunner(gate.ROOT).contains(pin["path"], pin["needle"])
            self.assertTrue(ok, detail)

    def test_every_pinned_evidence_exists(self):
        """All file/contains pins must resolve against the real repo now, so a
        stale pin fails this test rather than silently passing the gate."""
        root = gate.ROOT
        for task in gate.TASKS:
            for chk in task["checks"]:
                if chk["type"] == "file":
                    self.assertTrue(
                        os.path.isfile(os.path.join(root, chk["path"])),
                        f"task {task['seq']}: missing file pin {chk['path']}",
                    )
                elif chk["type"] == "contains":
                    self.assertTrue(
                        os.path.isfile(os.path.join(root, chk["path"])),
                        f"task {task['seq']}: unreadable contains pin {chk['path']}",
                    )
                elif chk["type"] == "tree-contains":
                    self.assertTrue(
                        os.path.isdir(os.path.join(root, chk["dir"])),
                        f"task {task['seq']}: missing dir pin {chk['dir']}",
                    )

    def test_cli_list_lists_all_tasks(self):
        rc = gate.main(["--list"])
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
