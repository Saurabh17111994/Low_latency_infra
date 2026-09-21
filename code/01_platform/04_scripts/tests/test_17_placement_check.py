"""Placement satisfiability, offline — the two-VM inventory must leave no service homeless.

Hermetic: no Docker, no cluster, no network. The deck is the real
`code/01_platform/01_docker/docker-stack.yml` (so the check is exercised against the services
that actually ship), and the inventories are written to a temporary directory.

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests    (TestCase-style on
purpose: the repo's pytest-only files are invisible to unittest discover.)
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPTS)))
STACK = os.path.join(REPO, "code", "01_platform", "01_docker", "docker-stack.yml")

if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)

import placement_check as pc  # noqa: E402


def _run(inventory, stack=STACK):
    """Invoke the checker over a written inventory; return (exit code, stdout)."""
    buf = io.StringIO()
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "prod_vms.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(inventory, fh)
        with contextlib.redirect_stdout(buf):
            code = pc.main(["--stack", stack, "--inventory", path])
    return code, buf.getvalue()


def _node(name, labels, swarm=True, availability=None):
    node = {"name": name, "host": "10.0.0.1", "role": "manager", "swarm": swarm,
            "labels": labels}
    if availability:
        node["expect_availability"] = availability
    return node


TWO_VM = {"nodes": [_node("T1", {"role": "worker"}),
                    _node("O1", {"observability": "true"})]}


class TestPlacementCheck(unittest.TestCase):

    def test_the_two_vm_inventory_leaves_every_service_a_home(self):
        """The first deploy's shape: one trading node, one observability node."""
        code, out = _run(TWO_VM)

        assert code == 0, out
        assert "every service has a home" in out
        # the 14 workload services land on the trading node, the 3 observability ones on O1
        assert out.count("eligible node(s): T1") >= 10
        assert out.count("eligible node(s): O1") == 3
        # and the three global agents are skipped, not judged
        assert out.count("mode: global") == 3

    def test_a_trading_node_without_the_worker_label_is_reported_by_name(self):
        """Drop role=worker and the check must name the services left homeless."""
        broken = {"nodes": [_node("T1", {}), _node("O1", {"observability": "true"})]}
        code, out = _run(broken)

        assert code >= 10, out
        assert "nowhere to land" in out
        assert "zookeeper-1" in out and "flink-taskmanager" in out
        # the observability services still pass — the check is precise, not just loud
        assert out.count("eligible node(s): O1") == 3

    def test_a_node_labelled_role_manager_excludes_it_from_every_workload(self):
        """The named trap: a node holds one value per label key, so role=manager is a downgrade."""
        trap = {"nodes": [_node("T1", {"role": "manager"}),
                          _node("O1", {"observability": "true"})]}
        code, out = _run(trap)

        assert code >= 10, out
        assert "nowhere to land" in out
        assert "role=manager is never useful here" in out
        assert out.count("eligible node(s): O1") == 3

    def test_a_drained_worker_cannot_host_a_workload(self):
        """Availability is a rule in the live checker; the offline one must agree."""
        drained = {"nodes": [_node("T1", {"role": "worker"}, availability="drained"),
                             _node("O1", {"observability": "true"})]}
        code, out = _run(drained)

        assert code >= 10, out
        assert "nowhere to land" in out

    def test_a_node_outside_the_swarm_is_not_a_home(self):
        outsider = {"nodes": [_node("T1", {"role": "worker"}, swarm=False),
                              _node("O1", {"observability": "true"})]}
        code, out = _run(outsider)

        assert code >= 10, out
        assert "nowhere to land" in out

    def test_an_inventory_with_no_swarm_member_fails_loudly(self):
        code, out = _run({"nodes": [_node("T1", {"role": "worker"}, swarm=False)]})

        assert code == 1
        assert "no node marked swarm: true" in out

    def test_an_unknown_constraint_form_is_reported_never_silently_passed(self):
        """A constraint we cannot evaluate must fail; guessing would be a false all-clear."""
        deck = {"services": {"mystery": {"deploy": {
            "placement": {"constraints": ["node.role == manager"]}}}}}
        with tempfile.TemporaryDirectory() as tmp:
            stack = os.path.join(tmp, "deck.yml")
            with open(stack, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(deck))
            code, out = _run(TWO_VM, stack=stack)

        assert code == 1, out
        assert "cannot evaluate" in out

    def test_the_real_deck_only_uses_forms_this_checker_understands(self):
        """Guard against the deck growing a constraint form the offline check cannot read.

        If this fails, `placement_check.py` would report `cannot evaluate` for that service — the
        honest outcome, but it means the offline gate silently stops being useful until the new
        form is supported here and in `cluster_check._matches` (they must stay the same function).
        """
        deck = pc.load_deck(STACK)
        forms = {c for _, constraints, _ in pc.service_requirements(deck) for c in constraints}

        assert forms, "the deck must pin at least one placement constraint"
        for form in forms:
            assert form.startswith("node.labels.") and "==" in form, (
                f"the deck uses {form!r}, which placement_check cannot evaluate")
