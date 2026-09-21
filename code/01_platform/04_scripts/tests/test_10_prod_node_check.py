"""D1.2 — offline tests for prod_node_check.py (no SSH, no VMs needed).

These exercise the checker's classification logic against a fake inventory +
in-process runner, proving reachability/disk/label/availability classification and
the non-zero-drift exit contract before any real VM exists (D1.3 is human-gated).
"""

import json
import os
import sys
import tempfile

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)
import prod_node_check as pnc  # noqa: E402


class FakeRunner(pnc.RemoteRunner):
    """Deterministic stand-in for both swarm surfaces the checker uses.

    `identities` is what a node's OWN daemon reports via `docker info`
    (hostname|localstate|control-available?|NodeID) — the one swarm call a worker
    can answer. `node_objects` is what a MANAGER reports for that NodeID via
    `docker node inspect` (labels|role|availability). `commands` records every
    (host, command) so tests can prove WHERE a lookup happened.
    """

    def __init__(self, access, swarm_hostnames=None, node_objects=None,
                 identities=None, clocks=None):
        super().__init__(access)
        self.reachable = {"10.0.0.11", "10.0.0.21", "10.0.0.40", "10.0.0.41"}
        self.disks = {"10.0.0.11": 600, "10.0.0.21": 480, "10.0.0.40": 512, "10.0.0.41": 520}
        hostnames = {"10.0.0.11": "M1", "10.0.0.21": "W1", "10.0.0.41": "W2"}
        hostnames.update(swarm_hostnames or {})
        self.identities = identities if identities is not None else {
            "10.0.0.11": (hostnames["10.0.0.11"], "active", True, "node-m1"),
            "10.0.0.21": (hostnames["10.0.0.21"], "active", False, "node-w1"),
            "10.0.0.41": (hostnames["10.0.0.41"], "active", False, "node-w2"),
        }
        self.node_objects = node_objects if node_objects is not None else {
            "node-m1": ("manager", "drained", {"role": "manager"}),
            "node-w1": ("worker", "active", {"role": "employee"}),  # label drift
            "node-w2": ("worker", "active", {"role": "worker"}),
        }
        # host -> CLOCK_PROBE output; a healthy node unless a test overrides it
        self.clocks = clocks or {}
        self.commands = []

    def run(self, host, command, timeout=20):
        self.commands.append((host, command))
        if host not in self.reachable:
            return 1, "Connection refused"
        if command == "true":
            return 0, ""
        if command.startswith("df -BG"):
            return 0, f"{self.disks[host]}G"
        if "timedatectl" in command:
            return 0, self.clocks.get(host, "SYNCED\n0.000123456")
        if "docker info --format" in command:
            ident = self.identities.get(host)
            if ident is None:
                return 1, "Cannot connect to the Docker daemon"
            name, state, control, node_id = ident
            return 0, f"{name}|{state}|{'true' if control else 'false'}|{node_id}"
        if "docker node inspect" in command:
            node_id = command.split("inspect ")[1].split()[0]
            if node_id not in self.node_objects:
                return 1, f"Error response from daemon: node {node_id} not found"
            role, avail, labels = self.node_objects[node_id]
            return 0, f"{json.dumps(labels)}|{role}|{avail}"
        return 0, ""


def _inv(access=None):
    return {
        "access": access or {"ssh_user": "ubuntu", "ssh_port": 22, "connect_timeout_s": 3},
        "nodes": [
            {"name": "M1", "host": "10.0.0.11", "role": "manager", "swarm": True,
             "labels": {"role": "manager"}, "expect_availability": "drained",
             "disk_min_gb": 10},
            {"name": "W1", "host": "10.0.0.21", "role": "worker", "swarm": True,
             "labels": {"role": "worker"}, "disk_min_gb": 500},
            {"name": "O1", "host": "10.0.0.40", "role": "observability", "swarm": False,
             "labels": {"observability": "true"}, "disk_min_gb": 500},
        ],
    }


def test_healthy_manager_passes():
    node = _inv()["nodes"][0]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is True
    assert checks["disk"][0] == "PASS"
    assert checks["swarm"][0] == "PASS"
    assert "10.0.0.11" not in [c[1] for c in checks.values()]


def test_worker_small_disk_fails_with_reason():
    node = _inv()["nodes"][1]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is False
    assert checks["disk"] == ("FAIL", "480G < floor 500G")


def test_worker_label_drift_fails():
    # W1's host is in the swarm with role=employee instead of role=worker; a
    # worker's node object is read from the manager, so hand one in.
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400  # neutralize the disk failure, keep label drift
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}),
                                manager_host="10.0.0.11")
    assert ok is False
    assert checks["swarm"][0] == "FAIL"
    assert "label role" in checks["swarm"][1]


def test_unreachable_node_fails_and_short_circuits():
    node = {"name": "WX", "host": "10.0.0.99", "role": "worker", "swarm": True,
            "labels": {"role": "worker"}, "disk_min_gb": 500}
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is False
    assert checks["reachability"][0] == "FAIL"
    assert "disk" not in checks  # short-circuit after reachability failure


def test_observability_skips_swarm_checks():
    node = _inv()["nodes"][2]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is True
    assert checks["swarm"][0] == "PASS"
    assert "outside swarm" in checks["swarm"][1]


def test_cli_self_check_exits_zero():
    with tempfile.TemporaryDirectory() as out:
        rc = pnc.main(["--self-check", "--out", out])
        assert rc == 0
        files = os.listdir(out)
        assert any(f.endswith("-prod-node-check-evidence.json") for f in files)
        ev = [f for f in files if f.endswith("-prod-node-check-evidence.json")][0]
        with open(os.path.join(out, ev), encoding="utf-8") as fh:
            checks = json.load(fh)["checks"]
        # W1 must fail for the LABEL DRIFT it was built to demonstrate — not for a
        # missing manager or for being asked a manager-only question
        assert checks["W1:swarm"] == "FAIL"
        assert "label role" in checks["W1:swarm_note"], checks["W1:swarm_note"]
        assert checks["M1:swarm"] == "PASS"
        assert "node-m1" in checks["M1:swarm_note"]


def test_cli_drift_exits_nonzero():
    """Real-mode exit contract: drift -> non-zero (the D2 gate)."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "out")

        rc = pnc.main(["--inventory", os.path.join(tmp, "missing.json"), "--out", out])
        assert rc == 2


def test_cli_real_run_against_fake_runner_is_fail_on_drift():
    """End-to-end main() with a fake runner swapped in: W1 drift -> exit 1."""
    import prod_node_check as pnc_mod

    real_runner = pnc_mod.RemoteRunner
    try:
        pnc_mod.RemoteRunner = lambda access: FakeRunner(access)
        with tempfile.TemporaryDirectory() as tmp:
            inv_path = os.path.join(tmp, "inv.json")
            out = os.path.join(tmp, "out")
            with open(inv_path, "w", encoding="utf-8") as fh:
                json.dump(_inv(), fh)
            rc = pnc.main(["--inventory", inv_path, "--out", out])
            assert rc == 1  # W1 is drifting -> D2 gate fails
            ev = os.listdir(out)[0]
            with open(os.path.join(out, ev), encoding="utf-8") as fh:
                assert json.load(fh)["result"] == "FAIL"
    finally:
        pnc_mod.RemoteRunner = real_runner


def test_load_inventory_validates_shape():
    with tempfile.TemporaryDirectory() as tmp:
        inv_path = os.path.join(tmp, "bad.json")
        with open(inv_path, "w", encoding="utf-8") as fh:
            json.dump({"access": {}, "nodes": []}, fh)
        import pytest
        with pytest.raises(ValueError):
            pnc.load_inventory(inv_path)


# ---------------------------------------------------------------------------
# Wave 24 (P6-153 / P6-763 / P6-764): a worker must not be asked a manager-only
# question, role/labels must be matched by NodeID, and the identity must come
# from a call every daemon answers.
# ---------------------------------------------------------------------------


def test_worker_is_inspected_through_a_manager_not_itself():
    """P6-153: `docker node` is manager-only; a worker's object is read on a manager."""
    node = _inv()["nodes"][1]  # W1, a swarm worker
    node["disk_min_gb"] = 400
    runner = FakeRunner({"ssh_user": "u"})
    checks, ok = pnc.check_node(node, runner, manager_host="10.0.0.11")
    assert ok is False  # the label drift is real, and now actually detected
    assert "label role" in checks["swarm"][1]
    inspects = [(h, c) for h, c in runner.commands if "docker node inspect" in c]
    assert inspects, "no node inspect happened"
    assert all(h == "10.0.0.11" for h, _ in inspects), inspects
    assert "node-w1" in inspects[0][1]
    # the manager-only listing is gone entirely
    assert not any("docker node ls" in c for _, c in runner.commands)


def test_worker_without_a_manager_fails_closed():
    """No manager in the inventory: FAIL loudly, never a silent pass."""
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is False
    assert checks["swarm"][0] == "FAIL"
    assert "manager-only" in checks["swarm"][1]


def test_worker_resolves_when_the_inventory_name_differs_from_the_swarm_hostname():
    """P6-764: match by NodeID, not by the inventory's `name`/`host`."""
    node = _inv()["nodes"][3] if len(_inv()["nodes"]) > 3 else {
        "name": "W2", "host": "10.0.0.41", "role": "worker", "swarm": True,
        "labels": {"role": "worker"}, "disk_min_gb": 500}
    runner = FakeRunner({"ssh_user": "u"},
                        swarm_hostnames={"10.0.0.41": "ip-10-0-0-41.ec2.internal"})
    checks, ok = pnc.check_node(node, runner, manager_host="10.0.0.11")
    assert ok is True, checks
    assert checks["swarm"][0] == "PASS"
    assert "node-w2" in checks["swarm"][1]


def test_a_node_that_is_not_an_active_swarm_member_fails():
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400
    runner = FakeRunner({"ssh_user": "u"}, identities={
        "10.0.0.21": ("W1", "inactive", False, "node-w1")})
    checks, ok = pnc.check_node(node, runner, manager_host="10.0.0.11")
    assert ok is False
    assert "not an active swarm node" in checks["swarm"][1]


def test_an_unreadable_membership_fails_closed():
    """`docker info` unavailable (daemon down) is a FAIL, not an unverified pass."""
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400
    runner = FakeRunner({"ssh_user": "u"}, identities={})
    checks, ok = pnc.check_node(node, runner, manager_host="10.0.0.11")
    assert ok is False
    assert "docker info failed" in checks["swarm"][1]


def test_a_missing_node_object_fails_closed():
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400
    runner = FakeRunner({"ssh_user": "u"},
                        node_objects={"node-m1": ("manager", "drained", {})})
    checks, ok = pnc.check_node(node, runner, manager_host="10.0.0.11")
    assert ok is False
    assert "not found" in checks["swarm"][1]


def test_the_swarm_verdict_names_the_identity_it_inspected():
    node = _inv()["nodes"][0]
    checks, ok = pnc.check_node(node, FakeRunner({"ssh_user": "u"}))
    assert ok is True
    assert "node-m1" in checks["swarm"][1]
    # evidence stays free of inventory IPs (hostname-free placement contract)
    assert "10.0.0.11" not in [c[1] for c in checks.values()]


def test_the_manager_is_resolved_from_the_inventory():
    assert pnc._resolve_manager_host(_inv()) == "10.0.0.11"
    assert pnc._resolve_manager_host({"nodes": [{"name": "W", "host": "h",
                                                 "role": "worker", "swarm": True}]}) is None
    # a manager-looking node outside the swarm is not a manager
    assert pnc._resolve_manager_host({"nodes": [{"name": "O", "host": "h",
                                                 "role": "manager", "swarm": False}]}) is None


def test_worker_identity_comes_from_a_call_a_worker_can_answer():
    """P6-763/P6-153: the removed `socket`/`tempfile` imports stay removed, and
    the identity call is `docker info`, which every daemon answers."""
    assert not hasattr(pnc, "socket")
    assert not hasattr(pnc, "tempfile")
    node = _inv()["nodes"][1]
    node["disk_min_gb"] = 400
    runner = FakeRunner({"ssh_user": "u"})
    pnc.check_node(node, runner, manager_host="10.0.0.11")
    identity_cmds = [c for _, c in runner.commands
                     if c.startswith("docker info")]
    assert len(identity_cmds) == 1
    for field in (".Name", ".Swarm.LocalNodeState", ".Swarm.ControlAvailable",
                  ".Swarm.NodeID"):
        assert field in identity_cmds[0]


def test_example_inventory_marks_o1_as_a_joined_worker():
    """The template an operator copies must survive its own gate (CHG-246).

    Three stack services pin `node.labels.observability == true` and fourteen pin
    `node.labels.role == worker`; a label only exists on a joined node, so an O1
    outside the swarm leaves those services with nowhere to land — and
    `swarm: false` skips the swarm check, so the gate passes without ever
    verifying that. The role is checked too: the checker compares the inventory's
    role against the role the swarm reports (`manager`/`worker` only), so a
    joined node declaring `observability` FAILS at D1.3.
    """
    with open(os.path.join(SCRIPTS, "prod_vms.example.json")) as fh:
        inventory = json.load(fh)

    joined = [n for n in inventory["nodes"] if n.get("swarm")]
    assert joined, "the example must contain at least one joined node"
    for node in joined:
        assert node["role"] in ("manager", "worker"), (
            f"{node['name']} is marked swarm:true but declares role "
            f"{node['role']!r}; a swarm only reports manager/worker, so the "
            "checker would FAIL against the live node")

    o1 = next(n for n in inventory["nodes"] if n["name"] == "O1")
    assert o1["swarm"] is True and o1["role"] == "worker"
    assert o1["labels"].get("observability") == "true"

    # and the observability label stays unique — it is what keeps the trading
    # stack off the observability VM
    carriers = [n["name"] for n in inventory["nodes"]
                if n.get("labels", {}).get("observability") == "true"]
    assert carriers == ["O1"]


def test_example_inventory_labels_every_workload_capable_node_as_a_worker():
    """A `role=manager` node label is never useful — it is the trap D5 named.

    Nothing in the stack pins `node.labels.role == manager`; fourteen services pin
    `role == worker`. A node holds one value per label key, so labelling a manager
    `role=manager` does not merely fail to help — it excludes that node from every
    workload service, which surfaces on deploy day as services stuck at 0/N. The
    runbook's v1 deck labels M1-M3 `role=worker` for exactly this reason
    ("v1 baseline" in `PROD_VM_PROVISIONING.md` section 1). A node's swarm role
    belongs in the `role` field, which is what the swarm itself reports; a node
    label is for placement only. A drained node accepts no new tasks either, so a
    `role=worker` row must not declare `drained`.
    """
    with open(os.path.join(SCRIPTS, "prod_vms.example.json")) as fh:
        inventory = json.load(fh)

    for node in inventory["nodes"]:
        labels = node.get("labels", {})
        role = labels.get("role")
        if role is not None:
            assert role == "worker", (
                f"{node['name']} carries role={role!r}: nothing pins role=manager, and "
                "the fourteen workload services would have nowhere to land")
        if role == "worker":
            assert node.get("expect_availability") != "drained", (
                f"{node['name']} is labelled role=worker but declared drained; a drained "
                "node takes no new tasks, so the workload services cannot land there")


# ------------------------------------------------------- the clock is measured, not labelled (CHG-272)

def test_a_drifting_clock_fails_and_names_the_offset():
    node = _inv()["nodes"][0]
    runner = FakeRunner({}, clocks={"10.0.0.11": "SYNCED\n0.350"})
    checks, ok = pnc.check_node(node, runner)
    assert ok is False
    assert checks["clock"][0] == "FAIL"
    assert "beyond" in checks["clock"][1] and "200 ms" in checks["clock"][1]


def test_an_offset_exactly_at_the_limit_is_within_it():
    """The executor's own contract, not this script's opinion: `DriftMonitor::new(200, ...)` reports
    Within(200) at exactly 200 ms (clockwatch.rs tests), so the gate must not be stricter than the
    platform it guards."""
    node = _inv()["nodes"][0]
    runner = FakeRunner({}, clocks={"10.0.0.11": "SYNCED\n0.200"})
    checks, ok = pnc.check_node(node, runner)
    assert ok is True, checks
    assert checks["clock"][0] == "PASS"


def test_the_clock_limit_is_configurable():
    node = _inv()["nodes"][0]
    runner = FakeRunner({}, clocks={"10.0.0.11": "SYNCED\n0.150"})
    assert pnc.check_node(node, runner)[1] is True
    assert pnc.check_node(node, runner, max_offset_ms=100)[1] is False


def test_an_unsynchronized_clock_fails():
    node = _inv()["nodes"][0]
    runner = FakeRunner({}, clocks={"10.0.0.11": "UNSYNCED\n0.0001"})
    checks, ok = pnc.check_node(node, runner)
    assert ok is False and "does not report" in checks["clock"][1]


def test_an_unreadable_clock_fails_closed():
    """No chronyc (or a missing one) must never be read as a disciplined clock."""
    node = _inv()["nodes"][0]
    runner = FakeRunner({}, clocks={"10.0.0.11": "SYNCED\n"})
    checks, ok = pnc.check_node(node, runner)
    assert ok is False and "cannot read the offset" in checks["clock"][1]


def test_the_probe_reads_the_field_the_bootstrap_reads():
    """Both scripts read field 4 of `chronyc tracking`'s `System time` line, so the node gate and
    S4's bootstrap cannot disagree about what the clock says."""
    assert "'/^System time/{print $4}'" in pnc.CLOCK_PROBE
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(SCRIPTS)))
    bootstrap = open(os.path.join(repo_root, "code/01_platform/04_scripts/vm-bootstrap.sh"),
                     encoding="utf-8").read()
    assert "awk '/^System time/{print $4}'" in bootstrap
    synced, offset = pnc._parse_clock("SYNCED\n0.000123456")
    assert synced is True and offset == 0.000123456
