#!/usr/bin/env python3
"""prod_node_check.py — per-VM provisioning gate for the production Swarm topology (D1.2).

Verifies every VM in the target topology (docs/05_deployment/PROD_VM_PROVISIONING.md,
D1.1 of the 2026-08-21 live-readiness plan) against its provisioned reality, using
SSH access constants from an inventory file. **Exits non-zero on any drift** — that is
the gate D1.3/D2 depends on.

Per-Node checks:
  * reachability   — SSH connect (BatchMode; no password prompt ever)
  * disk           — root filesystem size >= disk_min_gb (default 500, imported from
                     PROD_VM_PROVISIONING.md; manager nodes may set a smaller floor)
  * label/role     — Swarm node role + labels match the inventory expectation
                     (role=manager / role=worker / observability=true), and (optional)
                     availability is `drained` for v2 manager-only nodes. Swarm checks
                     run only when the inventory marks the node `swarm: true`, and the
                     node's role/availability/labels are read by NodeID: `docker info`
                     on the node itself (the one swarm call a worker can answer) and
                     `docker node inspect <NodeID>` on a manager, because the
                     `docker node` API is manager-only — a worker answers it with
                     "This node is not a swarm manager", which used to FAIL every
                     healthy worker.
  * placement rule — the stack must NEVER pin a hostname; this checker only confirms
                     labels because test_09_stack.py enforces no hostname in the stack.

No hostname pinning is introduced here: the inventory is the operator's record of
*which physical host carries which labels*, and the Swarm still places by label only.

Usage:
  python3 prod_node_check.py --inventory prod_vms.json [--out DIR] [--self-check]

  --self-check  run the checker logic against a bundled FAKE inventory with an
                in-process runner (no SSH, no VMs) — proves classification + exit
                codes offline. This is the only runnable mode until D1.3 provisions
                the real VMs.
  --out DIR     write an EvidenceRecord-shape JSON (foundation docs/08_implementation/
                01-foundation.md L159; mirror of audit_r2.py) under DIR.

Stdlib only (subprocess ssh; no paramiko/boto3). Secrets are never printed.
================================================================================="""

import argparse
import datetime as _dt
import json
import os
import subprocess
import sys

REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
)
EVIDENCE_DIR_DEFAULT = os.path.join(REPO_ROOT, "logs", "nautilus-execution")

DEFAULT_DISK_MIN_GB = 500  # PROD_VM_PROVISIONING.md §1 (workload/observability floor)

# Inventory shape (JSON):
# {
#   "access": {"ssh_user": "ubuntu", "ssh_port": 22, "ssh_key": "/abs/or/omit",
#              "connect_timeout_s": 8},
#   "disk_min_gb": 500,            # optional global default
#   "nodes": [
#     {"name": "M1", "host": "10.0.0.11", "role": "manager", "swarm": true,
#      "labels": {"role": "manager"}, "expect_availability": "drained",  # optional
#      "disk_min_gb": 10},          # optional per-node override
#     {"name": "W1", "host": "10.0.0.21", "role": "worker", "swarm": true,
#      "labels": {"role": "worker"}},
#     {"name": "O1", "host": "10.0.0.40", "role": "observability", "swarm": false,
#      "labels": {"observability": "true"}}
#   ]
# }


class RemoteRunner:
    """SSH-backed command runner. Swap for a fake in tests / --self-check."""

    def __init__(self, access):
        self.access = access

    def run(self, host, command, timeout=20):
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
        if self.access.get("ssh_port"):
            ssh += ["-p", str(self.access["ssh_port"])]
        if self.access.get("ssh_key"):
            ssh += ["-i", self.access["ssh_key"]]
        timeout_s = self.access.get("connect_timeout_s", 8)
        ssh += ["-o", f"ConnectTimeout={timeout_s}"]
        user = self.access.get("ssh_user", "root")
        try:
            proc = subprocess.run(
                ssh + [f"{user}@{host}", command],
                capture_output=True, text=True, timeout=timeout,
            )
            return proc.returncode, proc.stdout.strip()
        except (subprocess.TimeoutExpired, OSError) as exc:
            return 1, f"runner error: {exc}"


def load_inventory(path):
    with open(path, encoding="utf-8") as fh:
        inv = json.load(fh)
    access = inv.setdefault("access", {})
    default_disk = inv.get("disk_min_gb", DEFAULT_DISK_MIN_GB)
    nodes = inv.get("nodes") or []
    if not nodes:
        raise ValueError("inventory must define at least one node")
    for n in nodes:
        n.setdefault("disk_min_gb", default_disk)
        n.setdefault("labels", {})
        n.setdefault("swarm", True)
        if not n.get("name") or not n.get("host"):
            raise ValueError(f"node missing name/host: {n}")
    return {"access": access, "nodes": nodes}


def _parse_df_gb(stdout):
    """Turn `df -BG --output=size /` output ('500G' or 'Filesystem 500G') into int GB."""
    for token in stdout.replace(",", "").split():
        token = token.strip()
        if token.endswith("G") and token[:-1].isdigit():
            return int(token[:-1])
    return None


DOCKER_INFO_FMT = ("{{.Name}}|{{.Swarm.LocalNodeState}}|"
                   "{{.Swarm.ControlAvailable}}|{{.Swarm.NodeID}}")


def _local_swarm_identity(runner, node):
    """What a node's OWN daemon reports about its swarm membership.

    `docker node ls`/`docker node inspect` are manager-only API calls: run on a
    worker they exit non-zero ("This node is not a swarm manager"), so the old
    implementation FAILED every worker on healthy infrastructure (P6-153).
    `docker info` is answered by any daemon, manager or worker, and carries the
    daemon hostname, the local swarm state, whether this daemon is a manager and
    the local swarm NodeID.
    """
    rc, out = runner.run(node["host"], f"docker info --format '{DOCKER_INFO_FMT}'")
    if rc != 0:
        return None, f"docker info failed (rc={rc}): {out}"
    parts = [part.strip() for part in out.strip().split("|")]
    if len(parts) != 4 or not parts[3]:
        return None, f"unexpected docker info output for {node['name']}: {out.strip()!r}"
    hostname, state, control_available, node_id = parts
    return {
        "hostname": hostname,
        "local_state": state,
        "control_available": control_available.lower() == "true",
        "node_id": node_id,
    }, None


def _node_swarm_info(runner, node, manager_host=None):
    """Role + availability + labels, addressed by the node's own swarm NodeID.

    The node OBJECT (role/availability/labels) lives behind the manager-only
    `docker node` API, so a worker's is read from `manager_host`. Matching is by
    NodeID on purpose: swarm membership is keyed by NodeID/hostname, and the
    inventory's `host` (an IP) and `name` (M1/W1-style) are neither — matching on
    those reported a correctly joined node as "absent from swarm membership"
    (P6-764). Fail-closed: an unreadable or ambiguous membership is an error, not
    a pass.
    """
    local, err = _local_swarm_identity(runner, node)
    if err:
        return None, err
    if local["local_state"] != "active":
        return None, (f"node {node['name']} is not an active swarm node "
                      f"(localstate={local['local_state']})")
    if local["control_available"]:
        probe_host = node["host"]  # this daemon is a manager: ask it directly
    elif manager_host:
        probe_host = manager_host
    else:
        return None, (f"node {node['name']} is a worker: role/availability/labels "
                      "are manager-only API data and the inventory names no "
                      "manager to read them from (add the manager node)")
    cmd = ("docker node inspect " + local["node_id"] +
           " --format '{{json .Spec.Labels}}|{{.Spec.Role}}|{{.Spec.Availability}}'")
    rc, out = runner.run(probe_host, cmd)
    if rc != 0:
        return None, (f"docker node inspect {local['node_id']} on {probe_host} "
                      f"failed (rc={rc}): {out}")
    parts = [part.strip() for part in out.strip().split("|")]
    if len(parts) != 3:
        return None, f"unexpected docker node inspect output: {out.strip()!r}"
    labels_raw, role, availability = parts
    try:
        labels = json.loads(labels_raw) if labels_raw else {}
    except json.JSONDecodeError:
        labels = {}
    if not isinstance(labels, dict):
        labels = {}
    return {"role": role, "availability": availability, "labels": labels,
            "hostname": local["hostname"], "node_id": local["node_id"]}, None


def check_node(node, runner, manager_host=None):
    """Returns (checks, ok) for one node; checks is {name: (PASS|FAIL, detail)}.

    `manager_host` is where a worker's node object is inspected; a manager reads
    its own. Without one, a worker's swarm check fails closed (P6-153).
    """
    checks = {}
    # 1. reachability
    rc, out = runner.run(node["host"], "true")
    if rc != 0:
        checks["reachability"] = ("FAIL", f"ssh rc={rc}: {out}")
        return checks, False
    checks["reachability"] = ("PASS", "ssh ok")

    # 2. disk floor
    rc, out = runner.run(node["host"], "df -BG --output=size / | tail -1")
    size_gb = _parse_df_gb(out) if rc == 0 else None
    floor = int(node["disk_min_gb"])
    if size_gb is None:
        checks["disk"] = ("FAIL", f"could not parse df output (rc={rc}): {out!r}")
    elif size_gb < floor:
        checks["disk"] = ("FAIL", f"{size_gb}G < floor {floor}G")
    else:
        checks["disk"] = ("PASS", f"{size_gb}G >= floor {floor}G")

    # 3. swarm role/labels (only when the node is intended to be in the swarm)
    if node.get("swarm"):
        info, err = _node_swarm_info(runner, node, manager_host)
        if err:
            checks["swarm"] = ("FAIL", err)
        else:
            problems = []
            expect_role = node.get("role")
            if expect_role and info["role"].lower() != expect_role:
                problems.append(f"role {info['role']} != expected {expect_role}")
            for k, v in (node.get("labels") or {}).items():
                if info["labels"].get(k) != v:
                    problems.append(f"label {k}={info['labels'].get(k)!r} != expected {v!r}")
            want_avail = node.get("expect_availability")
            if want_avail and info["availability"].lower() != want_avail:
                problems.append(
                    f"availability {info['availability']} != expected {want_avail}"
                )
            # name the swarm identity we actually inspected: with NodeID matching
            # an operator can tell WHICH node the verdict is about (P6-764)
            identity = f"swarm node {info['hostname']} ({info['node_id']})"
            checks["swarm"] = (
                ("PASS", f"role={info['role']} availability={info['availability']} "
                         f"labels={info['labels']} — {identity}") if not problems
                else ("FAIL", "; ".join(problems) + f" — {identity}")
            )
    else:
        checks["swarm"] = ("PASS", "outside swarm (observability) — label check n/a; "
                                    "stack places by observability=true only if joined")
    ok = all(verdict == "PASS" for verdict, _ in checks.values())
    return checks, ok


def _resolve_manager_host(inventory):
    """First inventory node that is a swarm manager, else None.

    Workers cannot read their own node object — role, availability and labels sit
    behind the manager-only `docker node` API — so every worker's check inspects
    them from this host, addressed by the worker's own NodeID (P6-153).
    """
    for node in inventory["nodes"]:
        if node.get("swarm") and str(node.get("role", "")).lower() == "manager":
            return node["host"]
    return None


def build_evidence(inventory, per_node, run_id, utc_now):
    all_pass = all(ok for _, (_, ok) in per_node)
    checks = {}
    for node, (node_checks, _) in per_node:
        for name, (verdict, detail) in node_checks.items():
            checks[f"{node['name']}:{name}"] = verdict
            checks[f"{node['name']}:{name}_note"] = detail
    return {
        "work_item_id": f"PROD-NODE-CHECK-{run_id}",
        "requirement_ids": ["D1", "09-production-swarm", "02-environments"],
        "artifact": f"logs/nautilus-execution/{run_id}-prod-node-check-evidence.json",
        "version": "prod_node_check.py (stdlib ssh)",
        "environment": "production Swarm target topology (v1 4 -> v2 7 VMs)",
        "workload": "read-only SSH probes: reachability/disk/labels/role; no writes",
        "clock": "UTC",
        "result": "PASS" if all_pass else "FAIL",
        "owner": "Saurabh (DEC-044)",
        "date": utc_now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": checks,
        "limitations": [
            "swarm checks run only on nodes marked swarm:true (O1 observability is "
            "verified for reachability+disk unless it joins the swarm)",
            "label/role/availability come from the manager-only `docker node` API: "
            "`docker info` on the node itself for the identity, then `docker node "
            "inspect <NodeID>` on a manager (a worker cannot read its own node object)",
            "hostname-free placement itself is enforced by test_09_stack.py",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Per-VM production provisioning gate (D1.2)")
    parser.add_argument("--inventory", default="prod_vms.json", help="JSON inventory")
    parser.add_argument("--out", default=EVIDENCE_DIR_DEFAULT,
                        help="evidence output directory (EvidenceRecord JSON)")
    parser.add_argument("--self-check", action="store_true",
                        help="run against the bundled fake inventory + in-process runner")
    args = parser.parse_args(argv)

    utc_now = _dt.datetime.now(_dt.timezone.utc)
    run_id = utc_now.strftime("%Y%m%d-%H%M%S")

    if args.self_check:
        return _self_check(args, utc_now, run_id)

    if not os.path.exists(args.inventory):
        print(f"error: inventory file not found: {args.inventory}", file=sys.stderr)
        print("  (D1.3 provisions the VMs; until then run --self-check)", file=sys.stderr)
        return 2
    inventory = load_inventory(args.inventory)
    runner = RemoteRunner(inventory["access"])
    manager_host = _resolve_manager_host(inventory)
    per_node = []
    for node in inventory["nodes"]:
        node_checks, ok = check_node(node, runner, manager_host=manager_host)
        per_node.append((node, (node_checks, ok)))
        for name, (verdict, detail) in node_checks.items():
            print(f"{node['name']:<16} {name:<12} {verdict:<4} {detail}")
    evidence = build_evidence(inventory, per_node, run_id, utc_now)
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, f"{run_id}-prod-node-check-evidence.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"\nevidence: {path}")
    print(f"result: {evidence['result']} — {'all nodes healthy' if evidence['result'] == 'PASS' else 'drift detected, D2 GATE FAILED'}")
    return 0 if evidence["result"] == "PASS" else 1


def _self_check(args, utc_now, run_id):
    """Offline proof of the checker: fake inventory + in-process runner, no SSH/VMs."""

    class FakeRunner:
        def __init__(self, access):
            self.access = access
            self.reachable = {"10.0.0.11", "10.0.0.21", "10.0.0.40"}
            self.disks = {"10.0.0.11": 600, "10.0.0.21": 480, "10.0.0.40": 512}
            # what each node's OWN daemon reports: hostname|state|manager?|NodeID
            self.identities = {
                "10.0.0.11": ("M1", "active", True, "node-m1"),
                "10.0.0.21": ("W1", "active", False, "node-w1"),
            }
            # what a manager sees, keyed by NodeID: role, availability, labels
            self.node_objects = {
                "node-m1": ("manager", "drained", {"role": "manager"}),
                "node-w1": ("worker", "active", {"role": "employee"}),  # label drift!
            }

        def run(self, host, command, timeout=20):
            if host not in self.reachable:
                return 1, "Connection refused"
            if command == "true":
                return 0, ""
            if command.startswith("df -BG"):
                return 0, f"{self.disks[host]}G"
            if "docker info --format" in command:
                name, state, control, node_id = self.identities[host]
                return 0, f"{name}|{state}|{'true' if control else 'false'}|{node_id}"
            if "docker node inspect" in command:
                node_id = command.split("inspect ")[1].split()[0]
                role, avail, labels = self.node_objects[node_id]
                return 0, f"{json.dumps(labels)}|{role}|{avail}"
            return 0, ""

    fake_inv = {
        "access": {"ssh_user": "ubuntu", "ssh_port": 22, "connect_timeout_s": 3},
        "nodes": [
            {"name": "M1", "host": "10.0.0.11", "role": "manager", "swarm": True,
             "labels": {"role": "manager"}, "expect_availability": "drained",
             "disk_min_gb": 10},
            {"name": "W1", "host": "10.0.0.21", "role": "worker", "swarm": True,
             "labels": {"role": "worker"}, "disk_min_gb": 500},
            # deliberately swarm:False — this fixture is the only cover for the
            # "not joined" branch. The shipped template marks O1 a joined worker
            # (CHG-246); do not copy this entry into prod_vms.json.
            {"name": "O1", "host": "10.0.0.40", "role": "observability", "swarm": False,
             "labels": {"observability": "true"}, "disk_min_gb": 500},
        ],
    }
    runner = FakeRunner(fake_inv["access"])
    # the same manager resolution the real run does — without it a worker's swarm
    # check fails closed for the wrong reason ("no manager") and the self-check
    # would "pass" while demonstrating nothing about label drift
    manager_host = _resolve_manager_host(fake_inv)
    per_node = []
    expect_ok = {"M1": True, "W1": False, "O1": True}  # W1 disk 480<500 AND label drift
    # say it before the per-node lines, so nobody reads this inventory as topology
    print("[self-check] FAKE inventory — classification proof only, not the shipped topology")
    for node in fake_inv["nodes"]:
        node_checks, ok = check_node(node, runner, manager_host=manager_host)
        per_node.append((node, (node_checks, ok)))
        for name, (verdict, detail) in node_checks.items():
            print(f"[self-check] {node['name']:<6} {name:<12} {verdict:<4} {detail}")
        assert ok == expect_ok[node["name"]], (
            f"self-check classification drift on {node['name']}: expected "
            f"{expect_ok[node['name']]}, got {ok}"
        )
    evidence = build_evidence(fake_inv, per_node, run_id, utc_now)
    assert evidence["result"] == "FAIL"  # W1 is intentionally drifting
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, f"self-check-{run_id}-prod-node-check-evidence.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"\n[self-check] PASS — checker classifies reachable/disk/label/availability "
          f"correctly; evidence at {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
