#!/usr/bin/env python3
"""cluster_check.py — validate a DEPLOYED Swarm, not a machine and not a stack file.

`prod_node_check.py` gates provisioning (SSH, disk, labels, per-VM swarm identity) and
`test_09_stack.py` gates the stack file. Neither can see what a real cluster actually
does with that file, and every failure found on 2026-09-20 lived in that gap:

  * a task that never starts because its node has no eligible slot —
    `Pending — no suitable node (max replicas per node)` — while the deploy reports success;
  * a bind-mount source missing on the node the task landed on;
  * a global agent covering one node while three others have no metrics at all;
  * replicas of a service that must be spread all landing on one node;
  * a published port nobody meant to publish.

This checker reads the cluster the way an operator would (`docker service ls/ps/inspect`,
`docker node ls/inspect`) and classifies what it finds. It writes nothing and needs no
SSH: point it at a manager with `--context`.

Two kinds of shortfall are kept apart, because they need different reactions:

  FAIL — the cluster could satisfy this and does not. Something is broken.
  WARN — the current topology cannot satisfy it (three replicas, one eligible node).
         Not a deployment defect; a fact about how many nodes exist.

Usage:
  python3 cluster_check.py [--context NAME] [--expect prod_vms.json] [--out DIR]
  python3 cluster_check.py --self-check     # offline: canned cluster fixtures

Exit code = number of FAILs (0 = healthy), so it can gate a deploy.
Stdlib only. Read-only. Nothing secret is printed.
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

# The only service allowed to publish a port (CHG-260). The stack file's own test
# (TestPublishedPorts) enforces this at review time; this is the same policy checked
# against a live cluster, where a hand-run `--publish-add` would be visible.
# Names are compared after the stack prefix is removed: a deployed service is
# `prod_openobserve`, the stack file calls it `openobserve`, and the allow-list is
# deliberately written the way the stack file is.
ALLOWED_PUBLISHERS = {"openobserve"}

STUCK_STATES = ("Pending", "Rejected", "Assigned", "Preparing", "Starting", "Failed")


class DockerRunner:
    """Shell out to the docker CLI. Swap for a fake in tests / --self-check."""

    def __init__(self, context=None, timeout=30):
        self.context = context
        self.timeout = timeout

    def run(self, args):
        cmd = ["docker"] + (["--context", self.context] if self.context else []) + args
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        except (subprocess.TimeoutExpired, OSError) as exc:
            return 1, f"runner error: {exc}"
        return proc.returncode, (proc.stdout or proc.stderr).strip()


def _json_lines(stdout):
    """`--format '{{json .}}'` prints one JSON object per line."""
    out = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def _unstack(name):
    """`prod_openobserve` -> `openobserve`. A stack name cannot contain `_`, so the
    first underscore is always the deploy-time separator (docker stack rm/deploy)."""
    return name.split("_", 1)[-1]


def _desired_running(task):
    """The CLI prints DesiredState capitalised: `"DesiredState":"Running"` (measured).
    Comparing against lowercase 'running' silently matched nothing on a live cluster."""
    return _text(task.get("DesiredState")).strip().lower() == "running"


def _parse_replicas(text):
    """'2/3' -> (2, 3). Tolerates the '(max 1 per node)' suffix service ls adds."""
    count = str(text).split(" ")[0]
    if "/" not in count:
        return 0, 0
    running, desired = count.split("/", 1)
    return int(running or 0), int(desired or 0)


def _text(value):
    """Swarm format fields are sometimes plain strings, sometimes {Name,Labels}."""
    if isinstance(value, dict):
        return value.get("Name") or value.get("name") or ""
    return str(value or "")


def _labels(node):
    labels = node.get("Labels") or {}
    return labels if isinstance(labels, dict) else {}


def _label_counts(nodes):
    counts = {}
    for node in nodes:
        for key, value in _labels(node).items():
            counts[(key, str(value))] = counts.get((key, str(value)), 0) + 1
    return counts


def _constraints(spec):
    placement = (spec.get("TaskTemplate") or {}).get("Placement") or {}
    return [c for c in placement.get("Constraints") or []]


def _matches(constraint, labels):
    """Evaluate the one constraint form this stack uses: `node.labels.<k> == <v>`."""
    text = constraint.strip()
    if not text.startswith("node.labels.") or "==" not in text:
        return None  # unknown form: report it rather than guess
    key, want = (part.strip() for part in text[len("node.labels."):].split("==", 1))
    return str(labels.get(key)) == want


def _nodes_matching(constraints, nodes):
    """(matching hostnames, count of constraints whose form we could not evaluate)."""
    matching, unknown = [], 0
    for node in nodes:
        labels = _labels(node)
        verdicts = [_matches(c, labels) for c in constraints]
        if any(v is None for v in verdicts):
            unknown += 1
            break
        if all(verdicts) and _text(node.get("Availability")) != "Drain":
            matching.append(_text(node.get("Hostname")))
    return matching, unknown


def _is_global(spec):
    return "Global" in (spec.get("Mode") or {})


def _max_per_node(spec):
    return (spec.get("TaskTemplate") or {}).get("Placement", {}).get("MaxReplicas")


def _running_on(spec_name, data):
    """Nodes with a task for this service that is running and desired to run."""
    nodes = set()
    for task in data["tasks"].get(spec_name, []):
        if not _desired_running(task):
            continue
        if _text(task.get("CurrentState")).startswith("Running"):
            nodes.add(_text(task.get("Node")))
    return nodes


class ClusterReport:
    """Collects outcomes; FAILs decide the exit code, WARNs are printed only."""

    def __init__(self):
        self.failures = []
        self.warnings = []
        self.checks = []

    def ok(self, name, detail=""):
        self.checks.append({"name": name, "status": "PASS", "detail": detail})
        print(f"PASS  {name}" + (f" — {detail}" if detail else ""))

    def fail(self, name, detail):
        self.checks.append({"name": name, "status": "FAIL", "detail": detail})
        self.failures.append((name, detail))
        print(f"FAIL  {name} — {detail}")

    def warn(self, name, detail):
        self.checks.append({"name": name, "status": "WARN", "detail": detail})
        self.warnings.append((name, detail))
        print(f"WARN  {name} — {detail}")


def collect(runner):
    """Gather the cluster's own view. Returns parsed surfaces keyed by name."""
    data = {"swarm": {}, "nodes": [], "services": [], "tasks": {}, "specs": {}}
    _, out = runner.run(["info", "--format", "{{json .Swarm}}"])
    if out.startswith("{"):
        try:
            data["swarm"] = json.loads(out)
        except ValueError:
            pass
    data["nodes"] = _json_lines(runner.run(["node", "ls", "--format", "{{json .}}"])[1])
    for node in data["nodes"]:
        # `docker node ls` does NOT report labels — only `node inspect` does. Without
        # this every constraint (`node.labels.x == y`) evaluates against an empty label
        # set, which silently reads as "no node matches" and turns a real shortfall into
        # a harmless-looking topology warning.
        raw = runner.run(["node", "inspect", _text(node.get("Hostname")),
                          "--format", "{{json .Spec.Labels}}"])[1]
        try:
            labels = json.loads(raw)
        except ValueError:
            labels = None
        node["Labels"] = labels if isinstance(labels, dict) else {}
    data["services"] = _json_lines(runner.run(["service", "ls", "--format", "{{json .}}"])[1])
    for svc in data["services"]:
        name = svc.get("Name", "")
        data["tasks"][name] = _json_lines(
            runner.run(["service", "ps", name, "--format", "{{json .}}"])[1])
        try:
            data["specs"][name] = json.loads(
                runner.run(["service", "inspect", name, "--format", "{{json .Spec}}"])[1])
        except ValueError:
            data["specs"][name] = {}
    return data


def check_swarm_active(data, report):
    state = (data["swarm"] or {}).get("LocalNodeState", "")
    if state == "active":
        report.ok("swarm-active", f"state=active control_available={_text(data['swarm'].get('ControlAvailable')) or 'false'}")
    else:
        report.fail("swarm-active", f"LocalNodeState={state or 'unknown'} — not a manager, or not a swarm")


def check_nodes_ready(data, report):
    if not data["nodes"]:
        report.fail("nodes-ready", "no node answered `docker node ls` (not a manager?)")
        return
    bad = [n for n in data["nodes"] if _text(n.get("Status")) != "Ready"]
    if bad:
        report.fail("nodes-ready", "not Ready: " + ", ".join(
            f"{_text(n.get('Hostname'))}({_text(n.get('Status'))})" for n in bad))
    else:
        report.ok("nodes-ready", f"{len(data['nodes'])} node(s) Ready")
    drained = [n for n in data["nodes"] if _text(n.get("Availability")) == "Drain"]
    if drained:
        report.warn("nodes-drained",
                    "drained (no new tasks; running ones stay): "
                    + ", ".join(_text(n.get("Hostname")) for n in drained))


def _eligible_node_count(spec, label_counts):
    """How many nodes this spec's own constraints select. None when it has no constraint,
    or when a constraint uses a form this checker does not evaluate (never guess)."""
    constraints = _constraints(spec)
    if not constraints:
        return None
    eligible = None
    for constraint in constraints:
        text = constraint.strip()
        pair = [part.strip() for part in text[len("node.labels."):].split("==", 1)]
        if not text.startswith("node.labels.") or len(pair) != 2:
            return None
        count = label_counts.get((pair[0], pair[1]), 0)
        eligible = count if eligible is None else min(eligible, count)
    return eligible


def topology_limited(data):
    """Services whose replica target the nodes present cannot satisfy.

    Not a defect: three replicas with `max_replicas_per_node: 1` need three eligible
    nodes. On the 1-node rehearsal cluster every 3-replica service lands here, and on the
    four production VMs nothing should — which is exactly why it is worth printing.
    """
    label_counts = _label_counts(data["nodes"])
    limited = {}
    for svc in data["services"]:
        name = svc.get("Name", "")
        running, desired = _parse_replicas(svc.get("Replicas", "0/0"))
        if running >= desired:
            continue
        spec = data["specs"].get(name, {})
        if _is_global(spec):
            continue
        eligible = _eligible_node_count(spec, label_counts)
        if eligible is None:
            continue
        per_node = _max_per_node(spec)
        if eligible * int(per_node or desired) < desired:
            limited[name] = (f"{name} {running}/{desired} (only {eligible} node(s) match its "
                             f"constraint, max {per_node or 'unlimited'} per node)")
    return limited


def check_replicas(data, report, limited):
    """Short of replicas is a FAIL — unless the topology makes it impossible."""
    short = []
    for svc in data["services"]:
        name = svc.get("Name", "")
        running, desired = _parse_replicas(svc.get("Replicas", "0/0"))
        if running < desired and name not in limited:
            short.append(f"{name} {running}/{desired}")
    if short:
        report.fail("replicas-complete", "short of replicas: " + ", ".join(short))
    else:
        report.ok("replicas-complete", f"all {len(data['services'])} service(s) at their desired count")
    if limited:
        report.warn("replicas-topology-limited",
                    "the nodes present cannot satisfy these (expected on a rehearsal cluster, "
                    "a defect on the production cluster): " + ", ".join(limited.values()))


def check_no_stuck_tasks(data, report, explained=()):
    """The failure a deploy does not report: a task that never starts, or keeps dying.

    A task waiting on a placement that cannot exist is already reported by
    `replicas-topology-limited`, so it is not counted twice. A dead task only means
    something while its service is short of replicas — every long-lived cluster carries
    failed tasks from superseded attempts, and those are history, not a fault.
    """
    waiting, dead, waiting_names = [], [], set()
    for svc in data["services"]:
        name = svc.get("Name", "")
        running, desired = _parse_replicas(svc.get("Replicas", "0/0"))
        short = running < desired
        errors = []
        for task in data["tasks"].get(name, []):
            state = _text(task.get("CurrentState"))
            if _desired_running(task) and any(state.startswith(s) for s in STUCK_STATES):
                if name not in explained and name not in waiting_names:
                    waiting_names.add(name)
                    waiting.append(f"{name}: {state[:60]} — {_text(task.get('Error')) or 'no error text'}")
            elif short and (state.startswith("Failed") or state.startswith("Rejected")):
                errors.append(_text(task.get("Error")) or state[:60])
        for text in dedupe(errors)[:2]:
            dead.append(f"{name} {running}/{desired} — {text}")
    details = []
    if waiting:
        details.append("waiting on placement or a mount it cannot get: " + "; ".join(waiting))
    if dead:
        details.append("short of replicas with tasks dying: " + "; ".join(dead))
    if details:
        report.fail("no-stuck-tasks", " | ".join(details))
    else:
        report.ok("no-stuck-tasks", "no task is waiting, and no short service is losing tasks")


def dedupe(items):
    """Order-preserving dedupe (task history repeats the same error per attempt)."""
    return list(dict.fromkeys(items))


def check_placement_spread(data, report):
    """Replicas that must not share a node must not share a node."""
    collisions, checked = [], 0
    for name, spec in data["specs"].items():
        if int(_max_per_node(spec) or 0) != 1:
            continue
        checked += 1
        per_node = {}
        for task in data["tasks"].get(name, []):
            if not _desired_running(task):
                continue
            if not _text(task.get("CurrentState")).startswith("Running"):
                continue
            per_node.setdefault(_text(task.get("Node")), []).append(_text(task.get("Name")))
        for node, names in per_node.items():
            if len(names) > 1:
                collisions.append(f"{name}: {len(names)} tasks on {node}")
    if collisions:
        report.fail("placement-spread", "; ".join(collisions))
    elif checked == 0:
        report.ok("placement-spread", "no service restricts replicas per node")
    else:
        report.ok("placement-spread", f"{checked} single-replica-per-node service(s) spread correctly")


def check_global_coverage(data, report):
    """Every node a global service's constraint selects must run one task of it."""
    gaps, checked, unknown = [], 0, []
    for name, spec in data["specs"].items():
        if not _is_global(spec):
            continue
        checked += 1
        expected, unevaluable = _nodes_matching(_constraints(spec), data["nodes"])
        if unevaluable:
            unknown.append(f"{name} ({_constraints(spec)})")
            continue
        missing = [n for n in expected if n not in _running_on(name, data)]
        if missing:
            gaps.append(f"{name} has no task on {', '.join(missing)}")
    if unknown:
        report.warn("global-coverage-unchecked",
                    "constraint form not evaluated: " + "; ".join(sorted(set(unknown))))
    if gaps:
        report.fail("global-coverage", "; ".join(gaps))
    elif checked == 0:
        report.ok("global-coverage", "no global service in this cluster")
    else:
        report.ok("global-coverage",
                  f"{checked} global service(s) run on every node their constraint selects")


def check_published_ports(data, report):
    published = {}
    for name, spec in data["specs"].items():
        ports = (spec.get("EndpointSpec") or {}).get("Ports") or []
        if ports:
            published[name] = [p.get("PublishedPort") for p in ports]
    unexpected = sorted(n for n in published if _unstack(n) not in ALLOWED_PUBLISHERS)
    if unexpected:
        report.fail("published-ports",
                    f"{unexpected} publish a port; only {sorted(ALLOWED_PUBLISHERS)} may")
    elif published:
        report.ok("published-ports", f"only the allowed service publishes: {published}")
    else:
        report.warn("published-ports", "no service publishes a port — the dashboard is unreachable")


def check_expectations(data, expect, report):
    """Compare the cluster with the inventory the operator wrote down."""
    if not expect:
        return
    want = [n for n in expect.get("nodes", []) if n.get("swarm")]
    have = {_text(n.get("Hostname")): n for n in data["nodes"]}
    if len(want) != len(have):
        report.fail("expect-node-count",
                    f"inventory describes {len(want)} swarm node(s), the cluster has {len(have)}: "
                    f"{sorted(have)}")
    else:
        report.ok("expect-node-count", f"{len(have)} node(s), as the inventory describes")
    want_obs = sum(1 for n in want if (n.get("labels") or {}).get("observability") == "true")
    have_obs = sum(1 for n in data["nodes"] if _labels(n).get("observability") == "true")
    if have_obs < want_obs:
        report.fail("expect-observability-labels",
                    f"inventory labels {want_obs} node(s) observability=true, the cluster has "
                    f"{have_obs} — those nodes' host metrics and logs are not collected "
                    f"(CHG-257/263)")
    else:
        report.ok("expect-observability-labels", f"{have_obs} node(s) labelled observability=true")


def run_checks(data, report, expect=None):
    check_swarm_active(data, report)
    check_nodes_ready(data, report)
    # Computed once: a service the topology cannot satisfy is warned about, and its
    # waiting tasks are then not counted a second time as stuck tasks.
    limited = topology_limited(data)
    check_replicas(data, report, limited)
    check_no_stuck_tasks(data, report, explained=set(limited))
    check_placement_spread(data, report)
    check_global_coverage(data, report)
    check_published_ports(data, report)
    check_expectations(data, expect, report)


def build_evidence(data, report, stamp):
    return {
        "record": "cluster_check",
        "run_id": stamp,
        "utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "nodes": [_text(n.get("Hostname")) for n in data["nodes"]],
        "services": len(data["services"]),
        "checks": report.checks,
        "failures": len(report.failures),
        "warnings": len(report.warnings),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="validate a deployed Swarm (read-only)")
    parser.add_argument("--context", help="docker context to query (default: the current one)")
    parser.add_argument("--expect", help="prod_vms.json inventory to compare against")
    parser.add_argument("--out", help="directory for an evidence JSON")
    parser.add_argument("--self-check", action="store_true",
                        help="run against bundled fixtures (no cluster needed)")
    args = parser.parse_args(argv)

    if args.self_check:
        return self_check()

    data = collect(DockerRunner(args.context))
    if not data["services"]:
        print("FAIL  cluster-reachable — `docker service ls` returned nothing: wrong context, "
              "not a manager, or nothing deployed")
        return 1
    expect = None
    inventory = args.expect or os.path.join(REPO_ROOT, "code/01_platform/04_scripts/prod_vms.json")
    if os.path.exists(inventory):
        with open(inventory, encoding="utf-8") as fh:
            expect = json.load(fh)
    report = ClusterReport()
    run_checks(data, report, expect)
    if args.out:
        os.makedirs(args.out, exist_ok=True)
        stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = os.path.join(args.out, f"cluster-check-{stamp}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(build_evidence(data, report, stamp), fh, indent=2)
        print(f"evidence: {path}")
    print(f"\n{len(report.failures)} failure(s), {len(report.warnings)} warning(s)")
    return len(report.failures)


# --------------------------------------------------------------------------- #
# --self-check: the same classification driven by canned cluster fixtures.
# --------------------------------------------------------------------------- #
class FixtureRunner:
    """Serves fixture output for the exact commands collect() issues."""

    def __init__(self, fixture):
        self.fixture = fixture

    def run(self, args):
        key = " ".join(args)
        for pattern, payload in self.fixture.items():
            if key.startswith(pattern):
                return 0, payload
        return 0, ""


# Shaped exactly like the real CLI: `node ls` carries NO labels (measured 2026-09-20 on
# the rehearsal swarm: `{"Hostname":"saurabh-MS-7D90","Labels":null}`), so the labels can
# only come from `node inspect`. A fixture that puts labels in `node ls` would hide that.
NODES = [
    {"Hostname": "m1", "Status": "Ready", "Availability": "Active"},
    {"Hostname": "m2", "Status": "Ready", "Availability": "Active"},
    {"Hostname": "o1", "Status": "Ready", "Availability": "Active"},
]

LABELS = {
    "m1": {"role": "worker"},
    "m2": {"role": "worker"},
    "o1": {"observability": "true"},
}


def _node_inspects(labels):
    return {f"node inspect {host}": json.dumps(value) for host, value in labels.items()}


HAPPY = dict(_node_inspects(LABELS), **{
    "info --format": json.dumps({"LocalNodeState": "active", "ControlAvailable": True}),
    "node ls": "\n".join(json.dumps(n) for n in NODES),
    "service ls": json.dumps({"Name": "prod_node-exporter", "Replicas": "3/3"}),
    "service ps": "\n".join(json.dumps({"Name": f"prod_node-exporter.{i}", "Node": n["Hostname"],
                                        "DesiredState": "Running",
                                        "CurrentState": "Running 5 minutes ago", "Error": ""})
                            for i, n in enumerate(NODES, 1)),
    "service inspect": json.dumps({"Mode": {"Global": {}}, "EndpointSpec": {"Ports": []},
                                   "TaskTemplate": {"Placement": {
                                       "Constraints": ["node.labels.observability == true"]}}}),
})


def self_check():
    """Prove the classifications offline against scenarios the live cluster produced."""
    broken = []

    def scenario(name, fixture, expect_fail=(), expect_warn=(), forbid_fail=(), forbid_warn=()):
        report = ClusterReport()
        run_checks(collect(FixtureRunner(fixture)), report)
        got_fail = {n for n, _ in report.failures}
        got_warn = {n for n, _ in report.warnings}
        for want in expect_fail:
            if want not in got_fail:
                print(f"[{name}] MISSING expected FAIL {want}; got {sorted(got_fail)}")
                broken.append(name)
        for want in expect_warn:
            if want not in got_warn:
                print(f"[{name}] MISSING expected WARN {want}; got {sorted(got_warn)}")
                broken.append(name)
        for unwanted in forbid_fail:
            if unwanted in got_fail:
                print(f"[{name}] UNEXPECTED FAIL {unwanted}; got {sorted(got_fail)}")
                broken.append(name)
        for unwanted in forbid_warn:
            if unwanted in got_warn:
                print(f"[{name}] UNEXPECTED WARN {unwanted}; got {sorted(got_warn)}")
                broken.append(name)
        print(f"[{name}] FAILs={sorted(got_fail)} WARNs={sorted(got_warn)}")
        return len(report.failures)

    # 1. healthy: a global service on the one node the constraint selects, no FAILs
    if scenario("clean", HAPPY) != 0:
        print("[clean] expected exit 0")
        broken.append("clean")

    # 2. the failure a deploy does not report: a task pending on placement
    stuck = dict(HAPPY)
    stuck["service ps"] = json.dumps({"Name": "prod_node-exporter.2", "Node": "",
                                      "DesiredState": "Running", "CurrentState": "Pending 2 minutes ago",
                                      "Error": "no suitable node (max replicas per node)"})
    scenario("stuck-task", stuck, expect_fail={"no-stuck-tasks"})

    # 3. a global agent covering one node of the three its constraint selects
    uncovered = dict(HAPPY)
    uncovered["service inspect"] = json.dumps({
        "Mode": {"Global": {}}, "EndpointSpec": {"Ports": []},
        "TaskTemplate": {"Placement": {"Constraints": ["node.labels.role == worker"]}}})
    uncovered["service ps"] = json.dumps({"Name": "prod_node-exporter.1", "Node": "m1",
                                          "DesiredState": "Running",
                                          "CurrentState": "Running 5 minutes ago", "Error": ""})
    scenario("coverage-gap", uncovered, expect_fail={"global-coverage"})

    # 4. a service that may not publish a port, publishing one
    ported = dict(HAPPY)
    ported["service inspect"] = json.dumps({
        "Mode": {"Replicated": {"Replicas": 1}},
        "EndpointSpec": {"Ports": [{"PublishedPort": 9090}]},
        "TaskTemplate": {"Placement": {"MaxReplicas": 1}}})
    ported["service ls"] = json.dumps({"Name": "prod_alert-consumer", "Replicas": "1/1"})
    ported["service ps"] = json.dumps({"Name": "prod_alert-consumer.1", "Node": "o1",
                                       "DesiredState": "Running",
                                       "CurrentState": "Running 5 minutes ago", "Error": ""})
    scenario("stray-port", ported, expect_fail={"published-ports"})

    # 5. three replicas, one eligible node -> WARN, never FAIL
    limited = dict(HAPPY)
    limited["service ls"] = json.dumps({"Name": "prod_fluss-tablet-1", "Replicas": "1/3"})
    limited["service inspect"] = json.dumps({
        "Mode": {"Replicated": {"Replicas": 3}}, "EndpointSpec": {"Ports": []},
        "TaskTemplate": {"Placement": {"MaxReplicas": 1, "Constraints": ["node.labels.observability == true"]}}})
    limited["service ps"] = json.dumps({"Name": "prod_fluss-tablet-1.1", "Node": "o1",
                                        "DesiredState": "Running",
                                        "CurrentState": "Running 5 minutes ago", "Error": ""})
    if scenario("topology-limited", limited, expect_warn={"replicas-topology-limited"}) != 0:
        print("[topology-limited] expected 0 FAILs — a small cluster must not read as broken")
        broken.append("topology-limited")

    # 6. the deployed name is stack-prefixed: `prod_openobserve` IS the allowed publisher.
    #    (Regression: the live rehearsal swarm failed this as a stray port.)
    allowed = dict(HAPPY)
    allowed["service ls"] = json.dumps({"Name": "prod_openobserve", "Replicas": "1/1"})
    allowed["service inspect"] = json.dumps({
        "Mode": {"Replicated": {"Replicas": 1}},
        "EndpointSpec": {"Ports": [{"PublishedPort": 5080, "PublishMode": "host"}]},
        "TaskTemplate": {"Placement": {"Constraints": ["node.labels.observability == true"]}}})
    allowed["service ps"] = json.dumps({"Name": "prod_openobserve.1", "Node": "o1",
                                        "DesiredState": "Running",
                                        "CurrentState": "Running 5 minutes ago", "Error": ""})
    if scenario("allowed-publisher", allowed, forbid_fail={"published-ports"}) != 0:
        print("[allowed-publisher] expected 0 FAILs — openobserve is the one service allowed to publish")
        broken.append("allowed-publisher")

    # 7. a service short of replicas whose tasks keep dying (live: prod_ingestion 0/1)
    dying = dict(_node_inspects(LABELS))
    dying.update({
        "info --format": HAPPY["info --format"],
        "node ls": HAPPY["node ls"],
        "service ls": json.dumps({"Name": "prod_ingestion", "Replicas": "0/1"}),
        "service ps": "\n".join(json.dumps({"Name": "prod_ingestion.1", "Node": "m1",
                                            "DesiredState": "Shutdown",
                                            "CurrentState": "Failed 3 minutes ago",
                                            "Error": "task: non-zero exit (1)"}) for _ in range(3)),
        "service inspect": json.dumps({
            "Mode": {"Replicated": {"Replicas": 1}}, "EndpointSpec": {"Ports": []},
            "TaskTemplate": {"Placement": {"Constraints": ["node.labels.role == worker"]}}}),
    })
    scenario("dying-task", dying, expect_fail={"replicas-complete", "no-stuck-tasks"})

    # 8. a failed task from an earlier attempt, on a service that is now complete:
    #    history is not a problem and must not be reported as one.
    history = dict(HAPPY)
    history["service ls"] = json.dumps({"Name": "prod_flink-taskmanager", "Replicas": "3/3"})
    history["service ps"] = "\n".join(
        [json.dumps({"Name": "prod_flink-taskmanager.1", "Node": "m1", "DesiredState": "Running",
                     "CurrentState": "Running 30 minutes ago", "Error": ""}),
         json.dumps({"Name": "prod_flink-taskmanager.1", "Node": "m1", "DesiredState": "Shutdown",
                     "CurrentState": "Failed 31 minutes ago", "Error": "task: non-zero exit (1)"})])
    history["service inspect"] = json.dumps({
        "Mode": {"Replicated": {"Replicas": 3}}, "EndpointSpec": {"Ports": []},
        "TaskTemplate": {"Placement": {"MaxReplicas": 1,
                                       "Constraints": ["node.labels.role == worker"]}}})
    if scenario("superseded-failure", history, forbid_fail={"no-stuck-tasks"}) != 0:
        print("[superseded-failure] expected 0 FAILs — an old failed attempt is not a live fault")
        broken.append("superseded-failure")

    if broken:
        print(f"self-check FAILED: {sorted(set(broken))}")
        return 1
    print("self-check OK: clean, stuck-task, coverage-gap, stray-port and topology-limited "
          "classify as designed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
