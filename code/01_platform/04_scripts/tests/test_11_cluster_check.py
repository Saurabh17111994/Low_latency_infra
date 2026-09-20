"""CHG-264 — offline tests for cluster_check.py (no swarm, no docker CLI, no SSH).

Every fixture here is a shape the docker CLI really printed on the rehearsal swarm
(2026-09-20), because each of them broke the checker when it assumed otherwise:

  * `docker node ls --format '{{json .}}'` has no `Labels` key at all — labels come from
    `docker node inspect` (measured: `{"Hostname":"saurabh-MS-7D90","Labels":null}`). With
    labels unread, every constraint evaluates to "no node matches", which turned a real
    replica shortfall into a harmless-looking topology warning and let a coverage gap PASS;
  * tasks print `"DesiredState":"Running"`, capitalised — comparing lowercase matched
    nothing, so stuck-task detection was dead on a live cluster while passing here;
  * deployed services are stack-prefixed (`prod_openobserve`), the allow-list is written
    the way the stack file writes it (`openobserve`);
  * a service that keeps dying shows `desired=Shutdown current=Failed
    err="task: non-zero exit (1)"`, which the "is it desired-running?" filter skipped.
"""

import json
import os
import sys
import tempfile

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)
import cluster_check as cc  # noqa: E402


class Recorder(cc.FixtureRunner):
    """Serves fixture output and remembers every command the checker issued."""

    def __init__(self, fixture):
        super().__init__(fixture)
        self.commands = []

    def run(self, args):
        self.commands.append(" ".join(args))
        return super().run(args)


# The rehearsal inventory: two workers and the observability node.
NODE_RECORDS = [
    {"Hostname": "m1", "Status": "Ready", "Availability": "Active", "ID": "id-m1"},
    {"Hostname": "m2", "Status": "Ready", "Availability": "Active", "ID": "id-m2"},
    {"Hostname": "o1", "Status": "Ready", "Availability": "Active", "ID": "id-o1"},
]
LABELS = {"m1": {"role": "worker"}, "m2": {"role": "worker"}, "o1": {"observability": "true"}}
SWARM_ACTIVE = {"LocalNodeState": "active", "ControlAvailable": True}


def _fixture(services, tasks, specs, labels=None, nodes=None):
    """Build the exact surface collect() reads. `tasks`/`specs` are keyed by service."""
    fixture = {f"node inspect {host}": json.dumps(value)
               for host, value in (labels if labels is not None else LABELS).items()}
    fixture["info --format"] = json.dumps(SWARM_ACTIVE)
    fixture["node ls"] = "\n".join(json.dumps(n) for n in (nodes or NODE_RECORDS))
    fixture["service ls"] = "\n".join(json.dumps(s) for s in services)
    for name, rows in tasks.items():
        fixture[f"service ps {name}"] = "\n".join(json.dumps(r) for r in rows)
    for name, spec in specs.items():
        fixture[f"service inspect {name}"] = json.dumps(spec)
    return fixture


def _task(name, node, desired="Running", current="Running 5 minutes ago", error=""):
    return {"Name": name, "Node": node, "DesiredState": desired,
            "CurrentState": current, "Error": error}


def _global_spec(constraints, max_per_node=None):
    placement = {"Constraints": constraints}
    if max_per_node:
        placement["MaxReplicas"] = max_per_node
    return {"Mode": {"Global": {}}, "EndpointSpec": {"Ports": []},
            "TaskTemplate": {"Placement": placement}}


def _replicated_spec(replicas, constraints=None, max_per_node=None, ports=None):
    placement = {}
    if constraints:
        placement["Constraints"] = constraints
    if max_per_node:
        placement["MaxReplicas"] = max_per_node
    return {"Mode": {"Replicated": {"Replicas": replicas}},
            "EndpointSpec": {"Ports": ports or []},
            "TaskTemplate": {"Placement": placement}}


def _check(fixture, expect=None):
    rec = Recorder(fixture)
    report = cc.ClusterReport()
    cc.run_checks(cc.collect(rec), report, expect)
    return report, rec


def _fails(report):
    return {name for name, _ in report.failures}


def _warns(report):
    return {name for name, _ in report.warnings}


# --------------------------------------------------------------------------- #
# Labels: the CLI does not put them in `node ls`
# --------------------------------------------------------------------------- #
def test_labels_are_read_from_node_inspect_not_from_node_ls():
    """`node ls` carries no Labels key; the labels must come from `node inspect`."""
    fixture = _fixture([{"Name": "prod_node-exporter", "Replicas": "1/1"}],
                       {"prod_node-exporter": [_task("prod_node-exporter.1", "o1")]},
                       {"prod_node-exporter": _global_spec(["node.labels.observability == true"])})
    # the fixture's `node ls` rows deliberately have no Labels, exactly like the CLI
    assert all("Labels" not in n for n in NODE_RECORDS)
    data = cc.collect(Recorder(fixture))
    assert data["nodes"][0]["Labels"] == {"role": "worker"}, data["nodes"][0]


def test_a_coverage_gap_is_found_even_though_node_ls_has_no_labels():
    """With labels unread, expected==[] and this gap PASSED on the live cluster."""
    fixture = _fixture([{"Name": "prod_node-exporter", "Replicas": "1/3"}],
                       {"prod_node-exporter": [_task("prod_node-exporter.1", "m1")]},
                       {"prod_node-exporter": _global_spec(["node.labels.role == worker"])})
    report, _ = _check(fixture)
    assert "global-coverage" in _fails(report), report.checks
    detail = dict(report.failures)["global-coverage"]
    assert "m2" in detail and "m1" not in detail.replace("prod_node-exporter", ""), detail


def test_every_node_is_inspected_for_labels():
    report, rec = _check(_fixture([{"Name": "prod_x", "Replicas": "0/0"}], {}, {}))
    inspected = sorted(c for c in rec.commands if c.startswith("node inspect"))
    assert len(inspected) == 3, inspected


# --------------------------------------------------------------------------- #
# Task state: the CLI capitalises DesiredState
# --------------------------------------------------------------------------- #
def test_the_capitalised_desired_state_the_cli_prints_counts_as_running():
    """`"DesiredState":"Running"` must count as running, or stacks read as empty."""
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "o1", desired="Running")]},
                       {"prod_x": _replicated_spec(1, ["node.labels.observability == true"])})
    data = cc.collect(Recorder(fixture))
    assert cc._running_on("prod_x", data) == {"o1"}


def test_a_pending_task_on_an_impossible_placement_is_not_a_stuck_task():
    """The jobmanager/taskmanager shape on a 1-node cluster: WARN, not FAIL."""
    fixture = _fixture(
        [{"Name": "prod_flink-taskmanager", "Replicas": "1/3"}],
        {"prod_flink-taskmanager": [
            _task("prod_flink-taskmanager.1", "m1"),
            _task("prod_flink-taskmanager.2", "", current="Pending about an hour ago",
                  error="no suitable node (max replicas per node)")]},
        {"prod_flink-taskmanager": _replicated_spec(3, ["node.labels.role == worker"], 1)})
    report, _ = _check(fixture)
    assert report.failures == [], report.failures
    assert "replicas-topology-limited" in _warns(report)
    assert "no-stuck-tasks" not in _fails(report), "reported twice"


def test_a_stuck_task_on_a_service_the_topology_could_satisfy_is_a_failure():
    """Pending with no placement excuse (global service, one eligible node, one task)."""
    fixture = _fixture(
        [{"Name": "prod_otel-collector", "Replicas": "0/1"}],
        {"prod_otel-collector": [_task("prod_otel-collector.1", "",
                                       current="Pending 2 minutes ago",
                                       error="invalid mount config for type bind")]},
        {"prod_otel-collector": _global_spec(["node.labels.observability == true"])})
    report, _ = _check(fixture)
    assert "no-stuck-tasks" in _fails(report)
    assert "invalid mount config" in dict(report.failures)["no-stuck-tasks"]


def test_a_dying_task_on_a_short_service_is_reported_with_its_error():
    """The live ingestion shape: desired=Shutdown, current=Failed, exit code 1."""
    fixture = _fixture(
        [{"Name": "prod_ingestion", "Replicas": "0/1"}],
        {"prod_ingestion": [_task("prod_ingestion.1", "m1", desired="Shutdown",
                                  current="Failed about an hour ago",
                                  error="task: non-zero exit (1)")] * 3},
        {"prod_ingestion": _replicated_spec(1, ["node.labels.role == worker"])})
    report, _ = _check(fixture)
    assert "replicas-complete" in _fails(report)
    assert "no-stuck-tasks" in _fails(report)
    detail = dict(report.failures)["no-stuck-tasks"]
    assert "non-zero exit (1)" in detail, detail
    assert detail.count("non-zero exit (1)") == 1, "the same error repeated per attempt"


def test_a_superseded_failure_on_a_complete_service_is_not_reported():
    """History is not a fault: an earlier failed attempt under a healthy service."""
    fixture = _fixture(
        [{"Name": "prod_flink-taskmanager", "Replicas": "3/3"}],
        {"prod_flink-taskmanager": [
            _task("prod_flink-taskmanager.1", "m1"),
            _task("prod_flink-taskmanager.1", "m1", desired="Shutdown",
                  current="Failed 31 minutes ago", error="task: non-zero exit (1)")]},
        {"prod_flink-taskmanager": _replicated_spec(3, ["node.labels.role == worker"], 1)})
    report, _ = _check(fixture)
    assert report.failures == [], report.failures


# --------------------------------------------------------------------------- #
# Names and ports
# --------------------------------------------------------------------------- #
def test_the_only_allowed_publisher_is_matched_after_the_stack_prefix():
    """`prod_openobserve` IS the service the stack file calls `openobserve`."""
    fixture = _fixture(
        [{"Name": "prod_openobserve", "Replicas": "1/1"}],
        {"prod_openobserve": [_task("prod_openobserve.1", "o1")]},
        {"prod_openobserve": _replicated_spec(
            1, ["node.labels.observability == true"],
            ports=[{"PublishedPort": 5080, "PublishMode": "host"}])})
    report, _ = _check(fixture)
    assert report.failures == [], report.failures
    assert "prod_openobserve" in dict([(c["name"], c["detail"]) for c in report.checks])["published-ports"]


def test_any_other_publisher_is_a_failure():
    fixture = _fixture(
        [{"Name": "prod_alert-consumer", "Replicas": "1/1"}],
        {"prod_alert-consumer": [_task("prod_alert-consumer.1", "o1")]},
        {"prod_alert-consumer": _replicated_spec(1, ports=[{"PublishedPort": 9090}])})
    report, _ = _check(fixture)
    assert "published-ports" in _fails(report)
    assert "prod_alert-consumer" in dict(report.failures)["published-ports"]


def test_publishing_nothing_at_all_is_a_warning_not_a_failure():
    """No published port means the dashboard is unreachable — worth saying, not fatal."""
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m1")]},
                       {"prod_x": _replicated_spec(1)})
    report, _ = _check(fixture)
    assert report.failures == [], report.failures
    assert "published-ports" in _warns(report)


# --------------------------------------------------------------------------- #
# Placement, nodes, constraints
# --------------------------------------------------------------------------- #
def test_two_replicas_of_a_single_replica_per_node_service_on_one_node_is_a_failure():
    fixture = _fixture(
        [{"Name": "prod_fluss-tablet-1", "Replicas": "2/2"}],
        {"prod_fluss-tablet-1": [_task("prod_fluss-tablet-1.1", "m1"),
                                 _task("prod_fluss-tablet-1.2", "m1")]},
        {"prod_fluss-tablet-1": _replicated_spec(2, ["node.labels.role == worker"], 1)})
    report, _ = _check(fixture)
    assert "placement-spread" in _fails(report), report.checks
    assert "2 tasks on m1" in dict(report.failures)["placement-spread"]


def test_a_drained_node_is_a_warning_not_a_failure():
    nodes = [dict(NODE_RECORDS[0], Availability="Drain"), NODE_RECORDS[1], NODE_RECORDS[2]]
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m2")]},
                       {"prod_x": _replicated_spec(1)}, nodes=nodes)
    report, _ = _check(fixture)
    assert report.failures == [], report.failures
    assert "nodes-drained" in _warns(report) and "m1" in dict(report.warnings)["nodes-drained"]


def test_a_not_ready_node_is_a_failure():
    nodes = [dict(NODE_RECORDS[0], Status="Down"), NODE_RECORDS[1], NODE_RECORDS[2]]
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m2")]},
                       {"prod_x": _replicated_spec(1)}, nodes=nodes)
    report, _ = _check(fixture)
    assert "nodes-ready" in _fails(report)
    assert "m1(Down)" in dict(report.failures)["nodes-ready"]


def test_a_constraint_form_the_checker_cannot_evaluate_is_reported_not_guessed():
    """Never guess: an unknown constraint form is a WARN, not a silent PASS or FAIL."""
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m1")]},
                       {"prod_x": _global_spec(["node.hostname == m2"])})
    report, _ = _check(fixture)
    assert "global-coverage-unchecked" in _warns(report)
    assert "global-coverage" not in _fails(report), "guessed a verdict it could not evaluate"


def test_a_cluster_that_is_not_a_manager_fails_closed():
    """A non-manager answers `docker info` but cannot answer `docker node ls`."""
    fixture = _fixture([], {}, {})
    fixture["info --format"] = json.dumps({"LocalNodeState": "inactive"})
    fixture["node ls"] = ""
    report, _ = _check(fixture)
    assert "swarm-active" in _fails(report)
    assert "nodes-ready" in _fails(report)


def test_it_only_ever_issues_read_only_docker_commands():
    """The checker runs against production; prove every verb it uses is a read."""
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m1")]},
                       {"prod_x": _replicated_spec(1)})
    report, rec = _check(fixture)
    verbs = {c.split()[0] for c in rec.commands}
    assert verbs <= {"info", "node", "service"}, verbs
    assert rec.commands and all(
        c.split()[1] in {"ls", "inspect", "ps"} or c.startswith("info ") for c in rec.commands
    ), rec.commands


# --------------------------------------------------------------------------- #
# Inventory expectations and the CLI contract
# --------------------------------------------------------------------------- #
def test_the_inventory_is_compared_with_the_cluster():
    """4 VMs written down, 1 node answering, is exactly what must not pass silently."""
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m1")]},
                       {"prod_x": _replicated_spec(1)})
    expect = {"nodes": [
        {"name": "M1", "swarm": True, "labels": {"role": "manager"}},
        {"name": "W1", "swarm": True, "labels": {"role": "worker"}},
        {"name": "W2", "swarm": True, "labels": {"role": "worker"}},
        {"name": "O1", "swarm": True, "labels": {"observability": "true"}}]}
    report, _ = _check(fixture, expect)
    assert "expect-node-count" in _fails(report)
    assert "4 swarm node(s)" in dict(report.failures)["expect-node-count"]
    # one observability node expected, one present -> no second complaint
    assert "expect-observability-labels" not in _fails(report)


def test_an_observability_label_the_inventory_claims_but_the_cluster_lacks_is_a_failure():
    """The per-node gap, expressed as a check rather than a paragraph."""
    fixture = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                       {"prod_x": [_task("prod_x.1", "m1")]},
                       {"prod_x": _replicated_spec(1)},
                       labels={"m1": {"role": "worker"}, "m2": {"role": "worker"},
                               "o1": {"role": "worker"}})
    expect = {"nodes": [{"name": "M1", "swarm": True, "labels": {"role": "worker"}},
                        {"name": "M2", "swarm": True, "labels": {"role": "worker"}},
                        {"name": "O1", "swarm": True, "labels": {"observability": "true"}}]}
    report, _ = _check(fixture, expect)
    assert "expect-observability-labels" in _fails(report), report.checks


def test_the_cli_exit_code_is_the_number_of_failures():
    """0 on a healthy cluster, and one per FAIL otherwise — it is used as a gate."""
    healthy = _fixture([{"Name": "prod_openobserve", "Replicas": "1/1"}],
                       {"prod_openobserve": [_task("prod_openobserve.1", "o1")]},
                       {"prod_openobserve": _replicated_spec(
                           1, ["node.labels.observability == true"],
                           ports=[{"PublishedPort": 5080}])})
    broken = _fixture([{"Name": "prod_x", "Replicas": "1/1"}],
                      {"prod_x": [_task("prod_x.1", "m1")]},
                      {"prod_x": _replicated_spec(1, ports=[{"PublishedPort": 9090}])})
    real_runner = cc.DockerRunner
    try:
        cc.DockerRunner = lambda context=None: Recorder(healthy)
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out")
            assert cc.main(["--out", out]) == 0
            evidence = os.listdir(out)[0]
            with open(os.path.join(out, evidence), encoding="utf-8") as fh:
                payload = json.load(fh)
            assert payload["failures"] == 0 and payload["record"] == "cluster_check"
            assert {c["status"] for c in payload["checks"]} <= {"PASS", "WARN", "FAIL"}
            assert payload["nodes"] == ["m1", "m2", "o1"]
        cc.DockerRunner = lambda context=None: Recorder(broken)
        assert cc.main([]) == 1
    finally:
        cc.DockerRunner = real_runner


def test_a_cluster_that_answers_nothing_is_a_single_reachability_failure():
    """Wrong context / not a manager / nothing deployed must not print 20 FAILs."""
    real_runner = cc.DockerRunner
    try:
        cc.DockerRunner = lambda context=None: Recorder({})
        assert cc.main([]) == 1
    finally:
        cc.DockerRunner = real_runner


def test_self_check_passes():
    assert cc.main(["--self-check"]) == 0
