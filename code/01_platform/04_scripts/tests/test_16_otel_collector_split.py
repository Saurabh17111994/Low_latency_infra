"""The collector split (CHG-273, issue #54): one file, one reader; one scraper.

The rule this file guards, and why it is not a style preference:

* A **file** belongs to exactly one node — the host's `/var/log` and the
  node-local named volumes (`ingestion-logs`, `flink-logs`, `fluss-logs`). So the
  service that reads files must run on **every** node (`mode: global`, no
  placement constraint) or the logs written on the other nodes are collected from
  nowhere: an empty directory reads as silence, not as an error.
* The scrape jobs `infra-host` / `infra-containers` use `dns_sd_configs`, so
  `tasks.node-exporter` / `tasks.cadvisor` yields **one target per agent** — every
  node's agent, from one collector. (A static target for the same name keeps only
  one address: measured 2026-09-21, CHG-283.) The service that scrapes must
  therefore be a **single writer**, or every collector scrapes every agent and each
  host metric is stored once per collector — and every `rate()` and `sum()` in a
  panel is wrong.

Those two requirements point in opposite directions, which is why the collector
is two services. These tests fail if either half drifts back toward the other.
"""

import pathlib

import pytest
import yaml

DOCKER = pathlib.Path(__file__).resolve().parents[2] / "01_docker"
STACK = DOCKER / "docker-stack.yml"
COMPOSE = DOCKER / "docker-compose.yml"
NETWORK_CFG = DOCKER / "otel-collector-config.swarm.yaml"
LOGS_CFG = DOCKER / "otel-collector-logs.swarm.yaml"
LOGS_CFG_DEV = DOCKER / "otel-collector-logs.yaml"

FILE_MOUNTS = ("ingestion-logs", "flink-logs", "fluss-logs", "/var/log")


@pytest.fixture(scope="module")
def stack():
    return yaml.safe_load(STACK.read_text())


@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load(COMPOSE.read_text())["services"]


@pytest.fixture(scope="module")
def network_cfg():
    return yaml.safe_load(NETWORK_CFG.read_text())


@pytest.fixture(scope="module")
def logs_cfg():
    return yaml.safe_load(LOGS_CFG.read_text())


def _mounts(service):
    out = []
    for m in service.get("volumes", []):
        out.append(m if isinstance(m, str) else m.get("source", ""))
    return " ".join(out)


def test_the_file_half_runs_on_every_node_and_the_scraper_runs_once(stack):
    logs = stack["services"]["otel-collector-logs"]
    net = stack["services"]["otel-collector"]
    assert logs["deploy"]["mode"] == "global", "per-host file reading must be global"
    assert "placement" not in logs["deploy"], (
        "a placement constraint on a global service silently narrows it to the "
        "labelled nodes — that is exactly the four-VM gap CHG-273 closed"
    )
    assert net["deploy"].get("replicas") == 1
    assert "mode" not in net["deploy"], "the scraper is a singleton, not a global service"
    assert net["deploy"]["placement"]["constraints"] == ["node.labels.observability == true"]


def test_only_the_file_half_mounts_the_per_node_files(stack):
    logs = _mounts(stack["services"]["otel-collector-logs"])
    net = _mounts(stack["services"]["otel-collector"])
    for mount in FILE_MOUNTS:
        assert mount in logs, f"{mount} must be read by the per-host service"
        assert mount not in net, (
            f"{mount} is node-local; a singleton reading it mounts its own copy, "
            "which is either empty or a second reader of the same file"
        )


def test_the_file_half_carries_the_node_name(stack, logs_cfg):
    service = stack["services"]["otel-collector-logs"]
    assert service["hostname"] == "{{.Node.Hostname}}", (
        "HOSTNAME is what the resource/node processor turns into host.name; "
        "without it four nodes' logs land in one stream indistinguishable"
    )
    assert "resource/node" in logs_cfg["processors"]
    value = logs_cfg["processors"]["resource/node"]["attributes"][0]["value"]
    assert value.startswith("${env:HOSTNAME"), f"expected the HOSTNAME env, got {value}"
    for name, pipeline in logs_cfg["service"]["pipelines"].items():
        assert "resource/node" in pipeline["processors"], f"{name} would lose the node tag"


def test_neither_half_reads_what_the_other_owns(network_cfg, logs_cfg):
    # the singleton must not read files
    assert not [r for r in network_cfg["receivers"] if r.startswith("filelog")], (
        "a filelog receiver in the singleton is a second reader of the same files"
    )
    # the per-host service must not scrape or accept pushed telemetry
    assert "prometheus" not in logs_cfg["receivers"], (
        "scraping from every node stores every host metric once per node"
    )
    assert "otlp" not in logs_cfg["receivers"]
    assert not [p for p in logs_cfg["service"]["pipelines"] if p.startswith("metrics")]
    # and each config exports only its own signals
    assert all("logs" in e or "traces" not in e for e in logs_cfg["exporters"])


def test_the_otel_collector_service_name_still_exists(stack, compose):
    # OTEL_COLLECTOR_HOST defaults to `otel-collector:4318` in SignalJobConfig and
    # IngestionService, and scripts pass it explicitly; the split must not rename it.
    assert "otel-collector" in stack["services"]
    assert "otel-collector" in compose
    assert "otel-collector-logs" in stack["services"]
    assert "otel-collector-logs" in compose


def test_the_infra_scrapes_discover_every_agent_not_just_one(network_cfg):
    """CHG-283: a static target keeps ONE address of a multi-address name.

    Measured 2026-09-21 on a name with 8 addresses: `static_configs` produced 1
    target labelled with the name, `dns_sd_configs` produced 8 targets each
    labelled with its own address. With the static form this singleton scraped one
    arbitrary node and labelled it identically on every deploy, so the other nodes'
    metrics were absent and the one that did arrive could not be told from them.
    """
    jobs = {j["job_name"]: j for j in network_cfg["receivers"]["prometheus"]["config"]["scrape_configs"]}
    for job, name, port in (("infra-host", "tasks.node-exporter", 9100),
                            ("infra-containers", "tasks.cadvisor", 8080)):
        cfg = jobs[job]
        assert "static_configs" not in cfg, (
            f"{job} must not use a static target: for a name with several addresses it "
            "scrapes one of them and labels it with the name"
        )
        assert cfg["dns_sd_configs"] == [{"names": [name], "type": "A", "port": port}], (
            f"{job} must discover every agent by DNS, one target per address"
        )


def test_the_two_logs_config_variants_stay_in_sync():
    """dev and swarm differ in the credential source and nothing else."""

    def strip(text):
        body = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
        return (body.replace("${env:O2_AUTH_BASIC}", "CRED")
                    .replace("${file:/run/secrets/o2_auth_basic}", "CRED"))

    assert strip(LOGS_CFG_DEV.read_text()) == strip(LOGS_CFG.read_text())
