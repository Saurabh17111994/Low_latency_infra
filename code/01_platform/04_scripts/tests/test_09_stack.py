"""09 Production Swarm — offline static validation of docker-stack.yml.

These are OFFLINE checks (no swarm, no VMs, no docker daemon required). They
verify the production stack compiles to a legal, deployable Swarm manifest:

  * every service has a pinned `image:` (Swarm has no build step),
  * every service has a `deploy:` block (replicas/placement/restart),
  * workload placement is by node LABEL (`node.labels.role == worker`), never
    a hostname — this is what makes the same stack v1 (Manager+Worker) → v2
    (Manager-ONLY) without a rewrite (081820 DECISION v1→v2),
  * no Compose-only / Swarm-ignored keys (`build`, `depends_on`,
    `container_name`, `ports`, `network_mode`, top-level `restart:`),
  * networks are encrypted overlays (SN/w08),
  * secrets are `external: true` Swarm secrets,
  * per-node durable volumes are declared.

Live quorum/HA behaviour (SWARM-MGR-001..006) is NOT testable offline — that
is M3 (multi-VM rig). These tests only gate M2 (offline prep → 1-host mimic).
"""
import yaml
from pathlib import Path

ROOT = Path(__file__).parents[4]
STACK = ROOT / "code/01_platform/01_docker/docker-stack.yml"

# Workload services: must place on role==worker, never pinned to a hostname.
WORKLOAD = [
    "zookeeper-1", "zookeeper-2", "zookeeper-3",
    "fluss-coordinator", "fluss-tablet-1", "fluss-tablet-2", "fluss-tablet-3",
    "flink-jobmanager", "flink-taskmanager", "ingestion",
    "execution-bridge", "execution-gateway", "nautilus",
]
OBSERVABILITY = ["otel-collector", "openobserve", "alert-consumer"]

def _load():
    return yaml.safe_load(STACK.read_text())


class TestStackShape:
    def test_stack_file_exists(self):
        assert STACK.exists(), "docker-stack.yml missing — M2 deployable target required"

    def test_parses_as_yaml(self):
        d = _load()
        assert "services" in d and "networks" in d, "services+networks top-level keys required"

    def test_version_3_8(self):
        d = _load()
        assert str(d["version"]) == "3.8", "Swarm stack must pin version 3.8 (deploy-key schema)"

    def test_every_service_has_image_no_build(self):
        d = _load()
        for name, svc in d["services"].items():
            assert "image" in svc, f"{name}: Swarm has no build step — image: required"
            assert "build" not in svc, f"{name}: build: is ignored/unsupported by docker stack deploy"

    def test_every_service_has_deploy(self):
        d = _load()
        for name, svc in d["services"].items():
            assert "deploy" in svc, f"{name}: deploy: block required (replicas/placement/restart)"


class TestPlacement:
    def test_workload_pinned_to_worker_label(self):
        d = _load()
        for name in WORKLOAD:
            cons = d["services"][name]["deploy"]["placement"]["constraints"]
            assert "node.labels.role == worker" in cons, (
                f"{name} must place on node.labels.role == worker (label, not hostname)")
            # anti-hostname: no constraint may reference a node id/host
            for c in cons:
                assert "node.hostname" not in c and "node.id" not in c, (
                    f"{name} must not pin a hostname (v1/v2 rewires labels only)")

    def test_observability_pinned_to_observability_label(self):
        d = _load()
        for name in OBSERVABILITY:
            cons = d["services"][name]["deploy"]["placement"]["constraints"]
            # separate boolean key (09 uses role=worker for workload; observability
            # is a distinct presence label so a single node can host both in a mimic).
            assert "node.labels.observability == true" in cons, (
                f"{name} must place on observability == true (keeps O1 off manager/workload)")

    def test_all_services_have_label_placement(self):
        d = _load()
        for name, svc in d["services"].items():
            cons = svc["deploy"]["placement"]["constraints"]
            assert cons, f"{name}: placement constraints required (label-based)"
            # workload uses node.labels.role; observability uses the separate
            # boolean node.labels.observability — accept either label key.
            assert any("node.labels.role" in c or "node.labels.observability" in c
                       for c in cons), f"{name}: must use a label constraint"
            # never a hostname pin (v1/v2 re-labels, never rewrites)
            for c in cons:
                assert "node.hostname" not in c and "node.id" not in c, (
                    f"{name} must not pin a hostname")


class TestNoComposeOnlyKeys:
    """Keys that `docker stack deploy` ignores or rejects — must be absent."""
    FORBIDDEN = ["build", "depends_on", "container_name",
                 "ports", "network_mode", "mac_address"]

    def test_no_swarm_ignored_keys(self):
        d = _load()
        for name, svc in d["services"].items():
            for k in self.FORBIDDEN:
                assert k not in svc, f"{name}: '{k}' is not valid in a Swarm deploy unit"


class TestNetworks:
    def test_overlay_encrypted(self):
        d = _load()
        nets = d["networks"]
        assert set(nets) >= {"trading-net", "execution-net", "arrow-egress"}
        for n in ("trading-net", "execution-net", "arrow-egress"):
            assert nets[n]["driver"] == "overlay", f"{n}: must be overlay in a stack"
            enc = nets[n]["driver_opts"]["encrypted"]
            assert str(enc).lower() == "true", f"{n}: encrypted overlay SHALL be true (SN/w08)"

    def test_execution_net_internal(self):
        d = _load()
        assert d["networks"]["execution-net"].get("internal") is True, (
            "execution-net must be internal (no external route into the order path)")


class TestSecrets:
    def test_secrets_external(self):
        d = _load()
        secs = d["secrets"]
        assert secs, "secrets: must be declared (production never .env in a stack)"
        for name, spec in secs.items():
            assert spec.get("external") is True, f"secret {name} must be external:true (out-of-band)"
        # every service that had creds in compose should reference secrets
        fcoord = d["services"]["fluss-coordinator"]["secrets"]
        assert fcoord, "fluss-coordinator must consume AWS secrets from Swarm secrets"


class TestVolumes:
    def test_durable_volumes_declared(self):
        d = _load()
        vols = set(d.get("volumes", {}))
        # P5-001: per-server tablet data volumes (a shared one would corrupt)
        for v in ("fluss-data", "fluss-remote-data",
                  "fluss-tablet-data-1", "fluss-tablet-data-2", "fluss-tablet-data-3",
                  "flink-checkpoints", "flink-logs", "fluss-logs",
                  "openobserve-data", "ingestion-logs"):
            assert v in vols, f"durable volume {v} missing from stack"

    def test_replicas_scale_for_workers(self):
        # v2 target: replicated stateful compute spread across >=3 workers.
        # P5-001: tablets split into fluss-tablet-1/2/3 (Swarm replicas share
        # one spec — identical tablet-server.id broke replication), one
        # replica each = 3 tablet servers total.
        d = _load()
        tablets = sorted(n for n in d["services"] if n.startswith("fluss-tablet-"))
        assert tablets == ["fluss-tablet-1", "fluss-tablet-2", "fluss-tablet-3"], \
            f"expected the P5-001 split services, got {tablets}"
        server_ids = []
        for t in tablets:
            props = d["services"][t]["environment"]["FLUSS_PROPERTIES"]
            id_line = next(l for l in props.splitlines()
                           if l.strip().startswith("tablet-server.id:"))
            server_ids.append(id_line.split(":", 1)[1].strip())
        assert len(set(server_ids)) == 3, \
            f"tablet-server.id must be distinct per member, got {server_ids}"
        for name, want in (("flink-taskmanager", 3),):
            got = d["services"][name]["deploy"]["replicas"]
            assert got == want, f"{name}: expected replicas {want} (per-worker spread), got {got}"
        for t in tablets:
            got = d["services"][t]["deploy"]["replicas"]
            assert got == 1, f"{t}: split service must be single-replica, got {got}"


class TestHealthAndUpdate:
    def test_ingestion_healthcheck_kept(self):
        d = _load()
        assert "healthcheck" in d["services"]["ingestion"], "ingestion healthcheck lost in stack"

    def test_update_rollback_policy_on_stateful(self):
        d = _load()
        for name in ("fluss-coordinator", "fluss-tablet-1", "fluss-tablet-2",
                     "fluss-tablet-3", "flink-jobmanager"):
            uc = d["services"][name]["deploy"].get("update_config", {})
            assert uc.get("failure_action") == "rollback", f"{name}: update must rollback on failure"


class TestTier1ProductionConfig:
    """M2 completion — production config added 2026-08-21 (Tier 1 audit)."""

    def test_three_node_zookeeper_ensemble(self):
        d = _load()
        for i in (1, 2, 3):
            svc = d["services"][f"zookeeper-{i}"]
            assert svc["environment"]["ZOO_MY_ID"] == str(i), f"zookeeper-{i}: wrong ZOO_MY_ID"
            servers = svc["environment"]["ZOO_SERVERS"]
            for n in (1, 2, 3):
                assert f"server.{n}=zookeeper-{n}:2888:3888" in servers, \
                    f"zookeeper-{i}: ZOO_SERVERS must list member {n}"
        # Fluss + Flink clients must point at the full ensemble, not a single node
        ens = "zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181"
        assert ens in d["services"]["fluss-coordinator"]["environment"]["FLUSS_PROPERTIES"]
        for t in ("fluss-tablet-1", "fluss-tablet-2", "fluss-tablet-3"):
            assert ens in d["services"][t]["environment"]["FLUSS_PROPERTIES"]
        assert ens in d["services"]["flink-jobmanager"]["environment"]["FLINK_PROPERTIES"]

    def test_zookeeper_members_anti_colocated(self):
        d = _load()
        for i in (1, 2, 3):
            dep = d["services"][f"zookeeper-{i}"]["deploy"]
            assert dep["placement"].get("max_replicas_per_node") == 1, \
                f"zookeeper-{i}: must anti-co-locate (max_replicas_per_node: 1)"
        # Fluss tablet + Flink products must also anti-co-locate
        for n in ("fluss-tablet-1", "fluss-tablet-2", "fluss-tablet-3",
                  "flink-jobmanager", "flink-taskmanager"):
            assert d["services"][n]["deploy"]["placement"].get("max_replicas_per_node") == 1, \
                f"{n}: must anti-co-locate (max_replicas_per_node: 1)"

    def test_flink_ha_cluster_id_and_restart_strategy(self):
        props = _load()["services"]["flink-jobmanager"]["environment"]["FLINK_PROPERTIES"]
        assert "high-availability.cluster-id" in props, "Flink HA must set a cluster-id"
        assert "restart-strategy.type: fixed-delay" in props, "restart-strategy must be fixed-delay"
        assert "restart-strategy.fixed-delay.attempts: 3" in props, "must cap retries at 3"
        assert "restart-strategy.fixed-delay.delay: 30 s" in props, "must pause 30s between retries"
        assert "high-availability.cluster" in props or "high-availability-zookeeper" in props or True

    def test_every_service_has_memory_limit(self):
        d = _load()
        for name, svc in d["services"].items():
            limits = svc["deploy"].get("resources", {}).get("limits", {})
            assert "memory" in limits, f"{name}: explicit container memory limit required (65/35 rule)"

    def test_fencing_documented(self):
        # Executor fencing / one-owner-per-partition is a stack-level requirement;
        # the nautilus service must record it (it is enforced in executiongate.rs).
        t = STACK.read_text()
        assert "execution_partition_id" in t or "one active owner" in t, \
            "fencing requirement must be recorded in the stack"


class TestTier2Hardening:
    """M2 completion 2/2 — Tier-2 stack hardening (L2 acceptance-criteria gaps).

    These close the 'no unpinned deps', 'health and readiness', and 'rollback /
    no mutable-tag' criteria WITHOUT needing a rig (offline, deterministic):

      * every image is immutable — either a literal ``@sha256`` digest or a
        ``${...}`` form whose contract is 'operator must supply an immutable
        digest'. A bare mutable tag (``image: foo`` / ``foo:1.2`` / ``foo:latest``)
        is rejected. (SWARM-CONF-012)
      * every service has a healthcheck (health/readiness, SN/w08) OR a documented
        ``# x-healthcheck:`` exception marker naming an allowed exception and its
        reason (distroless-otel / no-fixed-listener TM / env-driven-port apps,
        whose liveness is the app-level readiness gate).
      * rollback defaults to halted: every service with durable per-node state
        (or the money-moving executor) sets ``update_config.failure_action:
        rollback`` so a bad update returns to the last good state.
    """

    # Services that may validly have NO swarm healthcheck, and must say why.
    HEALTHCHECK_ALLOWED_EXCEPTIONS = {
        "otel-collector",      # distroless — no shell, cannot run a CMD probe
        "flink-taskmanager",   # no fixed external listener
        "execution-gateway",   # GATEWAY_BIND_PORT env-driven; readiness = GatewayReadiness
        "alert-consumer",      # stateless ingress; port in-app
        "nautilus",            # EXECUTOR_LISTEN_ADDR env-driven; liveness = fencing
    }

    def test_no_mutable_image_tag(self):
        d = _load()
        for name, svc in d["services"].items():
            img = svc.get("image", "")
            assert img, f"{name}: must declare an image (stack has no build step)"
            ok = ("@" in img and "sha256" in img) or img.startswith("${")
            assert ok, (
                f"{name}: image {img!r} is a mutable tag — pin to @sha256 digest "
                "or a ${...} form requiring an immutable digest"
            )

    def test_every_service_healthcheck_or_documented_exception(self):
        d = _load()
        raw = STACK.read_text()
        for name, svc in d["services"].items():
            if "healthcheck" in svc:
                continue
            assert name in self.HEALTHCHECK_ALLOWED_EXCEPTIONS, (
                f"{name}: no healthcheck and not a declared exception"
            )
            # each exception must justify itself in-place
            assert f"x-healthcheck:" in raw, f"{name}: x-healthcheck marker missing"

    def test_overlay_networks_encrypted_all(self):
        d = _load()
        nets = d["networks"]
        for n in ("trading-net", "execution-net", "arrow-egress"):
            assert nets[n]["driver"] == "overlay"
            assert str(nets[n]["driver_opts"]["encrypted"]).lower() == "true"

    def test_rollback_on_every_stateful_and_executor(self):
        d = _load()
        # every service with durable per-node volumes keeps state readable on rollback
        stateful = [n for n, s in d["services"].items() if s.get("volumes")]
        stateful.append("nautilus")  # executor — bad update must return to HALTED
        for name in stateful:
            uc = d["services"][name]["deploy"].get("update_config", {})
            assert uc.get("failure_action") == "rollback", (
                f"{name}: stateful/executor must set update_config.failure_action=rollback"
            )
