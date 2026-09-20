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
import re

import yaml
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).parents[4]
STACK = ROOT / "code/01_platform/01_docker/docker-stack.yml"

# Workload services: must place on role==worker, never pinned to a hostname.
WORKLOAD = [
    "zookeeper-1", "zookeeper-2", "zookeeper-3",
    "fluss-coordinator", "fluss-tablet-1", "fluss-tablet-2", "fluss-tablet-3",
    "flink-jobmanager", "flink-taskmanager", "ingestion",
    "execution-bridge", "execution-gateway", "nautilus",
]
OBSERVABILITY = ["otel-collector", "openobserve", "alert-consumer",
                 "node-exporter", "cadvisor"]
COLLECTOR = ROOT / "code/01_platform/01_docker/otel-collector-config.swarm.yaml"

def _load():
    return yaml.safe_load(STACK.read_text())


def service_block_raw(raw, name):
    """One service's source from the stack, comments INCLUDED (P6-818).

    The healthcheck-exception markers are comments, so a block read for them must
    not be comment-stripped: scoping the check to the service's own block is what
    stops one marker from justifying every exception.
    """
    lines = raw.splitlines()
    start = next((i for i, l in enumerate(lines) if l.rstrip() == f"  {name}:"), None)
    if start is None:
        raise AssertionError(f"service {name!r} not found in docker-stack.yml")
    end = next((i for i in range(start + 1, len(lines)) if re.match(r"^  \S", lines[i])),
               len(lines))
    return "\n".join(lines[start:end])


def ha_backend_ok(props):
    """True when Flink's HA properties name a real backend and its ensemble (P6-615).

    A cluster-id alone proves nothing: an id with no backend is a JobManager that
    cannot fail over. The `or True` this replaces made the whole check unreachable.
    """
    return ("high-availability.type: zookeeper" in props
            and "high-availability.zookeeper.quorum: zookeeper-1:2181,zookeeper-2:2181,"
                "zookeeper-3:2181" in props)


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
    """Keys that `docker stack deploy` ignores or rejects — must be absent.

    `ports` used to be listed here as well, which was true about policy and false
    about the schema: the long syntax is valid in a Swarm stack and is how a
    service gets published. CHG-260 moved it to TestPublishedPorts, which states
    the policy ("loopback only, and only where the operator needs a UI") instead
    of banning a key the deploy engine understands.
    """
    FORBIDDEN = ["build", "depends_on", "container_name",
                 "network_mode", "mac_address"]

    def test_no_swarm_ignored_keys(self):
        d = _load()
        for name, svc in d["services"].items():
            for k in self.FORBIDDEN:
                assert k not in svc, f"{name}: '{k}' is not valid in a Swarm deploy unit"


class TestPublishedPorts:
    """CHG-260 — the stack publishes exactly one port, on loopback, for the UI.

    Everything else talks over the encrypted overlay, so nothing else may publish:
    a port on a host interface is attack surface plus a firewall rule, and the
    runbook's S4 table does not carry one for them. OpenObserve is the exception
    because it is the single pane of truth and the operator has to open it — and
    the exception is narrow enough to test: host mode (no ingress mesh), bound to
    127.0.0.1 (SSH tunnel or node-local only), and named explicitly.
    """

    ALLOWED = {"openobserve": {5080}}

    def test_only_the_allowed_service_publishes(self):
        d = _load()["services"]
        publishing = {n for n, svc in d.items() if "ports" in svc}
        assert publishing == set(self.ALLOWED), (
            f"services publishing ports: {sorted(publishing)} — the rule is "
            f"{sorted(self.ALLOWED)}; add a firewall rule and a reason first")

    def test_the_ui_port_is_host_mode_and_cannot_be_loopback(self):
        """`host_ip` is a deploy-blocking key, measured on 2026-09-20.

        The first version of this change tried `host_ip: 127.0.0.1` so the UI port
        would bind loopback only. `docker stack deploy` refused the whole stack:

            services.openobserve.ports.0 Additional property host_ip is not allowed

        and `docker stack deploy`'s schema is the authority here, not compose's. A
        loopback-only publish is therefore not expressible — `--publish-add` has no
        host_ip either — so the firewall rule in S4 is what restricts this port, and
        the test's job is to keep anyone from re-adding a key the engine rejects.
        """
        d = _load()["services"]
        for name, ports in self.ALLOWED.items():
            for entry in d[name]["ports"]:
                assert entry["mode"] == "host", (
                    f"{name}: ingress mode would round-robin the UI across nodes that "
                    f"do not run it")
                assert "host_ip" not in entry, (
                    f"{name}: host_ip makes the deploy fail with 'Additional property "
                    f"host_ip is not allowed' — the S4 firewall rule is the control")
                assert entry["published"] in ports, (
                    f"{name}: {entry['published']} is not a port this service is allowed to publish"
                )
                assert entry["target"] == entry["published"], (
                    f"{name}: target and published diverge ({entry['target']} vs "
                    f"{entry['published']}) — the firewall rule names one number")


class TestQuorumRolloutOrder:
    """CHG-262 — a quorum member must not need a free node to be replaced.

    The ZooKeeper services pin `max_replicas_per_node: 1`. With `start-first` the
    replacement task needs a *second* eligible node; measured 2026-09-20 on a
    one-worker rehearsal cluster, where the replacement stayed
    `Pending — no suitable node (max replicas per node)`, the service reported
    `update in progress`, and the old container kept serving the old `zoo.cfg` —
    so a configuration change never landed while the deploy exited 0. Stopping one
    member of three keeps quorum, which is the rolling-restart order ZooKeeper
    itself documents; the workload and observability services keep `start-first`
    because overlapping their old and new task is exactly what you want there.
    """

    def test_zookeeper_members_roll_stop_first(self):
        d = _load()["services"]
        for name in sorted(n for n in d if n.startswith("zookeeper-")):
            order = d[name]["deploy"]["update_config"]["order"]
            assert order == "stop-first", (
                f"{name}: a quorum member rolls stop-first — with start-first the "
                f"replacement needs a second eligible node and can stall as Pending "
                f"while the old task keeps serving (got {order})")

    def test_the_rest_still_roll_start_first(self):
        d = _load()["services"]
        others = {n: svc["deploy"].get("update_config", {}).get("order")
                  for n, svc in d.items() if not n.startswith("zookeeper-")}
        started = sorted(n for n, o in others.items() if o == "start-first")
        assert len(started) >= 12, (
            f"only {len(started)} services roll start-first — the workload and "
            f"observability classes are supposed to overlap old and new tasks")


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
        for v in ("fluss-data",
                  "fluss-tablet-data-1", "fluss-tablet-data-2", "fluss-tablet-data-3",
                  "flink-checkpoints", "flink-logs", "fluss-logs",
                  "openobserve-data", "ingestion-logs"):
            assert v in vols, f"durable volume {v} missing from stack"

    def test_no_local_remote_data_volume(self):
        """Tiered segments live on R2 - a node-local volume cannot serve reads."""
        d = _load()
        assert "fluss-remote-data" not in d.get("volumes", {}), (
            "fluss-remote-data volume must be gone with remote.data.dir on R2")
        for name, svc in d["services"].items():
            for m in svc.get("volumes", []) or []:
                src = m.split(":")[0] if isinstance(m, str) else m.get("source", "")
                assert src != "fluss-remote-data", (
                    f"{name} still mounts fluss-remote-data")

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

    def test_zookeeper_servers_separator_is_whitespace(self):
        """The image writes ONE zoo.cfg line per whitespace token (CHG-249).

        With ';' the whole ensemble collapses into a single invalid line and every member
        exits 2 with ``does not have the form server_config or server_config;client_config``
        — observed live, with 14 s of retrying before each failure.
        """
        d = _load()
        for i in (1, 2, 3):
            servers = d["services"][f"zookeeper-{i}"]["environment"]["ZOO_SERVERS"]
            assert ";" not in servers, f"zookeeper-{i}: ZOO_SERVERS must not use ';'"
            assert len(servers.split()) == 3, \
                f"zookeeper-{i}: ZOO_SERVERS must be 3 whitespace-separated members: {servers!r}"

    def test_zookeeper_generates_the_config_it_needs(self):
        """The image ships no zoo.cfg and generates one from ZOO_* env (CHG-249).

        Without ``clientPort`` nothing listens on 2181 for Flink/Fluss *and* the election
        port can only be bound by resolving the member's own service name — which fails
        (``<unresolved>:3888``, exit 14). ``quorumListenOnAllIPs`` binds it without DNS.
        """
        d = _load()
        for i in (1, 2, 3):
            extra = d["services"][f"zookeeper-{i}"]["environment"].get("ZOO_CFG_EXTRA", "")
            assert "clientPort=2181" in extra, \
                f"zookeeper-{i}: ZOO_CFG_EXTRA must set clientPort=2181"
            assert "quorumListenOnAllIPs=true" in extra, \
                f"zookeeper-{i}: ZOO_CFG_EXTRA must set quorumListenOnAllIPs=true"

    def test_no_healthcheck_requires_another_service_or_quorum(self):
        """Swarm publishes a DNS record only for tasks whose healthcheck PASSES.

        So a probe that waits for quorum (or for any peer) can never pass: the peers it
        waits for cannot resolve it either. ZK's ``zkServer.sh status`` did exactly that —
        every member sat in ``Starting`` until the health monitor killed it (exit 143).
        Cluster state belongs in metrics/logs/alerts; the probe stays liveness-only
        (CHG-249, DEC-034).
        """
        d = _load()
        banned = ("zkServer.sh status", "checkLeader", "getent hosts", "nslookup")
        for name, svc in d["services"].items():
            blob = " ".join(str(x) for x in ((svc.get("healthcheck") or {}).get("test") or []))
            for needle in banned:
                assert needle not in blob, \
                    f"{name}: healthcheck must not depend on another service or quorum ({needle!r})"

    def test_ingestion_mounts_the_manifest_where_the_app_looks(self):
        """The image bakes in no CSV (Dockerfile: "COPY removed"), so the stack must supply it.

        Without the mount ingestion refuses to start: ``readable manifest FILE is required``.
        """
        d = _load()
        ing = d["services"]["ingestion"]
        mounts = [c for c in (ing.get("configs") or []) if isinstance(c, dict)]
        assert mounts, "ingestion must mount the instrument manifest as a Swarm config"
        assert any(c.get("target") == "/instruments/NSE_CM_EQUITY.csv" for c in mounts), \
            f"manifest must land on the path the app defaults to: {mounts}"
        raw = (d.get("configs") or {}).get(mounts[0]["source"], {}).get("file", "")
        # file: may be an env-interpolated default, e.g. ${MANIFEST_FILE:-./instruments/x.csv}
        m = re.match(r"\$\{[A-Z_]+:-([^}]*)\}", str(raw))
        src = m.group(1) if m else str(raw)
        path = STACK.parent / src
        assert path.exists(), f"manifest source file does not exist: {path}"
        lines = path.read_text().splitlines()
        assert len(lines) > 100, f"manifest looks truncated: {len(lines)} lines from {path}"

    def test_every_mounted_secret_is_actually_consumed(self):
        """A mounted-but-unread secret fails SILENTLY — the service just starts without it.

        openobserve (password never read, crash-looped), execution-gateway and nautilus (no
        _FILE variable) and the ingestion arrow-bridge (no _FILE support at all) all shipped
        that way; found by rebuilding this matrix during CHG-249. Two mechanisms count as
        consumption: a ``*_FILE`` variable pointing at the mount, or a mounted config that
        reads the file itself (the OpenTelemetry collector's ``${file:...}`` expansion).
        """
        d = _load()
        unconsumed = []
        for name, svc in d["services"].items():
            env = svc.get("environment") or {}
            for entry in svc.get("secrets") or []:
                sname = entry if isinstance(entry, str) else entry.get("secret")
                target = "/run/secrets/" + sname
                if any(str(v).strip() == target for v in env.values()):
                    continue
                by_config = False
                for c in svc.get("configs") or []:
                    cname = c if isinstance(c, str) else c.get("source")
                    cfg_file = (d.get("configs") or {}).get(cname, {}).get("file")
                    if cfg_file and (STACK.parent / cfg_file).exists() \
                            and f"file:{target}" in (STACK.parent / cfg_file).read_text():
                        by_config = True
                if not by_config:
                    unconsumed.append(f"{name}:{sname}")
        assert not unconsumed, (
            "secret mounted but nothing reads it — add a *_FILE variable, teach the app the "
            "file form, or drop the mount: " + ", ".join(unconsumed))

    def test_money_path_services_read_the_gateway_secret_from_a_file(self):
        d = _load()
        for name in ("execution-gateway", "nautilus"):
            env = d["services"][name]["environment"]
            assert env.get("GATEWAY_SHARED_SECRET_FILE") == "/run/secrets/gateway_shared_secret", \
                f"{name}: must read the gateway secret from the mounted file"
            assert "GATEWAY_SHARED_SECRET" not in env, \
                f"{name}: the plaintext gateway secret must never appear in the service spec"

    def test_ingestion_reads_the_arrow_credentials_from_files(self):
        d = _load()
        env = d["services"]["ingestion"]["environment"]
        for key in ("ARROW_APP_SECRET", "ARROW_PASSWORD", "ARROW_TOTP_KEY"):
            assert key not in env, \
                f"ingestion: {key} must not appear in the service spec"
            assert env.get(f"{key}_FILE") == f"/run/secrets/{key.lower()}", \
                f"ingestion: {key}_FILE must point at the mounted secret"

    def test_openobserve_mounts_no_secret_it_cannot_read(self):
        """v0.91.5 has no *_FILE support — measured: it panics with the file present, so the
        password comes from the deploy environment and a mount would only be a lie (CHG-249)."""
        d = _load()
        oo = d["services"]["openobserve"]
        assert not oo.get("secrets"), "openobserve cannot read a mounted secret — drop the mount"
        assert "ZO_ROOT_USER_PASSWORD" in (oo.get("environment") or {}), \
            "openobserve needs ZO_ROOT_USER_PASSWORD supplied at deploy time"

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

    def test_fluss_tiers_to_r2_not_local_disk(self):
        """remote.data.dir on R2 with the dev s3 key set - node-local reads back nothing."""
        d = _load()
        for name in ("fluss-coordinator", "fluss-tablet-1", "fluss-tablet-2",
                     "fluss-tablet-3"):
            props = d["services"][name]["environment"]["FLUSS_PROPERTIES"]
            assert "remote.data.dir: s3://${R2_BUCKET" in props, (
                f"{name}: remote.data.dir must be an R2 URL gated on R2_BUCKET")
            assert "remote.data.dir: /tmp/fluss/remote-data" not in props, (
                f"{name}: node-local remote.data.dir cannot serve cluster reads")
            # Raw YAML carries doubled dollars ($$); compose render collapses
            # $${env...} to ${env...} inside the container.
            for key in ("s3.endpoint:", "s3.endpoint.region:",
                        "s3.access-key: $${env.AWS_ACCESS_KEY_ID}",
                        "s3.secret-key: $${env.AWS_SECRET_ACCESS_KEY}",
                        "s3.path.style.access: true",
                        "s3.connection.ssl.enabled: true"):
                assert key in props, f"{name}: FLUSS_PROPERTIES missing {key}"

    def test_flink_ha_cluster_id_and_restart_strategy(self):
        props = _load()["services"]["flink-jobmanager"]["environment"]["FLINK_PROPERTIES"]
        assert "high-availability.cluster-id" in props, "Flink HA must set a cluster-id"
        assert "restart-strategy.type: fixed-delay" in props, "restart-strategy must be fixed-delay"
        assert "restart-strategy.fixed-delay.attempts: 3" in props, "must cap retries at 3"
        assert "restart-strategy.fixed-delay.delay: 30 s" in props, "must pause 30s between retries"
        # P6-615: `… or True` could never fail. Assert the backend and its ensemble,
        # not a substring of the cluster-id already asserted above.
        assert ha_backend_ok(props), "Flink HA must name the ZooKeeper backend and the full ensemble"

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
        "openobserve",         # static /openobserve binary, no shell/curl/wget
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

    # Services that may validly run whatever their image's own default is, and
    # why. Every entry states the verified default, because the whole point of
    # the CHG-181 check is that "the image has a default" is not the question -
    # "the default is the real process" is. A default that is a usage/help path
    # exits 0 and the task is reported as started while nothing runs.
    COMMAND_ALLOWED_BY_IMAGE_DEFAULT = {
        # zookeeper:3.9.2 - Entrypoint [/docker-entrypoint.sh], CMD
        # [zkServer.sh start-foreground]: the default IS the server.
        "zookeeper-1": "zookeeper CMD is zkServer.sh start-foreground",
        "zookeeper-2": "zookeeper CMD is zkServer.sh start-foreground",
        "zookeeper-3": "zookeeper CMD is zkServer.sh start-foreground",
        # Verified on the running dev container: Entrypoint [], CMD
        # [/openobserve], and it has served traffic for days with no command:.
        "openobserve": "openobserve CMD [/openobserve] is the server",
        # Measured 2026-09-20: `docker image inspect gcr.io/cadvisor/cadvisor:v0.49.1`
        # -> Entrypoint [/usr/bin/cadvisor, -logtostderr], CMD null. The image's own
        # entrypoint is the server, so a command: here would duplicate its args.
        "cadvisor": "cadvisor Entrypoint [/usr/bin/cadvisor -logtostderr] is the server",
        # Our own images set ENTRYPOINT to the server binary itself, so no
        # command: is needed - the default cannot be a help path.
        "ingestion": "Dockerfile ENTRYPOINT /app/docker-entrypoint.sh execs java IngestionService",
        "execution-bridge": "Dockerfile ENTRYPOINT /app/execution-bridge is the binary",
        "execution-gateway": "Dockerfile ENTRYPOINT is java -jar /app/execution-gateway.jar",
        "nautilus": "Dockerfile ENTRYPOINT nautilus-execution-service is the binary",
    }

    def test_every_service_declares_what_to_run(self):
        """CHG-181: a service whose image CMD is a usage/help path starts nothing.

        Verified for ``apache/fluss:0.9.1-incubating`` (Entrypoint
        ``/docker-entrypoint.sh``, CMD ``help``): with no ``command:`` the
        container prints usage and **exits 0**, so Swarm reports the task as
        started while no server runs. The same defect class was fixed for Flink
        in CHG-179 but the Fluss half of this stack was left unpinned, so the
        check has to cover every service, not just the four that broke.

        A service may rely on its image's default CMD only when that default is
        the real process - each such service is named with its reason in
        COMMAND_ALLOWED_BY_IMAGE_DEFAULT.
        """
        d = _load()
        for name, svc in d["services"].items():
            if svc.get("command") or svc.get("entrypoint"):
                continue
            assert name in self.COMMAND_ALLOWED_BY_IMAGE_DEFAULT, (
                f"{name}: declares neither command: nor entrypoint: - if the image's "
                f"CMD is a usage/help path the task exits 0 and Swarm reports it as "
                f"started while nothing runs (CHG-181). Add command:, or name the "
                f"service in COMMAND_ALLOWED_BY_IMAGE_DEFAULT with the reason its "
                f"image default is the real process."
            )

    def test_fluss_services_run_the_server_they_are_named_for(self):
        """The exact four services CHG-181 found starting nothing.

        Asserted on the parsed value, so no amount of prose elsewhere in the file
        can satisfy it.
        """
        d = _load()
        expected = {
            "fluss-coordinator": "coordinatorServer",
            "fluss-tablet-1": "tabletServer",
            "fluss-tablet-2": "tabletServer",
            "fluss-tablet-3": "tabletServer",
        }
        for name, want in expected.items():
            got = d["services"][name].get("command")
            assert got == [want], (
                f"{name}: command must be [{want!r}] - the image's CMD is only the "
                f"usage fallback, so with no command the server never starts "
                f"(CHG-181). Got {got!r}."
            )

    # Services whose image has NO shell at all: no healthcheck probe can run,
    # so the service must carry no healthcheck and be a declared exception.
    # Verified live: exec of sh/bash/ls/curl/wget/busybox all fail with
    # "executable file not found in $PATH" (static binary image), and a
    # CMD-SHELL probe reports exit=-1
    # ("exec: \"/bin/sh\": stat /bin/sh: no such file or directory") while the
    # status flips to unhealthy - a permanently-failing probe is worse than
    # none, so the check must be ABSENT, not fixed. Compose interpolation
    # rejects a bare ``$(...)`` (render error: "you may need to escape any $
    # with another $"), which is why the probe strings below use ``$$``.
    SHELL_LESS_NO_HEALTHCHECK = {
        "otel-collector",   # distroless - no shell, cannot run a CMD probe
        "openobserve",      # static /openobserve binary, no shell/curl/wget
    }

    def test_healthchecks_can_actually_run_where_declared(self):
        """A probe that can never pass is worse than no probe (CHG-181 sibling).

        Verified broken **two** ways on the running dev containers, so the test
        pins both:

        1. **Interpreter.** The Fluss ``/dev/tcp`` probes are a **bash** feature,
           but ``CMD-SHELL`` runs ``/bin/sh`` - dash in the Fluss image
           (``/bin/sh -> dash``), which has no ``/dev/tcp``. Against a
           confirmed-open port the bash probe exits 0 while the identical probe
           under ``/bin/sh`` exits non-zero.
        2. **Address.** Fluss binds the container hostname, not loopback
           (``bind.listeners: ...://fluss-tablet:9124``), so ``127.0.0.1`` is
           refused even under bash: ``127.0.0.1:9124 -> rc=1`` while
           ``$HOSTNAME:9124 -> rc=0``. The fixed probe tries ``$HOSTNAME``
           first, then falls back to loopback for the Flink shape.

        3. **Which address is right depends on the bind.** A probe must target an
           address the service actually listens on, and the two shapes below are
           opposites. The ZooKeeper members set
           ``admin.serverAddress=127.0.0.1`` in ``ZOO_CFG_EXTRA``, so their admin
           server answers on loopback only: a ``$HOSTNAME`` probe would be
           refused there, and ``bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080'``
           returns 0 within 8 s of the process starting (measured, CHG-249).
           That exception is derived from the service's own environment rather
           than trusted from a list, so deleting the setting fails this test.

        Services with no shell at all (``SHELL_LESS_NO_HEALTHCHECK``) must carry
        NO healthcheck: removing the probe is the fix, so an absent probe is the
        passing state. ``test_every_service_healthcheck_or_documented_exception``
        passes on these dead probes because it only asks whether a healthcheck
        EXISTS. This one asks whether it can RUN.
        """
        d = _load()
        for name in self.SHELL_LESS_NO_HEALTHCHECK:
            hc = (d["services"][name].get("healthcheck") or {})
            assert not hc.get("test"), (
                f"{name}: image has no shell - a healthcheck probe can never run "
                f"(exit=-1, status flips to unhealthy), so the probe must be "
                f"removed, not repaired. Got {hc.get('test')!r}."
            )
        for name, svc in d["services"].items():
            hc = svc.get("healthcheck") or {}
            test = hc.get("test")
            if not test:
                continue
            joined = " ".join(test) if isinstance(test, list) else str(test)
            if "/dev/tcp" not in joined:
                continue
            assert test[0] != "CMD-SHELL", (
                f"{name}: CMD-SHELL runs /bin/sh (dash in the Fluss image), which "
                f"has no /dev/tcp - the probe can never pass. Use "
                f'["CMD", "bash", "-c", ...] instead.'
            )
            assert "bash" in joined, (
                f"{name}: a /dev/tcp probe needs bash, got {test!r}"
            )
            if "HOSTNAME" not in joined:
                # A loopback probe is only correct where the listener is bound to
                # loopback. The ZooKeeper admin server is: ZOO_CFG_EXTRA sets
                # admin.serverAddress=127.0.0.1, so probing $HOSTNAME would be
                # refused. Reading the setting out of this service's own
                # environment keeps the exception tied to the recipe - remove the
                # setting and this fails instead of passing on a stale claim.
                env = svc.get("environment") or {}
                extra = str(env.get("ZOO_CFG_EXTRA", "")) if isinstance(env, dict) else ""
                assert "admin.serverAddress=127.0.0.1" in extra, (
                    f"{name}: only a listener actually bound to loopback may be "
                    f"probed on 127.0.0.1, and {name} declares no "
                    f"admin.serverAddress=127.0.0.1 - so the probe must target "
                    f"$HOSTNAME instead (Fluss binds the container host, not "
                    f"loopback, and a loopback-only probe is refused even under "
                    f"bash). Got {test!r}."
                )
                assert "127.0.0.1" in joined, (
                    f"{name}: a loopback-bound listener must be probed on "
                    f"127.0.0.1, got {test!r}"
                )

    def test_fluss_binds_a_literal_and_advertises_a_name(self):
        """Binding your own service name can never succeed (CHG-250, DEC-047 sibling).

        Measured on the rehearsal Swarm: the coordinator logged ``Starting
        coordinator-server as a console application on host <container-id>`` and
        then ``java.io.IOException: Failed to start Netty server on endpoint
        INTERNAL://fluss-coordinator:0`` / ``Caused by:
        java.nio.channels.UnresolvedAddressException`` at Netty's
        ``checkResolvable``. Swarm publishes a service's DNS name only for tasks
        that are Running, so binding that name resolves to nothing on the first
        attempt: the task can never become Running (Swarm kept it in Starting,
        "failed 3x") and the coordinator never started. Pointing the same endpoint
        at ``0.0.0.0`` removed the failure - the next error was an unrelated
        placeholder S3 bucket - so both halves are pinned here:

        1. **bind.listeners must be a literal** (``0.0.0.0``, ``127.0.0.1``, or an
           empty host, Kafka-style "all interfaces"). Anything else is a name,
           and a name we cannot resolve before we are Running deadlocks us.
        2. **advertised.listeners must be the service's own name** - a literal
           there would tell every client to dial itself.
        D-A (same change, one layer deeper): the INTERNAL listener needs a fixed
        port and an advertised name too. Port 0 publishes an ephemeral port, and
        an unset advertised INTERNAL defaults to the bind value, so tablet-1
        registered ``INTERNAL://0.0.0.0:40917`` and the coordinator dialled
        0.0.0.0 refused, ongoing (run7). This test therefore also pins fixed
        bind ports and requires ``internal.listener.name`` to name a listener
        that is actually declared.
        """
        d = _load()
        literals = {"0.0.0.0", "127.0.0.1", "::", ""}

        def parse_listeners(key, props):
            """(listener-name, host, port) triples for a NAME://host:port,... line."""
            for line in props.splitlines():
                stripped = line.strip()
                if stripped.startswith(key + ":"):
                    value = stripped.split(":", 1)[1]
                    triples = []
                    for entry in value.split(","):
                        entry = entry.strip()
                        if "://" not in entry:
                            continue
                        lname, rest = entry.split("://", 1)
                        host, port = rest.rsplit(":", 1)
                        triples.append((lname.strip(), host.strip(), port.strip()))
                    return triples
            return []

        def prop_value(key, props):
            for line in props.splitlines():
                stripped = line.strip()
                if stripped.startswith(key + ":"):
                    return stripped.split(":", 1)[1].strip()
            return ""

        checked = 0
        for name, svc in d["services"].items():
            env = svc.get("environment") or {}
            if not isinstance(env, dict):
                continue
            props = str(env.get("FLUSS_PROPERTIES") or "")
            if "bind.listeners" not in props:
                continue
            checked += 1
            bound = parse_listeners("bind.listeners", props)
            assert bound, f"{name}: bind.listeners is missing"
            for lname, host, port in bound:
                assert host in literals, (
                    f"{name}: bind.listeners must use a literal address, got "
                    f"{host!r}. Swarm publishes a service name only for Running "
                    f"tasks, so binding it fails the first attempt and the task "
                    f"never reaches Running (measured: UnresolvedAddressException "
                    f"on INTERNAL://fluss-coordinator:0). Use 0.0.0.0."
                )
                assert port != "0", (
                    f"{name}: bind.listeners must use a fixed port, got :0 on "
                    f"{lname}. Port 0 publishes an ephemeral port, and with a "
                    f"wildcard bind the registered endpoint "
                    f"(0.0.0.0:<ephemeral>) is undiallable - measured run7: "
                    f"tablet-1 registered INTERNAL://0.0.0.0:40917 and the "
                    f"coordinator's dials were refused. Use a fixed port."
                )
            advertised = parse_listeners("advertised.listeners", props)
            assert advertised, f"{name}: advertised.listeners is missing"
            for lname, host, port in advertised:
                assert host not in literals, (
                    f"{name}: advertised.listeners must be the resolvable service "
                    f"name, not the literal {host!r} - clients would dial "
                    f"themselves."
                )
                assert host == name, (
                    f"{name}: advertised.listeners must equal the service name so "
                    f"peers and clients resolve the same endpoint, got {host!r}."
                )
            internal = prop_value("internal.listener.name", props)
            assert internal, (
                f"{name}: internal.listener.name is missing - server-to-server "
                f"traffic then falls back to an undocumented default."
            )
            declared = {triple[0] for triple in bound}
            assert internal in declared, (
                f"{name}: internal.listener.name={internal!r} names no declared "
                f"bind listener {sorted(declared)} - the servers would chase a "
                f"ghost endpoint."
            )
        assert checked >= 4, (
            f"the four Fluss services (coordinator + 3 tablets) must declare "
            f"listeners; checked {checked} - the stack or this test has drifted"
        )

    def test_flink_rpc_binds_the_port_peers_dial(self):
        """Flink must bind its RPC port, not an ephemeral one (CHG-253, D-J).

        Measured on the run8f rehearsal stack: the jobmanager was Running and
        healthy with leadership granted, yet **no TaskManager ever registered**.
        The taskmanager retried ``Could not resolve ResourceManager address
        pekko.tcp://flink@flink-jobmanager:6123/user/rpc/resourcemanager_*`` until
        ``RegistrationTimeoutException: Could not register at the ResourceManager
        within ... PT5M`` killed it - a silent 1/3 -> 0/3 flap with the JM looking
        fine. Probing the task container showed nothing listening on 6123 at all;
        Pekko had bound a random port instead and advertised it:
        ``Remoting started; listening on addresses
        :[pekko.tcp://flink@flink-jobmanager:42353]``.

        Cause, isolated by A/B on the deployed image: Flink 2.x splits the
        advertised RPC port from the port that is actually bound, and the two
        roles differ. A standalone jobmanager binds ``jobmanager.rpc.port``, but
        **with ``high-availability.type: zookeeper`` it binds an ephemeral port**
        (measured `…:35821` and `…:42955`) unless ``jobmanager.rpc.bind-port``
        pins it - consistent with HA meaning the address is discovered through
        ZooKeeper rather than dialled. A taskmanager binds ephemeral in every
        case (measured 33255/33801/46237) unless ``taskmanager.rpc.bind-port``
        pins it. Production is the HA jobmanager plus a statically addressed
        taskmanager, i.e. the one combination that cannot work, in both
        directions: the TM cannot reach the RM, and the RM cannot reach a
        registered TM. Pin both roles to the same fixed port they advertise.
        """
        d = _load()

        def prop_value(key, props):
            for line in props.splitlines():
                stripped = line.strip()
                if stripped.startswith(key + ":"):
                    return stripped.split(":", 1)[1].strip()
            return ""

        checked = 0
        for name, svc in d["services"].items():
            cmd = svc.get("command") or []
            cmd_s = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
            role = next(
                (r for r in ("jobmanager", "taskmanager") if r in cmd_s), None
            )
            if role is None:
                continue
            env = svc.get("environment") or {}
            props = str(env.get("FLINK_PROPERTIES") or "") if isinstance(env, dict) else ""
            assert props, f"{name}: runs {role} without FLINK_PROPERTIES"
            checked += 1
            port_key = f"{role}.rpc.port"
            bind_key = f"{role}.rpc.bind-port"
            port = prop_value(port_key, props)
            bind = prop_value(bind_key, props)
            assert port, (
                f"{name}: {port_key} is not pinned. The image default is 6123, "
                f"but a peer's dial target must be a value this repo states - "
                f"and it must match the bind port below."
            )
            assert bind, (
                f"{name}: {bind_key} is missing, so Flink binds an ephemeral "
                f"port while peers dial {port}. Measured run8f: Pekko listened "
                f"on 42353, the taskmanager dialled 6123 and died with "
                f"RegistrationTimeoutException PT5M - the JM looked healthy the "
                f"whole time. Pin {bind_key} to {port}."
            )
            assert bind == port, (
                f"{name}: {bind_key}={bind!r} but {port_key}={port!r} - the "
                f"bound port and the advertised port must match, otherwise the "
                f"published address has no listener."
            )
            assert bind != "0", (
                f"{name}: {bind_key}: 0 means 'bind an ephemeral port' (Flink "
                f"2.x default) - that is the defect this pins."
            )
        assert checked >= 2, (
            f"the jobmanager and taskmanager must pin their RPC ports; checked "
            f"{checked} - the stack or this test has drifted"
        )

    def test_taskmanager_reads_the_leader_through_the_same_ha_backend(self):
        """Both roles need the same ``high-availability.*`` keys (CHG-255).

        Measured run9, after ``jobmanager.rpc.bind-port`` was pinned: the JM
        binds 6123, the TaskManager's registration message now *reaches* the
        ResourceManager, and the RM throws it away:

            FencingTokenException: Fencing token mismatch: Ignoring message
            RemoteFencedMessage(00000000000000000000000000000000,
            RemoteRpcInvocation(ResourceManagerGateway.registerTaskExecutor(…)))
            because the fencing token 0…0 did not match the expected fencing
            token 9f3175f098076291f8095be45eee44b3

        The fencing token travels with the leader information that the HA
        service hands out, so a TaskManager without ``high-availability.*``
        discovers the ResourceManager statically, registers with token 0 and
        never appears (``{"taskmanagers":[]}``). The keys are therefore needed
        by BOTH roles, and must agree: a different ensemble, cluster id or root
        path is a different election to read.
        """

        def ha_props(props):
            out = {}
            for line in props.splitlines():
                stripped = line.strip()
                if stripped.startswith("high-availability.") and ":" in stripped:
                    key, value = stripped.split(":", 1)
                    out[key.strip()] = value.strip()
            return out

        blocks = {}
        for name, svc in _load()["services"].items():
            cmd = svc.get("command") or []
            cmd_s = " ".join(cmd) if isinstance(cmd, list) else str(cmd)
            for role in ("jobmanager", "taskmanager"):
                if role in cmd_s:
                    env = svc.get("environment") or {}
                    blocks[role] = ha_props(str(env.get("FLINK_PROPERTIES") or ""))

        assert set(blocks) == {"jobmanager", "taskmanager"}, sorted(blocks)
        jm_props = blocks["jobmanager"]
        assert jm_props, "the jobmanager declares no high-availability.* keys"
        for key in sorted(jm_props):
            assert key in blocks["taskmanager"], (
                f"taskmanager is missing {key!r}: it would then discover the "
                f"ResourceManager statically and register with fencing token 0, "
                f"which the ResourceManager rejects - mirror the jobmanager's "
                f"HA keys."
            )
            assert blocks["taskmanager"][key] == jm_props[key], (
                f"{key} differs between the roles: jobmanager="
                f"{jm_props[key]!r} taskmanager={blocks['taskmanager'][key]!r} "
                f"- both roles must read the same leader election state."
            )

    def test_interpolation_uses_only_the_portable_subset(self):
        """Ban interpolation forms this docker renders as garbage (CHG-252, D-E).

        Measured with a 5-line probe file (``interp-probe.yml``), same engine
        that deploys the stack - plain ``${VAR}``, ``${VAR:-plain}`` without a
        colon, empty ``${VAR:-}`` and short ``${VAR:?msg}`` all render
        correctly, but three forms do not:

        1. **nested defaults** ``${A:-${B}}`` render the inner reference
           LITERAL (run8: ``state.savepoints.dir: ${CHECKPOINT_DIR}``,
           ``SAVEPOINT_DIR=${CHECKPOINT_DIR}`` - Flink cannot parse that);
        2. **``:?`` error text containing `` - ``** renders the text after the
           dash as the VALUE (run8: ``CHECKPOINT_DIR= prod requires
           s3://...``) - the jobmanager then dies at startup and the failure
           looks like a storage problem;
        3. **sibling-key references** (``SERVERS: ${BOOTSTRAP}``) read the
           deploy env, never the sibling line, so they render EMPTY when the
           deploy env lacks the key - duplicated literals plus this pin stay
           in sync instead (``FLUSS_BOOTSTRAP_SERVERS`` equals a value that
           ``FLUSS_BOOTSTRAP`` actually takes).
        """
        raw = STACK.read_text()
        code_lines = [
            (n, line.split("#", 1)[0])
            for n, line in enumerate(raw.splitlines(), 1)
            if line.split("#", 1)[0].strip()
        ]
        for n, line in code_lines:
            assert not re.search(r"\${[^{}]*\${", line), (
                f"line {n}: nested interpolation renders literal on this "
                f"docker - use the proven-simple form. Got: {line.strip()!r}."
            )
            assert not re.search(r":\?[^}\n]* - ", line), (
                f"line {n}: ':?' error text containing ' - ' renders the tail "
                f"as the VALUE on this docker - shorten the message and move "
                f"the guidance to a comment. Got: {line.strip()!r}."
            )
        bootstraps = set()
        servers = set()
        for n, line in code_lines:
            m = re.match(r"\s+FLUSS_BOOTSTRAP: (\S+)\s*$", line)
            if m:
                assert not m.group(1).startswith("${"), (
                    f"line {n}: FLUSS_BOOTSTRAP must be a literal so sibling "
                    f"consumers can mirror it - interpolation reads the deploy "
                    f"env, never sibling keys."
                )
                bootstraps.add(m.group(1))
            m = re.match(r"\s+FLUSS_BOOTSTRAP_SERVERS: (\S+)\s*$", line)
            if m:
                assert not m.group(1).startswith("${"), (
                    f"line {n}: FLUSS_BOOTSTRAP_SERVERS must be a literal - a "
                    f"${{...}} reference renders EMPTY unless the deploy env "
                    f"sets it (measured run8)."
                )
                servers.add(m.group(1))
        assert servers, "no FLUSS_BOOTSTRAP_SERVERS lines found - the stack or this test has drifted"
        assert servers <= bootstraps, (
            f"FLUSS_BOOTSTRAP_SERVERS {sorted(servers)} names an endpoint no "
            f"FLUSS_BOOTSTRAP takes {sorted(bootstraps)} - keep them in sync."
        )

    def test_properties_blocks_carry_no_comment_text(self):
        """No ``#`` inside FLINK_PROPERTIES / FLUSS_PROPERTIES blocks (CHG-252).

        Those are YAML block scalars: the text is handed to the service as-is,
        so the YAML parser never sees a comment. Both consumers proved it in the
        rehearsal:

        * the Flink entrypoint loaded a prose line as a configuration property
          (run8c: ``Loading configuration property: #CHG-252, savepoints=...``);
        * a trailing ``# REHEARSAL-ONLY (production: s3://)`` note appended to a
          path value was kept, and ``high-availability.storageDir`` became
          ``file:/checkpoints/flink-ha#REHEARSAL-ONLY(production:s3:/)`` - a URI
          with a fragment. The jobmanager then failed its ``JobResultStore``
          accessibility check and exited 239 (run8c/run8d), which reads like a
          storage outage and is not.

        Unknown keys are merely tolerated by Flink, so a comment line is not
        fatal today - but a path value carrying a fragment is, and one strict
        parser is enough to turn the other kind fatal too. Keep prose above the
        key, never inside the block.
        """
        raw = STACK.read_text()
        lines = raw.splitlines()
        blocks = 0
        for n, line in enumerate(lines, 1):
            if line.strip() not in ("FLINK_PROPERTIES: |", "FLUSS_PROPERTIES: |"):
                continue
            blocks += 1
            for offset, inner in enumerate(lines[n:], 1):
                if not inner.startswith("        "):
                    break  # block ended
                assert "#" not in inner, (
                    f"line {n + offset}: '#' inside a properties block reaches "
                    f"the service verbatim (no YAML comment semantics) - move "
                    f"the prose above the key. Got: {inner.strip()!r}."
                )
        assert blocks == 6, (
            f"expected 6 properties blocks (4 FLUSS + 2 FLINK), scanned {blocks} "
            f"- the stack or this test has drifted"
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
            # P6-818: the marker must be in THIS service's block. Searching the
            # whole file let one marker justify every exception — and hid that
            # otel-collector had none at all.
            block = service_block_raw(raw, name)
            marker = next((l for l in block.splitlines() if "x-healthcheck:" in l), "")
            assert marker, f"{name}: x-healthcheck marker missing from its own block"
            assert "none" in marker and len(marker.split(":", 1)[1].strip()) >= 20, \
                f"{name}: the marker must state why the exception is valid: {marker.strip()}"

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

    def test_bridge_auth_secret_wired_to_env(self):
        """P5-013: the bridge secret must reach the binary — a Swarm secret
        is a FILE; the service must point the _FILE env at it or the bridge
        exits at startup (authToken empty) and replication of this wiring
        silently regresses."""
        svc = _load()["services"]["execution-bridge"]
        assert "EXECUTION_BRIDGE_AUTH_TOKEN_FILE" in svc["environment"], (
            "execution-bridge must resolve the token from the secret file")
        assert svc["environment"]["EXECUTION_BRIDGE_AUTH_TOKEN_FILE"] == \
            "/run/secrets/execution_bridge_auth_token"
        assert "execution_bridge_auth_token" in svc.get("secrets", []), (
            "the secret must stay mounted")


class TestCollectorScrapeTargets:
    """CHG-257 — a scrape target with no service is a silent empty panel.

    The collector scraped `node-exporter:9100` and `cadvisor:8080` for months while
    the stack declared neither service: every scrape failed with a warning the
    collector logs and nobody reads, and no deploy ever failed. The gap was found by
    reading the running collector's log, not by a test — this class is that test.

    A name is skipped only when it is genuinely not a service: a bind address, or a
    loopback endpoint inside the collector's own process.
    """

    SKIP = {"", "0.0.0.0", "127.0.0.1", "localhost", "::"}

    def _hosts(self):
        cfg = yaml.safe_load(COLLECTOR.read_text())
        found = set()

        def harvest(block):
            for job in block.get("config", {}).get("scrape_configs", []) or []:
                for static in job.get("static_configs", []) or []:
                    for t in static.get("targets", []) or []:
                        found.add(str(t).rsplit(":", 1)[0])

        for name, rec in (cfg.get("receivers") or {}).items():
            if name.startswith("prometheus"):
                harvest(rec)
        for name, exp in (cfg.get("exporters") or {}).items():
            ep = exp.get("endpoint")
            if isinstance(ep, str) and "://" in ep:
                found.add(ep.split("://", 1)[1].rsplit(":", 1)[0])
        return {h for h in found if h not in self.SKIP and not h[0].isdigit()}

    def test_every_scraped_host_is_a_stack_service(self):
        services = set(_load()["services"])
        unknown = sorted(h for h in self._hosts() if h not in services)
        assert not unknown, (
            f"the collector scrapes/exporters at {unknown}, which the stack does not "
            f"declare — the receiver will warn every interval and the panels stay empty"
        )

    def test_zookeeper_metrics_port_matches_what_the_collector_scrapes(self):
        """CHG-259 — the provider port and the scrape target are two files apart.

        The provider class ships in the pinned image (checked in
        zookeeper-prometheus-metrics-3.9.2.jar: org/apache/zookeeper/metrics/
        prometheus/PrometheusMetricsProvider.class, serving GET /metrics), so the
        switch is `ZOO_CFG_EXTRA`, and the entrypoint writes each whitespace-separated
        token as its own zoo.cfg line. Changing the port in one file and not the other
        is exactly the silent-empty-panel failure this class exists for.
        """
        d = _load()["services"]
        scraped = self._hosts_with_ports()
        for name in sorted(n for n in d if n.startswith("zookeeper-")):
            tokens = d[name]["environment"]["ZOO_CFG_EXTRA"].split()
            provider = [t for t in tokens if t.startswith("metricsProvider.className=")]
            port = [t for t in tokens if t.startswith("metricsProvider.httpPort=")]
            assert provider and port, f"{name}: no Prometheus metrics provider in ZOO_CFG_EXTRA"
            assert provider[0].endswith("PrometheusMetricsProvider"), provider[0]
            target = f"{name}:{port[0].split('=', 1)[1]}"
            assert target in scraped, (
                f"{name} publishes metrics at {target}, which the collector does not scrape "
                f"(it scrapes {sorted(scraped)})")

    def _hosts_with_ports(self):
        cfg = yaml.safe_load(COLLECTOR.read_text())
        found = set()
        for name, rec in (cfg.get("receivers") or {}).items():
            if not name.startswith("prometheus"):
                continue
            for job in rec.get("config", {}).get("scrape_configs", []) or []:
                for static in job.get("static_configs", []) or []:
                    for t in static.get("targets", []) or []:
                        found.add(str(t))
        return found

    def test_infra_agents_report_on_the_host_not_the_container(self):
        d = _load()["services"]
        for name in ("node-exporter", "cadvisor"):
            svc = d[name]
            assert svc["deploy"]["mode"] == "global", (
                f"{name} is a per-host agent: it must run on every observability node")
            assert "@sha256:" in svc["image"], f"{name} must be digest-pinned"
            assert svc["deploy"].get("restart_policy"), f"{name} needs a restart policy"
        mounts = {m.split(":")[1] for m in d["node-exporter"]["volumes"]}
        assert {"/host/proc", "/host/sys", "/rootfs"} <= mounts, (
            "node-exporter must mount the host paths its --path.* flags point at, "
            "or it reports the container's overlay as if it were the VM")
        assert any("--path.rootfs=/rootfs" == a for a in d["node-exporter"]["command"]), (
            "without --path.rootfs the filesystem collectors read the wrong root (P5-026)")
        cad = {m.split(":")[1] for m in d["cadvisor"]["volumes"]}
        assert "/var/lib/docker" in cad and "/sys" in cad, (
            "cadvisor needs the host's docker directory and /sys to see cgroups")


class TestCollectorHostLogs:
    """CHG-263 — a log path the collector cannot open is a silent empty stream.

    The same failure shape as `TestCollectorScrapeTargets`, one layer down: there a
    receiver named a service the stack did not declare, here it names a *path* the
    container cannot see. Nothing fails either way — a filelog receiver with no
    readable file emits nothing, no stream is created, and every deploy stays green.

    Measured 2026-09-20: `filelog/infrastructure` included `/data/infra/logs/*.log`
    (a named volume no service ever wrote to, in the stack *and* the dev compose file)
    and `/var/log/*.log` (a bind the collector never had — its own comment called that
    bind "optional"), and `/var/log/syslog` has no `.log` suffix, so even with the bind
    the system log was unreachable. These tests hold all three halves together.
    """

    def _include_dirs(self):
        """Directory of every include pattern per filelog receiver, as POSIX paths.

        The collector is Linux; the test may run anywhere, so paths are compared as
        strings rather than resolved against the test host's filesystem.
        """
        cfg = yaml.safe_load(COLLECTOR.read_text())
        dirs = {}
        for name, rec in (cfg.get("receivers") or {}).items():
            if not name.startswith("filelog"):
                continue
            for inc in rec.get("include") or []:
                parent = str(PurePosixPath(str(inc)).parent)
                dirs.setdefault(parent, set()).add(name)
        return dirs

    def _mounts(self):
        """Mount target -> (source, read_only) for the collector service."""
        out = {}
        for m in _load()["services"]["otel-collector"].get("volumes") or []:
            if isinstance(m, str):
                parts = m.split(":")
                # `source:target[:mode]` — a 2-part short form means read-write
                out[parts[1]] = (parts[0], len(parts) > 2 and parts[2] == "ro")
            else:
                out[m["target"]] = (m.get("source"), bool(m.get("read_only")))
        return out

    def test_every_log_path_the_collector_reads_is_mounted(self):
        mounts = self._mounts()
        missing = {
            d: sorted(r)
            for d, r in self._include_dirs().items()
            if d not in mounts
        }
        assert not missing, (
            f"the collector reads {missing} but mounts only {sorted(mounts)} — the "
            f"receiver emits nothing at all and the stream never appears in OpenObserve"
        )

    def test_the_only_host_path_the_collector_mounts_is_read_only_var_log(self):
        """Reading host logs must not become write access to the host."""
        binds = [
            (target, src, ro)
            for target, (src, ro) in self._mounts().items()
            if isinstance(src, str) and src.startswith("/") and not src.startswith("/data/")
        ]
        assert binds == [("/var/log", "/var/log", True)], (
            f"the collector's host mounts are {binds}; it may read /var/log and nothing "
            f"else, read-only — a writable host mount would let a log parser change the host"
        )

    def test_the_collector_runs_as_a_user_that_can_open_those_files(self):
        """`user` is forced by measurement: `group_add` is not expressible in a stack.

        The files that matter are not world-readable on a stock Ubuntu host —
        `/var/log/auth.log` and `/var/log/kern.log` are `640 syslog:adm`, `boot.log` is
        `600 root` — while the image defaults to uid 10001. A stack file cannot add a
        supplementary group (`group_add` is rejected with "Additional property group_add
        is not allowed"), so the only expressible way to read them is `user`, and this
        test requires that it is set deliberately rather than left to the image default.
        """
        svc = _load()["services"]["otel-collector"]
        assert svc.get("user"), (
            "the collector reads host logs that are 640 syslog:adm; without `user` it "
            "runs as the image's uid 10001 and silently collects nothing"
        )


class TestRestartBudget:
    """CHG-258 — a restart budget is a decision per service class, not a default.

    Infrastructure must never give up: a stopped ZooKeeper loses quorum, a stopped
    collector or agent puts a hole in the single pane, and a stopped alert consumer
    drops the alert path — all silently, because `docker service ls` shows `0/1`
    only if someone looks. The trading services are the other way round: four
    attempts in 120 s and then stay down, so a service that cannot start after a
    fixed configuration error stops churning and waits for an operator instead of
    hiding a real failure behind an endless restart loop.
    """

    BOUNDED = {"fluss-coordinator", "fluss-tablet-1", "fluss-tablet-2", "fluss-tablet-3",
               "flink-jobmanager", "flink-taskmanager", "ingestion", "execution-bridge"}
    UNBOUNDED = {"zookeeper-1", "zookeeper-2", "zookeeper-3", "execution-gateway", "nautilus",
                 "otel-collector", "openobserve", "alert-consumer",
                 "node-exporter", "cadvisor"}

    def test_every_service_is_classified(self):
        services = set(_load()["services"])
        assert services == self.BOUNDED | self.UNBOUNDED, (
            "a new service must be added to the bounded or unbounded set — an "
            "unclassified restart policy is how a service ends up retrying forever "
            f"or stopping silently: {sorted(services ^ (self.BOUNDED | self.UNBOUNDED))}")

    def test_trading_services_stop_retrying_after_a_bounded_budget(self):
        d = _load()["services"]
        for name in sorted(self.BOUNDED):
            rp = d[name]["deploy"]["restart_policy"]
            assert rp.get("max_attempts") and rp.get("window"), (
                f"{name}: a trading service must bound its retries (max_attempts + window)")

    def test_infrastructure_retries_forever(self):
        d = _load()["services"]
        for name in sorted(self.UNBOUNDED):
            rp = d[name]["deploy"]["restart_policy"]
            assert "max_attempts" not in rp, (
                f"{name}: infrastructure must not stop retrying — a stopped service here "
                f"is a silent hole (quorum, metrics, alerts), not a visible failure")
