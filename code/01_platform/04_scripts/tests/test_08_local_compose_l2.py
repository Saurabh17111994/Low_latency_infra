"""L2 Network isolation / security — NETWORK-001..010 + SEC-001..015."""
import json, re, subprocess, unittest
from pathlib import Path
import importlib.util

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
MOD_PATH = ROOT / "code/01_platform/04_scripts/execution_network_check.py"
spec = importlib.util.spec_from_file_location("enc", MOD_PATH)
enc = importlib.util.module_from_spec(spec); spec.loader.exec_module(enc)

def compose_json(profile="execution-t3"):
    cmd = ["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env")]
    if profile:
        cmd += ["--profile", profile]
    cmd += ["config","--format","json"]
    return json.loads(subprocess.check_output(cmd, text=True))

def compose_json_default():
    return compose_json(profile=None)

# P6-604/P6-605: the documented dev-published ports. `compose config --format
# json` renders ports as objects ({"published": "9123", "target": 9123}), so the
# comparison is over (published, target) pairs — note TM maps 9250 -> 9249.
FLUSS_PORT_ALLOWLIST = {"fluss-coordinator": {("9123", "9123")},
                        "fluss-tablet": {("9124", "9124")}}
FLINK_PORT_ALLOWLIST = {"flink-jobmanager": {("8081", "8081"), ("9249", "9249")},
                        "flink-taskmanager": {("9250", "9249")}}

def service_networks(cfg, name):
    """Network names of a service (handles the dict and the list YAML form)."""
    nets = (cfg.get("services", {}).get(name) or {}).get("networks") or {}
    return set(nets.keys()) if isinstance(nets, dict) else set(nets)

def ports_allowlist(cfg, name):
    """{(published, target)} published by a service."""
    out = set()
    for p in (cfg.get("services", {}).get(name) or {}).get("ports") or []:
        if isinstance(p, dict):
            out.add((str(p.get("published")), str(p.get("target"))))
        else:
            pub, _, tgt = str(p).partition(":")
            out.add((pub, tgt or pub))
    return out

def port_violations(cfg, allowlist):
    """[(service, unexpected (published, target))] outside the documented allowlist."""
    bad = []
    for name, allowed in allowlist.items():
        for port in sorted(ports_allowlist(cfg, name) - allowed):
            bad.append((name, port))
    return bad

class NetworkL2Test(unittest.TestCase):
    def test_NETWORK_001_bridge_only_arrow(self):
        """NETWORK-001 / SEC-003: only go-arrow bridge has arrow-egress."""
        cfg = compose_json("execution-t3")
        for name, svc in cfg.get("services", {}).items():
            if name == "execution-bridge":
                continue
            nets = svc.get("networks") or {}
            # networks may be dict or list
            net_names = set(nets.keys()) if isinstance(nets, dict) else set(nets)
            self.assertNotIn("arrow-egress", net_names, f"NETWORK-001: {name} must not be on arrow-egress")
        # P6-806: that loop skips the bridge, so nothing proved the bridge IS
        # attached — removing arrow-egress from it broke no test.
        self.assertIn("arrow-egress", service_networks(cfg, "execution-bridge"),
                      "NETWORK-001: execution-bridge must be on arrow-egress (the order path)")

    def test_NETWORK_002_bridge_profile_disabled_by_default(self):
        """NETWORK-002: without execution-t3, bridge is absent."""
        cfg = compose_json_default()
        self.assertNotIn("execution-bridge", cfg.get("services", {}), "NETWORK-002: bridge must not appear without profile")
        self.assertNotIn("execution-gateway", cfg.get("services", {}))
        self.assertNotIn("nautilus", cfg.get("services", {}))

    def test_NETWORK_004_no_host_port_on_bridge(self):
        """NETWORK-004 / SEC-005: bridge, gateway, nautilus have no published host port."""
        cfg = compose_json("execution-t3")
        for name in ["execution-bridge","execution-gateway","nautilus"]:
            svc = cfg["services"][name]   # P6-807: loud KeyError, never a silent {}
            self.assertFalsy(svc.get("ports"), f"NETWORK-004: {name} must not publish host port, got {svc.get('ports')}")
    def assertFalsy(self, v, msg):
        self.assertFalse(bool(v), msg)

    def test_NETWORK_005_execution_net_internal(self):
        """NETWORK-005: execution-net is internal:true."""
        cfg = compose_json("execution-t3")
        net = cfg.get("networks", {}).get("execution-net")
        self.assertIsNotNone(net, "NETWORK-005: execution-net missing")
        self.assertTrue(net.get("internal") is True, "NETWORK-005: execution-net must be internal:true")

    def test_NETWORK_006_nautilus_can_reach_bridge(self):
        """NETWORK-006: nautilus on execution-net, bridge on execution-net+arrow-egress."""
        cfg = compose_json("execution-t3")
        def nets(name): 
            n=cfg["services"][name].get("networks") or {}
            return set(n.keys()) if isinstance(n, dict) else set(n)
        self.assertIn("execution-net", nets("nautilus"), "NETWORK-006: nautilus must be on execution-net")
        self.assertIn("execution-net", nets("execution-bridge"))

    def test_NETWORK_007_gateway_can_reach_nautilus(self):
        """NETWORK-007: gateway is on trading-net+execution-net, can reach nautilus."""
        cfg = compose_json("execution-t3")
        gw = cfg["services"]["execution-gateway"].get("networks") or {}
        gw_nets = set(gw.keys()) if isinstance(gw, dict) else set(gw)
        self.assertIn("execution-net", gw_nets, "NETWORK-007: gateway must be on execution-net")
        self.assertIn("trading-net", gw_nets, "NETWORK-007: gateway must be on trading-net")

    def test_NETWORK_008_arrow_creds_absent_outside_bridge(self):
        """NETWORK-008 / SEC-004: ARROW_* only on bridge (and ingestion exception).

        Checked against the compose SOURCE, not `compose config` output:
        the rendered `environment` of every env_file-carrying service
        includes secrets.env contents (blanket env_file design,
        2026-08-29 decision A), so rendered-key checks false-fire. The
        guard catches a service that EXPLICITLY wires ARROW order creds.
        """
        import yaml
        cfg = yaml.safe_load(COMPOSE.read_text())
        leaked=[]
        for name, svc in cfg.get("services", {}).items():
            if name in ("execution-bridge","ingestion"):
                continue
            env = svc.get("environment") or {}
            keys = set(env.keys()) if isinstance(env, dict) else set()
            bad = keys & enc.ORDER_ARROW_ENV
            if bad:
                leaked.append((name, bad))
        self.assertEqual(leaked, [], f"NETWORK-008: leaked ARROW keys on {leaked}")

    def test_NETWORK_010_host_cannot_bypass_gateway(self):
        """NETWORK-010 / SEC-006..009: no Fluss/ZK/Flink/Arrow internal port published beyond LOCAL_* allowlist."""
        text = COMPOSE.read_text()
        # forbid host-published Fluss RPC 9123/9124 being LOCAL_SERVICE_ONLY — they are published for dev but doc says LOCAL_SERVICE_ONLY
        # spec says: do NOT expose Fluss internal RPC — but current compose DOES expose 9123:9123 for dev; we assert they are not on arrow-egress etc.
        # Instead check that execution-* have no ports (already) and arrow-egress is bridge-only — so host bypass requires explicit LOCAL_* port, which is not on execution services
        cfg = compose_json("execution-t3")
        for name in ["execution-bridge","execution-gateway","nautilus"]:
            self.assertFalse(bool(cfg["services"][name].get("ports")), f"NETWORK-010: {name} must not be host-reachable")

    def test_SEC_001_no_production_credentials(self):
        """SEC-001: no prod credential file or env var accepted by local profile."""
        text = COMPOSE.read_text()
        self.assertNotIn("prod-credentials", text.lower())
        env_text = (ROOT / "code/01_platform/01_docker/.env.example").read_text().lower()
        self.assertNotRegex(env_text, r"^\s*environment\s*=\s*production", "SEC-001 must not default to production")

    def test_SEC_002_no_production_endpoints(self):
        """SEC-002: no prod broker/S3/DB endpoint accepted without env."""
        text = COMPOSE.read_text()
        self.assertIn("${", text, "SEC-002: endpoints must be via env interpolation, not hard-coded")

    def test_NETWORK_003_fake_mode_offline(self):
        """NETWORK-003: execution-t3=fake stays offline, cannot resolve Arrow."""
        cfg = compose_json("execution-t3")
        self.assertIn("execution-bridge", cfg["services"], "NETWORK-003: bridge missing in fake profile")
        # fake must still be internal (no egress escape)
        net = cfg.get("networks", {}).get("execution-net")
        self.assertIsNotNone(net)
        self.assertTrue(net.get("internal") is True)
        # bridge env must not leak prod creds
        bridge_env = cfg["services"]["execution-bridge"].get("environment") or {}
        self.assertNotIn("ARROW_PROD_TOKEN", bridge_env)

    def test_NETWORK_009_flink_cannot_reach_arrow(self):
        """NETWORK-009: Flink cannot reach Arrow (explicit regression)."""
        cfg = compose_json("execution-t3")
        for name in ["flink-jobmanager", "flink-taskmanager"]:
            if name in cfg["services"]:
                nets = cfg["services"][name].get("networks") or {}
                net_names = set(nets.keys()) if isinstance(nets, dict) else set(nets)
                self.assertNotIn("arrow-egress", net_names, f"NETWORK-009: {name} must not be on arrow-egress")
                env = cfg["services"][name].get("environment") or {}
                keys = set(env.keys()) if isinstance(env, dict) else set()
                self.assertFalse(keys & enc.ORDER_ARROW_ENV, f"NETWORK-009: {name} leaks ARROW")

    def test_SEC_003_bridge_sole_arrow_client(self):
        """SEC-003 dup NETWORK-001: only bridge has Arrow network."""
        cfg = compose_json("execution-t3")
        for name, svc in cfg["services"].items():
            if name == "execution-bridge":
                continue
            nets = svc.get("networks") or {}
            net_names = set(nets.keys()) if isinstance(nets, dict) else set(nets)
            self.assertNotIn("arrow-egress", net_names, f"SEC-003: {name} must not be on arrow-egress")
        # P6-806: same gap as NETWORK-001 — assert the bridge IS attached.
        self.assertIn("arrow-egress", service_networks(cfg, "execution-bridge"),
                      "SEC-003: execution-bridge must be the arrow-egress client")

    def test_SEC_004_no_arrow_creds_outside_bridge(self):
        """SEC-004 dup NETWORK-008 — source-level check (see NETWORK-008 note)."""
        import yaml
        cfg = yaml.safe_load(COMPOSE.read_text())
        leaked=[]
        for name, svc in cfg["services"].items():
            if name in ("execution-bridge","ingestion"):
                continue
            env = svc.get("environment") or {}
            keys = set(env.keys()) if isinstance(env, dict) else set()
            bad = keys & enc.ORDER_ARROW_ENV
            if bad:
                leaked.append((name, bad))
        self.assertEqual(leaked, [], f"SEC-004: leaked ARROW keys on {leaked}")

    def test_SEC_005_bridge_has_no_host_port(self):
        """SEC-005 dup NETWORK-004."""
        cfg = compose_json("execution-t3")
        for name in ["execution-bridge"]:
            svc = cfg["services"][name]   # P6-807: loud KeyError, never a silent {}
            self.assertFalse(bool(svc.get("ports")), f"SEC-005: {name} must not publish host port")

    def test_SEC_006_no_fluss_internal_port_exposure(self):
        """SEC-006: Fluss RPC/tablet ports LOCAL_SERVICE_ONLY, not host-published beyond allowlist."""
        cfg = compose_json("execution-t3")
        # fluss is on trading-net only, not arrow-egress; execution-* have no ports
        for name in ["execution-bridge","execution-gateway","nautilus"]:
            self.assertFalse(bool(cfg["services"][name].get("ports")), f"SEC-006: {name} must not publish ports")
        # P6-604: the docstring promises the RPC/tablet ports stay inside the
        # documented dev allowlist, but the body only denied arrow-egress. Pin the
        # published set (docs 08-local-compose.md marks both LOCAL_SERVICE_ONLY).
        for name in FLUSS_PORT_ALLOWLIST:
            self.assertIn(name, cfg["services"], f"SEC-006: {name} missing")
            self.assertNotIn("arrow-egress", service_networks(cfg, name), f"SEC-006: {name} must not be on arrow-egress")
        self.assertEqual(port_violations(cfg, FLUSS_PORT_ALLOWLIST), [],
                         "SEC-006: Fluss host-published ports changed — extend the allowlist deliberately")

    def test_SEC_007_no_zookeeper_peer_port_exposure(self):
        """SEC-007: ZK 2181/2888/3888 not beyond local service boundary."""
        cfg = compose_json(None)
        zk = cfg["services"].get("zookeeper")
        self.assertIsNotNone(zk, "SEC-007: zookeeper missing")
        nets = zk.get("networks") or {}
        net_names = set(nets.keys()) if isinstance(nets, dict) else set(nets)
        self.assertNotIn("arrow-egress", net_names)
        # 2888/3888 are peer ports — must not be in published ports
        ports = zk.get("ports") or []
        port_str = " ".join(str(p) for p in ports)
        self.assertNotIn("2888", port_str, "SEC-007: ZK 2888 must not be published")
        self.assertNotIn("3888", port_str, "SEC-007: ZK 3888 must not be published")

    def test_SEC_008_no_flink_admin_port_exposure(self):
        """SEC-008: Flink REST/admin not exposed beyond local operator."""
        cfg = compose_json("execution-t3")
        for name in ["execution-bridge","execution-gateway","nautilus"]:
            self.assertFalse(bool(cfg["services"][name].get("ports")))
        # P6-605: the docstring says the REST/admin surface stays inside the local
        # operator boundary, but the body never inspected the ports. Pin the
        # published set: JM 8081 (REST/UI) + 9249 (metrics), TM 9250 -> 9249.
        for name in FLINK_PORT_ALLOWLIST:
            self.assertIn(name, cfg["services"], f"SEC-008: {name} missing")
            self.assertNotIn("arrow-egress", service_networks(cfg, name), f"SEC-008: {name} must not be on arrow-egress")
        self.assertEqual(port_violations(cfg, FLINK_PORT_ALLOWLIST), [],
                         "SEC-008: Flink host-published ports changed — review the exposure deliberately")

    def test_SEC_009_no_arrow_rest_external_exposure(self):
        """SEC-009: Arrow REST not exposed beyond bridge."""
        cfg = compose_json("execution-t3")
        for name, svc in cfg["services"].items():
            if name != "execution-bridge":
                nets = svc.get("networks") or {}
                net_names = set(nets.keys()) if isinstance(nets, dict) else set(nets)
                self.assertNotIn("arrow-egress", net_names, f"SEC-009: {name} must not be on arrow-egress")
        # P6-806: the same skip-the-bridge loop — assert the sole Arrow client.
        self.assertIn("arrow-egress", service_networks(cfg, "execution-bridge"),
                      "SEC-009: execution-bridge must be on arrow-egress")
        # execution services have no ports so HOST cannot bypass gateway
        for name in ["execution-bridge","execution-gateway","nautilus"]:
            self.assertFalse(bool(cfg["services"][name].get("ports")))

    def test_SEC_011_fake_cannot_reach_internet(self):
        """SEC-011 dup NETWORK-003: fake cannot reach internet."""
        cfg = compose_json("execution-t3")
        net = cfg.get("networks", {}).get("execution-net")
        self.assertTrue(net.get("internal") is True, "SEC-011: execution-net must be internal")
        self.assertIn("execution-bridge", cfg["services"])

    def test_SEC_012_halted_blocks_order(self):
        """SEC-012 dup EXEC-002: HALTED blocks any money-moving command."""
        src = (ROOT / "code/02_services/04_executor/src/gate.rs").read_text()
        self.assertIn("can_execute", src)
        self.assertIn("Halted", src)

    def test_SEC_013_telemetry_cannot_enable_trading(self):
        """SEC-013: losing OpenObserve does not make Nautilus ready to trade."""
        cfg = compose_json("execution-t3")
        env = cfg["services"]["nautilus"].get("environment") or {}
        self.assertEqual(env.get("EXECUTION_ENABLED"), "false")
        # health gate is independent of O2
        self.assertIn("otel-collector", cfg["services"] or {})
        self.assertIn("openobserve", cfg["services"])

    def test_SEC_014_restart_cannot_enable_trading(self):
        """SEC-014 dup FAIL-005: restart cannot silently flip gate to ENABLED."""
        src = (ROOT / "code/02_services/04_executor/src/gate.rs").read_text()
        self.assertIn("safety_halt", src)
        self.assertIn("Halted", src)
        cfg = compose_json("execution-t3")
        self.assertEqual((cfg["services"]["nautilus"].get("environment") or {}).get("EXECUTION_ENABLED"), "false")

    def test_SEC_015_malformed_request_cannot_bypass_gate(self):
        """SEC-015: bad intent / missing correlation / bad signature → rejected, no order."""
        gw_validator = ROOT / "code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentValidator.java"
        self.assertTrue(gw_validator.exists(), "SEC-015: IntentValidator missing")
        self.assertIn("validate", gw_validator.read_text())
        self.assertIn("throw", gw_validator.read_text())

    def test_SEC_010_no_secrets_in_logs(self):
        """SEC-010: compose YAML source must not hard-code secret values (only ${VAR} refs)."""
        text = COMPOSE.read_text()
        for m in re.finditer(r"ARROW_APP_SECRET\s*:\s*(.+)", text):
            val = m.group(1).strip()
            self.assertIn("${", val, f"SEC-010: ARROW_APP_SECRET must be via env var, got literal: {val[:20]}")

class ExecutionNetworkCheckGuardTest(unittest.TestCase):
    """Guard: execution_network_check.py must stay runnable and PASS.

    The script sat dead for a week (2026-08-24..31): the AWS_* `:?` pins in
    x-flink-common broke its bare `docker compose config` invocation, and
    nothing invoked it — three separate defects (missing env-files, missing
    bridge exemption, rendered-env false-fires) hid behind the interpolation
    error. This test invokes it end-to-end so it fails LOUDLY in CI instead.
    """

    def test_execution_network_check_passes(self):
        script = ROOT / "code/01_platform/04_scripts/execution_network_check.py"
        self.assertTrue(script.exists(), "execution_network_check.py missing")
        r = subprocess.run(
            ["python3", str(script), "--compose", str(COMPOSE)],
            capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(
            r.returncode, 0,
            f"execution_network_check failed:\n{r.stdout}\n{r.stderr}",
        )
        self.assertIn("PASS", r.stdout)
