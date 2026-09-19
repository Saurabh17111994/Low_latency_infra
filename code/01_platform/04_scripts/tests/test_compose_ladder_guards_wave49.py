"""Wave 49 hermetic guards for the compose-ladder assertions (l0 / l1_l3 / l2).

P6-597  CONFIG-004 asserts the required secrets are really supplied by an env file
P6-598  CONFIG-005 pins ARROW_TOKEN's absence and flags literal secret settings
P6-802  no dead `os` import in the l0 module
P6-603  START-003 asserts TM's OWN depends_on instead of a self-fulfilling union
P6-804  no dead `re` import / duplicate in-function imports in the l1_l3 module
P6-805  HEALTH-001 asserts the readiness proof each service actually declares
P6-604  SEC-006 checks Fluss' published ports against the documented allowlist
P6-605  SEC-008 checks Flink's published ports against the documented allowlist
P6-806  NETWORK-001/SEC-003/SEC-009 assert the bridge IS on arrow-egress
P6-807  NETWORK-004/SEC-005 subscript the service so a removal fails loudly

Every behavioural test is mutant-driven: the same helper the ladder module uses is
handed a mutated `compose config` shape and must report the violation. A helper
that quietly stops detecting fails HERE, which is exactly what the original
assertions could not do (they passed on both the good and the broken input).
Only `compose config` is parsed — no containers are started.
"""
import ast
import importlib.util
import re
import shutil
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
L0 = HERE / "test_08_local_compose_l0.py"
L13 = HERE / "test_08_local_compose_l1_l3.py"
L2 = HERE / "test_08_local_compose_l2.py"
MANIFEST_1024 = REPO.parent / "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"


def load(path, name):
    """Import a ladder module by path under a non-test name (pytest must not re-collect it)."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


l0 = load(L0, "wave49_l0_under_test")
l13 = load(L13, "wave49_l1l3_under_test")
l2 = load(L2, "wave49_l2_under_test")


class SecretPolicyTest(unittest.TestCase):
    """P6-597/P6-598: the secret assertions must fail on the broken shapes."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="w49-secrets-"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_empty_or_absent_secret_is_reported(self):
        env = self.tmp / "secrets.env"
        env.write_text("ARROW_PASSWORD=real-value\nO2_PASSWORD=\nEC=1\n")
        gone = l0.missing_secrets([env], ["O2_PASSWORD", "ARROW_PASSWORD", "ARROW_APP_SECRET"])
        self.assertEqual(gone, ["O2_PASSWORD", "ARROW_APP_SECRET"])   # empty counts as missing
        self.assertNotIn("ARROW_PASSWORD", gone)

    def test_missing_file_is_not_an_error(self):
        self.assertEqual(l0.missing_secrets([self.tmp / "nope.env"], ["A"]), ["A"])
        self.assertEqual(l0.env_values(self.tmp / "nope.env"), {})

    def test_literal_secret_setting_is_reported(self):
        hits = l0.literal_secret_settings("    ZO_ROOT_USER_PASSWORD: hunter2\n")
        self.assertEqual(len(hits), 1, hits)
        self.assertEqual(l0.literal_secret_settings("    ZO_ROOT_USER_PASSWORD: ${O2_PASSWORD}\n"), [])
        self.assertEqual(l0.literal_secret_settings("    # AWS_SECRET_ACCESS_KEY: minioadmin\n"), [])
        self.assertEqual(l0.literal_secret_settings("    SECRETS_VIA_ENV_FILE: \"1\"\n"), [])

    def test_arrow_token_pin_replaced_the_inert_scan(self):
        src = L0.read_text()
        self.assertIn('self.assertNotIn("ARROW_TOKEN", text', src)
        self.assertNotIn('ARROW_TOKEN=", out.replace', src, "the inert scan is back")

    def test_the_repo_env_files_really_supply_the_required_secrets(self):
        self.assertEqual(l0.missing_secrets(l0.ENV_FILES, l0.REQUIRED_ENV_SECRETS), [],
                         "secrets.env/.env no longer supply a required secret")


class ReadinessProofTest(unittest.TestCase):
    """P6-805: HEALTH-001 must flag a service that loses its declared proof."""

    def test_missing_service_is_a_gap(self):
        self.assertIn(("ingestion", "service missing"), l13.readiness_gaps({"services": {}}))

    def test_healthcheck_removed_is_a_gap(self):
        cfg = {"services": {"ingestion": {"environment": {"X": "1"}}}}
        self.assertEqual(l13.readiness_gaps(cfg, {"ingestion": "healthcheck"}),
                         [("ingestion", "no healthcheck declared")])

    def test_flink_without_rest_surface_is_a_gap(self):
        cfg = {"services": {"flink-jobmanager": {"environment": {"FLINK_PROPERTIES": "jobmanager.rpc.address: jm"}}}}
        self.assertEqual(l13.readiness_gaps(cfg, {"flink-jobmanager": "rest"}),
                         [("flink-jobmanager", "no REST/metrics surface")])

    def test_present_healthcheck_and_rest_surface_pass(self):
        cfg = {"services": {
            "ingestion": {"healthcheck": {"test": ["CMD", "true"]}},
            "flink-jobmanager": {"ports": [{"published": "8081", "target": 8081}]},
        }}
        self.assertEqual(l13.readiness_gaps(cfg, {"ingestion": "healthcheck", "flink-jobmanager": "rest"}), [])


class Start003Test(unittest.TestCase):
    """P6-603: the self-fulfilling union must stay gone."""

    def test_union_form_is_gone_and_tm_set_is_asserted(self):
        src = L13.read_text()
        self.assertNotIn("tm_set | jm_set", src, "the vacuous union is back")
        self.assertIn('self.assertIn("flink-jobmanager", tm_set', src)


class PortAllowlistTest(unittest.TestCase):
    """P6-604/P6-605: an extra or changed published port must be a violation."""

    def test_extra_fluss_port_is_a_violation(self):
        cfg = {"services": {"fluss-coordinator": {"ports": [
            {"published": "9123", "target": 9123},
            {"published": "9999", "target": 9999},
        ]}}}
        self.assertEqual(l2.port_violations(cfg, l2.FLUSS_PORT_ALLOWLIST),
                         [("fluss-coordinator", ("9999", "9999"))])

    def test_republished_port_is_a_violation(self):
        cfg = {"services": {"fluss-tablet": {"ports": [{"published": "9999", "target": 9124}]}}}
        self.assertEqual(l2.port_violations(cfg, l2.FLUSS_PORT_ALLOWLIST),
                         [("fluss-tablet", ("9999", "9124"))])

    def test_flink_taskmanager_mapping_is_exact(self):
        # the real mapping is 9250 -> 9249; a "tidy" 9250:9250 must be flagged
        cfg = {"services": {"flink-taskmanager": {"ports": [{"published": "9250", "target": 9250}]}}}
        self.assertEqual(l2.port_violations(cfg, l2.FLINK_PORT_ALLOWLIST),
                         [("flink-taskmanager", ("9250", "9250"))])

    def test_short_syntax_is_normalised(self):
        cfg = {"services": {"x": {"ports": ["8081:8081"]}}}
        self.assertEqual(l2.ports_allowlist(cfg, "x"), {("8081", "8081")})

    def test_both_ladders_are_wired_to_the_helper(self):
        # A helper nobody calls would pass every mutant above (the original defect
        # was exactly that: SEC-006/SEC-008 never inspected the ports at all).
        self.assertEqual(L2.read_text().count("assertEqual(port_violations(cfg,"), 2,
                         "SEC-006/SEC-008 must both compare against their allowlist")

    def test_allowlisted_ports_produce_no_violation(self):
        cfg = {"services": {"flink-jobmanager": {"ports": [
            {"published": "8081", "target": 8081}, {"published": "9249", "target": 9249}]}}}
        self.assertEqual(l2.port_violations(cfg, l2.FLINK_PORT_ALLOWLIST), [])


class ArrowEgressTest(unittest.TestCase):
    """P6-806: the bridge losing arrow-egress must be visible."""

    def test_bridge_without_arrow_egress_is_visible(self):
        cfg = {"services": {"execution-bridge": {"networks": ["execution-net"]}}}
        self.assertNotIn("arrow-egress", l2.service_networks(cfg, "execution-bridge"))

    def test_both_network_yaml_forms_are_normalised(self):
        self.assertEqual(l2.service_networks({"services": {"x": {"networks": {"arrow-egress": None}}}}, "x"),
                         {"arrow-egress"})
        self.assertEqual(l2.service_networks({"services": {"x": {"networks": ["execution-net"]}}}, "x"),
                         {"execution-net"})

    def test_all_three_ladders_assert_the_positive(self):
        src = L2.read_text()
        self.assertEqual(src.count('service_networks(cfg, "execution-bridge")'), 3,
                         "NETWORK-001/SEC-003/SEC-009 must each assert the bridge IS attached")


class MissingServiceIsLoudTest(unittest.TestCase):
    """P6-807: a removed service must raise, not be skipped by a silent default."""

    def test_removed_service_raises_keyerror(self):
        cfg = {"services": {"execution-gateway": {}}}
        with self.assertRaises(KeyError):
            cfg["services"]["nautilus"]

    def test_no_silent_default_left_in_the_two_checks(self):
        src = L2.read_text()
        self.assertNotIn('.get(name, {})', src, "a vacuous .get(name, {}) is back")


class DeadImportTest(unittest.TestCase):
    """P6-802/P6-804: imports must be used, and not re-imported inside a function."""

    @staticmethod
    def _imports(nodes):
        out = set()
        for n in nodes:
            if isinstance(n, ast.Import):
                out |= {(a.asname or a.name).split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                out |= {a.asname or a.name for a in n.names}
        return out

    def dead_names(self, path):
        tree = ast.parse(Path(path).read_text())
        imported = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    imported[(a.asname or a.name).split(".")[0]] = node.lineno
            elif isinstance(node, ast.ImportFrom):
                for a in node.names:
                    imported[a.asname or a.name] = node.lineno
        used = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        return sorted(k for k in imported if k not in used)

    def local_imports(self, path):
        tree = ast.parse(Path(path).read_text())
        module_level = self._imports([n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))])
        found = {}
        for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
            got = self._imports([n for n in ast.walk(fn) if isinstance(n, (ast.Import, ast.ImportFrom))])
            if got:
                found[fn.name] = got
        return module_level, found

    def test_no_dead_imports(self):
        for path in (L0, L13, L2):
            self.assertEqual(self.dead_names(path), [], f"{path.name}: dead imports")

    def test_no_duplicate_local_imports(self):
        for path in (L0, L13, L2):
            module_level, local = self.local_imports(path)
            for fn, got in local.items():
                self.assertEqual(got & module_level, set(),
                                 f"{path.name}:{fn}() re-imports module-level names {sorted(got & module_level)}")


class RealConfigTest(unittest.TestCase):
    """The fixed allowlists and proofs must hold for the real compose file."""

    @classmethod
    def setUpClass(cls):
        cls.cfg = l2.compose_json("execution-t3")

    def test_fluss_and_flink_ports_match_the_allowlists(self):
        self.assertEqual(l2.port_violations(self.cfg, l2.FLUSS_PORT_ALLOWLIST), [])
        self.assertEqual(l2.port_violations(self.cfg, l2.FLINK_PORT_ALLOWLIST), [])

    def test_bridge_is_on_arrow_egress(self):
        self.assertIn("arrow-egress", l2.service_networks(self.cfg, "execution-bridge"))

    def test_readiness_proofs_hold(self):
        self.assertEqual(l13.readiness_gaps(self.cfg), [])


class TestDManifestPiggybackTest(unittest.TestCase):
    """The approved wave-49 piggyback: test-d's default manifest matches its own comment."""

    SCRIPT = REPO / "code/01_platform/04_scripts/test-d-3x20k-run.sh"

    def test_default_is_the_1024_manifest_and_parsing_stays_quote_aware(self):
        src = self.SCRIPT.read_text()
        m = re.search(r'^MANIFEST="\$\{TEST_D_MANIFEST:-([^"}]*)', src, re.M)
        self.assertIsNotNone(m, "MANIFEST default not found")
        self.assertTrue(m.group(1).endswith("NSE_CM_EQUITY (1024).csv"), m.group(1))
        self.assertIn("csv.DictReader", src, "the default is only safe while parsing stays csv-aware")
        self.assertTrue(MANIFEST_1024.exists(), f"missing manifest: {MANIFEST_1024}")


if __name__ == "__main__":
    unittest.main()
