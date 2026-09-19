"""Guards for the wave-51 rungs: every shape this wave fixed must fail when broken.

Wave 51 removed six assertions that could not fail from the 25-step smoke, the
prod-hardening module and the Swarm stack module. This module pins that they stay
gone: AST detectors run over the three real modules, and each detector is then
handed the shape it hunts, so deleting a detector or reintroducing a shape fails
here rather than passing quietly.

Every check is config-only: YAML parsing, file reads, and module import. No
container is started and no live cluster is contacted.
"""
import ast
import importlib.util
import unittest
from pathlib import Path

TESTS = Path(__file__).resolve().parent
ROOT = TESTS.parents[3]
L25 = TESTS / "test_08_local_compose_25.py"
PROD = TESTS / "test_08_local_compose_prod.py"
STACK_T = TESTS / "test_09_stack.py"
STACK_YML = ROOT / "code/01_platform/01_docker/docker-stack.yml"
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"
DDL_ENTRYPOINT = ROOT / "code/01_platform/01_docker/ddl-apply/ddl-apply-entrypoint.sh"


def load(path, name):
    """Import a sibling test module by path — side-effect free for these three."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def parse(path):
    return ast.parse(path.read_text())


def assert_like(node):
    """The Call node's attribute name when it is an assertX/fail call, else ''."""
    func = getattr(node, "func", None)
    if isinstance(node, ast.Call) and isinstance(func, ast.Attribute):
        if func.attr.startswith("assert") or func.attr == "fail":
            return func.attr
    return ""


def or_true_asserts(tree):
    """`assert <expr> or True`: the condition can never be false (P6-615)."""
    hits = []
    for node in ast.walk(tree):
        tests = ([node.test] if isinstance(node, ast.Assert)
                 else list(node.args) if assert_like(node) else [])
        for t in tests:
            if (isinstance(t, ast.BoolOp) and isinstance(t.values[-1], ast.Constant)
                    and t.values[-1].value is True):
                hits.append(node.lineno)
    return hits


def else_true_asserts(tree):
    """`assertX(<expr> if <cond> else True)`: a missing file turns the check off (P6-612)."""
    hits = []
    for node in ast.walk(tree):
        if not assert_like(node):
            continue
        for arg in node.args:
            if (isinstance(arg, ast.IfExp) and isinstance(arg.orelse, ast.Constant)
                    and arg.orelse.value is True):
                hits.append(node.lineno)
    return hits


def dead_locals(tree):
    """Names stored inside a function and never read there — the P6-613 shape."""
    hits = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        stores, loads = {}, set()
        for node in ast.walk(fn):
            if isinstance(node, ast.Name):
                if isinstance(node.ctx, ast.Store):
                    stores.setdefault(node.id, node.lineno)
                elif isinstance(node.ctx, ast.Load):
                    loads.add(node.id)
        hits += [(fn.name, name, line) for name, line in stores.items() if name not in loads]
    return hits


def exists_guarded_asserts(tree):
    """`if <...>.exists(): <assertions>` — silently skips the assertions (P6-609/612).

    A body that *skips* is fine: a skip is visible in the run. A body that only
    asserts is the shape that hides a missing file.
    """
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If) or node.orelse or "exists" not in ast.dump(node.test):
            continue
        calls = [n for b in node.body for n in ast.walk(b) if isinstance(n, ast.Call)]
        asserts = [c for c in calls if assert_like(c) or getattr(getattr(c, "func", None), "attr", "") == "fail"]
        skips = [c for c in calls if getattr(getattr(c, "func", None), "attr", "") == "skipTest"]
        if asserts and not skips:
            hits.append(node.lineno)
    return hits


def import_hygiene(text):
    """(duplicated, unused) module-level imports — the P6-812/815 shape."""
    tree = ast.parse(text)
    seen, dup = [], []
    for node in tree.body:
        names = []
        if isinstance(node, ast.Import):
            names = [a.asname or a.name.split(".")[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [a.asname or a.name for a in node.names]
        for n in names:
            if n in seen:
                dup.append(n)
            seen.append(n)
    loads = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    return dup, sorted({n for n in seen if n not in loads})


class VacuityAbsentTest(unittest.TestCase):
    """The fixed shapes must not come back into the three real modules."""

    def test_no_always_true_assertion(self):
        for path in (L25, PROD, STACK_T):
            tree = parse(path)
            self.assertEqual([], or_true_asserts(tree), f"{path.name}: `assert … or True` is unreachable")
            self.assertEqual([], else_true_asserts(tree),
                             f"{path.name}: `assertX(… if cond else True)` disables itself")

    def test_no_dead_local_variable(self):
        for path in (L25, PROD, STACK_T):
            hits = dead_locals(parse(path))
            self.assertEqual([], hits, f"{path.name}: assigned but never read: {hits}")

    def test_no_silent_exists_guarded_assertions(self):
        for path in (L25, PROD, STACK_T):
            hits = exists_guarded_asserts(parse(path))
            self.assertEqual([], hits, f"{path.name}: assertions behind a bare exists() guard: {hits}")

    def test_no_duplicate_or_unused_imports(self):
        for path in (L25, PROD, STACK_T):
            dup, unused = import_hygiene(path.read_text())
            self.assertEqual([], dup, f"{path.name}: duplicated imports: {dup}")
            self.assertEqual([], unused, f"{path.name}: unused imports: {unused}")

    def test_removed_shapes_are_gone_from_the_source(self):
        l25, prod = L25.read_text(), PROD.read_text()
        self.assertNotIn('exec_module', l25, "P6-801: the harness must not run inside the suite process")
        self.assertNotIn('"python3"', l25, "P6-800: use sys.executable, not a PATH lookup")
        self.assertIn("sys.executable", l25)
        self.assertNotIn("ing_env", prod, "P6-613: the dead O2-cred collection must be gone")
        self.assertNotIn("props_block", prod, "P6-817: the indentation-guessing regex must be gone")
        self.assertIn("assertTrue(props", prod, "P6-817: a scan that finds no block must be loud")
        self.assertIn("pool too small", l25, "P6-801: the pool guard is what makes PASS [offline-25] mean something")


class DetectorsFireTest(unittest.TestCase):
    """Each detector must fire when handed the shape it hunts — else it guards nothing."""

    def test_or_true_detector_fires(self):
        src = "def t(self):\n    assert 'a' in props or 'b' in props or True\n"
        self.assertEqual(1, len(or_true_asserts(ast.parse(src))))

    def test_or_true_detector_ignores_a_real_disjunction(self):
        src = "def t(self):\n    assert 'a' in props or 'b' in props\n"
        self.assertEqual([], or_true_asserts(ast.parse(src)))

    def test_else_true_detector_fires(self):
        src = "def t(self):\n    self.assertTrue(p.exists() and 'x' in p.read_text() if p.exists() else True)\n"
        self.assertEqual(1, len(else_true_asserts(ast.parse(src))))

    def test_dead_local_detector_fires(self):
        src = "def t(self):\n    kept = 1\n    wasted = 2\n    self.assertEqual(1, kept)\n"
        self.assertEqual([("t", "wasted", 3)], dead_locals(ast.parse(src)))

    def test_exists_guard_detector_fires(self):
        src = ("def t(self):\n"
               "    if p.exists():\n"
               "        self.assertIn('x', p.read_text())\n")
        self.assertEqual(1, len(exists_guarded_asserts(ast.parse(src))))

    def test_exists_guard_detector_ignores_a_loud_skip(self):
        src = ("def t(self):\n"
               "    if not p.exists():\n"
               "        self.skipTest('needs the file')\n"
               "    self.assertIn('x', p.read_text())\n")
        self.assertEqual([], exists_guarded_asserts(ast.parse(src)))

    def test_import_hygiene_detector_fires(self):
        dup, unused = import_hygiene("import json, os\nimport json\n")
        self.assertEqual(["json"], dup, "the second `import json` is the duplicate")
        self.assertEqual(["json", "os"], unused,
                         "neither name is read — duplicated and unused are separate findings")

    def test_import_hygiene_detector_accepts_clean_imports(self):
        self.assertEqual(([], []), import_hygiene("import json\nprint(json.dumps({}))\n"))


class HarnessLegTest(unittest.TestCase):
    """P6-596/800/801: the 25-step live leg and the harness invocation."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load(L25, "w51_l25")

    def test_live_leg_skips_when_the_stack_is_down(self):
        mod = self.mod
        original = mod.live_stack_up
        mod.live_stack_up = lambda: False
        try:
            case = mod.Nautilus25SmokeTest("test_25_instruments_live_gated")
            with self.assertRaises(unittest.SkipTest):
                case.test_25_instruments_live_gated()
        finally:
            mod.live_stack_up = original

    def test_live_leg_asserts_when_the_stack_is_up(self):
        mod = self.mod
        original_up, original_run = mod.live_stack_up, mod.run_harness
        mod.live_stack_up = lambda: True
        mod.run_harness = lambda *a, **k: "PASS LOCAL-INT-004 [offline-25]: 25 instruments"
        try:
            case = mod.Nautilus25SmokeTest("test_25_instruments_live_gated")
            with self.assertRaises(AssertionError):
                case.test_25_instruments_live_gated()
        finally:
            mod.live_stack_up, mod.run_harness = original_up, original_run

    def test_harness_failure_carries_the_harness_output(self):
        mod = self.mod
        original = mod.HARNESS
        fixture = TESTS / ".w51_fake_harness.py"
        fixture.write_text("import sys\nprint('FAIL [offline-25]: boom')\nsys.exit(1)\n")
        mod.HARNESS = fixture
        try:
            with self.assertRaises(AssertionError) as caught:
                mod.run_harness("--offline")
            self.assertIn("boom", str(caught.exception))
        finally:
            mod.HARNESS = original
            fixture.unlink()


class StackGuardTest(unittest.TestCase):
    """P6-615/818: the HA backend assertion and the in-block healthcheck markers."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load(STACK_T, "w51_stack")
        cls.raw = STACK_YML.read_text()

    def test_ha_backend_helper_accepts_the_real_stack(self):
        import yaml
        props = yaml.safe_load(self.raw)["services"]["flink-jobmanager"]["environment"]["FLINK_PROPERTIES"]
        self.assertTrue(self.mod.ha_backend_ok(props))

    def test_ha_backend_helper_rejects_a_cluster_id_alone(self):
        props = "high-availability.cluster-id: prod\n"
        self.assertFalse(self.mod.ha_backend_ok(props), "a cluster-id without a backend cannot fail over")

    def test_ha_backend_helper_rejects_a_single_node_quorum(self):
        props = ("high-availability.type: zookeeper\n"
                 "high-availability.zookeeper.quorum: zookeeper-1:2181\n")
        self.assertFalse(self.mod.ha_backend_ok(props), "one ZK node is not an HA ensemble")

    def test_marker_is_scoped_to_its_own_service(self):
        fixture = ("services:\n"
                   "  alpha:\n"
                   "    image: x\n"
                   "    # x-healthcheck: none (alpha has no fixed listener)\n"
                   "  beta:\n"
                   "    image: y\n")
        self.assertIn("x-healthcheck:", self.mod.service_block_raw(fixture, "alpha"))
        self.assertNotIn("x-healthcheck:", self.mod.service_block_raw(fixture, "beta"),
                         "a marker in another service must not justify this one")

    def test_service_block_raw_is_loud_for_an_unknown_service(self):
        with self.assertRaises(AssertionError):
            self.mod.service_block_raw(self.raw, "no-such-service")

    def test_every_declared_exception_carries_an_in_block_marker(self):
        declared = set()
        for name in dir(self.mod):
            holder = getattr(self.mod, name)
            if isinstance(holder, type) and hasattr(holder, "HEALTHCHECK_ALLOWED_EXCEPTIONS"):
                declared |= set(holder.HEALTHCHECK_ALLOWED_EXCEPTIONS)
        self.assertTrue(declared, "the exception set disappeared — the check would pass on nothing")
        for name in sorted(declared):
            block = self.mod.service_block_raw(self.raw, name)
            marker = next((l for l in block.splitlines() if "x-healthcheck:" in l), "")
            self.assertTrue(marker, f"{name}: declared exception with no in-block justification")
            self.assertIn("none", marker)
            self.assertGreaterEqual(len(marker.split(":", 1)[1].strip()), 20,
                                    f"{name}: the marker must state why, not merely exist")


class ProdGuardTest(unittest.TestCase):
    """P6-612/613/614/816/817: the prod-hardening surface's own guards."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load(PROD, "w51_prod")
        cls.text = PROD.read_text()

    def test_fluss_properties_covers_every_block_the_stack_declares(self):
        import yaml
        props = self.mod.fluss_properties(COMPOSE.read_text())
        parsed = yaml.safe_load(COMPOSE.read_text())["services"]
        expected = [n for n, s in parsed.items()
                    if isinstance(s.get("environment"), dict) and "FLUSS_PROPERTIES" in s["environment"]]
        self.assertEqual(len(expected), len(props),
                         f"the parsed scan must see every FLUSS_PROPERTIES service: {expected}")
        self.assertTrue(props, "an empty scan would make the credential leak check vacuous")

    def test_fluss_properties_is_empty_without_the_key(self):
        self.assertEqual([], self.mod.fluss_properties("services:\n  a:\n    image: x\n"))

    def test_leak_scan_would_catch_a_credential(self):
        fixture = ('services:\n  a:\n    environment:\n      FLUSS_PROPERTIES: |\n'
                   '        s3.secret-key: AWS_SECRET_ACCESS_KEY: oops\n')
        self.assertEqual(1, len([b for b in self.mod.fluss_properties(fixture)
                                 if "AWS_SECRET_ACCESS_KEY:" in b]))

    def test_prod_011_gates_on_docker_and_env_files(self):
        self.assertIn('shutil.which("docker") is None', self.text,
                      "P6-614: a Docker-less checkout must skip, not error")
        self.assertIn("skipTest", self.text)
        self.assertIn("env_files", self.text)

    def test_ddl_nonroot_pins_exist_and_match_the_entrypoint(self):
        self.assertIn('self.assertIn("setpriv", entrypoint', self.text,
                      "P6-816: the non-root drop must be asserted, not merely read")
        self.assertIn('self.assertIn("10001", entrypoint', self.text)
        entrypoint = DDL_ENTRYPOINT.read_text()
        self.assertIn("setpriv", entrypoint)
        self.assertIn("10001", entrypoint)


if __name__ == "__main__":
    unittest.main()
