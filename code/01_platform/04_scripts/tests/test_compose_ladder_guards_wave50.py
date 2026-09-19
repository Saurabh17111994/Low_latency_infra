"""Wave 50 guards — every fixed shape must FAIL on the input it was written for.

The upper ladder modules are contract checks that read artifacts from disk, so a fix
that silently stops detecting cannot be caught by writing one more contract check.
These tests go the other way: each fixed shape is (a) proven absent from the real
module and (b) proven detectable by the same detector when handed that shape, so a
reintroduction fails here (P6-606/607/609/610/611/601/602/803/599/600/608/813 + the
dead-code set 808-812).

The detectors are AST-based on purpose: a reworded or reformatted reintroduction still
matches, where a text grep would not.
"""
import ast
import importlib.util
import re
import textwrap
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
MODULES = {
    "l4": HERE / "test_08_local_compose_l4.py",
    "l6_l7": HERE / "test_08_local_compose_l6_l7.py",
    "l8": HERE / "test_08_local_compose_l8.py",
    "l9": HERE / "test_08_local_compose_l9.py",
    "l10": HERE / "test_08_local_compose_l10.py",
    "l11": HERE / "test_08_local_compose_l11.py",
}
COMPOSE = REPO / "code/01_platform/01_docker/docker-compose.yml"
DOC = REPO / "docs/08_implementation/08-local-compose.md"


def load(path, name):
    """Import a test module by path so its helpers can be driven directly."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


L9 = load(MODULES["l9"], "w50_l9")
L10 = load(MODULES["l10"], "w50_l10")
L11 = load(MODULES["l11"], "w50_l11")


def parse(text):
    return ast.parse(textwrap.dedent(text))


def or_with_operand(tree, kind):
    """Asserts whose ANY argument contains `X or <kind>` (P6-606, P6-607).

    `kind="exists"` is the short-circuit ("Signal" in joined or <dir>.exists()):
    the directory assertion on the line above already made it true.
    `kind="literal"` is the self-satisfying fallback (gw_src or "ProjectionWriter"):
    the fallback itself contains the needle, so assertIn can never fail.

    An `or` of plain alternatives ("BIGINT" in ddl or "VARCHAR" in ddl) matches
    neither and is not flagged - that is a legitimate either-is-acceptable check.
    """
    hits = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr.startswith("assert")):
            continue
        for arg in node.args:
            if not (isinstance(arg, ast.BoolOp) and isinstance(arg.op, ast.Or)):
                continue
            for v in arg.values:
                if kind == "exists" and isinstance(v, ast.Call) \
                   and isinstance(v.func, ast.Attribute) and v.func.attr == "exists":
                    hits.append(ast.unparse(arg)[:100])
                elif kind == "literal" and isinstance(v, ast.Constant) and isinstance(v.value, str) and v.value:
                    hits.append(ast.unparse(arg)[:100])
    return hits


def exists_guarded(tree):
    """`if <path>.exists():` — a body that can quietly assert nothing (P6-609)."""
    return [ast.unparse(n.test)[:100] for n in ast.walk(tree)
            if isinstance(n, ast.If) and isinstance(n.test, ast.Call)
            and isinstance(n.test.func, ast.Attribute) and n.test.func.attr == "exists"]


def noop_assert(tree):
    """`assertTrue(True)` — a check with no observable failure (P6-611)."""
    return [ast.unparse(n)[:60] for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr in {"assertTrue", "assertFalse"} and len(n.args) == 1
            and isinstance(n.args[0], ast.Constant) and n.args[0].value is True]


def double_negative(tree):
    """`assertFalse("X" not in …)` — correct but reads backwards (P6-814)."""
    hits = []
    for n in ast.walk(tree):
        if not (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "assertFalse" and n.args):
            continue
        arg = n.args[0]
        if isinstance(arg, ast.Compare) and any(isinstance(op, ast.NotIn) for op in arg.ops):
            hits.append(ast.unparse(n)[:80])
        elif (isinstance(arg, ast.UnaryOp) and isinstance(arg.op, ast.Not)
              and isinstance(arg.operand, ast.Compare)):
            hits.append(ast.unparse(n)[:80])
    return hits


def ternary_literal(tree):
    """`value if cond else "literal"` — a fallback that satisfies itself (P6-610).

    `else ""` is the ordinary "no file, no text" idiom and is not flagged; a non-empty
    literal fallback is the shape that made the reconnect check unfalsifiable.
    """
    return [ast.unparse(n)[:90] for n in ast.walk(tree)
            if isinstance(n, ast.IfExp) and isinstance(n.orelse, ast.Constant)
            and isinstance(n.orelse.value, str) and n.orelse.value != ""]


def imported_names(tree):
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            names.update(a.asname or a.name.split(".")[0] for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            names.update(a.asname or a.name for a in n.names if a.name != "*")
    return names


def module_level_imports(tree):
    out = {}
    for n in tree.body:
        if isinstance(n, ast.Import):
            for a in n.names:
                out[a.asname or a.name.split(".")[0]] = n.lineno
        elif isinstance(n, ast.ImportFrom):
            for a in n.names:
                if a.name != "*":
                    out[a.asname or a.name] = n.lineno
    return out


class VacuityAbsentTest(unittest.TestCase):
    """The flagged shapes are gone from the real modules."""

    def test_no_assert_or_fallback_left(self):
        for key in ("l4", "l6_l7"):
            tree = parse(MODULES[key].read_text())
            self.assertEqual(or_with_operand(tree, "exists"), [],
                             f"{key}: an assert can short-circuit on a directory/file check again")
            self.assertEqual(or_with_operand(tree, "literal"), [],
                             f"{key}: an assert can satisfy itself with a literal fallback again")

    def test_no_exists_guarded_bodies_left(self):
        l8 = parse(MODULES["l8"].read_text())
        self.assertEqual(exists_guarded(l8), [], "l8: a contract file can go missing silently again")
        self.assertEqual(double_negative(l8), [], "l8: a HALTED check reads as a double negative again")

    def test_no_noop_or_self_satisfying_assertion_left(self):
        l9 = parse(MODULES["l9"].read_text())
        self.assertEqual(noop_assert(l9), [], "l9: a no-op assertion is back")
        self.assertEqual(ternary_literal(l9), [], "l9: a literal fallback is back")
        # the reconnect check must read the writer file, not a hedge
        self.assertIn('self.assertTrue(writer.exists(), "OBS-005', MODULES["l9"].read_text())


class DetectorsFireTest(unittest.TestCase):
    """Each detector fires on the exact shape it exists to catch."""

    def test_or_short_circuit_fires_on_both_original_shapes(self):
        p606 = '''self.assertTrue("Signal" in joined or (ROOT / "code/02_services/02_compute").exists(), "JOB-002")'''
        p607 = '''self.assertIn("Projection", gw_src or "ProjectionWriter")'''
        self.assertEqual(len(or_with_operand(parse(p606), "exists")), 1)
        self.assertEqual(len(or_with_operand(parse(p607), "literal")), 1)
        # a legitimate either-is-acceptable disjunction must NOT be flagged
        legit = '''self.assertTrue("BIGINT" in ddl or "VARCHAR" in ddl)'''
        self.assertEqual(or_with_operand(parse(legit), "exists"), [])
        self.assertEqual(or_with_operand(parse(legit), "literal"), [])

    def test_exists_guarded_fires(self):
        snippet = '''
        proj = ROOT / "x/mod.rs"
        if proj.exists():
            src = proj.read_text()
            self.assertTrue("Unknown" in src)
        '''
        self.assertEqual(len(exists_guarded(parse(snippet))), 1)

    def test_noop_assert_fires(self):
        snippet = '''
        for line in env_lines:
            self.assertTrue(True)
        '''
        self.assertEqual(len(noop_assert(parse(snippet))), 1)
        self.assertEqual(noop_assert(parse('self.assertTrue("real")')), [])

    def test_double_negative_fires(self):
        self.assertEqual(len(double_negative(parse('self.assertFalse("HALTED" not in src)'))), 1)
        self.assertEqual(double_negative(parse('self.assertIn("HALTED", src)')), [])

    def test_ternary_literal_fires(self):
        snippet = 'self.assertIn("reconnect", path.read_text().lower() if path.exists() else "reconnect")'
        self.assertEqual(len(ternary_literal(parse(snippet))), 1)


class ArrowCarrierTest(unittest.TestCase):
    """P6-611: the source-level Arrow credential carriers."""

    def test_real_compose_carries_arrow_only_on_the_order_path(self):
        carriers = L9.arrow_env_carriers(COMPOSE)
        self.assertEqual(sorted(carriers), ["execution-bridge", "ingestion"])
        self.assertEqual(sorted(L9.ARROW_CARRIERS_ALLOWED), ["execution-bridge", "ingestion"])

    def test_a_third_carrier_is_visible(self):
        import tempfile
        yml = """
        services:
          ingestion:
            environment:
              ARROW_APP_ID: ${ARROW_APP_ID}
          minio:
            environment:
              ARROW_APP_SECRET: ${ARROW_APP_SECRET}
        """
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "docker-compose.yml"
            p.write_text(textwrap.dedent(yml))
            carriers = L9.arrow_env_carriers(p)
        outside = sorted(set(carriers) - L9.ARROW_CARRIERS_ALLOWED)
        self.assertEqual(outside, ["minio"], "a new credential carrier must be reported")
        self.assertIn("no service wires ARROW_*", MODULES["l9"].read_text(),
                      "the guard must also fail when NO service carries it")

    def test_empty_compose_yields_no_carriers(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "docker-compose.yml"
            p.write_text("services:\n  zookeeper:\n    image: zookeeper\n")
            self.assertEqual(L9.arrow_env_carriers(p), {})


class HeapPolicyTest(unittest.TestCase):
    """P6-601: the compose must not pin a heap; comments may quote history."""

    def test_live_heap_pin_detected_comments_ignored(self):
        self.assertEqual(L11.heap_pinned_in_compose("    mem_limit: 4g\n"), [])
        self.assertEqual(len(L11.heap_pinned_in_compose("      - -Xmx2g\n")), 1)
        self.assertEqual(len(L11.heap_pinned_in_compose('      JAVA_TOOL_OPTIONS: "-XX:MaxRAMPercentage=65"\n')), 1)
        self.assertEqual(L11.heap_pinned_in_compose("#   observed -Xmx1658930959, which OOM'd\n"), [],
                         "a comment must not count as wiring")

    def test_real_compose_pins_no_heap(self):
        self.assertEqual(L11.heap_pinned_in_compose(COMPOSE.read_text()), [])


class DocPinTest(unittest.TestCase):
    """P6-602/P6-803: numbers pinned where they live, not as loose digits or prose."""

    def setUp(self):
        self.doc = DOC.read_text()

    def test_config_rows_pair_key_with_value(self):
        for key, val in (("JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT", "65"),
                         ("NON_HEAP_MEMORY_RESERVE_PERCENT", "35"),
                         ("CONTAINER_MEMORY_ALERT_PERCENT", "85")):
            self.assertTrue(L11.config_row(self.doc, key, val), f"real doc row missing: {key}={val}")
            self.assertFalse(L11.config_row(self.doc, key, "999"), f"a wrong value passed for {key}")

    def test_perf_sections_are_addressable(self):
        for name, tokens in (("PERF-001", ["1k"]), ("PERF-002", ["5k"]),
                             ("PERF-003", ["10k", "no drops"]), ("PERF-004", ["10k"]),
                             ("PERF-005", ["2k", "10k", "20k"])):
            sec = L11.perf_section(self.doc, name)
            self.assertTrue(sec, f"{name}: section not found")
            for tok in tokens:
                self.assertIn(tok, sec, f"{name}: {tok} missing from its own section")

    def test_a_missing_section_is_detected_even_when_the_number_is_elsewhere(self):
        """The old global-substring form could not see a deleted section at all."""
        stripped = re.sub(r"^#### PERF-005\b.*?(?=^#### |\Z)", "", self.doc, flags=re.M | re.S)
        self.assertEqual(L11.perf_section(stripped, "PERF-005"), "")
        # "10k" still lives in PERF-003, so the old global-substring form would have
        # passed with the whole PERF-005 section deleted; the section-scoped form cannot.
        self.assertIn("10k", stripped, "the token must survive elsewhere for this to prove anything")
        self.assertNotIn("20k", stripped)


class HarnessDiagnosticTest(unittest.TestCase):
    """P6-599/P6-600: the harness's own output survives, and nothing tests the exec bit."""

    def _fixture(self, body):
        import tempfile
        d = tempfile.mkdtemp()
        p = Path(d) / "fixture_smoke.py"
        p.write_text(textwrap.dedent(body))
        return p

    def test_failing_harness_reports_its_own_output(self):
        fixture = self._fixture('''
            import sys
            print("FAIL [offline]: instrument 7 produced no tick")
            sys.exit(1)
        ''')
        with self.assertRaises(AssertionError) as ctx:
            L10.LocalInt004Test().run_harness("--offline", harness=fixture)
        self.assertIn("FAIL [offline]: instrument 7 produced no tick", str(ctx.exception))
        self.assertIn("exited 1", str(ctx.exception))

    def test_passing_harness_output_is_returned(self):
        fixture = self._fixture('''
            print("PASS LOCAL-INT-004 [offline]: 10 instruments")
        ''')
        out = L10.LocalInt004Test().run_harness("--offline", harness=fixture)
        self.assertIn("PASS LOCAL-INT-004", out)

    def test_exec_bit_is_no_longer_asserted(self):
        src = MODULES["l10"].read_text()
        self.assertNotIn("st_mode & 0o111", src, "the exec-bit check is back")
        self.assertIn("spec_from_file_location", src, "the importability check is gone")


class CargoGuardTest(unittest.TestCase):
    """P6-608/P6-813: every Rust invocation is capped, and l8 degrades without cargo."""

    def test_timeouts_and_toolchain_guard(self):
        l6_l7 = MODULES["l6_l7"].read_text()
        self.assertIn("timeout=300", l6_l7)
        self.assertIn('shutil.which("cargo")', l6_l7)
        self.assertIn("assertIsNotNone(cargo", l6_l7)
        l8 = MODULES["l8"].read_text()
        self.assertIn("timeout=600", l8)
        self.assertIn('shutil.which("cargo")', l8)
        self.assertIn("skipTest", l8)


class DeadCodeTest(unittest.TestCase):
    """P6-808/809/810/811/812: nothing imported, assigned or read in vain."""

    def test_no_unused_or_duplicated_imports(self):
        for key, path in MODULES.items():
            src = path.read_text()
            tree = ast.parse(src)
            names = imported_names(tree)
            used = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
            used |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
            used |= {n.value.id for n in ast.walk(tree)
                     if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)}
            unused = sorted(n for n in names if n not in used and n not in src.split("import")[0])
            self.assertEqual(unused, [], f"{key}: unused import(s) {unused}")
            # a nested import of a module-level name masks the top-level one
            top = module_level_imports(tree)
            for fn in [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]:
                nested = imported_names(fn)
                dup = sorted(set(nested) & set(top))
                self.assertEqual(dup, [], f"{key}: {fn.name}() re-imports module-level {dup}")

    def test_the_dead_assignments_stay_deleted(self):
        l4 = MODULES["l4"].read_text()
        self.assertNotIn("MANIFEST", l4, "the unused MANIFEST constant is back")
        self.assertNotIn('text = (ROOT / "code/02_services/01_ingestion").rglob', l4,
                         "the unused rglob is back")
        self.assertNotIn("compose_text", l4, "the unused compose read is back")
        self.assertIn("installs exactly Signal and Babysitter jobs", l4,
                      "JOB-001 must assert the spec sentence, not the id")


if __name__ == "__main__":
    unittest.main()
