"""Guards for wave 53: checker tests, versioning and packaging.

Wave 53 made two checkers fail cleanly instead of tracebacking or accepting a
malformed pin (P6-644, P6-846), made a placeholder check match its own documented
marker shape (P6-856), thinned `common`'s dependency surface (P6-645, P6-847), and
removed eight vacuous-or-coupled shapes from the unit tests.

This module keeps all of it honest. Where a text pin would be enough it is used;
where the property is behavioural the mutant runs the real thing — the checkers on
bad input, the affected test modules under a hostile environment, and one test
module under a private TMPDIR to prove the temp-file leak is gone.

Nothing here needs the network, a container or a live cluster.
"""
import ast
import importlib.util
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

TESTS = Path(__file__).resolve().parent
ROOT = TESTS.parents[3]
SCRIPTS = TESTS.parent
VERIFY = SCRIPTS / "version_matrix_verify.py"
CHG_TEST = TESTS / "test_change_control_check.py"
DDL_TEST = TESTS / "test_ddl_apply.py"
EVID_TEST = TESTS / "test_evidence_ownership_check.py"
IMPL_TEST = TESTS / "test_implementation_gate.py"
CAPTURE_TEST = TESTS / "test_stage_capture_parse.py"
STALE_TEST = TESTS / "test_stale_table_kind_scan.py"
G7_TEST = TESTS / "test_holistic_g7_parity.py"
DISASTER_TEST = TESTS / "test_disaster_drills.py"
PLACEHOLDER = ROOT / "code/common/src/main/java/com/trading/common/version/PlaceholderVersions.java"
COMMON_POM = ROOT / "code/common/pom.xml"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def live_lines(path):
    """The file minus comment-only lines."""
    return "\n".join(l for l in path.read_text().splitlines()
                     if not l.lstrip().startswith("#"))


class VersionMatrixVerifyTest(unittest.TestCase):
    """P6-644/846: the checker must fail cleanly, and must reject a non-scalar pin."""

    def run_verify(self, target):
        proc = subprocess.run([sys.executable, str(VERIFY), str(target)],
                              capture_output=True, text=True)
        return proc.returncode, proc.stdout + proc.stderr

    def test_missing_path_fails_cleanly(self):
        rc, out = self.run_verify("/nonexistent-matrix.yaml")
        self.assertEqual(1, rc)
        self.assertIn("cannot parse", out)
        self.assertNotIn("Traceback", out, "a missing file must not produce a traceback (P6-644)")

    def test_directory_fails_cleanly(self):
        with tempfile.TemporaryDirectory() as tmp:
            rc, out = self.run_verify(tmp)
        self.assertEqual(1, rc)
        self.assertIn("cannot parse", out)
        self.assertNotIn("Traceback", out)

    def test_non_utf8_fails_cleanly(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as fh:
            fh.write(b"\xff\xfe\x00bad")
            path = fh.name
        try:
            rc, out = self.run_verify(path)
        finally:
            os.unlink(path)
        self.assertEqual(1, rc)
        self.assertIn("cannot parse", out)
        self.assertNotIn("Traceback", out)

    def test_non_scalar_pin_is_rejected(self):
        """P6-846: `[1.4, 2.0]` used to stringify and pass every pin check."""
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
            fh.write("boundaries:\n"
                     "  - compatibility_id: VM-TEST-001\n"
                     "    proposed_version: [1.4, 2.0]\n")
            path = fh.name
        try:
            rc, out = self.run_verify(path)
        finally:
            os.unlink(path)
        self.assertEqual(1, rc)
        self.assertIn("single pinned scalar", out)

    def test_the_real_matrix_still_verifies(self):
        rc, out = self.run_verify(SCRIPTS / "version_matrix.yaml")
        self.assertEqual(0, rc, out)

    def test_scalar_coercion_is_kept(self):
        """The non-scalar rejection must not undo R-093's scalar coercion."""
        src = VERIFY.read_text()
        self.assertIn("isinstance(row.get(\"proposed_version\"), (list, dict))", src)
        self.assertIn('str(v).strip() if v is not None else ""', src)


class MarkerShapeTest(unittest.TestCase):
    """P6-856: both sentinels are matched in their documented shape (R-268)."""

    def test_placeholder_matches_the_documented_marker(self):
        text = PLACEHOLDER.read_text()
        self.assertIn('v.contains("_TO_BE_PINNED")', text)
        self.assertIn('v.contains("_TO_BE_VERIFIED")', text)
        self.assertNotIn('|| v.contains("TO_BE_VERIFIED")', text,
                         "the bare form is prose, not a pin marker")

    def test_the_java_test_covers_both_forms(self):
        test = (ROOT / "code/common/src/test/java/com/trading/common/version/VersionGateTest.java").read_text()
        self.assertIn('isPlaceholder("SCHEMA_LIFECYCLE_TO_BE_VERIFIED")', test,
                      "the underscore form must stay a placeholder")
        self.assertIn('assertFalse(PlaceholderVersions.isPlaceholder("TO_BE_VERIFIED"))', test,
                      "the bare form must not be a placeholder")


class PackagingTest(unittest.TestCase):
    """P6-645/847: the client is opt-in, the test deps are explicit."""

    @classmethod
    def setUpClass(cls):
        cls.pom = COMMON_POM.read_text()

    def test_fluss_client_is_optional(self):
        block = re.search(r"<artifactId>fluss-client</artifactId>.*?</dependency>",
                          self.pom, re.S)
        self.assertIsNotNone(block)
        self.assertIn("<optional>true</optional>", block.group(0),
                      "P6-645: a future common consumer must opt in to the client")

    def test_test_dependencies_declare_their_scope(self):
        for artifact in ("junit-jupiter", "assertj-core"):
            block = re.search(r"<artifactId>%s</artifactId>.*?</dependency>" % artifact,
                              self.pom, re.S)
            self.assertIsNotNone(block)
            self.assertIn("<scope>test</scope>", block.group(0),
                          f"P6-847: {artifact} must declare its scope explicitly")

    def test_every_fluss_importing_module_declares_the_client(self):
        """The evidence behind P6-645: nothing resolves the client transitively."""
        importers = []
        for pom in sorted(ROOT.glob("code/**/pom.xml")):
            module = pom.parent
            sources = [p for p in module.rglob("*")
                       if p.suffix in (".java", ".xml") and "target" not in p.parts]
            if not any(p.is_file() and "org.apache.fluss" in p.read_text(errors="ignore")
                       for p in sources):
                continue
            if module.name == "code":
                continue
            importers.append(module)
            self.assertIn("fluss-", pom.read_text(),
                          f"{module} imports org.apache.fluss but declares no fluss dependency")
        self.assertTrue(importers, "the sweep found no importing module — it would pass vacuously")


class HermeticReferenceTest(unittest.TestCase):
    """P6-826: the reference cases use fixtures; one test owns the repo coupling."""

    def test_reference_classes_use_the_fixture_base(self):
        tree = ast.parse(CHG_TEST.read_text())
        bases = {n.name: [ast.unparse(b) for b in n.bases]
                 for n in tree.body if isinstance(n, ast.ClassDef)}
        for cls in ("ScanTests", "CliTests", "PlanTasksReferenceTests",
                    "AffectedArtifactsReferenceTests"):
            self.assertIn(cls, bases, f"{cls} disappeared from {CHG_TEST.name}")
            self.assertEqual(["HermeticReferenceTests"], bases[cls],
                             f"{cls} must resolve references against the fixture tree")
        self.assertIn("RepoReferenceIntegrityTests", bases,
                      "the repository-coupling half must exist under its own name")

    def test_the_fixture_tree_is_the_only_repo_dependency(self):
        src = CHG_TEST.read_text()
        self.assertIn("mock.patch.object(ccc, \"ROOT\"", src)
        self.assertIn("ccc._REPO_INDEX = None", src,
                      "the basename index is cached: it must be rebuilt per fixture")

    def test_a_renamed_dossier_does_not_break_the_hermetic_cases(self):
        """Behavioural proof: run the reference cases with the repo's dossier hidden.

        The fixture supplies its own 04-signal-job.md, so renaming the repository's
        copy must not matter here — that is what RepoReferenceIntegrityTests is for.
        """
        sys.path.insert(0, str(TESTS))
        sys.path.insert(0, str(SCRIPTS))
        real = SCRIPTS / "change_control_check.py"
        spec = importlib.util.spec_from_file_location("ccc_probe", real)
        ccc = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ccc)
        with tempfile.TemporaryDirectory() as tree:
            os.makedirs(os.path.join(tree, "docs", "08_implementation"))
            with open(os.path.join(tree, "docs", "08_implementation", "04-renamed.md"), "w") as fh:
                fh.write("# moved\n")
            saved_root, saved_index = ccc.ROOT, ccc._REPO_INDEX
            try:
                ccc.ROOT = tree
                ccc._REPO_INDEX = None
                issues = ccc.plan_task_issues("tracker-04 P10", tree)
            finally:
                ccc.ROOT, ccc._REPO_INDEX = saved_root, saved_index
        self.assertEqual([], issues, "tracker-04 must resolve to any 04-*.md")


class VacuousShapeTest(unittest.TestCase):
    """The shapes wave 53 removed must stay gone."""

    def assert_no_or_true(self, path):
        tree = ast.parse(path.read_text())
        hits = []
        for node in ast.walk(tree):
            tests = ([node.test] if isinstance(node, ast.Assert)
                     else list(node.args) if isinstance(node, ast.Call)
                     and isinstance(getattr(node, "func", None), ast.Attribute)
                     and node.func.attr.startswith("assert") else [])
            for t in tests:
                if (isinstance(t, ast.BoolOp) and isinstance(t.values[-1], ast.Constant)
                        and t.values[-1].value is True):
                    hits.append(node.lineno)
        self.assertEqual([], hits, f"{path.name}: `assert … or True` is unreachable")

    def test_no_always_true_assertion_in_the_touched_modules(self):
        for path in (DISASTER_TEST, DDL_TEST, CHG_TEST):
            self.assert_no_or_true(path)

    def test_the_dead_structural_comment_is_gone(self):
        # Live lines only: the replacement comment quotes the deleted line on purpose.
        self.assertNotIn('assert "log" not in text.lower()', live_lines(DISASTER_TEST))

    def test_stale_scan_probe_uses_a_stale_triple(self):
        # Scoped to the probe function: a sibling test legitimately reads the truth
        # dict to exercise the C6-triple path.
        text = STALE_TEST.read_text()
        tree = ast.parse(text)
        # The scanner's tests live inside a TestCase class, so walk the whole tree.
        fn = next((n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                   and n.name == "test_now_that_is_not_a_live_marker"), None)
        self.assertIsNotNone(fn, "the P6-628 probe test disappeared")
        body = ast.get_source_segment(text, fn) or ""
        # Comments in the body quote the old expression on purpose — compare code only.
        code = "\n".join(l for l in body.splitlines() if not l.lstrip().startswith("#"))
        self.assertNotIn("SUITE_TRIPLE_TRUTH", code,
                         "P6-628: probing with the current truth is suppressed by the filter")
        self.assertIn("340/0/1", code)

    def test_the_probe_reaches_the_classifier(self):
        """Behavioural half: with a stale triple the scan produces a hit whose tier
        is annotated (not LIVE-STALE) — before the fix the hit never existed."""
        mod = load(STALE_TEST, "w53_stale")
        hits = mod.scan_text("now that the suite is common 340/0/1, nothing fires 2026-08-13\n")
        self.assertTrue(hits, "the probe was suppressed — it proves nothing in this state")
        tiers = mod.claim_tiers(hits)
        self.assertNotIn(("live-count-stale", "LIVE-STALE"), tiers)
        self.assertNotIn(("test-count-stale", "LIVE-STALE"), tiers)

    def test_no_dead_helpers_or_duplicate_imports(self):
        self.assertNotIn("def run(args=None)", IMPL_TEST.read_text(), "P6-839")
        capture = CAPTURE_TEST.read_text()
        self.assertNotIn("import re as _re", capture, "P6-843")
        self.assertNotIn("    from pathlib import Path", capture, "P6-843")
        self.assertIn("jars = make_fake_m2(tmp)", DDL_TEST.read_text(),
                      "P6-830: the fixture's return value must be asserted on, not ignored")


class HostileEnvironmentTest(unittest.TestCase):
    """P6-618/834: the two reload-based tests must be immune to ambient overrides."""

    def run_module(self, path, env_extra):
        env = dict(os.environ, **env_extra)
        return subprocess.run([sys.executable, "-m", "pytest", str(path), "-q"],
                              capture_output=True, text=True, env=env, cwd=str(ROOT))

    def test_ddl_apply_test_survives_a_set_override(self):
        proc = self.run_module(DDL_TEST, {"DDL_APPLY_EVIDENCE_DIR": "/custom/evidence"})
        self.assertEqual(0, proc.returncode, proc.stdout[-1500:])

    def test_evidence_ownership_test_survives_ambient_overrides(self):
        proc = self.run_module(EVID_TEST, {"DDL_APPLY_EVIDENCE_DIR": "/custom/evidence",
                                           "DDL_APPLY_UID": "4242",
                                           "DDL_APPLY_GID": "4243"})
        self.assertEqual(0, proc.returncode, proc.stdout[-1500:])


class TempFileLeakTest(unittest.TestCase):
    """P6-623: the cleanup now runs, so the module stops leaking temp files."""

    def test_no_tsv_is_left_behind(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = dict(os.environ, TMPDIR=tmpdir)
            proc = subprocess.run([sys.executable, "-m", "pytest", str(G7_TEST), "-q"],
                                  capture_output=True, text=True, env=env, cwd=str(ROOT))
            self.assertEqual(0, proc.returncode, proc.stdout[-1500:])
            left = [p.name for p in Path(tmpdir).glob("*.tsv")]
        self.assertEqual([], left, f"the module leaked temp files: {left}")


if __name__ == "__main__":
    unittest.main()
