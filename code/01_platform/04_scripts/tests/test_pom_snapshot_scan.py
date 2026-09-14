#!/usr/bin/env python3
"""Tests for pom-snapshot-scan.py (P6-012, P6-484, P6-762).

The gate is loaded by path (its filename has dashes) and driven through
main(), with ROOT pointed at throwaway trees.
"""

import contextlib
import importlib.util
import io
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS = TESTS_DIR.parent
REPO_ROOT = SCRIPTS.parents[2]
SCRIPT = SCRIPTS / "pom-snapshot-scan.py"


def load_module():
    spec = importlib.util.spec_from_file_location("pom_snapshot_scan_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = load_module()

EXTERNAL_SNAPSHOT = """<project>
  <dependencies>
    <dependency>
      <groupId>com.external</groupId>
      <artifactId>lib</artifactId>
      <version>2.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
</project>
"""

WORKSPACE_SNAPSHOT = """<project>
  <dependencies>
    <dependency>
      <groupId>com.trading</groupId>
      <artifactId>common</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
</project>
"""

PLUGIN_SNAPSHOT = """<project>
  <build>
    <plugins>
      <plugin>
        <groupId>com.external</groupId>
        <artifactId>plug</artifactId>
        <version>9.9-SNAPSHOT</version>
      </plugin>
    </plugins>
  </build>
</project>
"""

PARENT_SNAPSHOT = """<project>
  <parent>
    <groupId>com.external</groupId>
    <artifactId>par</artifactId>
    <version>3.0-SNAPSHOT</version>
  </parent>
</project>
"""


class ScanHarness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.tree = pathlib.Path(self.tmp.name)
        original = mod.ROOT
        self.addCleanup(setattr, mod, "ROOT", original)

    def write_pom(self, name, xml):
        directory = self.tree / "code" / name
        directory.mkdir(parents=True, exist_ok=True)
        pom = directory / "pom.xml"
        pom.write_text(xml)
        return pom

    def run_main(self, root=None):
        if root is not None:
            mod.ROOT = str(root)
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = mod.main()
        return rc, out.getvalue(), err.getvalue()


class RootResolution(ScanHarness):
    def test_root_is_the_repository_root(self):
        """P6-012: three dirnames resolved to <repo>/code, so the glob found nothing."""
        self.assertEqual(pathlib.Path(mod.ROOT).resolve(), REPO_ROOT.resolve())

    def test_the_scan_finds_the_workspace_poms(self):
        """P6-012: the gate must actually see code/**/pom.xml."""
        self.assertGreaterEqual(len(mod.find_poms(mod.ROOT)), 6)

    def test_the_real_tree_is_clean_and_reports_its_coverage(self):
        rc, out, err = self.run_main()
        self.assertEqual(rc, 0, err)
        self.assertIn("no external SNAPSHOT dependencies", out)
        self.assertIn("pom.xml scanned", out)


class SnapshotRules(ScanHarness):
    def test_an_external_snapshot_dependency_fails(self):
        self.write_pom("svc", EXTERNAL_SNAPSHOT)
        rc, out, _ = self.run_main(self.tree)
        self.assertEqual(rc, 1)
        self.assertIn("com.external:2.0-SNAPSHOT", out)

    def test_the_workspace_snapshot_is_allowed(self):
        self.write_pom("svc", WORKSPACE_SNAPSHOT)
        rc, out, _ = self.run_main(self.tree)
        self.assertEqual(rc, 0, out)

    def test_a_snapshot_plugin_is_not_a_dependency(self):
        """P6-484: the loose match reported <plugin> pins as dependencies."""
        xml = PLUGIN_SNAPSHOT
        self.assertFalse(mod.DEP_RE.search(xml))
        self.write_pom("svc", xml)
        rc, out, _ = self.run_main(self.tree)
        self.assertEqual(rc, 0, out)

    def test_a_snapshot_parent_is_a_dependency(self):
        self.write_pom("svc", PARENT_SNAPSHOT)
        rc, out, _ = self.run_main(self.tree)
        self.assertEqual(rc, 1)
        self.assertIn("com.external:3.0-SNAPSHOT", out)

    def test_a_tree_without_poms_fails_closed(self):
        """P6-012: an empty glob must not read as a clean workspace."""
        (self.tree / "code").mkdir()
        rc, _, err = self.run_main(self.tree)
        self.assertEqual(rc, 2)
        self.assertIn("the scan verified nothing", err)

    def test_an_unreadable_pom_makes_the_result_inconclusive(self):
        """P6-762: read errors were silently skipped, so the gate still passed."""
        self.write_pom("svc", WORKSPACE_SNAPSHOT)
        (self.tree / "code" / "broken" / "pom.xml").mkdir(parents=True)
        rc, _, err = self.run_main(self.tree)
        self.assertEqual(rc, 2)
        self.assertIn("WARN: cannot read", err)
        self.assertIn("inconclusive", err)


class ScanRootSeam(ScanHarness):
    def test_pom_scan_root_points_the_gate_at_another_tree(self):
        """The documented test seam, exercised end to end."""
        self.write_pom("svc", EXTERNAL_SNAPSHOT)
        env = dict(os.environ, POM_SCAN_ROOT=str(self.tree))
        done = subprocess.run(
            [sys.executable, str(SCRIPT)],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn("com.external:2.0-SNAPSHOT", done.stdout)


if __name__ == "__main__":
    unittest.main()
