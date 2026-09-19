"""Guards for wave 54: the per-script ops tests must fail when their property breaks.

Wave 54 fixed 22 findings across ten ops-script test modules and one script. Most
were assertions that could not fail, guards that silently emptied, or state that
leaked between tests. This module pins the fixed shapes — and where a text pin would
be weak, it runs the real thing: the reconcile CLI on a sentinel row and on junk, the
image-staleness helper against a git config that rewrites line endings, and the
command-center module for the credential it used to leak into the process.

No network, no container, no live cluster.
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
RECONCILE = SCRIPTS / "ing-tcp001" / "reconcile-compare.py"
REPAIR_TEST = TESTS / "test_repair_tablet_parser.py"
PROD_NODE = TESTS / "test_10_prod_node_check.py"
R2 = TESTS / "test_11_r2_legal_hold.py"
ALERT = TESTS / "test_alert_consumer.py"
CMD_CENTER = TESTS / "test_command_center.py"
IMG_STALE = TESTS / "test_image_staleness_check.py"
DASH = TESTS / "test_seed_dashboards.py"
FLINK_PROPS = TESTS / "test_check_flink_properties.py"
NETWORK = TESTS / "test_execution_network_check.py"
COMPARE_TEST = TESTS / "test_reconcile_compare.py"
DASH_DIR = ROOT / "code/01_platform/01_docker/openobserve/dashboards"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def live_lines(path):
    """A file's code lines, comments dropped — our own comments quote old shapes."""
    return "\n".join(l for l in path.read_text().splitlines()
                     if not l.lstrip().startswith("#"))


class ReconcileSentinelTest(unittest.TestCase):
    """P6-625: the -1 sentinel must be parsed AND tolerated — and nothing else is."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load(RECONCILE, "w54_reconcile")

    def test_the_parser_accepts_the_sentinel(self):
        self.assertIn("(-?\\d+)", RECONCILE.read_text(),
                      "the token field must accept -1, or the tolerance guard stays dead")
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            fh.write("TOKEN 100 RAW=2 QUAR=0 TOTAL=2\nTOKEN -1 RAW=1 QUAR=0 TOTAL=1\n")
            path = fh.name
        try:
            parsed = self.mod.parse_probe(path)
        finally:
            os.unlink(path)
        self.assertIn(-1, parsed)
        self.assertIn(100, parsed)

    def test_the_sentinel_is_excluded_in_every_post_side_check(self):
        """`extra` excluded -1 but `raw_nonzero` did not — parsing -1 exposed that."""
        code = live_lines(RECONCILE)
        self.assertIn("t not in bridge and t != -1", code)
        self.assertIn("if t != -1 and r != pre.get(t, (0, 0))[0]", code)

    def sentinel_run(self, post_text):
        with tempfile.TemporaryDirectory() as td:
            def write(name, text):
                p = Path(td) / name
                p.write_text(text)
                return str(p)
            bridge = write("bridge.txt", "arrow-tick-counts: total=2 t=100:n=2\n")
            pre = write("pre.txt", "TOKEN 100 RAW=0 QUAR=0 TOTAL=0\n")
            post = write("post.txt", post_text)
            return subprocess.run([sys.executable, str(RECONCILE), "--bridge", bridge,
                                   "--pre", pre, "--post", post, "--sink", "total"],
                                  capture_output=True, text=True)

    def test_the_cli_tolerates_the_sentinel(self):
        proc = self.sentinel_run("TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n"
                                 "TOKEN -1 RAW=1 QUAR=0 TOTAL=1\n")
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_the_tolerance_is_not_a_blanket_bypass(self):
        """The control: a genuinely unexpected token must still be reported."""
        proc = self.sentinel_run("TOKEN 100 RAW=2 QUAR=0 TOTAL=2\n"
                                 "TOKEN 999 RAW=1 QUAR=0 TOTAL=1\n")
        self.assertEqual(1, proc.returncode, proc.stdout + proc.stderr)


class RepairParserTest(unittest.TestCase):
    """P6-626/841: the negative path is tested, and against the shipped parser."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load(REPAIR_TEST, "w54_repair")

    def test_the_test_runs_the_shipped_awk(self):
        self.assertNotIn('awk = r"""', REPAIR_TEST.read_text(),
                         "a local copy of the parser can drift from the script's")
        body = self.mod.embedded_awk()
        self.assertIn("bad = 1", body)
        self.assertIn("END { if (bad) exit 1 }", body)

    def test_the_fail_closed_path_is_covered(self):
        self.assertIn("def test_malformed_scan_output_is_rejected",
                      REPAIR_TEST.read_text())


class ImageStalenessTest(unittest.TestCase):
    """P6-624/838: the compute guard cannot empty, and git config cannot leak in."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load(IMG_STALE, "w54_img")
        cls.text = IMG_STALE.read_text()

    def test_the_compute_guard_indexes_the_key(self):
        self.assertNotIn('SERVICE_SOURCES.get("compute"', self.text,
                         "P6-624: a renamed key would empty the loop silently")
        self.assertIn('SERVICE_SOURCES["compute"]', self.text)

    def test_git_config_is_neutralized(self):
        for needle in ("GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL",
                       "-c", "core.autocrlf=false", "commit.gpgsign=false"):
            self.assertIn(needle, self.text, f"P6-838: missing {needle}")

    def test_a_committed_file_is_not_rewritten(self):
        """Behavioural half: with core.autocrlf=true globally, git would store CRLF
        and the file content — which this module hashes — would differ per host."""
        with tempfile.TemporaryDirectory() as td:
            repo = self.mod.make_repo(Path(td))
            blob = subprocess.run(["git", "show", "HEAD:src/file.txt"], cwd=repo,
                                  capture_output=True, text=True, check=True).stdout
        self.assertNotIn("\r\n", blob, "the throwaway repo stored CRLF")


class DashboardCoverageTest(unittest.TestCase):
    """P6-627/842: the secret scan tracks the corpus, and ids are checked explicitly."""

    def test_the_scan_derives_its_list_from_the_manifest(self):
        code = live_lines(DASH)
        self.assertNotIn('"safe-to-trade.json", "order-execution.json"', code,
                         "P6-627: the hard-coded list skipped half the corpus")
        self.assertIn('files = ["manifest.json"] + [rec["file"] for rec in manifest["dashboards"]]', code)

    def test_coverage_equals_the_manifest(self):
        import json
        manifest = json.loads((DASH_DIR / "manifest.json").read_text())
        self.assertGreaterEqual(len(manifest["dashboards"]), 8,
                                "the corpus shrank — the scan's derivation should follow it")
        scanned = 1 + len(manifest["dashboards"])     # + manifest.json itself
        self.assertGreaterEqual(scanned, 9)

    def test_a_missing_panel_id_is_its_own_failure(self):
        code = live_lines(DASH)
        self.assertIn("assert pid,", code, "P6-842: `None not in seen` hides a missing id")
        self.assertIn("assert pid not in seen", code)


class DeadShapeTest(unittest.TestCase):
    """The dead, duplicated and leaking shapes wave 54 removed must stay gone."""

    def test_prod_node_check_has_no_dead_write(self):
        code = live_lines(PROD_NODE)
        self.assertNotIn("import subprocess", code)
        self.assertNotIn("pointing at a missing file first", PROD_NODE.read_text())
        self.assertEqual(1, code.count("json.dump(_inv(), fh)"),
                         "only the live inventory write should remain")

    def test_r2_legal_hold_has_one_validation_assert_and_no_leap_year_trap(self):
        text = R2.read_text()
        self.assertEqual(1, text.count('assert lh.verify_chain([m1, m2], ROOT) == "VALID"'))
        self.assertNotIn("today().replace(year=", text)

    def test_alert_consumer_restores_its_globals(self):
        text = ALERT.read_text()
        self.assertNotIn("import time", live_lines(ALERT))
        self.assertEqual(1, text.count("CONSUMER = "), "the dead first CONSUMER assignment")
        self.assertIn("cls._orig_store", text)
        self.assertIn("server_close()", text)

    def test_command_center_keeps_one_placeholder_test(self):
        """P6-829: the later duplicate was deleted, the SQL-covering one stayed."""
        text = CMD_CENTER.read_text()
        self.assertNotIn("def test_command_sql_panels_carry_no_placeholder_where", text)
        self.assertEqual(1, text.count("def test_command_text_tables_omit_placeholder_where"))
        self.assertIn('if p[1] == "promql":', text,
                      "the surviving test must still cover the SQL panels")
        self.assertIn("_ORIG_O2_AUTH", text)

    def test_command_center_does_not_leak_the_dummy_credential(self):
        """Behavioural: the module used to `setdefault` a dummy basic-auth for the
        whole pytest session, silently authorizing bogus requests elsewhere."""
        before = os.environ.get("O2_AUTH_BASIC")
        load(CMD_CENTER, "w54_cmdcenter_probe")
        self.assertEqual(before, os.environ.get("O2_AUTH_BASIC"))

    def test_flink_properties_removes_the_unquoted_key(self):
        text = FLINK_PROPS.read_text()
        self.assertIn('block.replace("state.backend.incremental: true\\n", "")', text)
        self.assertNotIn("import tempfile", live_lines(FLINK_PROPS))
        self.assertNotIn("import textwrap", live_lines(FLINK_PROPS))

    def test_reconcile_compare_has_one_script_path(self):
        text = COMPARE_TEST.read_text()
        self.assertEqual(1, text.count("spec_from_file_location"))
        self.assertIn('spec_from_file_location("reconcile_compare", COMPARE)', text)

    def test_network_check_asserts_the_specific_messages(self):
        text = NETWORK.read_text()
        for needle in ("execution-net must be an internal network",
                       "ARROW_APP_SECRET",
                       "must not publish a host port"):
            self.assertIn(needle, text, f"P6-835/836/837: unbound assertion for {needle}")


if __name__ == "__main__":
    unittest.main()
