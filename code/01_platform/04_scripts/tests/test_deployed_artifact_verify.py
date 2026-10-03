"""Tests for deployed_artifact_verify.py (argv contract + inventory semantics).

Runnable with pytest or unittest. Tests that need docker or javap skip
themselves rather than failing, so the suite stays honest on a machine that
cannot run them.
"""
import importlib.util
import os
import shutil
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "deployed_artifact_verify.py")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))


def load_module():
    spec = importlib.util.spec_from_file_location("image_jar_verify", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run_tool(*args):
    return subprocess.run([sys.executable, SCRIPT] + list(args),
                          capture_output=True, text=True)


class ExtractCleanup(unittest.TestCase):
    def test_cp_failure_leaves_no_temp_dir(self):
        """XC-18: the docker-cp failure path must still clean its temp dir."""
        mod = load_module()
        created = []
        real_mkdtemp = mod.tempfile.mkdtemp

        def tracking_mkdtemp(*a, **k):
            d = real_mkdtemp(*a, **k)
            created.append(d)
            return d

        def fake_run(cmd):
            if cmd[:2] == ["docker", "create"]:
                return subprocess.CompletedProcess(cmd, 0, "cid123\n", "")
            return subprocess.CompletedProcess(cmd, 1, "", "boom")

        with unittest.mock.patch.object(mod, "run", fake_run), \
                unittest.mock.patch.object(mod.tempfile, "mkdtemp", tracking_mkdtemp):
            got, err = mod.extract_from_image("img:tag", "/x/y")
        self.assertIsNone(got)
        self.assertIn("docker cp", err)
        self.assertTrue(created, "the function must create a temp dir")
        for d in created:
            self.assertFalse(os.path.exists(d), f"leaked temp dir {d}")


class ArgsContract(unittest.TestCase):
    def test_list_checks_needs_no_docker(self):
        r = run_tool("--list-checks")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("ingestion", r.stdout)
        self.assertIn("compute", r.stdout)

    def test_help_exits_zero(self):
        r = run_tool("--help")
        self.assertEqual(r.returncode, 0)
        self.assertIn("usage:", r.stdout)

    def test_missing_target_is_usage_error(self):
        r = run_tool("--image", "whatever")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_bad_target_is_usage_error(self):
        r = run_tool("--target", "nope", "--image", "x")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_no_source_is_usage_error(self):
        r = run_tool("--target", "ingestion")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_unknown_flag_is_usage_error(self):
        r = run_tool("--target", "ingestion", "--image", "x", "--bogus")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_absent_image_is_skip_not_fail(self):
        """A fresh checkout must still be able to run `make up`: an image that is
        not here yet has nothing to verify and must not block the bring-up."""
        r = run_tool("--target", "ingestion", "--image", "nonexistent-artifact:does-not-exist")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("[SKIP]", r.stdout)
        self.assertIn("not present on this host", r.stdout)

    def test_makefile_wires_the_check_before_the_recreate(self):
        """The check is only useful if the restart path calls it, before the recreate."""
        text = open(os.path.join(ROOT, "Makefile"), encoding="utf-8").read()
        self.assertIn("deployed_artifact_verify.py", text)
        self.assertIn("--target ingestion --image 01_docker-ingestion:latest", text)
        parts = text.split("\nup:\n", 1)
        self.assertEqual(len(parts), 2, "no up: target found")
        body = parts[1].split("\n\ndown:", 1)[0]
        self.assertLess(body.index("deployed_artifact_verify.py"), body.index("up -d"),
                        "the artifact check must run before the recreate")

    def test_positional_args_are_rejected(self):
        r = run_tool("ingestion", "01_docker-ingestion:latest")
        self.assertEqual(r.returncode, 2)


class InventorySemantics(unittest.TestCase):
    def setUp(self):
        self.mod = load_module()

    def test_tick_type_check_requires_absence_of_old_rule(self):
        """The whole point of the tool: VALID_NON_TRADE must be required ABSENT."""
        specs = self.mod.CLASS_CHECKS["RealFlussRowConverter tick_type"]
        self.assertIn(("com/trading/ingestion/RealFlussRowConverter.class", "disasm", "VALID_TRADE", True), specs)
        self.assertIn(("com/trading/ingestion/RealFlussRowConverter.class", "disasm", "VALID_NON_TRADE", False), specs)

    def test_ingestion_inventory_has_no_compute_owned_class(self):
        """RawTableColumns lives in the compute module; promising it on the
        ingestion image would always report a vacuous SKIP."""
        for target, name, _source, _kind, _what in self.mod.CHECKS:
            if target == "ingestion":
                self.assertNotIn("RawTableColumns", name)

    def test_compute_checks_split_launcher_from_jar(self):
        """The compute image carries no jar, so its checks must name a host jar."""
        sources = {name: source for target, name, source, _k, _w in self.mod.CHECKS if target == "compute"}
        self.assertEqual(sources.get("launcher idempotency"), "image-script")
        self.assertTrue(any(s == "host-jar" for s in sources.values()))

    def test_script_markers_match_the_landed_fix(self):
        markers = self.mod.SCRIPT_CHECKS["launcher idempotency"]
        self.assertIn("job_already_running", markers)
        self.assertIn('.state=="RUNNING"', markers)


class BehaviourWithoutDocker(unittest.TestCase):
    def test_unreadable_jar_is_a_failure_not_a_crash(self):
        """A real failure path with no docker involved: point --jar at a text file."""
        bad = os.path.join(ROOT, "code", "01_platform", "04_scripts", "deployed_artifact_verify.py")
        r = run_tool("--target", "compute", "--jar", bad)
        self.assertEqual(r.returncode, 1)
        self.assertIn("FAIL", r.stdout)


@unittest.skipUnless(shutil.which("docker") and shutil.which("javap"), "needs docker and javap")
class LiveArtifacts(unittest.TestCase):
    def _image_present(self, ref):
        r = subprocess.run(["docker", "image", "inspect", ref], capture_output=True, text=True)
        return r.returncode == 0

    def test_live_ingestion_image(self):
        ref = "01_docker-ingestion:latest"
        if not self._image_present(ref):
            self.skipTest("%s not built here" % ref)
        r = run_tool("--target", "ingestion", "--image", ref)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("[OK  ]", r.stdout)

    def test_live_compute_launcher(self):
        ref = "01_docker-compute:latest"
        if not self._image_present(ref):
            self.skipTest("%s not built here" % ref)
        r = run_tool("--target", "compute", "--image", ref)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("launcher idempotency", r.stdout)


if __name__ == "__main__":
    unittest.main()
