"""Tests for legacy-candle-tables-drop.sh (argv contract + stubbed-docker traps; live is opt-in).

The runner drops the three retired candle/feature tables on dev after the Wave C
cutover (CHG-481): W-C6 proved they receive zero rows while the merged writer runs.
The traps mirror test_candle_features_table_runner.py: a network name that does not
match <compose project>_<network>, and a mktemp build dir (0700) the container's
non-root user cannot traverse. The confirm guard is the safety seam: a drop without
--confirm must fail before any container starts.
"""
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "legacy-candle-tables-drop.sh")
PROBE = os.path.join(os.path.dirname(HERE), "fluss-probes", "LegacyTableDropProbe.java")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))


def run(*args):
    return subprocess.run(["bash", SCRIPT] + list(args), capture_output=True, text=True)


class ArgvContract(unittest.TestCase):
    def test_syntax(self):
        r = subprocess.run(["bash", "-n", SCRIPT], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_missing_value_is_usage_error(self):
        r = run("--mode")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_unknown_mode_is_usage_error(self):
        r = run("--mode", "nope")
        self.assertEqual(r.returncode, 2)

    def test_unknown_flag_is_usage_error(self):
        r = run("--nope")
        self.assertEqual(r.returncode, 2)

    def test_drop_without_confirm_refuses_before_any_container(self):
        r = run("--mode", "drop")
        self.assertEqual(r.returncode, 3)
        self.assertIn("DROP-REFUSED", r.stderr)

    def test_probe_source_pins_the_fixed_targets_and_confirm_guard(self):
        self.assertTrue(os.path.exists(PROBE), PROBE)
        text = open(PROBE, encoding="utf-8").read()
        self.assertIn("class LegacyTableDropProbe", text)
        # fixed target set -- never parameterized
        for target in ("candle_live", "candle_closed", "feature_values"):
            self.assertIn('"%s"' % target, text, target)
        self.assertNotIn('"--table"', text, "targets must not be parameterized")
        # the confirm guard and the drop call
        self.assertIn("--confirm", text)
        self.assertIn("dropTable(path, false)", text)
        for mode in ("list", "drop"):
            self.assertIn('"%s"' % mode, text, mode)

    def test_default_network_matches_the_compose_project_and_network(self):
        compose_dir = os.path.join(ROOT, "code", "01_platform", "01_docker")
        project = os.path.basename(compose_dir)
        compose = open(os.path.join(compose_dir, "docker-compose.yml"), encoding="utf-8").read()
        self.assertIn("trading-net:", compose, "compose no longer defines trading-net")
        expected = "%s_trading-net" % project
        runner = open(SCRIPT, encoding="utf-8").read()
        self.assertIn('NETWORK="${NETWORK:-%s}"' % expected, runner,
                      "runner default network must be %s" % expected)

    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs a JDK and a built ingestion classpath")
    def test_probe_compiles(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copy(PROBE, os.path.join(d, "LegacyTableDropProbe.java"))
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt"),
                      encoding="utf-8").read().strip()
            r = subprocess.run(["javac", "-nowarn", "-cp", cp, "-d", d,
                                os.path.join(d, "LegacyTableDropProbe.java")],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


class StubbedDocker(unittest.TestCase):
    """Runs the runner against a docker stub: pins the mount mode and the argv shape,
    neither of which needs a cluster."""

    def _stub(self, td):
        stub = os.path.join(td, "docker")
        log = os.path.join(td, "docker-observed.txt")
        with open(stub, "w", encoding="utf-8") as fh:
            fh.write(
                '#!/usr/bin/env bash\n'
                'set -u\n'
                'printf "argv %s\\n" "$*" >> "$DOCKER_STUB_LOG"\n'
                'case "$1" in\n'
                '  run)\n'
                '    src=""\n'
                '    for a in "$@"; do\n'
                '      case "$a" in *:/tmp/probe:ro) src="${a%%:/tmp/probe:ro}" ;; esac\n'
                '    done\n'
                '    printf "mount_mode %s\\n" "$(stat -c %a "$src")" >> "$DOCKER_STUB_LOG"\n'
                '    printf "mount_classes %s\\n" "$(ls -1 "$src" | grep -c "\\.class$")"'
                ' >> "$DOCKER_STUB_LOG"\n'
                '    printf "LEGACY-DROP list=candle_live absent\\n"\n'
                '    ;;\n'
                'esac\n'
                'exit 0\n')
        os.chmod(stub, 0o755)
        return stub, log

    def test_list_run_passes_mode_and_mounts_readable(self):
        with tempfile.TemporaryDirectory() as td:
            stub, log = self._stub(td)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            r = subprocess.run(["bash", SCRIPT, "--mode", "list"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("LEGACY-DROP list=candle_live absent", r.stdout)
            observed = open(log, encoding="utf-8").read()
            argv = [line for line in observed.splitlines() if line.startswith("argv ")][0]
            self.assertIn("LegacyTableDropProbe", argv)
            self.assertIn("--mode list", argv)
            self.assertNotIn("--confirm", argv)
            self.assertIn("mount_mode 755", observed)
            self.assertNotIn("mount_classes 0", observed)

    def test_drop_run_passes_confirm(self):
        with tempfile.TemporaryDirectory() as td:
            stub, log = self._stub(td)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            r = subprocess.run(["bash", SCRIPT, "--mode", "drop", "--confirm"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            observed = open(log, encoding="utf-8").read()
            argv = [line for line in observed.splitlines() if line.startswith("argv ")][0]
            self.assertIn("--mode drop", argv)
            self.assertIn("--confirm", argv)

    def test_refused_drop_never_invokes_docker(self):
        with tempfile.TemporaryDirectory() as td:
            _, log = self._stub(td)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            r = subprocess.run(["bash", SCRIPT, "--mode", "drop"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 3)
            self.assertFalse(os.path.exists(log), "docker must not run on a refused drop")


if __name__ == "__main__":
    unittest.main()
