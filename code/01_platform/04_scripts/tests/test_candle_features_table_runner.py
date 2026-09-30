"""Tests for candle-features-table.sh (argv contract + the stubbed-docker traps; live is opt-in).

The live run creates the dev scratch table for the merged candle+feature table
(Wave B/DEC-059, CHG-474) — the same operator-approved scratch route CHG-350 used
for feature_values: the DDL 35 file stays an unapplied proposal. Mirrors
test_feature_values_table_runner.py because the same two traps bite: a network
name that does not match <compose project>_<network>, and a mktemp build dir
(0700) the container's non-root user cannot traverse.
"""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "candle-features-table.sh")
PROBE = os.path.join(os.path.dirname(HERE), "fluss-probes", "CandleFeaturesTableProbe.java")
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

    def test_non_numeric_rows_is_usage_error(self):
        r = run("--rows", "abc")
        self.assertEqual(r.returncode, 2)

    def test_unknown_flag_is_usage_error(self):
        r = run("--nope")
        self.assertEqual(r.returncode, 2)

    def test_probe_source_pins_the_ddl_35_shape(self):
        self.assertTrue(os.path.exists(PROBE), PROBE)
        text = open(PROBE, encoding="utf-8").read()
        # the runner compiles exactly this file name, which must match the public class
        self.assertIn("class CandleFeaturesTableProbe", text)
        # DDL 35 (proposal) shape: candle_closed's 15 columns + features + sealed
        self.assertIn("DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE())", text)
        self.assertIn("DataTypes.BOOLEAN()", text)
        self.assertIn('"sealed"', text)
        self.assertIn('primaryKey("instrument_token", "tf", "window_start")', text)
        self.assertIn('distributedBy(16, "instrument_token")', text)
        self.assertIn('property("table.log.ttl", "3d")', text)
        self.assertIn('property("table.kv.format-version", "2")', text)
        self.assertIn('property("table.datalake.enabled", "false")', text)
        # the four modes the runner may ask for
        for mode in ("create", "describe", "tail", "upsert-check"):
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
            shutil.copy(PROBE, os.path.join(d, "CandleFeaturesTableProbe.java"))
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt"),
                      encoding="utf-8").read().strip()
            r = subprocess.run(["javac", "-nowarn", "-cp", cp, "-d", d,
                                os.path.join(d, "CandleFeaturesTableProbe.java")],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


class StubbedDocker(unittest.TestCase):
    """Runs the runner against a docker stub: pins the mount mode and the argv shape,
    neither of which needs a cluster."""

    def test_tail_run_passes_mode_and_table_and_mounts_readable(self):
        with tempfile.TemporaryDirectory() as td:
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
                    '    printf "CANDLE-FEATURES-TABLE rows=7 sealed=3\\n"\n'
                    '    ;;\n'
                    'esac\n'
                    'exit 0\n')
            os.chmod(stub, 0o755)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            r = subprocess.run(["bash", SCRIPT, "--mode", "tail", "--rows", "3"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            self.assertIn("CANDLE-FEATURES-TABLE rows=7 sealed=3", r.stdout)

            observed = open(log, encoding="utf-8").read()
            argv = [line for line in observed.splitlines() if line.startswith("argv ")][0]
            self.assertIn("CandleFeaturesTableProbe", argv)
            self.assertIn("--mode tail", argv)
            self.assertIn("--rows 3", argv)
            self.assertIn("--database default", argv)
            self.assertIn("--table candle_features", argv)
            self.assertIn("/tmp/probe:/app/ingestion.jar", argv)

            mode = int([line.split(" ", 1)[1] for line in observed.splitlines()
                        if line.startswith("mount_mode ")][0], 8)
            self.assertTrue(mode & stat.S_IROTH and mode & stat.S_IXOTH,
                            "container uid must be able to traverse the mounted build dir")
            classes = int([line.split(" ", 1)[1] for line in observed.splitlines()
                           if line.startswith("mount_classes ")][0])
            self.assertGreater(classes, 0, "probe was not compiled into the build dir")


if __name__ == "__main__":
    unittest.main()
