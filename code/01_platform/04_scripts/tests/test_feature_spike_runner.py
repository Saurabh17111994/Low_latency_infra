"""Tests for feature-spike.sh (argv contract + the stubbed-docker traps; the live run is opt-in).

The live run is the DEC-056 MAP-vs-JSON spike against the dev cluster; these tests pin the
runner's contract without a docker daemon, mirroring test_candle_verify_runner.py because the
same two traps bite: a network name that does not match <compose project>_<network>, and a
mktemp build dir (0700) the container's uid 65532 cannot traverse.
"""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "feature-spike.sh")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))


def run(*args):
    return subprocess.run(["bash", SCRIPT] + list(args), capture_output=True, text=True)


class ArgvContract(unittest.TestCase):
    def test_syntax(self):
        r = subprocess.run(["bash", "-n", SCRIPT], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_missing_value_is_usage_error(self):
        r = run("--windows")
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_non_numeric_windows_is_usage_error(self):
        r = run("--windows", "abc")
        self.assertEqual(r.returncode, 2)

    def test_zero_windows_is_usage_error(self):
        r = run("--windows", "0")
        self.assertEqual(r.returncode, 2)

    def test_unknown_flag_is_usage_error(self):
        r = run("--nope")
        self.assertEqual(r.returncode, 2)

    def test_probe_source_is_where_the_runner_expects_it(self):
        probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "FeatureSpikeProbe.java")
        self.assertTrue(os.path.exists(probe), probe)
        text = open(probe, encoding="utf-8").read()
        # the runner compiles exactly this file name, which must match the public class
        self.assertIn("class FeatureSpikeProbe", text)
        # the three DEC-056 candidates are what the probe exists to compare
        self.assertIn("map_str", text)
        self.assertIn("map_int", text)
        self.assertIn("zz_feature_spike_", text)
        # the conditional ARRAY<DOUBLE> + version candidate from DEC-056
        self.assertIn("DataTypes.ARRAY", text)
        self.assertIn("feature_version", text)

    def test_default_network_matches_the_compose_project_and_network(self):
        compose_dir = os.path.join(ROOT, "code", "01_platform", "01_docker")
        project = os.path.basename(compose_dir)
        compose = open(os.path.join(compose_dir, "docker-compose.yml"), encoding="utf-8").read()
        self.assertIn("trading-net:", compose, "compose no longer defines trading-net")
        expected = "%s_trading-net" % project
        runner = open(SCRIPT, encoding="utf-8").read()
        self.assertIn('NETWORK="${NETWORK:-%s}"' % expected, runner,
                      "runner default network must be %s" % expected)

    def test_default_tablet_container_matches_the_compose_project(self):
        compose_dir = os.path.join(ROOT, "code", "01_platform", "01_docker")
        project = os.path.basename(compose_dir)
        compose = open(os.path.join(compose_dir, "docker-compose.yml"), encoding="utf-8").read()
        self.assertIn("fluss-tablet:", compose, "compose no longer defines fluss-tablet")
        expected = "%s-fluss-tablet-1" % project
        runner = open(SCRIPT, encoding="utf-8").read()
        self.assertIn('TABLET="${TABLET:-%s}"' % expected, runner,
                      "runner default tablet container must be %s" % expected)

    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs a JDK and a built ingestion classpath")
    def test_probe_compiles(self):
        with tempfile.TemporaryDirectory() as d:
            probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "FeatureSpikeProbe.java")
            shutil.copy(probe, os.path.join(d, "FeatureSpikeProbe.java"))
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt"),
                      encoding="utf-8").read().strip()
            r = subprocess.run(["javac", "-nowarn", "-cp", cp, "-d", d,
                                os.path.join(d, "FeatureSpikeProbe.java")],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


class StubbedDocker(unittest.TestCase):
    """Runs the whole runner against a docker stub: pins the mount mode, the byte lines and
    the cleanup call, none of which need a cluster."""

    def test_spike_runs_end_to_end_and_measures_bytes(self):
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
                    '    ;;\n'
                    '  exec)\n'
                    '    case "$*" in\n'
                    '      *"du -sb"*"kv-"*) printf "5000\\t/tmp/fluss/data/default/table-1/kv-1\\n" ;;\n'
                    '      *"du -sb"*) printf "123456789\\t/tmp/fluss/data/default/table-1\\n" ;;\n'
                    '      *"-name *.log"*) printf "100\\n101\\n" ;;\n'
                    '      *"-name *.index"*) printf "200\\n201\\n" ;;\n'
                    '      *) printf "/tmp/fluss/data/default/table-1\\n" ;;\n'
                    '    esac\n'
                    '    ;;\n'
                    'esac\n'
                    'exit 0\n')
            os.chmod(stub, 0o755)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            env["POLL_SECS"] = "0"
            r = subprocess.run(["bash", SCRIPT, "--windows", "1", "--features", "2",
                                "--reads", "1", "--cleanup"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

            observed = open(log, encoding="utf-8").read()
            self.assertIn("SPIKE bytes variant=map_str log_bytes=201", r.stdout)
            self.assertIn("SPIKE bytes variant=map_int log_bytes=201", r.stdout)
            self.assertIn("SPIKE bytes variant=json log_bytes=201", r.stdout)
            self.assertIn("SPIKE bytes variant=array log_bytes=201", r.stdout)
            self.assertIn("kv_bytes=5000", r.stdout)
            self.assertIn("index_bytes=401", r.stdout)
            self.assertIn("bytes_per_row=", r.stdout)

            mode = int([line.split(" ", 1)[1] for line in observed.splitlines()
                        if line.startswith("mount_mode ")][0], 8)
            self.assertTrue(mode & stat.S_IROTH and mode & stat.S_IXOTH,
                            "mount dir is not readable/traversable by the container user: %s" % oct(mode))
            classes = int([line.split(" ", 1)[1] for line in observed.splitlines()
                           if line.startswith("mount_classes ")][0])
            self.assertGreaterEqual(classes, 2, "the mount had no compiled classes in it")

            run_argv = [line for line in observed.splitlines() if line.startswith("argv ")
                        and " --rm " in line]
            project = os.path.basename(os.path.join(ROOT, "code", "01_platform", "01_docker"))
            for line in run_argv:
                argv = line.split()
                self.assertIn("--network", argv)
                self.assertEqual(argv[argv.index("--network") + 1], "%s_trading-net" % project)
            self.assertTrue(any(" drop " in line for line in run_argv),
                            "cleanup must invoke the probe's drop mode")


if __name__ == "__main__":
    unittest.main()
