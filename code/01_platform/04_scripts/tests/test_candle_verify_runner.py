"""Tests for candle-verify.sh (argv contract; the live run is opt-in)."""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(os.path.dirname(HERE), "candle-verify.sh")
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))


def run(*args):
    return subprocess.run(["bash", SCRIPT] + list(args), capture_output=True, text=True)


class ArgvContract(unittest.TestCase):
    def test_syntax(self):
        r = subprocess.run(["bash", "-n", SCRIPT], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_no_args_is_usage_error(self):
        r = run()
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage:", r.stderr)

    def test_non_numeric_minutes_is_usage_error(self):
        r = run("--minutes", "abc")
        self.assertEqual(r.returncode, 2)

    def test_missing_value_is_usage_error(self):
        r = run("--minutes")
        self.assertEqual(r.returncode, 2)

    def test_unknown_flag_is_usage_error(self):
        r = run("--minutes", "1", "--nope")
        self.assertEqual(r.returncode, 2)

    def test_probe_source_is_where_the_runner_expects_it(self):
        probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "CandleVerify.java")
        self.assertTrue(os.path.exists(probe), probe)
        text = open(probe, encoding="utf-8").read()
        # the runner compiles exactly this file name, which must match the public class
        self.assertIn("public class CandleVerify", text)
        self.assertIn("absentNoTrades", text)

    def test_default_network_matches_the_compose_project_and_network(self):
        """The default must be <compose project>_<network>. Getting this wrong is
        invisible until a run fails with docker exit 125, which is how it was
        found: the default had a hyphen where compose uses the project prefix."""
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
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "CandleVerify.java")
            shutil.copy(probe, os.path.join(d, "CandleVerify.java"))
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt"),
                      encoding="utf-8").read().strip()
            r = subprocess.run(["javac", "-nowarn", "-cp", cp, "-d", d, os.path.join(d, "CandleVerify.java")],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)


class StubbedDocker(unittest.TestCase):
    """Catches, without a docker daemon, the two ways this runner failed live:

      1. a network name that does not match <compose project>_<network>
         (docker exit 125, "Could not attach to network");
      2. a build dir the container user cannot traverse - mktemp makes it 0700 and
         the container runs as uid 65532, so java reports ClassNotFoundException
         for a class that is sitting right there.
    """

    def test_mount_is_readable_by_the_container_user_and_network_matches_compose(self):
        with tempfile.TemporaryDirectory() as td:
            stub = os.path.join(td, "docker")
            log = os.path.join(td, "docker-observed.txt")
            with open(stub, "w", encoding="utf-8") as fh:
                fh.write('#!/usr/bin/env bash\n'
                         'set -u\n'
                         'src=""\n'
                         'for a in "$@"; do\n'
                         '  case "$a" in *:/tmp/probe:ro) src="${a%%:/tmp/probe:ro}" ;; esac\n'
                         'done\n'
                         '{\n'
                         '  printf "argv %s\\n" "$*"\n'
                         '  printf "mount_mode %s\\n" "$(stat -c %a "$src")"\n'
                         '  printf "mount_classes %s\\n" "$(ls -1 "$src" | grep -c "\\.class$")"\n'
                         '} > "$DOCKER_STUB_LOG"\n'
                         'exit 0\n')
            os.chmod(stub, 0o755)
            env = os.environ.copy()
            env["PATH"] = td + os.pathsep + env["PATH"]
            env["DOCKER_STUB_LOG"] = log
            r = subprocess.run(["bash", SCRIPT, "--minutes", "1"],
                               capture_output=True, text=True, env=env)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            observed = dict(line.split(" ", 1) for line in
                            open(log, encoding="utf-8").read().strip().split("\n"))
            mode = int(observed["mount_mode"], 8)
            self.assertTrue(mode & stat.S_IROTH and mode & stat.S_IXOTH,
                            "mount dir %s is not readable/traversable by the container user" % oct(mode))
            self.assertGreaterEqual(int(observed["mount_classes"]), 2,
                                    "the mount had no compiled classes in it")
            argv = observed["argv"].split()
            self.assertIn("--network", argv)
            project = os.path.basename(os.path.join(ROOT, "code", "01_platform", "01_docker"))
            self.assertEqual(argv[argv.index("--network") + 1], "%s_trading-net" % project)


if __name__ == "__main__":
    unittest.main()
