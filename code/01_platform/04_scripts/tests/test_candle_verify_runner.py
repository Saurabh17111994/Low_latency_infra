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


    def test_probe_flag_without_a_value_is_usage_error(self):
        r = run("--minutes", "1", "--probe")
        self.assertEqual(r.returncode, 2, r.stderr + r.stdout)
        self.assertIn("--probe", r.stderr)

    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs javac and the ingestion classpath file")
    def test_unknown_probe_is_refused_before_anything_runs(self):
        # The probe name picks the source file; a typo must stop before docker is
        # touched, not after the image has started.
        r = run("--minutes", "1", "--probe", "NopeProbe")
        self.assertEqual(r.returncode, 3, r.stderr + r.stdout)
        self.assertIn("no such probe", r.stderr)

    def test_eventday_probe_source_is_where_the_runner_expects_it(self):
        # EventDayProbe answers the plan's T5.1/B1 question, and the runner resolves it
        # by class name, so the file name is part of the contract.
        path = os.path.join(ROOT, "code", "01_platform", "04_scripts", "fluss-probes",
                            "EventDayProbe.java")
        self.assertTrue(os.path.exists(path), path)
        text = open(path, encoding="utf-8").read()
        self.assertIn("public class EventDayProbe", text)
        self.assertIn('col("event_day")', text)

    @unittest.skipUnless(shutil.which("javac") and os.path.exists(
        os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt")),
        "needs javac and the ingestion classpath file")
    def test_eventday_probe_compiles(self):
        with tempfile.TemporaryDirectory() as d:
            cp = open(os.path.join(ROOT, "code", "02_services", "01_ingestion", "target", "cp.txt"),
                      encoding="utf-8").read().strip()
            src_dir = os.path.join(ROOT, "code", "01_platform", "04_scripts", "fluss-probes")
            shutil.copy(os.path.join(src_dir, "EventDayProbe.java"),
                        os.path.join(d, "EventDayProbe.java"))
            r = subprocess.run(
                ["javac", "-nowarn", "-cp", cp, "-d", d, os.path.join(d, "EventDayProbe.java")],
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


    def test_trade_accumulation_stays_inside_the_trade_branch(self):
        """XC-4: `sumDeltaTrade`/`tradeRows` must sit inside `if ("TRADE"...)`.

        A misplaced brace moved them outside: every row (quotes included)
        then fed the parity sum, and `tradeRows` was never 0, killing the
        "absent window had no trades" diagnostic. Source-level guard because
        the probe has no JVM test harness (the runner test only compiles it).
        """
        probe = os.path.join(os.path.dirname(HERE), "fluss-probes", "CandleVerify.java")
        text = open(probe, encoding="utf-8").read()
        start = text.index('if ("TRADE".equals(type)) {')
        depth = 0
        end = None
        for j in range(start, len(text)):
            if text[j] == "{":
                depth += 1
            elif text[j] == "}":
                depth -= 1
                if depth == 0:
                    end = j
                    break
        self.assertIsNotNone(end, "unbalanced TRADE branch in CandleVerify.java")
        block = text[start:end]
        self.assertIn("a.sumDeltaTrade += delta;", block)
        self.assertIn("a.tradeRows++;", block)


if __name__ == "__main__":
    unittest.main()
