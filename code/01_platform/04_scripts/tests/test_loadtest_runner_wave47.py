"""Wave 47 hermetic tests for loadtest-run.sh.

P6-127/128  stray reap must precede the port assert
P6-129/133  per-run readiness marker (a fixed /tmp path false-passes after kill -9)
P6-130/132  one token counter, header-validated slice
P6-131/134  collector/watcher reaped on abort, no unbounded watcher wait
P6-447/452  credentials from the 0600 secrets file, never inline
P6-448/454  ss-based bind wait + early death
P6-449/456  startup waits notice a dead JVM
P6-450/455  G2 real-rate assert polls and tails the log
P6-451/453  numeric args validated, OUT unique per run

Everything here is offline and runs in seconds: the script is copied into a
temp tree shaped like the repo (so its ROOT is a sandbox), functions are lifted
out of the real source with the same col-0 regex test-loadtest-guards.sh uses,
and no JVM, cluster or port is required except where noted.
"""
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
SCRIPT = os.path.join(REPO, "code", "01_platform", "04_scripts", "loadtest-run.sh")
PORT = 8899
MANIFEST_REL = "Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv"


def source_text():
    with open(SCRIPT, encoding="utf-8") as fh:
        return fh.read()


def fn_body(name):
    """Lift `name() { ... }` out of the real script (col-0 closing brace)."""
    # NB: some definitions carry a trailing comment on the `{` line, so no
    # newline may be required right after the brace.
    m = re.search(r"^%s\(\) \{.*?^\}\n" % re.escape(name), source_text(), re.M | re.S)
    if not m:
        raise AssertionError("function %s() not found in %s" % (name, SCRIPT))
    return m.group(0)


def fail_fn():
    m = re.search(r"^fail\(\) \{.*?\}\n", source_text(), re.M)
    if not m:
        raise AssertionError("fail() not found in %s" % SCRIPT)
    return m.group(0)


def run_bash(body, env=None, timeout=60):
    full = dict(os.environ)
    full.update(env or {})
    # merged streams: fail() reports on stderr, the harness asserts on messages
    return subprocess.run(["bash", "-c", body], stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True,
                          env=full, timeout=timeout)


def port_listening(port=PORT):
    s = socket.socket()
    s.settimeout(0.5)
    try:
        s.connect(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def wait_for(pred, timeout=5.0, step=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


class Sandbox:
    """A temp repo-shaped tree: <tmp>/Flink_Fluss_Infrastructure/streaming_project_New.

    Mirrors the real layout on purpose: the runner resolves MANIFEST as
    $ROOT/../Arrow_broker/..., which is exactly one level above the project root.
    """

    def __init__(self, fixtures=True):
        self.tmp = tempfile.mkdtemp(prefix="w47-sandbox-")
        self.parent = os.path.join(self.tmp, "Flink_Fluss_Infrastructure")
        self.root = os.path.join(self.parent, "streaming_project_New")
        os.makedirs(os.path.join(self.root, "code", "01_platform", "04_scripts"))
        self.run = os.path.join(self.root, "code", "01_platform", "04_scripts", "loadtest-run.sh")
        shutil.copy2(SCRIPT, self.run)
        if fixtures:
            self._make_fixtures()

    def _make_fixtures(self):
        ing = os.path.join(self.root, "code", "02_services", "01_ingestion")
        os.makedirs(os.path.join(ing, "target"))
        os.makedirs(os.path.join(ing, "go-bridge", "faketool"))
        open(os.path.join(ing, "target", "ingestion.jar"), "w").close()
        open(os.path.join(ing, "go-bridge", "arrow-bridge"), "w").close()
        open(os.path.join(ing, "go-bridge", "faketool", "main.go"), "w").close()
        manifest = os.path.join(self.parent, MANIFEST_REL)
        os.makedirs(os.path.dirname(manifest))
        with open(manifest, "w", encoding="utf-8") as fh:
            fh.write("Exchange,Segment,ExchSeg,Token,Symbol\n")
            for i in range(1024):
                fh.write("NSE,CM,NSE,TOKEN%04d,SYM%04d\n" % (i, i))

    def bash(self, *args, env=None, timeout=90):
        full = dict(os.environ)
        full.update(env or {})
        # one merged stream: the tests assert message ORDER (WARN before FATAL)
        return subprocess.run(["bash", self.run] + list(args), stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, env=full,
                              timeout=timeout, cwd=self.root)

    def cleanup(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


class ArgumentValidationTest(unittest.TestCase):
    """P6-451/453: a typo must fail before anything is created."""

    def setUp(self):
        self.sb = Sandbox(fixtures=False)

    def tearDown(self):
        self.sb.cleanup()

    def assert_rejected(self, args, needle):
        r = self.sb.bash(*args)
        self.assertEqual(r.returncode, 1, "expected rc=1 for %r, got %s:\n%s"
                         % (args, r.returncode, r.stdout))
        self.assertIn("FATAL: %s" % needle, r.stdout)
        logs = os.path.join(self.sb.root, "logs")
        self.assertFalse(os.path.exists(logs), "validation ran too late — it created %s" % logs)

    def test_duration_must_be_a_positive_integer(self):
        self.assert_rejected(["abc", "30"], "DURATION_S='abc' is not a positive integer")
        self.assert_rejected(["240s", "30"], "DURATION_S='240s' is not a positive integer")
        self.assert_rejected(["0", "30"], "DURATION_S must be a positive integer, got '0'")
        self.assert_rejected(["-5", "30"], "DURATION_S='-5' is not a positive integer")

    def test_interval_must_be_a_positive_integer(self):
        self.assert_rejected(["240", "abc"], "INTERVAL_S='abc' is not a positive integer")
        self.assert_rejected(["240", "0"], "INTERVAL_S must be a positive integer, got '0'")

    def test_validation_precedes_the_file_preflight(self):
        # No fixtures exist here, so if the arg check were too late the failure
        # would be "jar missing" instead of the argument message.
        r = self.sb.bash("abc", "30")
        self.assertIn("DURATION_S='abc'", r.stdout)
        self.assertNotIn("jar missing", r.stdout)


class TokenCounterTest(unittest.TestCase):
    """P6-130/132: one counter — data rows only, CR stripped."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w47-count-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def count(self, content):
        csv = os.path.join(self.tmp, "m.csv")
        with open(csv, "w", encoding="utf-8", newline="") as fh:
            fh.write(content)
        body = "%s\n%s\ncount_tokens %s\n" % (fail_fn(), fn_body("count_tokens"), csv)
        r = run_bash(body)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def test_skips_empty_lines_and_strips_cr(self):
        # 4 data rows, one truly-empty line, CRLF endings. A whitespace-only line
        # is counted like pipeline-lib.sh counts it (`grep .` matches any byte),
        # so parity with the lib is asserted separately below.
        content = ("h1,h2\r\nTOKEN1,x\r\nTOKEN2,x\r\n\r\nTOKEN3,x\r\nTOKEN4,x\r\n")
        self.assertEqual(self.count(content), "4")

    def test_whitespace_only_line_behaves_like_the_lib(self):
        content = "h1,h2\nTOKEN1,x\n   \n"
        self.assertEqual(self.count(content), "2")

    def test_zero_rows_is_zero(self):
        self.assertEqual(self.count("h1,h2\n"), "0")

    def test_matches_the_pipeline_lib_slice_idiom(self):
        # P6-130/132 is about ONE counting path; pipeline-lib.sh's P6-473 slice
        # idiom is the reference, so the two must agree byte for byte.
        csv = os.path.join(self.tmp, "parity.csv")
        with open(csv, "w", encoding="utf-8", newline="") as fh:
            fh.write("h1,h2\r\nA,1\r\n\r\nB,2\r\nC,3\r\n")
        body = ("lib=$(tail -n +2 %s | tr -d '\\r' | grep . | wc -l)\n"
                "%s\nmine=$(count_tokens %s)\n"
                "printf 'lib=%%s mine=%%s\\n' \"$lib\" \"$mine\"\n"
                % (csv, fn_body("count_tokens"), csv))
        r = run_bash(body)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "lib=3 mine=3")


class CredentialLoaderTest(unittest.TestCase):
    """P6-447/452: credentials from an owner-only file, never inline."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="w47-secrets-")
        self.secrets = os.path.join(self.tmp, "secrets.env")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def resolve(self, path=None):
        # Built by concatenation, not %-formatting: the shell printf below has
        # its own %s specifiers that Python must not try to consume.
        body = (fail_fn() + "\n"
                "ROOT=/nonexistent\n"
                "SECRETS_FILE=" + (path or self.secrets) + "\n"
                + fn_body("resolve_arrow_credentials") + "\n"
                "resolve_arrow_credentials\n"
                "printf 'loaded app=%s pass=%s totp=%s\\n' "
                "\"${ARROW_APP_SECRET:+set}\" \"${ARROW_PASSWORD:+set}\" "
                "\"${ARROW_TOTP_KEY:+set}\"\n")
        return run_bash(body)

    def write_secrets(self, mode=0o600):
        with open(self.secrets, "w", encoding="utf-8") as fh:
            fh.write("ARROW_APP_SECRET=dummy-secret-not-real\n"
                     "ARROW_PASSWORD=dummy-pass-not-real\n"
                     "ARROW_TOTP_KEY=JBSWY3DPEHPK3PXPDUMMY\n")
        os.chmod(self.secrets, mode)

    def test_owner_only_file_loads(self):
        self.write_secrets(0o600)
        r = self.resolve()
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("secrets: loaded from", r.stdout)
        self.assertIn("(mode 600", r.stdout)
        self.assertIn("loaded app=set pass=set totp=set", r.stdout)
        # values must never be echoed anywhere
        self.assertNotIn("dummy-secret-not-real", r.stdout)
        self.assertNotIn("dummy-pass-not-real", r.stdout)

    def test_mode_400_also_accepted(self):
        self.write_secrets(0o400)
        r = self.resolve()
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("(mode 400", r.stdout)

    def test_permissive_mode_refused(self):
        self.write_secrets(0o644)
        r = self.resolve()
        self.assertEqual(r.returncode, 1)
        self.assertIn("is not owner-only (mode 644)", r.stdout)
        self.assertNotIn("loaded app=set", r.stdout)

    def test_symlink_refused(self):
        self.write_secrets(0o600)
        link = os.path.join(self.tmp, "link.env")
        os.symlink(self.secrets, link)
        r = self.resolve(link)
        self.assertEqual(r.returncode, 1)
        self.assertIn("is a symlink", r.stdout)

    def test_missing_file_refused(self):
        r = self.resolve(os.path.join(self.tmp, "nope.env"))
        self.assertEqual(r.returncode, 1)
        self.assertIn("secrets file missing", r.stdout)


class StrayReapTest(unittest.TestCase):
    """P6-127/128: reap by exact name, then assert the port — in that order."""

    HOLDER = ("import socket,time\n"
              "s=socket.socket()\n"
              "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
              "s.bind(('127.0.0.1', %d))\n"
              "s.listen(1)\n"
              "time.sleep(120)\n" % PORT)

    def setUp(self):
        # A real loadtest holds :8899. Refuse to touch anything in that case:
        # this test reaps processes named `faketool` (that is the fix under test).
        if port_listening(PORT):
            self.skipTest("port %d is already in use — not reaping anything" % PORT)
        self.sb = Sandbox(fixtures=False)
        self.holder = subprocess.Popen([sys.executable, "-c", self.HOLDER],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not wait_for(lambda: port_listening(PORT), 10):
            self.holder.kill()
            self.holder.wait(timeout=5)
            self.skipTest("port holder never bound :%d" % PORT)
        # a stray named exactly `faketool` (comm = the binary's name)
        self.stray_bin = os.path.join(self.sb.tmp, "faketool")
        shutil.copy2("/bin/sleep", self.stray_bin)
        self.stray = subprocess.Popen([self.stray_bin, "120"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def tearDown(self):
        for p in (getattr(self, "stray", None), getattr(self, "holder", None)):
            if p is None:
                continue
            if p.poll() is None:
                p.kill()
            p.wait(timeout=5)   # reap explicitly: an unwaited child is a ResourceWarning
        if hasattr(self, "sb"):
            self.sb.cleanup()

    def test_reap_precedes_the_port_assert(self):
        self.assertTrue(wait_for(lambda: subprocess.run(
            ["pgrep", "-x", "faketool"], capture_output=True).returncode == 0, 5),
            "stray faketool never showed up in pgrep")
        r = self.sb.bash("240", "30")
        self.assertEqual(r.returncode, 1, r.stdout)
        self.assertIn("WARN: killing stray faketool", r.stdout)
        self.assertIn("FATAL: port 8899 already in use", r.stdout)
        self.assertLess(r.stdout.index("WARN: killing stray faketool"),
                        r.stdout.index("FATAL: port 8899 already in use"),
                        "the port assert fired before the reap:\n%s" % r.stdout)
        self.assertTrue(wait_for(lambda: self.stray.poll() is not None, 5),
                        "the stray faketool survived the reap")


class CheckOnlyTest(unittest.TestCase):
    """The --check-only contract that `make check-loadtest-env` depends on."""

    def setUp(self):
        if port_listening(PORT):
            self.skipTest("port %d is already in use — preflight would refuse" % PORT)
        self.sb = Sandbox()

    def tearDown(self):
        if hasattr(self, "sb"):
            self.sb.cleanup()

    def test_complete_fixture_tree_passes(self):
        r = self.sb.bash("--check-only")
        self.assertEqual(r.returncode, 0, r.stdout)
        self.assertIn("check-loadtest-env: OK", r.stdout)
        self.assertIn("manifest >=1024 tokens", r.stdout)

    def test_missing_manifest_is_refused(self):
        manifest = os.path.join(self.sb.parent, MANIFEST_REL)
        os.unlink(manifest)
        r = self.sb.bash("--check-only")
        self.assertEqual(r.returncode, 1)
        self.assertIn("manifest CSV missing", r.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
