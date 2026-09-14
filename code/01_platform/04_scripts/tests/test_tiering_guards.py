"""Guardrails of tiering-start.sh and tiering-smoke.sh.

tiering-start.sh is driven through a stub `docker` on PATH: it answers the three
Flink REST endpoints, the classpath probes and `flink run` from fixture files, and
appends every argv it received to a log the tests assert on. That is how the
shell-injection case proves the unvalidated job id never reached a command line.

tiering-smoke.sh's cluster half needs a live Fluss + R2, so only its input
validation is executed here — which runs before the script reads any config, so
these tests stay hermetic. The verification guards are covered in CHG-145 as
inspected-not-executed.
"""

import fcntl
import json
import os
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

# Copy-a-pre-fix-script-into-a-fixture-tree overrides: the red run of this suite
# points these at the old scripts so the same assertions are exercised against
# them, without editing the repo copies.
SCRIPTS = Path(__file__).resolve().parent.parent
START = Path(os.environ.get("TIERING_START_SCRIPT") or SCRIPTS / "tiering-start.sh")
SMOKE = Path(os.environ.get("TIERING_SMOKE_SCRIPT") or SCRIPTS / "tiering-smoke.sh")

DOCKER_STUB = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import json, os, sys

    args = sys.argv[1:]
    with open(os.environ["STUB_CALLS"], "a") as fh:
        fh.write(json.dumps(args) + "\\n")

    def emit(var, default=""):
        path = os.environ.get(var, "")
        try:
            sys.stdout.write(open(path).read())
        except OSError:
            sys.stdout.write(default)

    def exec_sh(cmd):
        if "overview" in cmd:
            emit("STUB_OVERVIEW", '{"jobs": []}')
        elif cmd.endswith("/config"):
            emit("STUB_CONFIG", "")
        elif cmd.endswith("/exceptions"):
            emit("STUB_EXCEPTIONS", '{"root-exception": "stub failure"}')
        elif "/jobs/" in cmd:                      # /jobs/<jid>
            print(json.dumps({"state": os.environ.get("STUB_STATE", "RUNNING")}))
        elif "/opt/flink/lib/" in cmd:             # classpath guards
            if "uber" in cmd and os.environ.get("STUB_UBER_PRESENT") == "1":
                sys.exit(0)                        # the known-BAD jar is present
            sys.exit(1 if "uber" in cmd else 0)    # compat jar present, uber absent
        sys.exit(0)

    if args[:1] == ["exec"] and len(args) > 2:
        rest = args[2:]
        if rest[:2] == ["flink", "run"]:
            sys.stdout.write(os.environ.get("STUB_SUBMIT_OUT", ""))
            sys.exit(int(os.environ.get("STUB_SUBMIT_RC", "0")))
        if rest[:1] == ["sh"] and len(rest) > 2:
            exec_sh(rest[2])
        sys.exit(0)

    if args[:1] == ["logs"]:
        emit("STUB_LOGS", "")
    sys.exit(0)
    """
)

OVERVIEW_NONE = '{"jobs": []}'


def overview(*jobs):
    return json.dumps({"jobs": [dict(jid=j, name=n, state=s) for j, n, s in jobs]})


def flink_config(strategy=None, description="Restart with fixed delay (30 s)"):
    cfg = {"restart-strategy.note": description}
    if strategy is not None:
        cfg["restart-strategy"] = strategy
    return json.dumps({"jid": "0" * 32, "execution-config": cfg})


class TieringStartTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="w8-tiering-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        stub = self.bin / "docker"
        stub.write_text(DOCKER_STUB)
        stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
        self.calls = self.tmp / "calls.jsonl"

        self.env_file = self.tmp / "env"
        self.write_env()
        self.overview_file = self.tmp / "overview.json"
        self.config_file = self.tmp / "config.json"
        self.overview_file.write_text(OVERVIEW_NONE)
        self.config_file.write_text(flink_config("fixed-delay"))

    def write_env(self, body=None):
        # .env as compose accepts it: `export ` prefix, quotes, CRLF, trailing blanks
        self.env_file.write_bytes(
            (body if body is not None else
             'export R2_ENDPOINT="https://acct.r2.cloudflarestorage.com"\r\n'
             "S3_WAREHOUSE_PATH='s3://test-bucket/lake'  \r\n").encode())

    def run_start(self, *args, **env):
        environ = dict(os.environ)
        environ.update({
            "PATH": f"{self.bin}:{os.environ['PATH']}",
            "STUB_CALLS": str(self.calls),
            "STUB_OVERVIEW": str(self.overview_file),
            "STUB_CONFIG": str(self.config_file),
            "TIERING_ENV_FILE": str(self.env_file),
            "TMPDIR": str(self.tmp),
        })
        environ.update({k: str(v) for k, v in env.items()})
        return subprocess.run(["/usr/bin/env", "bash", str(START), *args],
                              capture_output=True, text=True, timeout=120, env=environ)

    def recorded(self):
        if not self.calls.exists():
            return []
        return [json.loads(line) for line in self.calls.read_text().splitlines() if line]

    def joined_calls(self):
        return "\n".join(" ".join(call) for call in self.recorded())

    # ---- P6-247: config is validated before anything is submitted ----------

    def test_missing_env_file_is_refused_with_a_reason(self):
        missing = self.tmp / "nope.env"
        result = self.run_start(TIERING_ENV_FILE=str(missing))
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing or unreadable", result.stderr)
        self.assertEqual(self.recorded(), [], "nothing should be submitted")

    def test_empty_endpoint_or_warehouse_is_refused(self):
        for body, expected in (
            ("R2_ENDPOINT=\nS3_WAREHOUSE_PATH=s3://b/lake\n", "R2_ENDPOINT is empty"),
            ("R2_ENDPOINT=https://e.r2\nS3_WAREHOUSE_PATH=\n", "S3_WAREHOUSE_PATH is empty"),
            ("R2_BUCKET=other\n", "R2_ENDPOINT is empty"),
        ):
            with self.subTest(body=body):
                self.calls.unlink(missing_ok=True)
                self.write_env(body)
                result = self.run_start()
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(expected, result.stderr)
                self.assertEqual(self.recorded(), [])

    def test_quoted_exported_crlf_values_reach_the_submit_intact(self):
        self.overview_file.write_text(OVERVIEW_NONE)
        result = self.run_start(STUB_SUBMIT_OUT="JobID " + "a" * 32 + "\n",
                                STUB_STATE="RUNNING")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.joined_calls()
        self.assertIn("--datalake.iceberg.warehouse s3://test-bucket/lake", calls)
        self.assertIn("--datalake.iceberg.iceberg.hadoop.fs.s3a.endpoint "
                      "https://acct.r2.cloudflarestorage.com", calls)

    # ---- P6-248: which jobs count as "already running" --------------------

    def test_status_is_green_for_a_non_terminal_job_with_fixed_delay(self):
        self.overview_file.write_text(overview(("f" * 32, "Fluss Lake Tiering", "RESTARTING")))
        result = self.run_start("--status")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("fixed-delay restart", result.stdout)

    def test_status_ignores_terminal_and_unrelated_jobs(self):
        for name, state in (("Fluss Lake Tiering", "FINISHED"),
                            ("Unrelated tiering reader", "RUNNING")):
            with self.subTest(name=name, state=state):
                self.overview_file.write_text(overview(("f" * 32, name, state)))
                result = self.run_start("--status")
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("no RUNNING tiering job", result.stdout)

    def test_wrong_restart_strategy_is_exit_2_and_names_the_cancel_command(self):
        self.overview_file.write_text(overview(("f" * 32, "Fluss Lake Tiering", "RUNNING")))
        self.config_file.write_text(flink_config("none"))
        status = self.run_start("--status")
        self.assertEqual(status.returncode, 2, status.stdout)

        start = self.run_start()
        self.assertEqual(start.returncode, 2, start.stdout)
        self.assertIn("flink cancel", start.stderr)
        self.assertIn("bash " + str(START), start.stderr)

    def test_second_start_is_refused_while_the_lock_is_held(self):
        self.overview_file.write_text(OVERVIEW_NONE)
        lock_path = self.tmp / f"tiering-start-{os.getuid()}.lock"
        with open(lock_path, "w") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                result = self.run_start()
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("another tiering-start is in progress", result.stderr)
        self.assertNotIn("flink run", self.joined_calls())

    # ---- P6-249: the job id is untrusted input ----------------------------

    def test_shell_metacharacters_in_a_job_id_are_refused_before_use(self):
        hostile = "x; rm -rf /; #"
        self.overview_file.write_text(overview((hostile, "Fluss Lake Tiering", "RUNNING")))
        result = self.run_start("--status")
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("non-hex job id", result.stderr)
        self.assertNotIn("rm -rf", self.joined_calls(), "the job id reached a command line")

    def test_strategy_is_read_from_the_config_not_from_a_description(self):
        self.overview_file.write_text(overview(("f" * 32, "Fluss Lake Tiering", "RUNNING")))
        # Description mentions fixed delay; the effective strategy is none.
        self.config_file.write_text(flink_config(None, "was Restart with fixed delay"))
        result = self.run_start("--status")
        self.assertEqual(result.returncode, 2, result.stdout)
        # And an unparseable body (transient curl failure) is not "no strategy fixed".
        self.config_file.write_text("")
        self.assertEqual(self.run_start("--status").returncode, 2)

    # ---- P6-250 / P6-633 / P6-634: submit, job id and state polling -------

    def test_submit_asks_for_effectively_unbounded_fixed_delay_restarts(self):
        self.overview_file.write_text(OVERVIEW_NONE)
        self.run_start(STUB_SUBMIT_OUT="JobID " + "b" * 32 + "\n", STUB_STATE="RUNNING")
        argv = [a for call in self.recorded() for a in call]
        calls = "\n".join(argv)
        self.assertIn("-Drestart-strategy.type=fixed-delay", calls)
        self.assertIn("-Drestart-strategy.fixed-delay.attempts=2147483647", calls)
        self.assertNotIn("-Drestart-strategy.fixed-delay.attempts=3", argv)

    def test_truncated_job_id_is_a_failed_submit_with_the_exit_status(self):
        self.overview_file.write_text(OVERVIEW_NONE)
        result = self.run_start(STUB_SUBMIT_OUT="JobID " + "c" * 31 + "\n",
                                STUB_SUBMIT_RC="9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("submit failed (flink run exit status 9)", result.stderr)

    def test_terminal_states_fail_immediately_with_the_exception(self):
        for state in ("FAILED", "FINISHED", "SUSPENDED"):
            with self.subTest(state=state):
                self.calls.unlink(missing_ok=True)
                self.overview_file.write_text(OVERVIEW_NONE)
                result = self.run_start(STUB_SUBMIT_OUT="JobID " + "d" * 32 + "\n",
                                        STUB_STATE=state)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertIn(f"terminal state {state}", result.stderr)
                self.assertIn("stub failure", result.stderr)

    def test_rest_calls_carry_a_timeout(self):
        self.overview_file.write_text(overview(("f" * 32, "Fluss Lake Tiering", "RUNNING")))
        self.run_start("--status")
        for call in self.recorded():
            joined = " ".join(call)
            if "curl" in joined:
                self.assertIn("-m 5", joined, "an unbounded curl can hang the poll budget")


class TieringSmokeInputTest(unittest.TestCase):
    """Only the pre-flight validation — it runs before the script reads any
    config, so nothing here touches docker, Fluss or R2."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="w8-smoke-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.out = self.tmp / "smoke-out"

    def run_smoke(self, *args, timeout=30, **env):
        """Run the smoke script. Returns (returncode, stdout, stderr); the
        returncode is None when the 30s budget expired — which for these tests
        means validation did not stop the run and real work began."""
        environ = dict(os.environ)
        environ["TIERING_SMOKE_OUT"] = str(self.out)
        environ.update({k: str(v) for k, v in env.items()})
        try:
            done = subprocess.run(["/usr/bin/env", "bash", str(SMOKE), *args],
                                  capture_output=True, text=True, timeout=timeout, env=environ)
            return done.returncode, done.stdout, done.stderr
        except subprocess.TimeoutExpired as exc:
            def text(value):
                return value.decode() if isinstance(value, bytes) else (value or "")
            return None, text(exc.stdout), text(exc.stderr)

    def test_non_numeric_or_zero_inputs_are_refused_before_any_work(self):
        # (positional seconds, extra env, expected message)
        for seconds, env, expected in (
            ("foo", {}, "SMOKE_T must be a non-negative integer"),
            ("0", {}, "must all be > 0"),
            ("300", {"RATE_HZ": "0"}, "must all be > 0"),
            ("300", {"NTOK": "x"}, "NTOK must be a non-negative integer"),
            ("300", {"TIER_WAIT": "1.5"}, "TIER_WAIT must be a non-negative integer"),
        ):
            with self.subTest(seconds=seconds, env=env):
                rc, out, err = self.run_smoke(seconds, **env)
                self.assertEqual(rc, 2, f"rc={rc}; stdout={out!r}; stderr={err!r}")
                self.assertIn(expected, err)
                # validation happens before mkdir, so no run directory is created
                self.assertFalse(self.out.exists(), "the script did work before validating")

    def test_short_timing_warns_and_the_long_timing_does_not(self):
        # The default timing used to be below the 360s floor (5-min freshness +
        # 1-min tier interval), so a run could only pass on a previous run's data.
        # Both runs are cut off by the budget: the warning is emitted before the
        # script reaches the cluster probe, and the run cannot be allowed to
        # finish (it would need a live Fluss + R2).
        _, _, short_err = self.run_smoke("60", TIER_WAIT="120", timeout=8)
        self.assertIn("below the 360s floor", short_err)
        _, _, long_err = self.run_smoke("300", TIER_WAIT="180", timeout=8)
        self.assertNotIn("below the 360s floor", long_err)


if __name__ == "__main__":
    unittest.main()
