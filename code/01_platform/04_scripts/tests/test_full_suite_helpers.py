#!/usr/bin/env python3
"""Behavioural tests for the helpers inside run-full-suite.sh (P6 wave 15).

The suite runner needs a live Fluss (:9123), docker compose and an hour of gates,
so the script cannot be executed here. It has a seam for exactly this:
`RUN_FULL_SUITE_LIB=true . run-full-suite.sh` defines every helper and returns
before any stage runs. These tests drive those helpers with fixtures and stub
binaries, so the runner's exit-code and evidence semantics are pinned without a
cluster: "a run that reached a verdict of FAIL exits non-zero", "a counter that
did not advance is not an advance", "the credential is not in argv".

Provenance: wave 15, P6-015/016/177/178/179/180/181/183/185/513/514/515/516/519/
520/521/774. Each test names the finding it pins.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SUITE = ROOT / "code/01_platform/04_scripts/run-full-suite.sh"
assert SUITE.is_file(), f"suite script missing: {SUITE}"


def _lib(body: str, *, env: dict[str, str] | None = None, stub_dir: Path | None = None,
         timeout: int = 90) -> subprocess.CompletedProcess:
    """Source the suite in lib mode (no stage runs) and then execute `body`."""
    full = dict(os.environ)
    full["RUN_FULL_SUITE_LIB"] = "true"
    if stub_dir is not None:
        full["PATH"] = f"{stub_dir}:{full['PATH']}"
    if env:
        full.update(env)
    cmd = f". {shlex.quote(str(SUITE))}\nset +e\n{body}\n"
    return subprocess.run(["bash", "-c", cmd], capture_output=True, text=True,
                          env=full, timeout=timeout)


def _stats_stdout(r: subprocess.CompletedProcess) -> dict[str, str]:
    out = {}
    for line in r.stdout.splitlines():
        if "=" in line and " " not in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


class JournalStatsTest(unittest.TestCase):
    """P6-180/516: counts come from the log4j2 `message` field, never a substring."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _journal(self, rows: list) -> Path:
        path = self.dir / "ingestion.json"
        path.write_text("\n".join(r if isinstance(r, str) else json.dumps(r) for r in rows) + "\n")
        return path

    def test_counts_ack_errors_warns_epochs_and_torn_lines(self) -> None:
        j = self._journal([
            {"level": "INFO", "message": "bridge lifecycle event=subscription_ack slot=hft-0 state=ACTIVE epoch=1"},
            {"level": "WARN", "message": "bridge lifecycle event=subscription_ack slot=hft-0 state=ACTIVE epoch=2"},
            {"level": "ERROR", "message": "feed gap"},
            {"level": "INFO", "threadName": "event=subscription_ack", "message": "unrelated health tick"},
            '{"level":"INFO","message":"torn tail line',
        ])
        r = _lib(f"journal_stats {shlex.quote(str(j))}")
        self.assertEqual(r.returncode, 0, r.stderr)
        s = _stats_stdout(r)
        # two real acks: the third mention lives in threadName, not in message
        self.assertEqual(s["acks"], "2")
        self.assertEqual(s["errors"], "1")
        self.assertEqual(s["warns"], "1")
        self.assertEqual(s["torn"], "1")
        self.assertEqual(s["maxepoch"], "2")
        self.assertEqual(s["distinct_epochs"], "2")
        self.assertEqual(s["monotonic"], "1")

    def test_a_missing_file_reports_zeros_not_nothing(self) -> None:
        r = _lib(f"journal_stats {shlex.quote(str(self.dir / 'absent.json'))}")
        self.assertEqual(r.returncode, 0, r.stderr)
        s = _stats_stdout(r)
        self.assertEqual({k: s[k] for k in ("acks", "errors", "warns", "maxepoch", "distinct_epochs")},
                         {"acks": "0", "errors": "0", "warns": "0", "maxepoch": "0", "distinct_epochs": "0"})

    def test_a_gap_behind_one_big_epoch_is_visible(self) -> None:
        """P6-180: distinct epochs expose a jump; maxepoch alone does not."""
        j = self._journal([
            {"level": "INFO", "message": "bridge lifecycle event=subscription_ack epoch=1"},
            {"level": "INFO", "message": "bridge lifecycle event=subscription_ack epoch=100"},
        ])
        r = _lib(f"echo acks=$(journal_acks {shlex.quote(str(j))}) "
                 f"epochs=$(journal_distinct_epochs {shlex.quote(str(j))}) "
                 f"max=$(journal_maxepoch {shlex.quote(str(j))})")
        self.assertEqual(r.stdout.strip(), "acks=2 epochs=2 max=100")

    def test_field_accessors_read_the_missing_file_as_zero(self) -> None:
        absent = shlex.quote(str(self.dir / "absent.json"))
        r = _lib(f"echo $(journal_errors {absent})/$(journal_warns {absent})/$(journal_acks {absent})")
        self.assertEqual(r.stdout.strip(), "0/0/0")

    def test_the_per_host_filename_is_resolved_from_the_fixed_name(self) -> None:
        """P1-132: the journal is ingestion-<HOST>-<VM_ID>.json, not ingestion.json.

        Every caller in the suite still passes the fixed name, so the resolver
        has to find the real file — otherwise every ack/error/epoch gate reads
        zeros and reports a healthy run as empty.
        """
        (self.dir / "ingestion-somehost-vm0.json").write_text(json.dumps(
            {"level": "INFO",
             "message": "bridge lifecycle event=subscription_ack epoch=3"}) + "\n")
        fixed = shlex.quote(str(self.dir / "ingestion.json"))
        r = _lib(f"echo acks=$(journal_acks {fixed}) epochs=$(journal_maxepoch {fixed})")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "acks=1 epochs=3")

    def test_journal_file_resolves_the_path_for_existence_checks(self) -> None:
        """P1-132: callers that need the PATH (not the counts) must resolve too.

        `[ -f .../ingestion.json ]` guards several waits; on the per-host name it
        is always false, so the wait spins to its deadline on a healthy run.
        """
        real = self.dir / "ingestion-somehost-vm0.json"
        real.write_text("{}\n")
        fixed = shlex.quote(str(self.dir / "ingestion.json"))
        r = _lib(f"journal_file {fixed}")
        self.assertEqual(r.stdout.strip(), str(real))

    def test_journal_file_passes_an_unmatched_path_through(self) -> None:
        """No journal yet must keep the old missing-file behaviour, not invent one."""
        absent = self.dir / "ingestion.json"
        r = _lib(f"journal_file {shlex.quote(str(absent))}")
        self.assertEqual(r.stdout.strip(), str(absent))

    def test_the_newest_journal_wins_when_several_exist(self) -> None:
        """A stale file from an earlier run must not satisfy the gate."""
        old_j = self.dir / "ingestion-oldhost-vm0.json"
        old_j.write_text(json.dumps(
            {"level": "INFO", "message": "bridge lifecycle event=subscription_ack epoch=1"}) + "\n")
        os.utime(old_j, (1_600_000_000, 1_600_000_000))
        (self.dir / "ingestion-newhost-vm0.json").write_text(json.dumps(
            {"level": "INFO", "message": "bridge lifecycle event=subscription_ack epoch=9"}) + "\n")
        fixed = shlex.quote(str(self.dir / "ingestion.json"))
        r = _lib(f"echo epochs=$(journal_maxepoch {fixed})")
        self.assertEqual(r.stdout.strip(), "epochs=9")


class O2AuthTest(unittest.TestCase):
    """P6-177: the `awk -F=` form truncated a base64 secret at its padding."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _run(self, text: str) -> str:
        envf = self.dir / ".env"
        envf.write_text(text)
        r = _lib(f"read_o2_auth {shlex.quote(str(envf))}")
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout

    def test_base64_padding_survives(self) -> None:
        self.assertEqual(self._run("O2_AUTH_BASIC=Basic dXNlcjpwYXNzPT0=\nOTHER=x\n"),
                         "Basic dXNlcjpwYXNzPT0=")

    def test_quotes_and_crlf_are_stripped(self) -> None:
        self.assertEqual(self._run('O2_AUTH_BASIC="Basic Zm9vYmFy=="\r\n'), "Basic Zm9vYmFy==")

    def test_absent_key_is_empty_not_an_error(self) -> None:
        self.assertEqual(self._run("# nothing here\n"), "")


class PortOpenTest(unittest.TestCase):
    """P6-515: no silent `ss` absence, no silently ignored host argument."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _ss(self, body: str) -> None:
        stub = self.dir / "ss"
        stub.write_text("#!/bin/sh\n" + body)
        stub.chmod(0o755)

    def test_a_listening_port_matches(self) -> None:
        self._ss("printf 'LISTEN 0 128 127.0.0.1:9123 0.0.0.0:*\\n'\n")
        r = _lib("port_open localhost 9123; echo rc=$?", stub_dir=self.dir)
        self.assertIn("rc=0", r.stdout)

    def test_a_different_port_does_not_match(self) -> None:
        self._ss("printf 'LISTEN 0 128 127.0.0.1:9124 0.0.0.0:*\\n'\n")
        r = _lib("port_open localhost 9123; echo rc=$?", stub_dir=self.dir)
        self.assertIn("rc=1", r.stdout)

    def test_absent_ss_is_reported_loudly(self) -> None:
        # A PATH that holds every tool this code path needs, except ss. (Pointing
        # PATH at an empty dir would stop the child bash itself from being found.)
        minimal = self.dir / "minimal-bin"
        minimal.mkdir()
        for tool in ("bash", "grep", "dirname", "awk"):
            found = shutil.which(tool)
            if found:
                (minimal / tool).symlink_to(found)
        r = _lib("port_open localhost 9123; echo rc=$?", env={"PATH": str(minimal)})
        self.assertIn("rc=1", r.stdout)
        self.assertIn("ss not installed", r.stderr)

    def test_a_non_local_host_is_refused_instead_of_guessed(self) -> None:
        self._ss("printf 'LISTEN 0 128 127.0.0.1:80 0.0.0.0:*\\n'\n")
        r = _lib("port_open example.com 80; echo rc=$?", stub_dir=self.dir)
        self.assertIn("rc=1", r.stdout)
        self.assertIn("refusing non-local host", r.stderr)


class O2QueryTest(unittest.TestCase):
    """P6-179/521: the credential travels in a 0600 curl config file, and the
    caller can widen the window past the 1h default."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_no_authorization_header_in_argv_and_the_window_is_honoured(self) -> None:
        argv_log = self.dir / "curl-argv.txt"
        stub = self.dir / "curl"
        stub.write_text(
            "#!/bin/sh\n"
            f'printf "%s\\n" "$@" >> {argv_log}\n'
            'printf \'{"hits":[{"_source":{"value":41}}]}\\n\'\n'
        )
        stub.chmod(0o755)
        auth = self.dir / "curlrc"
        auth.write_text('header = "Authorization: Basic c2VjcmV0"\n')

        r = _lib(
            "O2_AUTH_FILE=" + shlex.quote(str(auth)) + "\n"
            "O2_BASE=http://127.0.0.1:5080\n"
            "o2_query 'select 1' 28800000000",
            stub_dir=self.dir,
        )
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.strip(), "41")
        argv = argv_log.read_text()
        lines = argv.splitlines()
        self.assertIn(f"-K", lines, argv)
        self.assertIn(str(auth), lines, argv)
        self.assertNotIn("Authorization", argv)
        self.assertNotIn("c2VjcmV0", argv)

        payload = json.loads(lines[lines.index("-d") + 1])
        q = payload["query"]
        self.assertEqual(q["end_time"] - q["start_time"], 28800000000,
                         "the 8h window argument did not reach the payload")

    def test_no_hits_and_a_broken_body_are_distinguishable(self) -> None:
        stub = self.dir / "curl"
        stub.write_text('#!/bin/sh\nprintf \'{"hits":[]}\\n\'\n')
        stub.chmod(0o755)
        auth = self.dir / "curlrc"
        auth.write_text("")
        r = _lib("O2_AUTH_FILE=" + shlex.quote(str(auth)) + "\nO2_BASE=http://x\n"
                 "o2_query 'select 1'", stub_dir=self.dir)
        self.assertEqual(r.stdout.strip(), "NO_HITS")


class FaketoolUpTest(unittest.TestCase):
    """P6-181/184: a broker that never binds fails the stage instead of
    leaving a stage to time out against nothing."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        (self.dir / "bin").mkdir()
        fake = self.dir / "bin" / "faketool"
        fake.write_text("#!/bin/sh\nexec sleep 300\n")
        fake.chmod(0o755)
        (self.dir / "log").write_text("")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_a_bound_broker_returns_zero_and_keeps_its_pid(self) -> None:
        body = (f'OUT={self.dir}\n'
                'port_open() { return 0; }\n'
                f'faketool_up {self.dir}/log -disconnect-every 0; echo rc=$? pid=${{FAKETOOL_PID:-none}}\n'
                'kill "$FAKETOOL_PID" 2>/dev/null')
        r = _lib(body)
        self.assertIn("rc=0", r.stdout)
        self.assertNotIn("pid=none", r.stdout)

    def test_a_broker_that_never_binds_fails_and_is_killed(self) -> None:
        body = (f'OUT={self.dir}\nFAKETOOL_WAIT_SECS=1\n'
                'port_open() { return 1; }\n'
                'sleep() { :; }\n'
                f'faketool_up {self.dir}/log; echo rc=$? pid=${{FAKETOOL_PID:-none}}\n')
        r = _lib(body)
        self.assertIn("rc=1", r.stdout)
        self.assertIn("pid=none", r.stdout)
        self.assertIn("did not bind", r.stderr)


class WaitUntilTest(unittest.TestCase):
    """P6-774: 20s steps overshot a deadline by up to 20s."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_deadline_in_the_past_returns_immediately_without_sleeping(self) -> None:
        slept = self.dir / "slept"
        body = (f'date() {{ echo 1000; }}\n'
                f'sleep() {{ echo x >> {slept}; }}\n'
                'wait_until 999; echo rc=$?\n')
        r = _lib(body)
        self.assertIn("rc=0", r.stdout)
        self.assertFalse(slept.exists(), "wait_until slept although the deadline had passed")

    def test_it_polls_and_warns_when_the_clock_jumped(self) -> None:
        slept = self.dir / "slept"
        body = (f'date() {{ echo 2000; }}\n'
                f'sleep() {{ echo x >> {slept}; }}\n'
                'wait_until 1000; echo rc=$?\n')
        r = _lib(body)
        self.assertIn("rc=0", r.stdout)
        self.assertIn("clock jump", r.stderr)


class CleanupTest(unittest.TestCase):
    """P6-178/183/514: an abort leaves nothing running, stops the container it
    started, removes the credential file and reports FAIL — not RUNNING."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.summary = self.dir / "SUMMARY.txt"
        self.auth = self.dir / "curlrc"
        self.auth.write_text("header = \"Authorization: Basic x\"\n")
        self.docker_log = self.dir / "docker.txt"
        stub = self.dir / "docker"
        stub.write_text("#!/bin/sh\n" + f'printf "%s\\n" "$*" >> {self.docker_log}\n')
        stub.chmod(0o755)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _cleanup_body(self, extra: str = "") -> str:
        return (
            f'OUT={self.dir}; STAMP=test; SUMMARY={self.summary}; O2_AUTH_FILE={self.auth}\n'
            f'RESULT=RUNNING; CONTAINER_STARTED=0\n{extra}'
            # stdio redirected: a child that inherits the capture pipes keeps them
            # open after it is killed, and the test then waits for an EOF that
            # only arrives when the child's 300s elapse.
            'sleep 300 >/dev/null 2>&1 & MONITOR_PID=$!\n'
            'sleep 300 >/dev/null 2>&1 & JAVA_PID=$!\n'
            'cleanup\n'
            # Cleanup now TERM→KILLs with a grace poll, so by return both
            # children are really gone; `gone` counts `ps`-observed states only.
            'gone=9\n'
            'for p in "$MONITOR_PID" "$JAVA_PID"; do\n'
            '  if ps -o stat= -p "$p" >/dev/null 2>&1; then\n'
            '    s="$(ps -o stat= -p "$p" 2>/dev/null)"\n'
            '    case "$s" in ""|Z*) gone=$((gone+1)) ;; *) echo STILL_RUNNING ;; esac\n'
            '  else\n'
            '    gone=$((gone+1))\n'
            '  fi\n'
            'done\n'
            'if [ "$gone" = 11 ]; then echo CHILD_GONE; echo CHILD_GONE; fi\n'
            f'[ -f {self.auth} ] && echo AUTH_PRESENT || echo AUTH_GONE\n'
        )

    def test_cleanup_kills_children_drops_the_credential_and_reports_fail(self) -> None:
        r = _lib(self._cleanup_body(), stub_dir=self.dir)
        self.assertEqual(r.stdout.count("CHILD_GONE"), 2, r.stdout)
        self.assertIn("AUTH_GONE", r.stdout)
        self.assertIn("FAIL (aborted before a verdict)", self.summary.read_text())
        self.assertFalse(self.docker_log.exists(), "cleanup stopped a container this run did not start")

    def test_a_started_container_is_stopped_unless_kept(self) -> None:
        r = _lib(self._cleanup_body("CONTAINER_STARTED=1\n"), stub_dir=self.dir)
        self.assertIn("stop ingestion", self.docker_log.read_text(), r.stderr)

    def test_soak_keep_container_leaves_it_running(self) -> None:
        _lib(self._cleanup_body("CONTAINER_STARTED=1\nSOAK_KEEP_CONTAINER=true\n"), stub_dir=self.dir)
        self.assertFalse(self.docker_log.exists(), "SOAK_KEEP_CONTAINER=true still stopped the container")


class SoakVerdictTest(unittest.TestCase):
    """P6-185/521: the soak verdict reads evidence, not the existence of files."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        (self.dir / "soak" / "journal").mkdir(parents=True)
        (self.dir / "soak" / "monitor.log").write_text("tick\n")
        self.monitor = self.dir / "soak" / "monitor.log"
        self.journal = self.dir / "soak" / "journal" / "ingestion.json"
        self.snapshots = self.dir / "soak" / "snapshots.tsv"
        self.write_snapshots(11)
        self.write_errors(0)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_snapshots(self, n: int) -> None:
        self.snapshots.write_text("".join(f"row{i}\n" for i in range(n)))

    def write_errors(self, n: int) -> None:
        self.journal.write_text("".join(
            json.dumps({"level": "ERROR", "message": f"e{i}"}) + "\n" for i in range(n)))

    def verdict(self, *, recoveries: int = 3, append0: str = "100", append1: str = "250",
                budget: int = 20) -> str:
        body = (f'OUT={self.dir}; SOAK_JOURNAL={self.dir}/soak/journal\n'
                f'RECOVERIES={recoveries}; SOAK_ERROR_BUDGET={budget}\n'
                f'APPEND0={append0}; APPEND1={append1}\n'
                'soak_verdict; echo "FAIL=$SOAK_FAIL"\n')
        r = _lib(body)
        self.assertEqual(r.returncode, 0, r.stderr)
        for line in r.stdout.splitlines():
            if line.startswith("FAIL="):
                return line[len("FAIL="):]
        self.fail(f"no verdict line in: {r.stdout!r} / {r.stderr!r}")

    def test_a_complete_soak_passes(self) -> None:
        self.assertEqual(self.verdict(), "")

    def test_an_empty_monitor_log_is_not_evidence(self) -> None:
        self.monitor.write_text("")
        self.assertIn("monitor missing or empty", self.verdict())

    def test_ten_snapshots_are_not_eleven(self) -> None:
        self.write_snapshots(10)
        self.assertIn("snapshots incomplete (10/11", self.verdict())

    def test_a_counter_that_did_not_advance_fails(self) -> None:
        self.assertIn("did not advance", self.verdict(append0="250", append1="250"))

    def test_a_reset_counter_fails(self) -> None:
        self.assertIn("did not advance", self.verdict(append0="900", append1="12"))

    def test_a_non_numeric_counter_fails_instead_of_comparing_strings(self) -> None:
        self.assertIn("not numeric", self.verdict(append1="UNAVAILABLE"))

    def test_too_few_recoveries_fail(self) -> None:
        self.assertIn("recoveries=2/3", self.verdict(recoveries=2))

    def test_an_error_budget_breach_fails(self) -> None:
        self.write_errors(21)
        self.assertIn("over budget 20", self.verdict())


if __name__ == "__main__":
    unittest.main()
