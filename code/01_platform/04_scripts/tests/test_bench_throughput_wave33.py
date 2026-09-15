#!/usr/bin/env python3
"""Wave 33 — bench-throughput.sh must fail when the stream is not there (P6-001).

The bench claims to generate 20,480 frames/s (1024 subscribed ids x 20 Hz) and
its gate asked for 15,000 rows per 60s window — 250 rows/s, about 1/82nd of the
claim. A run delivering 2% of the intended stream, or nothing at all for most of
the window, still printed "RESULT: PASS". These tests are about that gate and
the measurement feeding it:

  * a trickle the old gate passed must now FAIL, and a healthy stream must
    still PASS (the gate is derived, not merely tightened);
  * the rate is divided by the window actually measured, not a hardcoded 60;
  * a missing latency sample is a FAIL, not a -1 that compares false;
  * an O2 serving nothing fails at the baseline instead of after 3 windows;
  * a teardown failure reaches the exit code, not just result.txt.

The real script runs end to end here: the sandbox mirrors the repo layout so its
own path derivation works, `docker`/`go`/`mvn`/`java` are PATH shims, and
OpenObserve is a scripted HTTP stub — so the credential file, the request
payload and the JSON extraction all go through the script's own code.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import HTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
BENCH_SRC = REPO / "code/01_platform/04_scripts/bench-throughput.sh"
STUBS = Path(__file__).resolve().parent / "stubs"
sys.path.insert(0, str(STUBS))
import wave33_o2  # noqa: E402  (path set above)

# Two seconds keeps the suite fast. The gate is derived from WINDOW_S, so the
# arithmetic under test is the same at 60s as at 2s.
WINDOW_S = "2"
EXPECTED_RPS = 20480

# The bench's own broker: binds :8899 so preflight and the startup wait succeed,
# and exits on the teardown kill, which frees the port (the healthy path).
FAKETOOL = '''\
#!/usr/bin/env python3
import socket, sys, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 8899))
s.listen(16)
while True:
    try:
        s.accept()[0].close()
    except OSError:
        time.sleep(0.1)
'''

# The P6-030 case: the process the bench kills is NOT the one holding the port.
# It double-forks a detached listener and then waits, so teardown's kill
# succeeds while :8899 stays bound — the branch that sets FAIL on the exit-0
# path. (The real faketool does not fork; this shape exists to make a race the
# rewrite has to survive reachable without wedging the bench on `wait`.)
FAKETOOL_ORPHANING = '''\
#!/usr/bin/env python3
import os, signal, socket, sys, time
pidfile = os.environ["W33_LISTENER_PID"]
r, w = os.pipe()
if os.fork() == 0:                       # listener: detached from this pid
    os.setsid()
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 8899))
    s.listen(16)
    open(pidfile, "w").write(str(os.getpid()))
    os.write(w, b"1")
    while True:
        try:
            s.accept()[0].close()
        except OSError:
            time.sleep(0.1)
os.close(w)
os.read(r, 1)                            # reply only once the port is bound
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
while True:
    time.sleep(0.2)
'''

# `docker inspect -f '...'` must report a container the health gate accepts.
DOCKER = '''\
#!/usr/bin/env bash
if [ "$1" = "inspect" ]; then
  echo "healthy"
fi
exit 0
'''

# The bench resolves DOCKER_DIR from its own location, so the sandbox mirrors
# the repo layout; these shims stand in for the toolchain.
GO = '''\
#!/usr/bin/env bash
for ((i=1; i<=$#; i++)); do
  if [ "${!i}" = "-o" ]; then
    j=$((i+1)); dst="${!j}"
    mkdir -p "$(dirname "$dst")"
    case "$dst" in
      */faketool) cp "$W33_FAKETOOL_SRC" "$dst"; chmod +x "$dst" ;;
      *) : > "$dst" ;;
    esac
  fi
done
exit 0
'''
MVN = '#!/usr/bin/env bash\nexit 0\n'
JAVA = '#!/usr/bin/env bash\necho \'openjdk version "17.0.19" 2026-04-21\'\n'


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class O2Server:
    """wave33_o2's handler on an ephemeral port, in a background thread."""

    def __init__(self, mode: str = "ok", rate: int = EXPECTED_RPS) -> None:
        self.port = free_port()
        self.state = Path(tempfile.mkdtemp(prefix="w33-o2-")) / "state.json"
        wave33_o2.STATE = str(self.state)
        wave33_o2.MODE = mode
        wave33_o2.RATE = rate
        wave33_o2.WINDOW_S = float(WINDOW_S)
        self.httpd = HTTPServer(("127.0.0.1", self.port), wave33_o2.Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class Sandbox:
    """A throwaway tree laid out like the repo, holding the real bench script."""

    def __init__(self, root: Path, faketool_src: str = FAKETOOL) -> None:
        self.root = root
        self.code = root / "code"
        self.bin = root / "bin"
        self.bin.mkdir()
        self.out = root / "out"
        self.out.mkdir()

        # ── the layout the script's own path derivation expects ──────────────
        self.scripts_dir = self.code / "01_platform/04_scripts"
        self.scripts_dir.mkdir(parents=True)
        self.script = self.scripts_dir / "bench-throughput.sh"
        shutil.copy2(BENCH_SRC, self.script)
        self.docker_dir = self.code / "01_platform/01_docker"
        self.docker_dir.mkdir(parents=True)
        # The padding on this value is the P6-028 shape: the old `awk -F=` read
        # dropped it, and the credential then failed every request.
        (self.docker_dir / ".env").write_text(
            "O2_AUTH_BASIC=Basic dXNlcjpwYXNz==\n")
        bridge = self.code / "02_services/01_ingestion/go-bridge"
        bridge.mkdir(parents=True)
        target = self.code / "02_services/01_ingestion/target"
        target.mkdir(parents=True)
        (target / "ingestion.jar").write_bytes(b"")

        # ── shims ───────────────────────────────────────────────────────────
        self.faketool_src = root / "faketool.py"
        self.faketool_src.write_text(faketool_src)
        self.listener_pid = root / "listener.pid"
        self.shim("docker", DOCKER)
        self.shim("go", GO)
        self.shim("mvn", MVN)
        self.shim("java", JAVA)

        # ── the manifest the bench checks for ───────────────────────────────
        self.manifest = root / "manifest.csv"
        self.manifest.write_text("Token\n1\n")

        # ── the journal the ack gate reads ─────────────────────────────────
        journal = self.out / "bench/journal"
        journal.mkdir(parents=True)
        (journal / "ingestion.json").write_text(
            '{"message":"bridge lifecycle event=subscription_ack slot=hft-0 '
            'state=ACTIVE epoch=1 assigned=1024 acknowledged=1024 rejected=0"}\n')

        # ── a stand-in for Fluss :9123 (preflight requires it reachable) ────
        # The preflight needs :9123 reachable, and usually nothing is there, so
        # this stub provides it. When a real Fluss already holds the port the
        # check is satisfied without us — binding would then fail the test for
        # the environment being in its CORRECT state.
        self.fluss = socket.socket()
        self.fluss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.fluss.bind(("127.0.0.1", 9123))
        except OSError:
            self.fluss.close()
        else:
            self.fluss.listen(8)

    def shim(self, name: str, body: str) -> None:
        p = self.bin / name
        p.write_text(body)
        p.chmod(0o755)

    def env(self, o2: O2Server, **extra) -> dict:
        environ = dict(os.environ)
        environ["PATH"] = f"{self.bin}:{environ['PATH']}"
        environ.update({
            "OUT_DIR": str(self.out),
            "O2_BASE_URL": f"http://127.0.0.1:{o2.port}",
            "INGESTION_CONTAINER_NAME": "w33-test-ingestion-1",
            "INSTRUMENT_MANIFEST_HOST_PATH": str(self.manifest),
            "BENCH_WINDOW_S": WINDOW_S,
            "BENCH_EXPECTED_RPS": str(EXPECTED_RPS),
            "BENCH_WINDOWS": "1",
            "BENCH_BASELINE_SETTLE_S": "0",
            "W33_O2_STATE": str(o2.state),
            "W33_O2_MODE": wave33_o2.MODE,
            "W33_O2_RATE": str(wave33_o2.RATE),
            "W33_O2_WINDOW_S": WINDOW_S,
            "W33_FAKETOOL_SRC": str(self.faketool_src),
            "W33_LISTENER_PID": str(self.listener_pid),
        })
        environ.update(extra)
        return environ

    def run(self, o2: O2Server, timeout: float = 120, script: Path | None = None,
            **extra):
        proc = subprocess.Popen(
            ["bash", str(script or self.script)], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=self.env(o2, **extra),
            start_new_session=True,
        )
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.reap(proc)
            raise
        return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)

    @staticmethod
    def reap(proc) -> None:
        """Kill the whole process GROUP — subprocess signals only the child."""
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)

    def kill_listener(self) -> None:
        """Stop the detached 8899 listener, if the faketool started one."""
        if not self.listener_pid.exists():
            return
        try:
            pid = int(self.listener_pid.read_text().strip())
        except (OSError, ValueError):
            return
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                return
            for _ in range(20):
                if not Path(f"/proc/{pid}").exists():
                    return
                time.sleep(0.05)

    def result_text(self) -> str:
        p = self.out / "bench/result.txt"
        return p.read_text() if p.exists() else ""

    def tsv_text(self) -> str:
        p = self.out / "bench/bench-throughput.tsv"
        return p.read_text() if p.exists() else ""


class BenchCase(unittest.TestCase):
    """Shared sandbox lifecycle."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w33-"))
        self.sb = Sandbox(self.tmp)

    def tearDown(self) -> None:
        self.sb.kill_listener()
        self.sb.fluss.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def joined(self, res) -> str:
        return res.stdout + res.stderr


class BenchGateTest(BenchCase):
    """P6-001/P6-031/P6-032/P6-033/P6-316 — the gate and the numbers behind it."""

    # A rate the OLD gate accepts and the NEW gate rejects, at this suite's
    # WINDOW_S. The old gate was an absolute `rows < 15000` regardless of the
    # window, so at 2s it accepted anything at or above 7500 rows/s; the derived
    # floor at 2s is 0.90 x 20480 x 2 = 36,864 rows, i.e. 18,432 rows/s. Any
    # rate in [7500, 18432) discriminates; 10,000 sits inside that band.
    #
    # The first draft of this test used 250 rows/s — the rate the finding is
    # *about* — and did not discriminate at all: 250 rows/s over 2s is 500
    # rows, which the old gate rejected too (500 < 15000). It failed in the red
    # leg only because the pre-fix script has no O2_BASE_URL seam and never
    # reached the stub, so it proved nothing about either gate.
    DISCRIMINATING_RPS = 10000

    def test_trickle_the_old_gate_passed_now_fails(self):
        """The regression itself: a rate the old gate accepted now fails.

        `_old_gate_verdict` below states the pre-fix predicate, so the claim in
        this docstring is checked here rather than assumed: the same row count
        that the script now rejects is one the old arithmetic accepted.
        """
        o2 = O2Server(rate=self.DISCRIMINATING_RPS)
        self.addCleanup(o2.stop)
        res = self.sb.run(o2)
        joined = self.joined(res)

        # What the window measured, straight from the script's own line.
        m = re.search(r"window \d+: rows=(\d+)", joined)
        self.assertIsNotNone(m, "no row count reported:\n" + joined[-2500:])
        rows = int(m.group(1))
        self.assertEqual(
            self._old_gate_verdict(rows), "PASS",
            "the test does not discriminate: the old gate rejects %d rows too"
            % rows)

        self.assertIn("RESULT: FAIL", joined, joined[-2500:])
        self.assertNotEqual(
            res.returncode, 0,
            "a %d rows/s stream must not pass" % self.DISCRIMINATING_RPS)
        # The failure must name the derived floor, not the old constant.
        self.assertIn("<", joined)
        self.assertNotIn("< 15000", joined)

    @staticmethod
    def _old_gate_verdict(rows: int) -> str:
        """The pre-fix predicate, verbatim: `if [ "$rows" -lt 15000 ]`."""
        return "FAIL" if rows < 15000 else "PASS"

    def test_expected_rate_still_passes(self):
        """The derived gate must not reject a healthy stream.

        20483 rows/s is what the recorded bench run actually delivered.
        """
        o2 = O2Server(rate=20483)
        self.addCleanup(o2.stop)
        res = self.sb.run(o2)
        joined = self.joined(res)
        self.assertIn("RESULT: PASS", joined, joined[-2500:])
        self.assertEqual(res.returncode, 0, joined[-2500:])

    def test_rate_divides_by_the_measured_window(self):
        """P6-032: the divisor is the span actually measured.

        The window is slept for WINDOW_S=2s; the log line must report a span
        near that, not the hardcoded 60 the old code divided by.
        """
        o2 = O2Server()
        self.addCleanup(o2.stop)
        res = self.sb.run(o2)
        joined = self.joined(res)
        spans = []
        for line in joined.splitlines():
            if "rows=" in line and " over " in line:
                spans.append(float(line.split(" over ")[1].split("s)")[0]))
        self.assertTrue(spans, f"no measured span reported:\n{joined[-2000:]}")
        for span in spans:
            self.assertGreaterEqual(span, 2.0)
            self.assertLess(span, 30.0, "a fixed 60s divisor would show up here")

    def test_missing_latency_sample_fails_the_window(self):
        """P6-033: no latency evidence is a FAIL, not a -1 that compares false.

        With no p50/p99 gauge and no usable sum/count, the old code set
        p99=-1 and then `-1 >= 1000` was false — so the window PASSED with no
        latency measurement at all.
        """
        o2 = O2Server(mode="no-latency")
        self.addCleanup(o2.stop)
        res = self.sb.run(o2)
        joined = self.joined(res)
        self.assertIn("no latency sample", joined, joined[-2500:])
        self.assertIn("RESULT: FAIL", joined, joined[-2500:])
        self.assertNotEqual(res.returncode, 0)

    def test_tsv_header_describes_the_rows(self):
        """P6-316: five columns of header over six of data misaligns evidence."""
        o2 = O2Server(rate=20483)
        self.addCleanup(o2.stop)
        self.sb.run(o2)
        header = self.sb.tsv_text().splitlines()[0]
        self.assertIn("verdict", header)
        self.assertEqual(len(header.split("\t")), 6)


class BenchBaselineTest(BenchCase):
    """P6-315 — an O2 that answers nothing must fail at the baseline."""

    def test_no_baseline_fails_before_any_window(self):
        o2 = O2Server(mode="no-baseline")
        self.addCleanup(o2.stop)
        started = time.monotonic()
        res = self.sb.run(o2)
        elapsed = time.monotonic() - started
        joined = self.joined(res)
        self.assertIn("baseline unavailable", joined, joined[-2500:])
        self.assertNotEqual(res.returncode, 0)
        self.assertNotIn("window 1:", joined, "no window should have been run")
        self.assertLess(elapsed, 60.0)


class BenchTeardownTest(BenchCase):
    """P6-030 — a failure found during teardown must reach the exit code."""

    def setUp(self) -> None:
        # This class is the one that needs the port to outlive the kill.
        self.tmp = Path(tempfile.mkdtemp(prefix="w33-"))
        self.sb = Sandbox(self.tmp, faketool_src=FAKETOOL_ORPHANING)

    def test_teardown_failure_is_not_reported_as_success(self):
        """The broker's listener outlives the kill, so teardown sees :8899 busy.

        Every window passes; the failure is discovered in teardown, i.e. on the
        exit-0 path. The old cleanup printed the failure and then `exit "$rc"`
        with rc=0 — CI read a green build while result.txt said FAIL.
        """
        o2 = O2Server(rate=20483)
        self.addCleanup(o2.stop)
        res = self.sb.run(o2)
        joined = self.joined(res)
        # The discriminator: the script must have reached its own success path
        # FIRST, then be overruled by teardown. Without this the test is green
        # for the wrong reason — an early failure (e.g. unreadable credentials
        # leaving every window "unavailable") also exits 1 and also prints the
        # busy-port line, so it satisfies the assertions below without ever
        # exercising the exit-0 path this finding is about.
        self.assertIn("=== RESULT: PASS (all windows)", joined, joined[-2500:])
        self.assertNotIn("!! window", joined, joined[-2500:])
        verdict = self.sb.result_text().splitlines()[0]
        self.assertTrue(verdict.endswith("FAIL"), verdict)
        self.assertIn("port 8899 still busy", joined, joined[-2500:])
        self.assertEqual(
            res.returncode, 1,
            "result.txt says FAIL but the script exited %d" % res.returncode)


class BenchSourceContractTest(unittest.TestCase):
    """Pins for findings that are about source shape, not runtime behaviour."""

    def setUp(self) -> None:
        self.text = BENCH_SRC.read_text(encoding="utf-8")
        self.code = "\n".join(
            l for l in self.text.splitlines() if not l.lstrip().startswith("#"))

    def test_flat_row_gate_is_gone(self):
        """P6-001: no flat constant, no hardcoded divisor."""
        self.assertNotIn("-lt 15000", self.code)
        self.assertNotIn("rows / 60", self.code)
        self.assertIn("MIN_RATE", self.code)

    def test_credentials_never_on_a_command_line(self):
        """P6-029: the header travels in curl's 0600 config file, and is removed."""
        self.assertIn('-K "$O2_AUTH_FILE"', self.code)
        self.assertNotIn('-H "Authorization: Basic $O2_AUTH"', self.code)
        self.assertIn('rm -f "${O2_AUTH_FILE:-}"', self.code)

    def test_auth_reader_strips_the_key_prefix(self):
        """P6-028: base64 padding must survive the read."""
        self.assertIn('sub(/^[^=]*=/,"")', self.code)

    def test_port_open_uses_its_host_argument(self):
        """P6-311: the host parameter must reach the connect, not be ignored."""
        self.assertIn('/dev/tcp/"$1"/"$2"', self.code)
        self.assertNotIn("ss -ltn", self.code)

    def test_num_validates(self):
        """P6-711: the function itself, over the shapes that matter.

        Driven from the script's own definition (it is a one-liner), so the test
        cannot drift from what the bench actually calls.
        """
        fn = [l for l in self.text.splitlines() if l.startswith("num() {")]
        self.assertEqual(len(fn), 1, "num() moved or was reformatted")
        # Only the ARGUMENT is interpolated: the printf format itself has to
        # survive verbatim, so it is not substituted into.
        script = fn[0] + "\n" + "".join(
            'printf "%s|" "$(num ' + json.dumps(v) + ')"\n'
            for v in ["-", ".", "1.2.3", "--5", "1e6", "5637597.0", "NO_HITS", ""])
        out = subprocess.run(["bash", "-c", script], capture_output=True,
                             text=True, check=True).stdout
        self.assertEqual(out.split("|")[:-1],
                         ["", "", "", "", "1000000", "5637597", "", ""])

    def test_jdk_gate_accepts_newer_majors(self):
        """P6-712: the preflight rejected 21 and 25."""
        self.assertNotIn('version "17', self.code)
        self.assertIn("JAVA_MAJOR", self.code)

    def test_ack_pattern_is_anchored_and_counts_acks(self):
        """P6-314: `acknowledged=10240` must not count, and acks != lines."""
        self.assertIn(r"1024\b", self.code)
        # `-h` suppresses the filename prefix so the count stays a count of acks
        # across the globbed files (P1-132: ingestion-<HOST>-<VM_ID>.json).
        self.assertIn("grep -hoE", self.code)
        self.assertIn('"$JOURNAL_DIR"/ingestion*.json', self.code)

    def test_health_gate_tolerates_a_missing_healthcheck(self):
        """P6-313: no .State.Health is not a failure if the container is up."""
        self.assertIn("running=", self.code)

    def test_manifest_is_not_a_machine_specific_absolute_path(self):
        """P6-309: derived from the checkout, and overridable."""
        self.assertIn("INSTRUMENT_MANIFEST_HOST_PATH", self.code)
        self.assertNotIn('MANIFEST="/home/', self.code)

    def test_endpoints_are_overridable(self):
        """P6-310: a renamed compose project or relocated O2 must not break it."""
        self.assertIn("O2_BASE_URL", self.code)
        self.assertIn("INGESTION_CONTAINER_NAME", self.code)

    def test_skipped_windows_are_recorded(self):
        """P6-713: a missing sample must not vanish from the failure list."""
        self.assertIn("skip_window", self.code)
        self.assertIn('WINDOW_FAILS="$WINDOW_FAILS $1"', self.code)

    def test_broker_liveness_is_rechecked_per_window(self):
        """P6-312: a dead broker must be named, not blamed on throughput."""
        self.assertIn("kill -0", self.code)
        self.assertIn("fake broker died", self.code)

    def test_verdicts_are_not_split_from_evidence(self):
        """The script merges its own streams, so out/err routing cannot apply.

        `exec > >(tee -a "$RUN_LOG") 2>&1` sends both to the same log before any
        verdict is printed; a `>&2` on the `!!` lines would change nothing an
        operator or a caller can observe. Pinned so a future edit that removes
        the merge is noticed, because THEN the convention would start to matter.
        """
        self.assertIn("exec > >(tee -a", self.code)


class BenchJournalPermTest(BenchCase):
    """The journal bind mount must be writable by the ingestion container's uid.

    P1-137 dropped the ingestion container to uid 65532, and the bench hands it
    a host bind mount created by the invoking user (typically uid 1000, mode
    0775). The entrypoint probes LOG_DIR and exits 2 when it cannot write, so
    every bench run after 2026-09-08 died at `container health` with no window
    ever measured — while the recorded pass in logs/soak/bench-20260831-212352
    predates that change and ran as root.

    Asserted by MODE, not by grepping for `chmod`: the thing that regressed is
    whether the container can write, and the test says so directly.
    """

    def test_journal_dir_is_group_and_world_writable(self):
        o2 = O2Server(rate=EXPECTED_RPS)
        self.addCleanup(o2.stop)
        self.sb.run(o2)

        journal = self.sb.out / "bench/journal"
        self.assertTrue(journal.is_dir(), "bench did not create its journal dir")
        mode = journal.stat().st_mode & 0o777
        self.assertEqual(mode & 0o022, 0o022,
                         f"journal dir mode {oct(mode)} is not writable by "
                         "uid 65532 (world/group write bit missing)")



class BenchProcessBoundaryTest(BenchCase):
    """A process boundary must not be read as a decode-error spike.

    Not a map finding: this pair was found by running the bench live, so it is
    recorded as CHG-176 rather than cited to a P6 id.

    Live incident 2026-09-15 (run 20260915-215449): the baseline read
    `append_latency_ms_count=0 decode_errors=7` while window 1 read them from
    the NEXT process, so the window reported `decode_errors delta=-4 != 0` and
    the verdict blamed the feed for sending junk. Two defects, one cause:

      * the baseline settled for a fixed 15s and never checked the sample was
        this process's (the live process's first OTLP point landed a hair later);
      * the rows counter refused to measure across a process boundary, but the
        decode-errors counter had no such guard.

    Both legs below run the PRE-FIX script as their red leg, so they prove the
    fix is what changes the outcome rather than asserting the new text exists.
    """

    # The pre-fix baseline: sleep, then read, with no freshness proof.
    @staticmethod
    def _old_baseline(text: str) -> str:
        start = text.index('SETTLE_S="${BENCH_BASELINE_SETTLE_S:-15}"')
        end = text.index('ERR0="$(num')
        return (text[:start]
                + 'SETTLE_S="${BENCH_BASELINE_SETTLE_S:-15}"\n'
                  'echo "=== baseline settle (${SETTLE_S}s for fresh OTLP counter)"\n'
                  'sleep "$SETTLE_S"\n'
                  'echo "=== baseline (O2)"\n'
                  'CNT0="$(num "$(o2_query \'select value from '
                  '"append_latency_ms_count" order by _timestamp desc limit 1\')")"\n'
                + text[end:])

    # The pre-fix window: no backwards guard on decode_errors.
    @staticmethod
    def _old_guard(text: str) -> str:
        return text.replace(
            'if [ "$err_delta" -lt 0 ]; then\n'
            '\t\tskip_window "$w" "decode_errors counter went backwards '
            '($ERR_A → $ERR_B) — stale process data?"\n'
            '\t\tcontinue\n'
            '\tfi\n', "")

    def _variant(self, text: str, name: str) -> Path:
        """The pre-fix script, at the sandbox's own mirrored depth.

        The bench derives every path from `$BASH_SOURCE`, so a red-leg copy
        dropped anywhere else resolves PROJECT_ROOT to the wrong tree and dies
        at preflight before reaching the code under test.
        """
        p = self.sb.scripts_dir / name
        p.write_text(text)
        return p

    def test_a_stale_baseline_is_not_used_as_the_reference(self):
        """The baseline must be a sample the LIVE process produced."""
        o2 = O2Server(mode="stale-once")
        self.addCleanup(o2.stop)

        # Red leg: the pre-fix script, where the stale zero IS the baseline.
        red = self._variant(self._old_baseline(BENCH_SRC.read_text()),
                            "bench-red-baseline.sh")
        red_res = self.sb.run(o2, script=red)

        # Green leg: the fixed script, which waits for an advance.
        o2b = O2Server(mode="stale-once")
        self.addCleanup(o2b.stop)
        green_res = self.sb.run(o2b)

        red_joined, green_joined = self.joined(red_res), self.joined(green_res)

        # The red leg must have taken the stale zero as its baseline: that is
        # what makes this test discriminate rather than merely pass.
        self.assertIn("baseline: append_latency_ms_count=0", red_joined,
                      "the red leg did not capture the stale baseline it is "
                      "built from:\n" + red_joined[-2500:])

        # Green: the baseline is the live process's, so it is not 0.
        m = re.search(r"baseline: append_latency_ms_count=(\d+)", green_joined)
        self.assertIsNotNone(m, "no baseline line:\n" + green_joined[-2500:])
        self.assertNotEqual(
            m.group(1), "0",
            "the fixed script accepted the dead process's sample as its baseline")
        self.assertIn("RESULT: PASS", green_joined, green_joined[-2500:])
        self.assertEqual(green_res.returncode, 0)

    def test_a_frozen_counter_fails_before_any_window(self):
        """A stopped emitter is a baseline fault, not a throughput one."""
        o2 = O2Server(mode="frozen")
        self.addCleanup(o2.stop)
        started = time.monotonic()
        res = self.sb.run(o2)
        elapsed = time.monotonic() - started
        joined = self.joined(res)

        self.assertIn("baseline counter frozen", joined, joined[-2500:])
        self.assertNotEqual(res.returncode, 0)
        self.assertNotIn("window 1:", joined, "no window should have run")
        self.assertLess(elapsed, 60.0)

    def test_a_backwards_decode_counter_skips_instead_of_blaming_the_feed(self):
        """`delta=-4 != 0` must not be reported when the counter went backwards."""
        red = self._variant(self._old_guard(BENCH_SRC.read_text()),
                            "bench-red-guard.sh")

        o2 = O2Server(mode="decode-backwards")
        self.addCleanup(o2.stop)
        red_res = self.sb.run(o2, script=red)

        o2b = O2Server(mode="decode-backwards")
        self.addCleanup(o2b.stop)
        green_res = self.sb.run(o2b)

        red_joined, green_joined = self.joined(red_res), self.joined(green_res)

        # Red leg: exactly the incident's message, blaming the feed.
        self.assertIn("decode_errors delta=-4 != 0", red_joined,
                      "the red leg did not reproduce the incident's verdict:\n"
                      + red_joined[-2500:])

        # Green leg: named as a stale-sample fault instead.
        self.assertIn("decode_errors counter went backwards", green_joined,
                      green_joined[-2500:])
        self.assertNotIn("!= 0", green_joined)
        self.assertNotEqual(green_res.returncode, 0,
                            "an unmeasurable window must still fail closed")

if __name__ == "__main__":
    unittest.main()
