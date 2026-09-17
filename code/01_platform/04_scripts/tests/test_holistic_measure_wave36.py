#!/usr/bin/env python3
"""Wave 36 — holistic-measure.sh can finish a phase, and its evidence is true.

The harness had NEVER produced a run directory. `logs/tracker-14/` holds no
`holistic-measure-*`, and `git log -S 'FAKETOOL_PID='` shows the variable was
never assigned anywhere in the file's history. Under `set -u` the liveness poll
`kill -0 "$FAKETOOL_PID"` died on the FIRST iteration, so nothing below it —
every later fix in this wave — had ever executed.

`shellcheck -S warning` (the CI gate, Makefile L476) reports NOTHING on this
file, even at `-S style`: the undefined variables are invisible to it because
the script sources pipeline-lib.sh and shellcheck cannot see through a source.
**This suite is the only guard for these changes.**

The real script runs here against a stub pipeline-lib.sh in a sandbox that
mirrors the repo layout, with `docker`/`curl`/`sleep` as PATH shims. The layout
mirrors the repo because the script derives ROOT from its own location
(`SCRIPT_DIR/../../..`): a copy placed anywhere else writes its evidence into
the wrong tree.

Task 1 (this file's scope): liveness probes the containers that carry the data
path, the unreadable /proc rows are gone, per-phase resolution runs once per
phase, and invalid knobs fail by name before any evidence directory exists.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "code/01_platform/04_scripts"
SCRIPT_SRC = SCRIPTS / "holistic-measure.sh"

# The phase driver starts here; everything above it is definitions. Tests that
# exercise run_phase keep the body and append their own driver, so they never
# touch the two-phase gate chain.
PHASES_MARKER = "# ---------------------------------------------------------------- phases"

# The lib contract that run_phase touches, stubbed. Every function below is a
# real call site in the script.
STUB_LIB = """\
LIB_FAKETOOL_CONTAINER="pipeline-faketool"
LIB_INGESTION_CONTAINER="pipeline-ingestion"
JOB_ID="job-under-test"
FAKETOOL_LOG_PID=""
INGESTION_LOG_PID=""
pipeline_log() { echo "[pipeline] $*"; }
pipeline_fail() { echo "[pipeline FATAL] $*" >&2; return 1; }
pipeline_install_cleanup_trap() { :; }
# Mirrors the real lib's purge contract (pipeline-lib.sh L798-817): a failed
# purge refuses UNLESS the caller set ALLOW_STALE_TABLE=true, in which case the
# real function falls off the end and returns 0 — the exact shape that made the
# `|| true` in the caller dead code.
# Reports the injection offset that run_phase exports immediately before
# calling preflight, so a test can observe the script's OWN arithmetic rather
# than re-deriving the formula in Python. With H39_OFFSET_PROBE=1 it then stops
# the phase — the offset is fully determined by this point, and a 900s phase
# would otherwise run 900 poll iterations to assert one number.
pipeline_preflight() {
  echo "OFFSET=${INJECT_AFTER_MS:-unset}" >&2
  [ "${H39_OFFSET_PROBE:-0}" = "1" ] && return 1
  return 0
}
pipeline_purge_raw_table() {
  if [ "${H39_PURGE_FAILS:-0}" = "1" ] && [ "${ALLOW_STALE_TABLE:-false}" != "true" ]; then
    pipeline_fail "raw purge failed and ALLOW_STALE_TABLE!=true — refusing to measure on stale data"
    return 1
  fi
  return 0
}
pipeline_start_faketool() { return 0; }
# CHG-191: run_phase purges + ensures the candle tables (the multi-tf sinks
# write them, and the job preflights both). Announced on stderr so a test can
# assert both the calls and their order relative to ingestion start, which is
# the ordering the purge contract depends on.
pipeline_purge_table() { echo "PURGE_TABLE=$2" >&2; return 0; }
pipeline_ensure_candle_tables() { echo "ENSURE_TABLE=$2" >&2; return 0; }
# Announces itself so a test can prove the phase stopped BEFORE ingestion.
pipeline_start_ingestion() { echo "INGESTION_STARTED" >&2; return 0; }
# Echoes the flag the harness hands the job, so a test asserts the submitted
# value instead of grepping the script for its own export line.
pipeline_submit_job() { echo "SUBMIT MULTITF_ENABLED=${MULTITF_ENABLED:-unset}" >&2; return 0; }
pipeline_cleanup() { echo "cleanup: stub"; }
flink_wait_state() { return 0; }
# The dump is scripted so a test can feed the REAL emitter shape
# (pipeline-lib.sh L1253: print(name, "|", read, "|", write)).
flink_metric_dump() {
  echo "STATE RUNNING"
  if [ -n "${H39_DUMP:-}" ]; then printf '%s\\n' "$H39_DUMP"; else
    printf 'op_alpha | 12345 | 67890\\n'
  fi
}
pipeline_metric_input_progress() { echo 42; }
capture_checkpoint_history() { return 0; }
harvest_tm_gc_log() { return 0; }
"""

# Answers the two inspect formats the script uses and records every call.
# H39_RUNNING lists the containers that exist and are running; anything else is
# absent (inspect fails), which is how a recreated/removed container answers.
DOCKER_SHIM = """\
#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
with open(os.environ["H39_DOCKER_LOG"], "a") as fh:
    fh.write(" ".join(args) + "\\n")
running = set(filter(None, os.environ.get("H39_RUNNING", "").split(",")))
if args and args[0] == "inspect":
    fmt = ""
    name = ""
    if "--format" in args:
        i = args.index("--format")
        fmt = args[i + 1]
        rest = args[i + 2:]
        name = rest[0] if rest else ""
    if name not in running:
        sys.exit(1)
    if "State.Running" in fmt:
        print("true")
    elif ".Id" in fmt:
        # Deterministic: each `docker` call is a new process, so Python's
        # hash() would differ per invocation and never match a fixture path.
        print("deadbeef-" + name)
    sys.exit(0)
sys.exit(0)
"""

# `sleep` is the only reason a phase takes wall-clock time; returning at once
# keeps the suite at seconds. The pacing arithmetic under test is untouched.
SLEEP_SHIM = """\
#!/usr/bin/env bash
if [ -n "${H39_SLEEP_LOG:-}" ]; then echo "sleep $*" >> "$H39_SLEEP_LOG"; fi
exit 0
"""

# Serves the two Flink REST shapes collect_vertex_metrics needs and records
# every URL, so a test can count requests per vertex.
CURL_SHIM = """\
#!/usr/bin/env python3
import json, os, re, sys
url = sys.argv[-1]
with open(os.environ["H39_CURL_LOG"], "a") as fh:
    fh.write(url + "\\n")
if "/jobs/" in url and "/vertices/" not in url:
    print(json.dumps({"vertices": [
        {"id": "vid1", "name": "op_alpha"},
        {"id": "vid2", "name": "op_beta"},
        {"id": "vid3", "name": "op_gamma"},
    ]}))
    sys.exit(0)
m = re.search(r"/vertices/([^/]+)/", url)
if m:
    vid = m.group(1)
    get = url.split("get=")[-1] if "get=" in url else ""
    print(json.dumps([{"id": f"0.{name}", "value": "1"}
                      for name in get.split(",") if name]))
    sys.exit(0)
sys.exit(0)
"""


def code_lines(text: str) -> list[str]:
    """Lines that execute — comments excluded.

    The old PID names appear in the explanatory comments on purpose (they
    record WHY the probes changed), so a naive substring check would fail on
    the documentation rather than on the code.
    """
    return [ln for ln in text.splitlines()
            if ln.strip() and not ln.lstrip().startswith("#")]


# Every container the script probes: the 6 stack containers it resolves for
# cgroup sampling, plus the two pipeline containers that carry the data path.
# The dev cluster has all of them up, so this is the default; a test overrides
# H39_RUNNING to simulate a container that is absent or has died.
ALL_CONTAINERS = (
    "01_docker-fluss-tablet-1,01_docker-flink-taskmanager-1,01_docker-minio-1,"
    "01_docker-openobserve-1,01_docker-otel-collector-1,"
    "01_docker-flink-jobmanager-1,pipeline-faketool,pipeline-ingestion"
)

# Stands in for holistic-analyze.py. SCRIPT_DIR is derived from BASH_SOURCE, so
# a copy in the sandbox's own scripts directory intercepts the real call. The
# real analyzer is 1000+ lines of pandas work and is irrelevant here — these
# tests assert the WRAPPER's contract: process the phases and check the exit
# code. H39_ANALYZER_RC simulates the G6 latency guard failing.
ANALYZER_STUB = """\
import os, sys
print("## latency (stub analyzer)")
sys.exit(int(os.environ.get("H39_ANALYZER_RC", "0")))
"""


class Sandbox:
    """A throwaway tree laid out like the repo, holding the real script."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir(parents=True)
        self.scripts = root / "code/01_platform/04_scripts"
        self.scripts.mkdir(parents=True)
        (self.scripts / "tests").mkdir(parents=True)
        self.script = self.scripts / "holistic-measure.sh"
        shutil.copy2(SCRIPT_SRC, self.script)
        (self.scripts / "pipeline-lib.sh").write_text(STUB_LIB)
        # Stands in for the real analyzer. SCRIPT_DIR resolves to THIS directory
        # (it is derived from BASH_SOURCE), so the stub intercepts the call
        # without touching the repo. H39_ANALYZER_RC makes it fail on demand.
        (self.scripts / "holistic-analyze.py").write_text(ANALYZER_STUB)
        self.out = root / "phase-out"
        self.out.mkdir()
        self.cg_root = root / "cg"
        self.cg_root.mkdir()
        self.docker_log = root / "docker.log"
        self.docker_log.write_text("")
        self.curl_log = root / "curl.log"
        self.curl_log.write_text("")
        self.sleep_log = root / "sleep.log"
        self._shim("docker", DOCKER_SHIM)
        self._shim("sleep", SLEEP_SHIM)
        self._shim("curl", CURL_SHIM)

    def _shim(self, name: str, body: str) -> None:
        p = self.bin / name
        p.write_text(body)
        p.chmod(0o755)

    def env(self, **extra) -> dict:
        environ = dict(os.environ)
        environ["PATH"] = f"{self.bin}:{environ['PATH']}"
        environ.update({
            "H39_DOCKER_LOG": str(self.docker_log),
            "H39_CURL_LOG": str(self.curl_log),
            "H39_SLEEP_LOG": str(self.sleep_log),
            "H39_RUNNING": ALL_CONTAINERS,
            "OUT": str(self.out),
            "HOLISTIC_CG_ROOT": str(self.cg_root),
            "SMOKE_S": "1",
            "MAIN_S": "1",
            "POLL_S": "1",
            "WARMUP_S": "1",
            "RATE_HZ": "10",
            # The inject-window margin is calibrated for real phases (15s candle
            # window + one POLL_S sample). These suites run 1-3s phases that
            # could never host a real injection, so the margin is relaxed here;
            # TestInjectOffsetIsClamped sets it back to the real 20 to check the
            # documented arithmetic.
            "HOLISTIC_INJECT_MARGIN": "0",
        })
        environ.update(extra)
        return environ

    def run(self, script: Path | None = None, timeout: float = 60, **extra):
        return subprocess.run(
            ["bash", str(script or self.script)], capture_output=True, text=True,
            env=self.env(**extra), timeout=timeout,
        )

    def body_script(self, text: str, name: str, phase: str,
                    duration: str) -> Path:
        """The script's definitions plus a driver that calls run_phase."""
        body = text[:text.index(PHASES_MARKER)]
        p = self.scripts / name
        p.write_text(body + f'\nrun_phase "{phase}" "{duration}"\necho "PHASE_RC=$?"\n')
        return p

    def gate_script(self, text: str, name: str, driver: str) -> Path:
        """The script's definitions plus an arbitrary driver.

        The two gate definitions (`smoke_inject_gate`, `fingerprint_gate`) live
        BELOW the phases marker, interleaved with the phase driver — so a plain
        "cut at the marker" truncates them away. This keeps every definition and
        drops only the driver's own executable lines:

          * `run_phase` and the warm-up/cgroup helpers are above the marker;
          * the PHASE_OUT/OUT setup is needed by both gates;
          * the phase invocations and the driver's gate CALLS are execution;
          * the gate DEFINITIONS are kept — they are the units under test.
        """
        head = text[:text.index('if OUT="$PHASE_OUT/smoke" run_phase smoke')]
        gates = text[text.index("smoke_inject_gate() {"):
                     text.index('\nif ! OUT="$PHASE_OUT/main" run_phase main')]
        # Drop the driver's `if ! <gate>; then ... fi` invocations (multi-line,
        # so the whole block has to go), keeping only the definitions.
        gates = re.sub(r"\nif ! \w+[^\n]*\n(?:.*?\n)*?fi\n", "\n", gates)
        p = self.scripts / name
        p.write_text(head + "\n" + gates + "\n" + driver)
        return p

    def docker_calls(self, fmt_fragment: str) -> list[str]:
        return [ln for ln in self.docker_log.read_text().splitlines()
                if fmt_fragment in ln]

    def vertex_metric_calls(self) -> list[str]:
        """URLs that fetch per-vertex metrics (the fan-out under test)."""
        return [ln for ln in self.curl_log.read_text().splitlines()
                if "/vertices/" in ln and "metrics?" in ln]

    def evidence_dirs(self) -> list[Path]:
        base = self.root / "logs/tracker-14"
        return sorted(base.iterdir()) if base.exists() else []

    def write_cgroup_fixture(self, container: str, body: str) -> None:
        """A fixture io.stat at the path the script builds from HOLISTIC_CG_ROOT.

        The shim answers `inspect --format {{.Id}}` with `deadbeef-<name>`, so
        the script looks for `<CG_ROOT>/docker-deadbeef-<name>.scope/io.stat`.
        """
        scope = self.cg_root / f"docker-deadbeef-{container}.scope"
        scope.mkdir(parents=True, exist_ok=True)
        (scope / "io.stat").write_text(body)


def tsv_rows(path: Path) -> list[dict[str, str]]:
    """TSV rows keyed by column NAME from the header, never by index.

    The header is the contract; an index-keyed assertion silently keeps passing
    if the columns are ever reordered, which is the exact failure this wave is
    fixing.
    """
    lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
    if not lines:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, ln.split("\t"))) for ln in lines[1:]]


class Wave36Case(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="w36-"))
        self.sb = Sandbox(self.tmp)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def pre_fix_liveness(self, text: str) -> str:
        """Restore the two pre-fix `kill -0` probes at their own call sites.

        This is the red leg for the liveness tests: without it,
        `assertNotIn("unbound variable")` would pass for any reason at all,
        including a phase that never reaches the poll loop.
        """
        old = text
        old = old.replace('container_running "$LIB_FAKETOOL_CONTAINER"',
                          'kill -0 "$FAKETOOL_PID"')
        old = old.replace('container_running "$LIB_INGESTION_CONTAINER"',
                          'kill -0 "$JVM_PID"')
        self.assertNotEqual(old, text, "the pre-fix liveness text was not found")
        return old

    def gate_evidence(self, dups: str, late: str, want_dups: int,
                      want_late: int):
        """Drive smoke_inject_gate with fabricated evidence.

        smoke_inject_gate reads "$PHASE_OUT/smoke", and PHASE_OUT is the
        timestamped run directory the script creates under $OUT/logs, NOT $OUT
        itself. The driver below pins PHASE_OUT to a directory this test owns,
        so the evidence and the gate agree on where to look.
        """
        phase_out = self.sb.out / "phase"
        d = phase_out / "smoke"
        d.mkdir(parents=True, exist_ok=True)
        (d / "faketool.log").write_text(
            f"INJECT round=1 token=1 dups={want_dups} late={want_late} "
            f"late_ms=1\n")
        # The counters are cumulative series; first sample then last sample.
        (d / "tm-prom-dedup-late.tsv").write_text(
            f"1 flink_x_compute_dedup_duplicates 0\n"
            f"1 flink_x_compute_candles_late_dropped 0\n"
            f"2 flink_x_compute_dedup_duplicates {dups}\n"
            f"2 flink_x_compute_candles_late_dropped {late}\n")
        p = self.sb.gate_script(
            SCRIPT_SRC.read_text(), "hm-gate.sh",
            f'PHASE_OUT="{phase_out}"\n'
            'smoke_inject_gate\necho "GATE_RC=$?"\n')
        return self.sb.run(p)


class TestLivenessProbesContainers(Wave36Case):
    """P6-008/P6-009 — the data path is containers, not host PIDs."""

    def test_a_dead_loadgen_container_ends_the_phase_by_name(self):
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-live.sh", "smoke", "3")

        res = self.sb.run(drv, H39_RUNNING="pipeline-ingestion")

        self.assertNotIn(
            "unbound variable", res.stderr,
            "the phase died on an unbound variable instead of reporting the "
            "dead container:\n" + res.stderr)
        self.assertIn("pipeline-faketool", res.stderr,
                      "the failure does not name the dead container:\n" + res.stderr)
        self.assertIn("PHASE_RC=1", res.stdout, res.stdout[-2000:])

    def test_a_dead_ingestion_container_ends_the_phase_by_name(self):
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-live2.sh", "smoke", "3")

        res = self.sb.run(drv, H39_RUNNING="pipeline-faketool")

        self.assertNotIn("unbound variable", res.stderr, res.stderr)
        self.assertIn("pipeline-ingestion", res.stderr, res.stderr)
        self.assertIn("PHASE_RC=1", res.stdout, res.stdout[-2000:])

    def test_the_pre_fix_liveness_poll_really_dies_on_an_unbound_variable(self):
        """Red leg — proves the changed lines are what fixes this."""
        old = self.pre_fix_liveness(SCRIPT_SRC.read_text())
        drv = self.sb.body_script(old, "hm-red-live.sh", "smoke", "1")

        res = self.sb.run(drv)

        self.assertIn("unbound variable", res.stderr,
                      "the red leg did not reproduce the original death:\n"
                      + res.stderr)

    def test_no_host_pid_is_referenced_by_any_executable_line(self):
        """Acceptance criterion 1 — no host-PID read remains."""
        offenders = [ln.strip() for ln in code_lines(SCRIPT_SRC.read_text())
                     if any(n in ln for n in ("JVM_PID", "FAKETOOL_PID", "BPID"))]
        self.assertEqual(offenders, [], f"host-PID reads remain: {offenders}")


class TestPerPhaseResolutionIsHoisted(Wave36Case):
    """P6-107 — resolution runs once per PHASE, not once per poll."""

    def test_container_resolution_does_not_scale_with_poll_count(self):
        """One resolution per container per phase, whatever the phase length.

        Counts `{{.Id}}` (the resolution format), not every inspect: the
        liveness probe uses `{{.State.Running}}` and MUST still run per poll, so
        a flat total would be unachievable and would not mean "hoisted".
        """
        text = SCRIPT_SRC.read_text()
        short = self.sb.body_script(text, "hm-short.sh", "smoke", "1")
        long_ = self.sb.body_script(text, "hm-long.sh", "smoke", "3")

        res_short = self.sb.run(short)
        short_resolutions = len(self.sb.docker_calls("{{.Id}}"))
        short_liveness = len(self.sb.docker_calls("{{.State.Running}}"))

        self.sb.docker_log.write_text("")
        res_long = self.sb.run(long_)
        long_resolutions = len(self.sb.docker_calls("{{.Id}}"))
        long_liveness = len(self.sb.docker_calls("{{.State.Running}}"))

        # Both phases must have reached the poll loop, or the counts are vacuous.
        self.assertIn("PHASE_RC=0", res_short.stdout, res_short.stderr[-2000:])
        self.assertIn("PHASE_RC=0", res_long.stdout, res_long.stderr[-2000:])

        self.assertGreater(short_resolutions, 0,
                           "no {{.Id}} resolution ran at all")
        self.assertEqual(
            short_resolutions, long_resolutions,
            "container-ID resolution scales with the poll count — it is still "
            "inside the loop")
        self.assertGreater(long_liveness, short_liveness,
                           "no per-poll liveness calls were observed, so this "
                           "test cannot see loop behaviour at all")


def proc_io_rows(path: Path) -> list[dict[str, str]]:
    """proc-io.tsv rows as {label, read, write} — the analyzer's own shape.

    The file is space-separated (not tab), so it gets its own reader rather
    than reusing tsv_rows.
    """
    rows = []
    for ln in path.read_text().splitlines():
        m = re.match(r"^(\d+) (\w+) read_bytes: (\d+) write_bytes: (\d+)$",
                     ln.strip())
        if m:
            rows.append({"epoch": m.group(1), "label": m.group(2),
                         "read": m.group(3), "write": m.group(4)})
    return rows


class TestThroughputColumnsAreCorrect(Wave36Case):
    """P6-106 — read and write land in their own columns."""

    def test_the_real_emitter_row_lands_in_the_right_columns(self):
        """Feed pipeline-lib.sh's actual output shape through the real script.

        L1253 emits `print(name, "|", read, "|", write)`, which joins its
        arguments with single spaces — so the row is `src_raw | 12345 | 67890`.
        The old `-F'| '` split that into $1=src_raw $2='|' $3=12345, putting the
        READ count in the write column. The finding's proposed `-F' \\| '` is
        byte-identical (gawk resolves `\\|` to a plain `|`), so it fixes nothing.
        """
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-cols.sh", "smoke", "1")

        self.sb.run(drv, H39_DUMP="src_raw | 12345 | 67890")

        rows = tsv_rows(self.sb.out / "throughput.tsv")
        alpha = [r for r in rows if r["operator"] == "src_raw"]
        self.assertTrue(alpha, f"no src_raw row was written: {rows}")
        self.assertEqual(alpha[0]["read"], "12345",
                         "the read count is not in the read column")
        self.assertEqual(alpha[0]["write"], "67890",
                         "the write count is not in the write column")
        self.assertEqual(alpha[0]["state"], "RUNNING")

    def test_the_separator_never_leaks_into_a_value(self):
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-cols2.sh", "smoke", "1")

        self.sb.run(drv, H39_DUMP="src_raw | 12345 | 67890")

        for row in tsv_rows(self.sb.out / "throughput.tsv"):
            for col in ("operator", "read", "write", "state"):
                self.assertNotIn("|", row[col],
                                 f"the separator leaked into {col}: {row}")


class TestProcIoRowsAreWellFormed(Wave36Case):
    """P6-007/P6-407 — one integer row per container per poll."""

    TABLET = "01_docker-fluss-tablet-1"

    def test_a_multi_device_cgroup_produces_one_summed_row(self):
        """A cgroup with two devices must sum, not emit two newlines.

        `print $i` returned one line per device, so _r/_w carried embedded
        newlines and one sample spanned several TSV rows.
        """
        self.sb.write_cgroup_fixture(
            self.TABLET,
            "8:0 rbytes=1000 wbytes=2000 rios=1 wios=2\n"
            "8:16 rbytes=500 wbytes=300 rios=3 wios=4\n")
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-io.sh", "smoke", "1")

        self.sb.run(drv)

        tablet = [r for r in proc_io_rows(self.sb.out / "proc-io.tsv")
                  if r["label"] == "tablet"]
        self.assertEqual(len(tablet), 1,
                         f"expected one tablet row, got {len(tablet)}")
        self.assertEqual(tablet[0]["read"], "1500", "reads not summed")
        self.assertEqual(tablet[0]["write"], "2300", "writes not summed")

    def test_a_device_without_wbytes_still_reports_a_row(self):
        self.sb.write_cgroup_fixture("01_docker-minio-1", "8:0 rbytes=77 rios=9\n")
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-io2.sh", "smoke", "1")

        self.sb.run(drv)

        minio = [r for r in proc_io_rows(self.sb.out / "proc-io.tsv")
                 if r["label"] == "minio"]
        self.assertTrue(minio, "the minio row vanished entirely")
        self.assertEqual(minio[0]["read"], "77")
        self.assertEqual(minio[0]["write"], "0",
                         "a missing wbytes must read 0, not crash")

    def test_no_jvm_or_bridge_row_is_written(self):
        """P6-007 — /proc/<pid>/io is unreadable; a host bridge is a policy
        violation, so both rows are unreachable and were deleted."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-proc.sh", "smoke", "1")

        # The bridge row's old `pgrep -f arrow-bridge` would have matched this
        # process if it still existed.
        self.sb.run(drv, H39_DUMP="src_raw | 1 | 2")

        labels = {r["label"] for r in proc_io_rows(self.sb.out / "proc-io.tsv")}
        self.assertNotIn("jvm", labels, "the jvm row survives")
        self.assertNotIn("bridge", labels, "the bridge row survives")

    def test_the_data_path_containers_are_sampled(self):
        """The rows that replaced them must actually carry data."""
        self.sb.write_cgroup_fixture(
            "pipeline-ingestion", "8:0 rbytes=4242 wbytes=8888 rios=1 wios=1\n")
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-proc2.sh", "smoke", "1")

        self.sb.run(drv)

        ing = [r for r in proc_io_rows(self.sb.out / "proc-io.tsv")
               if r["label"] == "ingestion"]
        self.assertTrue(ing, "the ingestion container is never sampled")
        self.assertEqual(ing[0]["read"], "4242")

    def test_labels_stay_within_the_analyzers_word_class(self):
        """The analyzer's regex is (\\d+) (\\w+): a hyphen or space in a label
        silently drops every row for that container."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-io4.sh", "smoke", "1")

        self.sb.run(drv)

        text = (self.sb.out / "proc-io.tsv").read_text()
        for ln in text.splitlines():
            if ln.strip():
                self.assertRegex(ln.strip(),
                                 r"^\d+ \w+ read_bytes: \d+ write_bytes: \d+$")


class TestFailedPurgeFailsClosed(Wave36Case):
    """P6-102 — a failed purge stops the run before any measurement.

    Both edits are required together: while the caller exported
    ALLOW_STALE_TABLE=true, `pipeline_purge_raw_table` fell off the end and
    returned 0, so changing only `|| true` to `|| return 1` changed nothing.
    """

    def test_a_failed_purge_stops_the_phase_before_ingestion(self):
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-purge.sh", "smoke", "1")

        res = self.sb.run(drv, H39_PURGE_FAILS="1")

        self.assertIn("PHASE_RC=1", res.stdout,
                      "the phase continued after a failed purge:\n"
                      + res.stdout[-2000:] + res.stderr[-2000:])
        self.assertNotIn("INGESTION_STARTED", res.stderr,
                         "ingestion started despite the purge failing")

    def test_the_operator_can_still_opt_in_explicitly(self):
        """The opt-in survives — only the DEFAULT flips.

        This is what makes the record COMPATIBLE_WITH_LIMITATION rather than
        INCOMPATIBLE: an operator who sets ALLOW_STALE_TABLE=true themselves
        still gets the old behaviour.
        """
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-purge2.sh", "smoke", "1")

        res = self.sb.run(drv, H39_PURGE_FAILS="1", ALLOW_STALE_TABLE="true")

        self.assertIn("INGESTION_STARTED", res.stderr,
                      "the explicit opt-in no longer works:\n" + res.stderr[-2000:])
        self.assertIn("PHASE_RC=0", res.stdout, res.stdout[-2000:])

    def test_the_script_does_not_force_the_opt_in_on_its_own(self):
        """The removal must not be replaced by an explicit `=false` export.

        `export ALLOW_STALE_TABLE=false` would override an operator's own
        opt-in from the environment, breaking the documented opt-in.
        """
        offenders = [ln.strip() for ln in code_lines(SCRIPT_SRC.read_text())
                     if "ALLOW_STALE_TABLE" in ln]
        self.assertEqual(
            offenders, [],
            "the script still assigns ALLOW_STALE_TABLE; deleting the export "
            f"is what lets the library default (and an operator's opt-in) "
            f"apply: {offenders}")

    def test_a_successful_purge_still_proceeds_normally(self):
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-purge3.sh", "smoke", "1")

        res = self.sb.run(drv)

        self.assertIn("PHASE_RC=0", res.stdout, res.stdout[-2000:])


class TestMetricFanOutIsBatched(Wave36Case):
    """P6-409 — one request per vertex, not one per metric."""

    def test_each_vertex_is_fetched_once_per_sample(self):
        """3 vertices must cost 3 requests, not 18.

        The old body opened a separate urllib request per metric (6 per vertex),
        so a 20-vertex cluster issued 120 sequential HTTP calls inside a loop
        whose cadence was POLL_S.
        """
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-fan.sh", "smoke", "1")

        self.sb.run(drv)

        urls = self.sb.vertex_metric_calls()
        self.assertTrue(urls, "no per-vertex metric requests were made at all")
        per_vertex: dict[str, int] = {}
        for u in urls:
            vid = u.split("/vertices/")[1].split("/")[0]
            per_vertex[vid] = per_vertex.get(vid, 0) + 1
        self.assertEqual(
            set(per_vertex), {"vid1", "vid2", "vid3"},
            f"expected all three vertices to be sampled, got {per_vertex}")
        for vid, n in per_vertex.items():
            self.assertEqual(n, 1,
                             f"vertex {vid} was fetched {n} times in one sample "
                             f"— the fan-out was not batched: {urls}")

    def test_the_batched_request_asks_for_every_metric(self):
        """Batching must not silently drop metrics from the contract."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-fan2.sh", "smoke", "1")

        self.sb.run(drv)

        url = self.sb.vertex_metric_calls()[0]
        for metric in ("busyTimeMsPerSecond", "backPressuredTimeMsPerSecond",
                       "idleTimeMsPerSecond", "latencyP50", "latencyP95",
                       "latencyP99"):
            self.assertIn(metric, url,
                          f"{metric} is missing from the batched request: {url}")

    def test_a_missing_metric_is_an_absent_row_not_a_failure(self):
        """Best-effort contract: the phase must not fail on metric trouble."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-fan3.sh", "smoke", "1")

        res = self.sb.run(drv)

        self.assertIn("PHASE_RC=0", res.stdout, res.stdout[-2000:])


class TestPacingIsDeadlineBased(Wave36Case):
    """P6-408 — a slow poll body must not silently stretch the cadence."""

    def test_no_sleep_happens_when_the_body_already_consumed_the_budget(self):
        """A body that outruns POLL_S must not sleep at all.

        The old flat `sleep "$POLL_S"` ADDED the body's own duration to every
        interval, so a slow body stretched the sampling cadence without saying
        so. With an absolute deadline the sleep is simply skipped.
        """
        # A curl shim that really consumes 2s per call. It uses the ABSOLUTE
        # /bin/sleep so it is not swallowed by our own sleep shim.
        self.sb._shim("curl", """\
#!/usr/bin/env python3
import json, os, re, subprocess, sys
url = sys.argv[-1]
with open(os.environ["H39_CURL_LOG"], "a") as fh:
    fh.write(url + "\\n")
subprocess.run(["/bin/sleep", "2"])
if "/jobs/" in url and "/vertices/" not in url:
    print(json.dumps({"vertices": [{"id": "vid1", "name": "op_alpha"}]}))
    sys.exit(0)
print(json.dumps([{"id": "0.busyTimeMsPerSecond", "value": "1"}]))
sys.exit(0)
""")
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-pace.sh", "smoke", "1")

        res = self.sb.run(drv, POLL_S="1")

        # The body took >POLL_S, so the deadline had already passed and no
        # PACING sleep should have been issued. The warm-up `sleep "$WARMUP_S"`
        # is a separate, expected sleep — so exactly one remains, and the
        # overrun is reported instead of being hidden.
        sleeps = [s for s in self.sb.sleep_log.read_text().splitlines() if s.strip()]
        self.assertEqual(sleeps, ["sleep 1"],
                         "expected only the warm-up sleep; a pacing sleep "
                         "means the deadline was ignored: " + repr(sleeps))
        self.assertIn("overran its", res.stderr,
                      "the overrun was not reported:\n" + res.stderr)

    def test_the_pacing_uses_an_absolute_deadline(self):
        """The sleep argument must be the REMAINING time, not a flat POLL_S.

        Proven structurally: the source must compute the sleep from a deadline
        difference. A flat `sleep "$POLL_S"` cannot express "sleep only what is
        left", which is the whole fix.
        """
        text = SCRIPT_SRC.read_text()
        body = [ln.strip() for ln in code_lines(text) if "sleep" in ln]
        flat = [ln for ln in body if ln == 'sleep "$POLL_S"']
        self.assertEqual(
            flat, [],
            f"a flat per-iteration sleep remains — the cadence can still be "
            f"stretched by a slow poll body: {flat}")
        self.assertTrue(
            any(re.search(r"sleep \$\(\(next - now\)\)", ln) for ln in body),
            f"no deadline-relative sleep found: {body}")

    def test_an_overrun_is_reported_rather_than_silent(self):
        """An under-sampled run must be visible in the log."""
        text = SCRIPT_SRC.read_text()
        self.assertIn("overran its", text,
                      "an overrun is not reported anywhere, so a run that "
                      "sampled sparser than POLL_S looks normal")


class TestFingerprintGateFailsClosed(Wave36Case):
    """P6-411 — a missing java.out must FAIL, not silently pass."""

    def gate(self, build) -> Path:
        """Build the phase directory, then pin PHASE_OUT to it.

        The gate reads "$dir/j1/java.out" where dir is the argument it is
        given — but the generated script also sets its own timestamped
        PHASE_OUT. These tests pass the directory explicitly so the evidence
        and the assertion cannot drift apart.
        """
        phase_out = self.sb.out / "phase"
        (phase_out / "j1").mkdir(parents=True, exist_ok=True)
        build(phase_out / "j1" / "java.out")
        return self.sb.gate_script(
            SCRIPT_SRC.read_text(), "hm-fp.sh",
            f'fingerprint_gate "{phase_out}"\necho "GATE_RC=$?"\n')

    def test_a_missing_java_out_fails_the_gate(self):
        """The old code left `n` empty for a missing file and `${n:-0} -eq 0`
        PASSED — success reported for evidence never read."""
        drv = self.gate(lambda p: None)  # never created

        res = self.sb.run(drv)

        self.assertIn("GATE_RC=1", res.stdout,
                      "the gate passed with no java.out at all:\n"
                      + res.stdout[-1500:])
        self.assertIn("missing or empty", res.stderr, res.stderr)

    def test_an_empty_java_out_fails_the_gate(self):
        drv = self.gate(lambda p: p.write_text(""))

        res = self.sb.run(drv)

        self.assertIn("GATE_RC=1", res.stdout, res.stdout[-1500:])

    def test_a_clean_log_still_passes(self):
        """The guard must not false-fail a healthy phase."""
        drv = self.gate(lambda p: p.write_text(
            "HFT subscribed 1024 instruments\nall good\n"))

        res = self.sb.run(drv)

        self.assertIn("GATE_RC=0", res.stdout,
                      "a healthy log failed the gate:\n" + res.stdout[-1500:])
        self.assertIn("PASS", res.stdout, res.stdout)

    def test_a_mismatch_line_still_fails(self):
        drv = self.gate(lambda p: p.write_text(
            "manifest_fingerprint mismatch: go=abc java=def\n"))

        res = self.sb.run(drv)

        self.assertIn("GATE_RC=1", res.stdout, res.stdout[-1500:])


class TestInjectOffsetIsClamped(Wave36Case):
    """P6-405 — the injection must land inside the SAMPLED window."""

    # The documented defaults (holistic-measure.sh L44-54): WARMUP_S=45 and a
    # MARGIN of 20s. The margin is restored here because the sandbox default
    # relaxes it for short phases — this class checks the REAL arithmetic.
    REAL_WARMUP = "45"
    REAL_MARGIN = "20"

    def offset_for(self, duration: str) -> str:
        """Read the offset the REAL run_phase exports, not a copy of the formula.

        The stub lib's preflight reports the exported value, so the assertion
        covers the script's own arithmetic rather than re-deriving it here.
        """
        res = self.sb.run(
            self.sb.body_script(SCRIPT_SRC.read_text(), "hm-inj.sh",
                                "smoke", duration),
            WARMUP_S=self.REAL_WARMUP,
            HOLISTIC_INJECT_MARGIN=self.REAL_MARGIN,
            H39_OFFSET_PROBE="1",
        )
        # The stub lib announces the exported offset on stderr.
        m = re.search(r"OFFSET=(\d+)", res.stderr)
        self.assertIsNotNone(
            m, f"no offset captured for duration={duration}:\n{res.stdout}\n"
               f"{res.stderr}")
        return m.group(1)
        m = re.search(r"OFFSET=(\d+)", res.stdout)
        self.assertIsNotNone(m, f"no offset captured:\n{res.stdout}\n{res.stderr}")
        return m.group(1)

    def test_the_default_phase_keeps_the_verified_offset(self):
        """The live-verified 180s path must not move: 120000 exactly."""
        self.assertEqual(self.offset_for("180"), "120000",
                         "the default smoke's injection offset changed")

    def test_a_short_phase_clamps_instead_of_using_an_impossible_offset(self):
        """60s cannot host a 120s injection; it clamps to 85000."""
        self.assertEqual(self.offset_for("60"), "85000")

    def test_a_long_phase_keeps_the_default_offset(self):
        for d in ("300", "900"):
            self.assertEqual(self.offset_for(d), "120000",
                             f"a {d}s phase moved the verified offset")

    def test_the_offset_is_never_warmup_plus_120s(self):
        """Adding WARMUP_S on top was the bug: it silently became 165000."""
        for d in ("180", "300", "900"):
            self.assertNotEqual(
                self.offset_for(d), "165000",
                "the offset is being shifted by WARMUP_S on top of an "
                "already-absolute value")

    def test_a_phase_too_short_to_host_an_injection_is_rejected(self):
        """A 30s phase cannot host the 20s-margin window; fail fast, don't
        silently measure a window that cannot contain the injection."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-inj3.sh", "smoke", "30")

        res = self.sb.run(drv, HOLISTIC_INJECT_MARGIN=self.REAL_MARGIN)

        self.assertIn("too short to host an injection", res.stderr, res.stderr)
        # `fail` exits the whole script, so the driver's `echo PHASE_RC` never
        # runs — the non-zero exit IS the evidence that the phase stopped.
        self.assertNotEqual(res.returncode, 0,
                            "the phase continued past an impossible injection "
                            "window:\n" + res.stdout[-1500:])


class TestInjectGateTolerance(Wave36Case):
    """P6-410 — the delta comparison tolerates sampling jitter."""

    def gate(self, dups: str, late: str, want_dups: int, want_late: int) -> str:
        """Drive smoke_inject_gate with fabricated evidence."""
        return self.gate_evidence(dups, late, want_dups, want_late)

    def test_an_exact_match_passes(self):
        res = self.gate("200", "20", 200, 20)
        self.assertIn("GATE_RC=0", res.stdout, res.stdout[-1500:])

    def test_a_one_off_delta_passes(self):
        """A single missed sample is jitter, not a pipeline bug."""
        res = self.gate("199", "20", 200, 20)
        self.assertIn("GATE_RC=0", res.stdout,
                      "an off-by-one failed the gate:\n" + res.stdout[-1500:])

    def test_a_large_delta_still_fails(self):
        """A 50% shortfall is a real defect and must fail."""
        res = self.gate("100", "20", 200, 20)
        self.assertIn("GATE_RC=1", res.stdout, res.stdout[-1500:])
        self.assertIn("tolerance", res.stdout, res.stdout[-1500:])

    def test_the_tolerance_is_reported_on_every_run(self):
        """A tuned tolerance must stay auditable."""
        res = self.gate("200", "20", 200, 20)
        self.assertIn("tolerance", res.stdout,
                      "the tolerance is not printed:\n" + res.stdout)


class TestLateGateIsOneSided(Wave36Case):
    """P6-487 — the late-drop counter is a superset, so only shortfall fails.

    `compute.candles.late.dropped` counts every late/out-of-order drop the
    multi-TF aggregator makes, not just the injected frames. Run 9 (2026-09-17)
    measured it directly: the injected 20 landed on subtask 2 exactly (0 -> 20
    in the sample right after the injection fired), while subtask 1 had already
    accumulated 112 natural re-feeds BEFORE the injection and reached 226 by
    phase end. Summed across subtasks the gate saw 134 and failed a run whose
    injection was counted perfectly. These tests pin the arithmetic that
    replaces it.
    """

    def gate(self, dups: str, late: str, want_dups: int, want_late: int) -> str:
        return self.gate_evidence(dups, late, want_dups, want_late)

    def test_run_9s_natural_surplus_passes(self):
        """The exact shape that failed run 9: 134 counted, 20 injected."""
        res = self.gate("200", "134", 200, 20)

        self.assertIn("GATE_RC=0", res.stdout,
                      "natural feed re-feeds failed the gate:\n" + res.stdout)
        self.assertIn("natural-late(not injected)=114", res.stdout, res.stdout)

    def test_a_shortfall_still_fails(self):
        """Run 8's defect — the counter never moved, so the gate must fire."""
        res = self.gate("200", "0", 200, 20)

        self.assertIn("GATE_RC=1", res.stdout, res.stdout)
        # The gate's failure text goes to stderr (the phase result is stderr).
        self.assertIn("BELOW the injected 20", res.stderr, res.stderr)

    def test_an_exact_cover_reports_no_surplus(self):
        res = self.gate("200", "20", 200, 20)

        self.assertIn("GATE_RC=0", res.stdout, res.stdout)
        self.assertIn("natural-late(not injected)=0", res.stdout, res.stdout)

    def test_the_dedup_half_stays_two_sided(self):
        """Dedup drops ARE exclusively the injection, so over-count is real."""
        res = self.gate("250", "20", 200, 20)

        self.assertIn("GATE_RC=1", res.stdout,
                      "an over-counting dedup counter passed:\n" + res.stdout)


class TestAnalyzerInvocationIsGuarded(Wave36Case):
    """P6-105/P6-108 — the analyzer tail of the script."""

    def tail_script(self, name: str, driver: str) -> Path:
        """The script's definitions plus a driver standing in for the tail.

        The real tail runs after both phases have already executed, so it cannot
        be reached by running the script; the definitions are reused verbatim
        and only the phases are skipped.
        """
        text = SCRIPT_SRC.read_text()
        head = text[:text.index('if OUT="$PHASE_OUT/smoke" run_phase smoke')]
        # The analyzer tail: from the pipefail comment to the end of the rc
        # check. Reused verbatim so the assertions cover the real code path.
        start = text.index("# P6-108: CP is assigned only as a side effect")
        end = text.index('if [ "$analyze_rc" -ne 0 ]; then')
        end = text.index("fi", end) + 2
        tail = text[start:end]
        p = self.sb.scripts / name
        p.write_text(head + f'''
PHASE_OUT="$OUT/phase"
mkdir -p "$PHASE_OUT/main"
MAIN_START=1700000000
MAIN_END=1700000900
echo 1700000000 > "$PHASE_OUT/main/run-start-epoch"
echo 1700000900 > "$PHASE_OUT/main/run-end-epoch"
{driver}
{tail}
echo "TAIL_RC=0"
''')
        return p

    def test_an_empty_cp_fails_loudly_instead_of_analysing(self):
        """CP comes from pipeline_preflight as a side effect. Relying on an
        unset variable under `set -u` aborts with an opaque message; the guard
        must name the actual problem."""
        drv = self.tail_script("hm-cp.sh", "unset CP 2>/dev/null || true")

        res = self.sb.run(drv)

        self.assertNotEqual(res.returncode, 0, res.stdout)
        self.assertIn("CP is empty", res.stderr,
                      "the failure is not explained:\n" + res.stderr)
        self.assertNotIn("unbound variable", res.stderr,
                         "an opaque unset-variable abort instead of the guard:\n"
                         + res.stderr)

    def test_a_zero_analyzer_exit_lets_the_run_continue(self):
        """The guard must not false-fail a healthy analysis."""
        drv = self.tail_script("hm-cp-ok.sh", 'CP="0"')

        res = self.sb.run(drv)

        self.assertIn("TAIL_RC=0", res.stdout,
                      "a successful analysis stopped the run:\n"
                      + res.stdout + res.stderr)

    def test_a_failing_analyzer_still_fails_the_run_after_pipefail(self):
        """The G6 guard exits non-zero; `tee` must not swallow it."""
        drv = self.tail_script("hm-cp-fail.sh", 'CP="0"')

        res = self.sb.run(drv, H39_ANALYZER_RC="1")

        self.assertNotEqual(res.returncode, 0,
                            "a failing latency guard was swallowed:\n"
                            + res.stdout + res.stderr)
        self.assertIn("LATENCY GUARD FAILED", res.stderr, res.stderr)

    def test_pipefail_is_not_cleared_after_the_analyzer(self):
        """`set +o pipefail` permanently cleared the script-global mode."""
        src = SCRIPT_SRC.read_text()
        live = [ln for ln in src.splitlines()
                if "set +o pipefail" in ln and not ln.strip().startswith("#")]
        self.assertEqual(
            live, [],
            "pipefail is cleared at runtime, disabling the mode set at L38: "
            + repr(live))

    def test_the_dead_analyze_latency_helper_is_gone(self):
        """It had one occurrence in the repo (its own definition) and a
        2-argument shape the 4-argument call site no longer matches."""
        src = SCRIPT_SRC.read_text()
        live = [ln for ln in src.splitlines()
                if "analyze_latency" in ln and not ln.strip().startswith("#")]
        self.assertEqual(live, [],
                         "analyze_latency is still defined/called: "
                         + repr(live))


class TestKnobValidationFailsFast(Wave36Case):
    """P6-406 — invalid knobs fail by name, before any evidence is created."""

    def test_valid_knobs_do_not_trip_the_guard(self):
        """The defaults must survive the new validation."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-ok.sh", "smoke", "1")

        res = self.sb.run(drv)

        self.assertNotIn("FATAL", res.stderr, res.stderr)
        self.assertIn("PHASE_RC=0", res.stdout, res.stdout[-2000:])

    def assert_rejected(self, knob: str, value: str) -> None:
        res = self.sb.run(**{knob: value})

        self.assertNotEqual(res.returncode, 0,
                            f"{knob}={value} was accepted:\n" + res.stderr)
        self.assertIn(knob, res.stderr, res.stderr)
        self.assertEqual(self.sb.evidence_dirs(), [],
                         "an evidence directory was created for invalid input")

    def test_a_non_integer_duration_is_rejected_by_name(self):
        self.assert_rejected("SMOKE_S", "abc")

    def test_a_zero_poll_interval_is_rejected_by_name(self):
        self.assert_rejected("POLL_S", "0")

    def test_zero_warmup_is_rejected_by_name(self):
        self.assert_rejected("WARMUP_S", "0")

    def test_valid_knobs_still_pass_the_guard(self):
        """The guard must not reject the documented defaults."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-ok.sh", "smoke", "1")

        res = self.sb.run(drv)

        self.assertNotIn("FATAL", res.stderr, res.stderr)
        self.assertIn("PHASE_RC=0", res.stdout, res.stdout[-2000:])


class TestCandlePathIsWired(Wave36Case):
    """CHG-191 — the harness gates on the candle path, so it must run it.

    `compute.candles.late.dropped` has exactly ONE registration site, in
    `MultiTimeframeAggregateFunction`, and that operator is wired only when
    `MULTITF_ENABLED=true` (`SignalJob.java`). The 15s candle path that used to
    own the counter unconditionally was retired in `0f3e5952`, which touched no
    file under `04_scripts` — so the harness went on asserting a counter its own
    job graph no longer created and the inject gate could never pass (run 8,
    2026-09-17: the metric name had 0 occurrences in the whole scrape while the
    dedup half passed 200/200).

    These tests pin the coupling from BOTH ends: the harness must submit
    `MULTITF_ENABLED=true`, and the metric must still live in the gated
    operator. Either one drifting alone re-breaks the gate.
    """

    def test_the_submitted_job_gets_the_candle_path(self):
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-mt.sh", "smoke", "1")

        res = self.sb.run(drv)

        self.assertIn("SUBMIT MULTITF_ENABLED=true", res.stderr, res.stderr)

    def test_an_explicit_opt_out_is_refused_by_name(self):
        """A candle-less run cannot satisfy the gate; refuse it up front.

        Failing later with "late-drop counter 0 differs from injected 20 by 20"
        is the misleading shape this replaces: the counter is absent by
        construction, not short by 20.
        """
        res = self.sb.run(MULTITF_ENABLED="false")

        self.assertNotEqual(res.returncode, 0, res.stderr)
        self.assertIn("MULTITF_ENABLED", res.stderr, res.stderr)
        self.assertEqual(self.sb.evidence_dirs(), [],
                         "an evidence directory was created for a refused run")

    def test_candle_tables_are_purged_and_ensured_before_ingestion(self):
        """LOG-append tables accumulate across runs, and the job preflights
        both — so purge-then-ensure must precede ingestion start."""
        drv = self.sb.body_script(SCRIPT_SRC.read_text(), "hm-candle.sh", "smoke", "1")

        res = self.sb.run(drv)

        order = [ln for ln in res.stderr.splitlines()
                 if ln.startswith(("PURGE_TABLE=", "ENSURE_TABLE=",
                                   "INGESTION_STARTED"))]
        self.assertEqual(
            order,
            ["PURGE_TABLE=candle_closed", "PURGE_TABLE=candle_live",
             "ENSURE_TABLE=candle_live", "ENSURE_TABLE=candle_closed",
             "INGESTION_STARTED"],
            "candle-table provisioning must run, in this order, before "
            "ingestion starts:\n" + res.stderr[-2000:])

    def test_the_gated_metric_still_lives_in_the_gated_operator(self):
        """The cross-file half of the coupling.

        Pins that `compute.candles.late.dropped` is registered exactly once, in
        `MultiTimeframeAggregateFunction`, and that `SignalJob` wires that
        operator inside the `multiTfEnabled()` branch. If either moves, the
        harness's opt-in above stops being sufficient and this fails.
        """
        java_root = REPO / "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob"
        # Code lines only: SignalJob.java NAMES the metric in a comment (the
        # counter-export list), and a naive substring check fails on the
        # documentation instead of on the registration — the same trap the
        # module's code_lines() helper exists for.
        register = 'counter("compute.candles.late.dropped")'
        sites = [p.name for p in sorted(java_root.rglob("*.java"))
                 if any(register in ln for ln in code_lines(p.read_text()))]
        self.assertEqual(
            sites, ["MultiTimeframeAggregateFunction.java"],
            "the late-drop counter must have exactly one registration site — "
            "the inject gate's scrape and the analyzer's G7 audit both assume it")

        signal_job = (java_root / "SignalJob.java").read_text()
        self.assertIn(
            "if (config.multiTfEnabled()) {", signal_job,
            "SignalJob no longer gates the multi-TF branch on multiTfEnabled() — "
            "the harness's MULTITF_ENABLED=true opt-in is no longer what decides "
            "whether the late-drop counter exists")
        # Bounded at the branch's own `} else` (8-space indent) so a later
        # branch constructing the same operator cannot make this pass.
        gated = signal_job.split("if (config.multiTfEnabled()) {", 1)[1]
        gated = gated.partition("\n        } else")[0]
        self.assertIn(
            "new MultiTimeframeAggregateFunction(", gated,
            "the multi-tf aggregator moved out of the multiTfEnabled() branch")


if __name__ == "__main__":
    unittest.main()
