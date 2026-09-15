"""Wave-32 regression tests for stage-soak-e2e.sh
(P6-217, P6-218, P6-219, P6-220, P6-566, P6-567, P6-568, P6-569, P6-570, P6-571).

The script's body drives a live stack (Fluss, Flink, three ingestion
containers), so the gates are not reachable by launching it. Instead each
gate's own lines are extracted from the real file and run in isolation with
stub `docker`/`curl`/`java` on PATH — extraction is done by anchoring on the
gate's opening and closing lines, so a gate that is deleted or renamed fails
the test that needs it rather than silently testing nothing.

`W32_STAGE_SCRIPT` points at the script under test; it defaults to the real
repository file and exists so the red leg can run against the pre-wave copy.

Tests marked `# disc` fail against the pre-wave script.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

def _repo_root() -> Path:
    """The checkout holding code/01_platform/04_scripts.

    Searched from this file first, then from the working directory: while the
    suite is being drafted in /tmp the file path alone cannot find the repo.
    """
    for start in (Path(__file__).resolve(), Path.cwd().resolve()):
        for parent in [start, *start.parents]:
            if (parent / "code" / "01_platform" / "04_scripts").is_dir():
                return parent
    raise RuntimeError("cannot locate the repository root (code/01_platform/04_scripts)")


ROOT = _repo_root()

SCRIPT = Path(os.environ.get(
    "W32_STAGE_SCRIPT",
    ROOT / "code" / "01_platform" / "04_scripts" / "stage-soak-e2e.sh"))

BASH = "/bin/bash"


def script_text() -> str:
    return SCRIPT.read_text()


def _line(prefix: str) -> str:
    """The one script line starting with `prefix` (anchors are unique by design)."""
    hits = [l for l in script_text().splitlines() if l.startswith(prefix)]
    assert len(hits) == 1, f"prefix {prefix!r} matched {len(hits)} lines in {SCRIPT.name}"
    return hits[0]


def block(start: str, end: str, *, count_from: str | None = None,
          include_start: bool = True, include_end: bool = True) -> str:
    """Return the lines from `start` through the next `end` line.

    `start` may be a literal line or the unique prefix of one (via `_line`).
    `count_from`, when given, is the line the closing `end` is matched from —
    needed when the same line-closing token appears inside the block.
    """
    lines = script_text().splitlines()
    if start not in lines:
        start = _line(start)
    end = end if end in lines else _line(end)
    i = lines.index(start)
    j = lines.index(_line(count_from)) if count_from else i
    while j < len(lines) and lines[j] != end:
        j += 1
    if j == len(lines):
        raise AssertionError(f"closing anchor not found after {start!r}: {end!r}")
    lo = i if include_start else i + 1
    hi = j + 1 if include_end else j
    return "\n".join(lines[lo:hi])


PRELUDE = """
set -uo pipefail
OUT="$W32_OUT"
DURATION_S="${DURATION_S:-300}"
RATE_HZ="${RATE_HZ:-20}"
PER_SLICE="${PER_SLICE:-811}"
UNIVERSE_ROWS="${UNIVERSE_ROWS:-2433}"
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
LIB_SECRETS_FILE="${LIB_SECRETS_FILE:-/nonexistent/secrets.env}"
fatal() { echo "SOAK-E2E: FAIL — $*" >&2; printf 'FAIL\\n%s\\n' "$*" > "$OUT/FAILURE.txt"; exit 1; }
"""


class _Base(unittest.TestCase):
    """Shared scaffolding: a scratch dir, stub binaries and a runner."""

    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.tmp = Path(self._dir.name)
        self.out = self.tmp / "capture"
        self.out.mkdir(parents=True)
        self.bindir = self.tmp / "bin"
        self.bindir.mkdir()

    def stub(self, name: str, body: str) -> None:
        p = self.bindir / name
        p.write_text("#!/bin/bash\n" + body)
        p.chmod(0o755)

    def run_snippet(self, snippet: str, *, env: dict | None = None, timeout: int = 60):
        script = self.tmp / "case.sh"
        script.write_text(PRELUDE + snippet + "\n")
        run_env = dict(os.environ)
        # `command -v` must see only our stubs: /usr/bin and /bin still answer
        # for binaries we did not stub, so PATH is exactly the stub dir plus
        # the system path the harness itself needs.
        run_env["PATH"] = f"{self.bindir}:/usr/bin:/bin"
        run_env["W32_OUT"] = str(self.out)
        if env:
            run_env.update(env)
        return subprocess.run([BASH, str(script)], capture_output=True, text=True,
                              env=run_env, timeout=timeout)

    def input(self, name: str, rows: int, header: str = "Symbol,OptionType,X,Token") -> Path:
        p = self.tmp / name
        body = [header] + [f"SYM{i},CE,0.0,{1000 + i}" for i in range(rows)]
        p.write_text("\n".join(body) + "\n")
        return p


class TestSequenceGates(_Base):
    """P6-217: universe size drives the slice arithmetic, not literals."""

    def slice_snippet(self) -> str:
        return PRELUDE + block(_line('head -1 "$NSE" | grep -q'),
                               "done", count_from='i=0 && for s in')

    def universe_snippet(self) -> str:
        return PRELUDE + block('UNIVERSE_ROWS="$(tail -n +2 "$NSE" | grep -c . || true)"',
                               'PER_SLICE=$((UNIVERSE_ROWS / 3))')

    def test_universe_rows_and_per_slice_are_derived(self) -> None:  # disc
        nse = self.input("u.csv", 2433)
        r = self.run_snippet(self.universe_snippet() + '\necho "ROWS=$UNIVERSE_ROWS SLICE=$PER_SLICE"\n',
                     env={"NSE": str(nse)})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("ROWS=2433 SLICE=811", r.stdout)

    def test_grown_universe_changes_the_slice(self) -> None:  # disc
        nse = self.input("u.csv", 3630)
        r = self.run_snippet(self.universe_snippet() + '\necho "SLICE=$PER_SLICE"\n', env={"NSE": str(nse)})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("SLICE=1210", r.stdout)

    def test_universe_not_divisible_by_three_is_a_clear_failure(self) -> None:  # disc
        nse = self.input("u.csv", 2500)
        r = self.run_snippet(self.universe_snippet(), env={"NSE": str(nse)})
        self.assertEqual(r.returncode, 1)
        self.assertIn("not divisible by 3", r.stderr)

    def test_empty_universe_is_a_clear_failure(self) -> None:  # disc
        nse = self.input("u.csv", 0)
        r = self.run_snippet(self.universe_snippet(), env={"NSE": str(nse)})
        self.assertEqual(r.returncode, 1)
        self.assertIn("no instrument rows", r.stderr)

    def test_three_slices_produce_three_manifests(self) -> None:  # pin
        nse = self.input("u.csv", 2433)
        r = self.run_snippet(self.slice_snippet() + '\necho "MANIFESTS=$(ls "$OUT"/manifest-*.csv | wc -l)"\n',
                     env={"NSE": str(nse), "PER_SLICE": "811", "UNIVERSE_ROWS": "2433"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("MANIFESTS=3", r.stdout)
        for i, want in enumerate((811, 811, 811)):  # the derived count, pinned
            got = subprocess.run(["bash", "-c", f'tail -n +2 "$1" | grep -c .',
                                  "_", str(self.out / f"manifest-{i}.csv")],
                                 capture_output=True, text=True).stdout.strip()
            self.assertEqual(got, str(want))

    def test_a_fourth_slice_is_rejected_not_ignored(self) -> None:  # disc
        """PRE-WAVE: split produced 4 slices; only aa/ab/ac were loaded, so the
        run claimed full coverage while ignoring 1/4 of the universe."""
        nse = self.input("u.csv", 3244)   # 811 x 4
        r = self.run_snippet(self.slice_snippet() + '\necho "MANIFESTS=$(ls "$OUT"/manifest-*.csv | wc -l)"\n',
                     env={"NSE": str(nse), "PER_SLICE": "811", "UNIVERSE_ROWS": "3244"})
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("expected 3", r.stderr)

    def test_missing_header_column_is_rejected(self) -> None:
        nse = self.input("u.csv", 2433, header="Symbol,Expiry,X,Token")
        r = self.run_snippet(self.slice_snippet(), env={"NSE": str(nse)})
        self.assertEqual(r.returncode, 1)
        self.assertIn("OptionType", r.stderr)


class TestPollGates(_Base):
    """P6-567: REST polls validate their reply and stop on terminal states."""

    def tm_poll_snippet(self) -> str:
        # Through the gate's own verdict line, so a poll that never sees a
        # registered TM is exercised end to end.
        return PRELUDE + 'reg=""\n' + \
               block('reg="" && for i in $(seq 1 12); do', '[ "${reg:-0}" -ge 1 ]') + \
               '\necho "REG=${reg:-<unset>}"\n'

    def job_poll_snippet(self) -> str:
        return PRELUDE + 'JOB_ID="abc123"\nstate=""\n' + \
               block('state="" && for i in $(seq 1 40); do', '[ "$state" = "RUNNING" ]') + \
               '\necho "STATE=${state:-<unset>}"\n'

    def test_tm_poll_accepts_a_valid_reply(self) -> None:
        self.stub("curl", 'echo \'{"taskmanagers":[{"id":"a"}]}\'\n')
        r = self.run_snippet(self.tm_poll_snippet())
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("REG=1", r.stdout)

    def test_tm_poll_reports_a_malformed_reply_and_keeps_trying(self) -> None:  # disc
        """PRE-WAVE: a non-JSON body produced an empty reg, which compared as an
        integer — "integer expression expected" instead of a retry."""
        self.stub("curl", 'echo "not json at all"\n')
        # The gate's own retry cadence (12 x sleep 5) is not what is under test
        # here, so the sleeps are collapsed.
        snippet = self.tm_poll_snippet().replace("sleep 5", "sleep 0")
        r = self.run_snippet(snippet, env={"FLINK_REST_URL": "http://x"})
        self.assertEqual(r.returncode, 1)
        self.assertNotIn("integer expression expected", r.stderr)
        self.assertIn("TM never registered", r.stderr)
        self.assertTrue((self.out / "tm-poll.err").exists(), "the poll reason is not kept")
        self.assertIn("TM registration poll 1/12", r.stdout)

    def test_tm_poll_retries_then_succeeds(self) -> None:
        state = self.tmp / "n"
        state.write_text("0")
        self.stub("curl", f'n=$(cat {state}); n=$((n+1)); echo $n > {state}\n'
                          'if [ "$n" -ge 3 ]; then echo \'{"taskmanagers":[{"id":"a"}]}\'; '
                          'else echo "boom"; fi\n')
        r = self.run_snippet(self.tm_poll_snippet().replace("sleep 5", "sleep 0.1"))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("REG=1", r.stdout)

    def test_job_poll_fails_fast_on_a_terminal_state(self) -> None:  # disc
        """PRE-WAVE: FAILED was polled for the full 200s then reported as
        "never RUNNING"."""
        self.stub("curl", 'echo \'{"state":"FAILED"}\'\n')
        r = self.run_snippet(self.job_poll_snippet())
        self.assertEqual(r.returncode, 1)
        self.assertIn("state=FAILED", r.stderr)
        self.assertIn("exceptions", r.stderr)

    def test_job_poll_accepts_running(self) -> None:
        self.stub("curl", 'echo \'{"state":"RUNNING"}\'\n')
        r = self.run_snippet(self.job_poll_snippet())
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("STATE=RUNNING", r.stdout)


class TestCredentials(_Base):
    """P6-218: no credential literals in the container argv."""

    def docker_run_block(self) -> str:
        return block('  docker run -d --name "e2e-ingestion-$i" --network "$LIB_TRADING_NET" \\',
                     '  LOG_PIDS="$LOG_PIDS $!"')

    def test_no_secret_literals_remain(self) -> None:  # disc
        body = self.docker_run_block()
        for literal in ("testd-pass", "JBSWY3DPEHPK3PXP", "ARROW_APP_SECRET=testd",
                        "ARROW_PASSWORD=testd"):
            self.assertNotIn(literal, body, f"{literal} is still in the container argv")

    def test_secrets_come_from_the_env_file(self) -> None:  # disc
        body = self.docker_run_block()
        self.assertIn('--env-file "$LIB_SECRETS_FILE"', body)
        self.assertIn("SECRETS_VIA_ENV_FILE=1", body)

    def test_the_ids_are_overridable_not_baked(self) -> None:  # disc
        body = self.docker_run_block()
        self.assertIn('${ARROW_APP_ID:-testd}', body)
        self.assertIn('${ARROW_USER_ID:-testd-user}', body)


class TestTickGate(_Base):
    """P6-219/P6-220: the tick leg is an absolute, self-contained rate gate."""

    def tick_snippet(self) -> str:
        # Both the pre-wave and post-wave revisions have these two anchors, so
        # the extraction succeeds on both and the red leg exercises real
        # behaviour rather than failing on a missing anchor.
        return PRELUDE + block('DRAIN_EPOCH="$(date +%s)"', "PYEOF") + "\ndone\n"

    def write_ticks(self, container: str, count: int, tokens: int, *,
                    total: int | None = None, hist_per_token: int = 1,
                    stragglers: dict | None = None) -> None:
        """Write a tick file whose per-token counts are `count` ticks each."""
        rows = []
        for t in range(1, tokens + 1):
            n = count * hist_per_token
            if stragglers and t in stragglers:
                n = stragglers[t]
            rows.append(f"t={t}:n={n}")
        body = " ".join(rows)
        if total is None:
            total = sum(int(r.split(":n=")[1]) for r in rows)
        (self.out / f"{container}.tick-counts.txt").write_text(
            f"arrow-tick-counts: total={total} chunk=0/1 {body}\n")

    def live_container(self, started_s_ago: int) -> None:
        # The script parses StartedAt with `date -d`, so the stub prints a real
        # timestamp that many seconds in the past.
        self.stub("docker",
                  f'if [ "$1" = inspect ]; then\n'
                  f'  echo "$(date -d "@$(( $(date +%s) - {started_s_ago} ))" +%Y-%m-%dT%H:%M:%SZ)"\n'
                  f'fi\n')

    def test_a_healthy_run_passes(self) -> None:  # disc
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 811)
        r = self.run_snippet(self.tick_snippet(), env={"PER_SLICE": "811", "RATE_HZ": "20",
                                               "DURATION_S": "300"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("ticks OK", r.stdout)
        self.assertIn("== sum", r.stdout)

    def test_a_uniform_stall_fails_the_absolute_gate(self) -> None:  # disc
        """PRE-WAVE: 10 Hz instead of 20 Hz is perfectly uniform, so the old
        uniformity-only check PASSED a feed running at half rate."""
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 3000, 811)   # 3000/20 = 150s of 310s
        r = self.run_snippet(self.tick_snippet(),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("GLOBAL rate shortfall", r.stderr)

    def test_a_partial_token_coverage_fails(self) -> None:  # disc
        """PRE-WAVE: a container reporting only 100 of its 811 tokens passed."""
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 100)
        r = self.run_snippet(self.tick_snippet(),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("covers 100 tokens, expected 811", r.stderr)

    def test_a_header_total_that_disagrees_with_the_rows_fails(self) -> None:  # disc
        """PRE-WAVE: `total` was parsed and printed but never checked."""
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 811, total=1)
        r = self.run_snippet(self.tick_snippet(),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("sum to", r.stderr)

    def test_a_per_token_straggler_still_fails(self) -> None:
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 811,
                             stragglers={7: 5900, 9: 100})
        r = self.run_snippet(self.tick_snippet(),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("outside 2.0% of median", r.stderr)

    def test_a_lifetime_beyond_the_budget_is_config_failure_not_rate_failure(self) -> None:  # disc
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 811)
        r = self.run_snippet(self.tick_snippet().replace('TICK_STARTUP_BUDGET_S="${TICK_STARTUP_BUDGET_S:-45}"',
                                                 'TICK_STARTUP_BUDGET_S="${TICK_STARTUP_BUDGET_S:-notanumber}"'),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 1, r.stdout + r.stderr)
        self.assertIn("must be a non-negative integer", r.stderr)

    def test_a_small_startup_skew_is_accepted(self) -> None:
        self.live_container(303)   # 3s of idle, seen in the archive
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 811)
        r = self.run_snippet(self.tick_snippet(),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_the_missing_file_still_fails_closed(self) -> None:
        self.live_container(310)
        for c in range(3):
            self.write_ticks(f"e2e-ingestion-{c}", 6000, 811)
        (self.out / "e2e-ingestion-1.tick-counts.txt").unlink()
        r = self.run_snippet(self.tick_snippet(),
                     env={"PER_SLICE": "811", "RATE_HZ": "20", "DURATION_S": "300"})
        self.assertEqual(r.returncode, 2)
        self.assertIn("missing/empty", r.stderr)


class TestSourceGates(_Base):
    """Gates that are structural rather than driveable inline."""

    def test_no_developer_absolute_path(self) -> None:  # disc
        for line in script_text().splitlines():
            if line.lstrip().startswith("#"):
                continue
            self.assertNotIn("/home/saurabh/", line,
                             f"developer path in executable line: {line.strip()}")

    def test_the_universe_comes_from_the_library_default(self) -> None:  # disc
        self.assertIn('NSE="${NSE_PATH:-$LIB_MANIFEST}"', script_text())

    def test_log_mirror_pids_are_tracked_and_killed(self) -> None:  # disc
        text = script_text()
        self.assertIn('LOG_PIDS="$LOG_PIDS $!"', text)
        cleanup = block("cleanup() {", "trap cleanup EXIT")
        self.assertIn('for p in $LOG_PIDS; do kill "$p"', cleanup)

    def test_a_failed_job_cancel_is_reported(self) -> None:  # disc
        cleanup = block("cleanup() {", "trap cleanup EXIT")
        self.assertIn("could not cancel SignalJob", cleanup)

    def test_the_submit_side_effect_is_validated(self) -> None:  # disc
        self.assertIn('[ -n "${JOB_ID:-}" ] || fatal "pipeline_submit_job returned success but JOB_ID is empty',
                      script_text())

    def test_container_liveness_is_checked_while_waiting(self) -> None:  # disc
        text = script_text()
        self.assertGreaterEqual(text.count("{{.State.Running}}"), 2,
                                "readiness and subscription gates must check liveness")

    def test_the_n7_census_parse_is_anchored(self) -> None:  # disc
        # Comment lines are skipped: the fix's own comment names the old parser.
        code = "\n".join(l for l in script_text().splitlines()
                         if not l.lstrip().startswith("#"))
        self.assertNotIn("cut -d= -f3", code)
        self.assertIn("gate_n=\"$(printf '%s\\n' \"$gate_out\" | sed -n", code)

    def test_the_n7_probe_stderr_is_kept(self) -> None:  # disc
        self.assertIn("n7-gate-$gate_table.err", script_text())

    def test_the_subscription_gate_uses_the_derived_slice_size(self) -> None:  # disc
        self.assertIn('grep -q "HFT subscribed $PER_SLICE"', script_text())

    def test_the_reported_universe_size_is_not_hardcoded(self) -> None:  # disc
        for line in script_text().splitlines():
            if line.lstrip().startswith("#"):
                continue
            self.assertNotRegex(line, r"stocks=2433|3x811",
                                f"hardcoded universe size: {line.strip()}")


if __name__ == "__main__":
    unittest.main()
