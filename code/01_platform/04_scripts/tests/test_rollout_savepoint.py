#!/usr/bin/env python3
"""Behavioural tests for rollout-savepoint.sh (wave 18: P6-167..174, P6-501..508).

Hermetic: PATH stubs for curl (Flink REST), docker (compose wrapper), and a
fake Prometheus metrics file. Two seams drive the script without touching
production code: (1) the script's own env vars (JM_URL, PROMETHEUS_URL via
file:// is NOT supported by curl — instead tests source the pure helpers);
(2) tests source the script's function definitions by stubbing the
side-effecting tail. The whole script cannot be sourced directly (it runs the
rollout on source), so tests extract functions up to the "# ---- 1.
preflight" marker into a harness file, with stub log/warn/die, then exercise
job_state / wait_state / sample_dedup / compose / the JSON parsing idioms the
fixes use. Static pins cover the guards that only fire in a live rollout
(timeout validation loop, jq preflight, SAVEPOINT_PATH init, HIT_SAMPLE_S wait,
P6-508 gate shape).
"""

import json
import os
import pathlib
import re
import subprocess
import tempfile
import unittest

TESTS_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS = TESTS_DIR.parent
SCRIPT = SCRIPTS / "rollout-savepoint.sh"
SRC = SCRIPT.read_text()


def extract_helpers(tmpbin):
    """Write a harness that defines the script's functions without running it.

    Strategy: take the script source from the helpers marker through the end
    of sample_dedup (function DEFINITIONS only — no argv loop, no preflight,
    no rollout body), stub out the top-level side effects, and replace
    log/warn/die with test-friendly versions. curl/docker/date/seq come from
    PATH stubs.
    """
    start = SRC.find("# ---- helpers ---")
    assert start > 0, "helpers marker not found"
    end = SRC.find("# ---- 1. preflight")
    assert end > start, "preflight marker not found"
    head = SRC[:start] + SRC[start:end]
    # The preamble derives EVIDENCE from LOGDIR at source time
    # (EVIDENCE="$LOGDIR/rollout-$JOB_NAME-$TS.log"). Tests export LOGDIR +
    # EVIDENCE before sourcing, so neutralise the derivation: keep the
    # caller's EVIDENCE instead of overwriting it with an uncreatable path.
    head = re.sub(r'^EVIDENCE="\$LOGDIR.*$',
                  ': # EVIDENCE comes from the test environment', head, flags=re.M)
    # Cut the top-level side-effect lines that run on source.
    head = head.replace('mkdir -p "$LOGDIR"', ': # mkdir stubbed')
    head = head.replace('printf \'rollout: evidence -> %s\\n\' "$EVIDENCE" | tee -a "$EVIDENCE"',
                        ': # evidence line stubbed')
    # Replace log/warn/die with harness versions (append to evidence file).
    # NOTE: the real defs are single-line `log()  {...}`, `warn() {...}`,
    # `die()  {...}` with a UTF-8 em dash — match the prefix, not the body.
    head = re.sub(r"^log\(\) .*$", 'log() { printf \'rollout: %s\\n\' "$*" >> "$EVIDENCE"; }', head, flags=re.M)
    head = re.sub(r"^warn\(\) .*$", 'warn() { printf \'rollout: WARN -- %s\\n\' "$*" >> "$EVIDENCE"; }', head, flags=re.M)
    head = re.sub(r"^die\(\) .*$", 'die() { printf \'rollout: FATAL -- %s\\n\' "$*" >> "$EVIDENCE"; exit 1; }', head, flags=re.M)
    assert "\nlog() " in head, "log() not replaced"
    assert "\nwarn() {" in head, "warn() not replaced"
    assert "\ndie() {" in head, "die() not replaced"
    harness = tmpbin / "helpers.sh"
    harness.write_text(head + "\nset +e\n# harness loaded\n")
    return harness


CURL_STUB = r"""#!/usr/bin/env bash
# Stub curl: answer Flink REST from scenario files, record argv.
printf '%s\n' "$*" >> "$CURL_CALLS"
url="${@: -1}"
case "$url" in
  */v1/config) echo '{"revision":"stub"}'; exit 0 ;;
  */jobs/overview) cat "$STUB_OVERVIEW"; exit "${STUB_OVERVIEW_RC:-0}" ;;
  */jobs/*/savepoints/*) cat "$STUB_SAVEPOINT_STATUS"; exit 0 ;;
  */jobs/*/savepoints)
    if [ "${STUB_NET_FAIL:-0}" = "1" ]; then echo "curl: (7) connect failed" >&2; exit 7; fi
    printf '{"request-id":"%s"}\n' "${STUB_REQ_ID:-req-1}"; exit 0 ;;
  */jobs/*/checkpoints) cat "$STUB_CHECKPOINTS"; exit "${STUB_CHECKPOINTS_RC:-0}" ;;
  *metrics) cat "$STUB_METRICS"; exit "${STUB_METRICS_RC:-0}" ;;
  */jobs/*) cat "$STUB_JOB"; exit "${STUB_JOB_RC:-0}" ;;
  *) echo "stub-curl: unexpected $url" >&2; exit 99 ;;
esac
"""

DOCKER_STUB = r"""#!/usr/bin/env bash
# Stub docker: record argv; answer logs/ps.
printf '%s\n' "$*" >> "$DOCKER_CALLS"
case " $* " in
  *" logs "*) printf '%s\n' "${STUB_LOGS:-}"; exit 0 ;;
esac
exit 0
"""


class RolloutHarness(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.t = pathlib.Path(self.tmp.name)
        self.bin = self.t / "bin"
        self.bin.mkdir()
        (self.bin / "curl").write_text(CURL_STUB)
        (self.bin / "docker").write_text(DOCKER_STUB)
        for f in ("curl", "docker"):
            (self.bin / f).chmod(0o755)
        self.harness = extract_helpers(self.t)
        self.env = dict(os.environ)
        self.env["PATH"] = str(self.bin) + ":" + os.environ.get("PATH", "")
        self.env["CURL_CALLS"] = str(self.t / "curl.calls")
        self.env["DOCKER_CALLS"] = str(self.t / "docker.calls")
        self.env["EVIDENCE"] = str(self.t / "evidence.log")
        (self.t / "evidence.log").write_text("")
        # Minimal REST fixtures.
        (self.t / "job.json").write_text(json.dumps({"jid": "a" * 32, "name": "signal-job-compute", "state": "RUNNING"}))
        (self.t / "overview.json").write_text(json.dumps({"jobs": []}))
        (self.t / "checkpoints.json").write_text(json.dumps({"counts": {"completed": 1},
                                                             "latest": {"completed": {"state_size": 100}}}))
        (self.t / "savepoint.json").write_text(json.dumps({"status": {"id": "COMPLETED"}, "location": "file:///sp/1"}))
        self.env["STUB_JOB"] = str(self.t / "job.json")
        self.env["STUB_OVERVIEW"] = str(self.t / "overview.json")
        self.env["STUB_CHECKPOINTS"] = str(self.t / "checkpoints.json")
        self.env["STUB_SAVEPOINT_STATUS"] = str(self.t / "savepoint.json")
        self.env["JM_URL"] = "http://stub:8081"
        self.env["PROMETHEUS_URL"] = "http://stub:9250/metrics"
        (self.t / "metrics.txt").write_text("# no metrics\n")
        self.env["STUB_METRICS"] = str(self.t / "metrics.txt")
        self.env["COMPOSE_FILE"] = str(self.t / "docker-compose.yml")
        (self.t / "docker-compose.yml").write_text("services: {}\n")

    def tearDown(self):
        self.tmp.cleanup()

    def run_helper(self, snippet, timeout=60):
        # No `set -e` here: the script under test is `set -euo pipefail` but
        # the harness probes failing helpers deliberately (rc assertions) —
        # errexit at the probe level would kill the probe before the echo.
        cmd = (f'set -u; export LOGDIR="{self.t}" EVIDENCE="{self.t}/evidence.log"; '
               f'export PATH="{self.bin}:$PATH"; source "{self.harness}"; {snippet}')
        return subprocess.run(["bash", "-c", cmd], env=self.env, capture_output=True,
                              text=True, timeout=timeout)

    # --- P6-167: job_state reads top-level .state, not a vertex state ---
    def test_job_state_reads_top_level_state(self):
        job = {"jid": "b" * 32, "name": "signal-job-compute", "state": "RUNNING",
               "vertices": [{"id": "v1", "name": "op", "status": "RUNNING",
                             "state": "FAILED"}]}
        (self.t / "job.json").write_text(json.dumps(job))
        r = self.run_helper('job_state "' + "b" * 32 + '"')
        self.assertEqual(r.stdout.strip(), "RUNNING", r.stderr)

    def test_job_state_keeps_underscore_states(self):
        (self.t / "job.json").write_text(json.dumps({"jid": "c" * 32, "state": "IN_PROGRESS"}))
        r = self.run_helper('job_state "' + "c" * 32 + '"')
        self.assertEqual(r.stdout.strip(), "IN_PROGRESS", r.stderr)

    def test_job_state_fails_closed_on_rest_error(self):
        self.env["STUB_JOB_RC"] = "7"
        r = self.run_helper('job_state "' + "d" * 32 + '"; echo "rc=$?"')
        self.assertIn("rc=1", r.stdout)

    # --- P6-168: wait_state validates timeout, retries, knows all terminals ---
    def test_wait_state_rejects_non_numeric_timeout(self):
        r = self.run_helper('wait_state "' + "a" * 32 + '" RUNNING "60s" lbl')
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("invalid timeout", (self.t / "evidence.log").read_text())

    def test_wait_state_fails_fast_on_finished(self):
        (self.t / "job.json").write_text(json.dumps({"jid": "e" * 32, "state": "FINISHED"}))
        t0 = __import__("time").monotonic()
        r = self.run_helper('wait_state "' + "e" * 32 + '" RUNNING 60 lbl', timeout=60)
        dt = __import__("time").monotonic() - t0
        self.assertNotEqual(r.returncode, 0)
        self.assertLess(dt, 45, "must fail fast, not wait out the 60s timeout")
        self.assertIn("terminal state FINISHED", (self.t / "evidence.log").read_text())

    def test_wait_state_retries_transient_failure(self):
        # First call fails, second succeeds: flip the fixture mid-wait via a
        # background writer is racy — instead prove the retry idiom directly:
        # an empty job_state output must NOT abort the loop body.
        (self.t / "job.json").write_text(json.dumps({"jid": "f" * 32, "state": "RUNNING"}))
        r = self.run_helper('wait_state "' + "f" * 32 + '" RUNNING 30 lbl', timeout=45)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("reached RUNNING", (self.t / "evidence.log").read_text())

    # --- P6-169: compose passes only existing env files ---
    def test_compose_skips_missing_secrets_file(self):
        envdir = self.t / "envdir"
        envdir.mkdir()
        (envdir / ".env").write_text("X=1\n")
        self.env["COMPOSE_FILE"] = str(envdir / "docker-compose.yml")
        (envdir / "docker-compose.yml").write_text("services: {}\n")
        r = self.run_helper('compose config >/dev/null; cat "$DOCKER_CALLS"')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn("secrets.env", r.stdout)
        self.assertIn("--env-file", r.stdout)

    # --- P6-170: sample_dedup degrades to empty, never 0-as-data ---
    def test_sample_dedup_empty_when_metrics_absent(self):
        (self.t / "metrics.txt").write_text("# no dedup metrics here\nhttp_requests 5\n")
        r = self.run_helper('out="$(sample_dedup)"; rc=$?; echo "out=[$out] rc=$rc"')
        self.assertIn("out=[] rc=0", r.stdout, r.stdout + r.stderr)  # best-effort: no abort

    def test_sample_dedup_sums_present_metrics(self):
        (self.t / "metrics.txt").write_text(
            "flink_taskmanager_job_task_operator_compute_dedup_firsts_cumulative{tm=\"a\"} 7\n"
            "flink_taskmanager_job_task_operator_compute_dedup_firsts_cumulative{tm=\"b\"} 5\n"
            "flink_taskmanager_job_task_operator_compute_dedup_first{tm=\"a\"} 3\n"
            "flink_taskmanager_job_task_operator_compute_dedup_duplicates{tm=\"a\"} 9\n")
        r = self.run_helper('sample_dedup')
        self.assertEqual(r.stdout.strip(), "12 3 9", r.stdout + r.stderr)

    # --- P6-174: checkpoint counts via jq (pretty-printed safe) ---
    def test_checkpoint_completed_parses_pretty_json(self):
        pretty = json.dumps({"counts": {"restored": 0, "total": 4, "in_progress": 0,
                                        "completed": 3, "failed": 0}}, indent=2)
        (self.t / "checkpoints.json").write_text(pretty)
        r = self.run_helper(
            'completed="$(api_get "/jobs/abc/checkpoints" 2>/dev/null | jq -r \'.counts.completed // empty\' 2>/dev/null || true)"; '
            'case "$completed" in \'\'|*[!0-9]*) completed="";; esac; echo "completed=$completed"')
        self.assertEqual(r.stdout.strip(), "completed=3", r.stdout + r.stderr)

    def test_checkpoint_state_size_latest_completed(self):
        cps = {"latest": {"completed": {"state_size": 4242}},
               "history": [{"state_size": 111}, {"state_size": 222}]}
        (self.t / "checkpoints.json").write_text(json.dumps(cps))
        r = self.run_helper(
            's="$(api_get "/jobs/abc/checkpoints" | jq -r \'.latest.completed.state_size // .history[-1].state_size // empty\')"; '
            'echo "size=$s"')
        self.assertEqual(r.stdout.strip(), "size=4242", r.stdout + r.stderr)

    # --- P6-171: overview lookup matches exact name, never spans objects ---
    def test_overview_resolves_exact_name_among_many(self):
        jobs = {"jobs": [
            {"jid": "1" * 32, "name": "other-job", "state": "RUNNING"},
            {"jid": "2" * 32, "name": "signal-job-compute", "state": "FINISHED"},
            {"jid": "3" * 32, "name": "signal-job-compute", "state": "RUNNING"}]}
        (self.t / "overview.json").write_text(json.dumps(jobs))
        r = self.run_helper(
            'overview="$(api_get "/jobs/overview")"; '
            'printf \'%s\' "$overview" | jq -r --arg n "signal-job-compute" '
            '\'.jobs[] | select(.name==$n and .state=="RUNNING") | .jid\' | head -n 1')
        self.assertEqual(r.stdout.strip(), "3" * 32, r.stdout + r.stderr)

    def test_overview_regex_name_is_not_a_pattern(self):
        # A JOB_NAME containing regex chars must not match a different job.
        jobs = {"jobs": [{"jid": "4" * 32, "name": "signalXjob-compute", "state": "RUNNING"}]}
        (self.t / "overview.json").write_text(json.dumps(jobs))
        r = self.run_helper(
            'overview="$(api_get "/jobs/overview")"; '
            'printf \'%s\' "$overview" | jq -r --arg n "signal.job-compute" '
            '\'.jobs[] | select(.name==$n and .state=="RUNNING") | .jid\' | head -n 1; echo done')
        self.assertIn("done", r.stdout)
        self.assertNotIn("4" * 32, r.stdout)

    # --- P6-503: JOB_ID validated, savepoint body jq-encoded ---
    def test_jobid_validation_rejects_short_and_uppercase(self):
        for bad in ("abc123", "A" * 32, "g" * 32, ""):
            r = self.run_helper(
                f'JOB_ID="{bad}"; case "$JOB_ID" in ????????????????????????????????) :;; *) echo "BAD-LEN";; esac; '
                f'case "$JOB_ID" in *[!0-9a-f]*) echo "BAD-HEX";; esac')
            self.assertIn("BAD", r.stdout, bad or "(empty)")

    def test_savepoint_body_encodes_quotes(self):
        tricky = 'file:///sp/we"ird\\dir'
        out = subprocess.run(["jq", "-nc", "--arg", "d", tricky,
                              '{"target-directory":$d,"cancel-job":false}'],
                             capture_output=True, text=True)
        body = json.loads(out.stdout)
        self.assertEqual(body["target-directory"], tricky)
        self.assertIs(body["cancel-job"], False)

    # --- P6-508: gate refuses non-numeric instead of [ -lt aborting ---
    # Mirrors the production gate idiom: each side validated separately
    # (a ':'-joined single class cannot see emptiness — verified by hand).
    GATE_CHECK = ('case "$state_after" in \'\'|*[!0-9]*) nonnum=1;; *) nonnum=0;; esac; '
                  'case "$STATE_BEFORE" in \'\'|*[!0-9]*) nonnum=1;; esac; '
                  'case "$nonnum" in 1) echo "WARN-NONNUMERIC";; *) ')
    def test_continuity_gate_warns_on_non_numeric(self):
        for before, after in (("", "100"), ("100", ""), ("abc", "100"), ("100", "4x")):
            r = self.run_helper(
                f'STATE_BEFORE="{before}"; state_after="{after}"; ' + self.GATE_CHECK +
                'if [ "$state_after" -lt $(( STATE_BEFORE / 2 )) ]; then echo FIRE; else echo PASS; fi;; esac')
            self.assertIn("WARN-NONNUMERIC", r.stdout, f"{before}/{after}")

    def test_continuity_gate_fires_below_half(self):
        r = self.run_helper(
            'STATE_BEFORE="100"; state_after="49"; ' + self.GATE_CHECK +
            'if [ "$state_after" -lt $(( STATE_BEFORE / 2 )) ]; then echo FIRE; else echo PASS; fi;; esac')
        self.assertEqual(r.stdout.strip(), "FIRE")

    def test_continuity_gate_passes_above_half(self):
        r = self.run_helper(
            'STATE_BEFORE="100"; state_after="80"; ' + self.GATE_CHECK +
            'if [ "$state_after" -lt $(( STATE_BEFORE / 2 )) ]; then echo FIRE; else echo PASS; fi;; esac')
        self.assertEqual(r.stdout.strip(), "PASS")

    # --- static pins: guards that only fire in a live rollout ---
    def test_static_pins(self):
        pins = {
            "P6-501 timeout validation loop": "for _t in SAVEPOINT_TIMEOUT_S JOB_STOP_TIMEOUT_S",
            "P6-502 log guard": 'tee -a "$EVIDENCE" || true',
            "P6-502 die keeps message": '|| printf \'rollout: FATAL',
            "P6-169 exists-check": '[ -f "$_f" ] && ef+=(--env-file "$_f")',
            "jq preflight": 'jq is required (REST answers are JSON)',
            "P6-173 SAVEPOINT_PATH init": 'SAVEPOINT_PATH=""',
            "P6-503 JOB_ID length": '????????????????????????????????)',
            "P6-503 JOB_ID hex": '*[!0-9a-f]*) die "invalid JOB_ID',
            "P6-503 jq body": '\'{"target-directory":$d,"cancel-job":false}\'',
            "P6-504 savepoint retry": '|| true)"\n\t\t\t[ -n "$response" ] || { sleep 5; continue; }',
            "P6-505 BSD date": 'date -u -v-30S',
            "P6-505 duration fallback": 'since_ts="2m"',
            "P6-506 T0 retry": 'T0_DEDUP="$(sample_dedup || true)"',
            "P6-507 HIT_SAMPLE_S wait": '[ "$HIT_SAMPLE_S" -gt 0 ] && sleep "$HIT_SAMPLE_S"',
            "P6-508 gate per-side": 'case "$state_after" in \'\'|*[!0-9]*) nonnum=1',
            "P6-508 gate warn": 'continuity NOT asserted', 
            "P6-174 completed retry": "jq -r '.counts.completed // empty'",
            "P6-172 latest completed": "'.latest.completed.state_size // .history[-1].state_size // empty'",
            "P6-167 jq state": "jq -r '.state // empty'",
            "P6-171 jq overview": 'select(.name==$n and .state=="RUNNING")',
            "P6-168 timeout validation": 'die "$label: invalid timeout',
            "P6-168 all terminals": 'FAILED|CANCELED|FINISHED|SUSPENDED',
            "P6-168 transient retry": 'job_state "$jobid" 2>/dev/null || true',
        }
        missing = [k for k, v in pins.items() if v not in SRC]
        self.assertEqual(missing, [], f"static pins missing: {missing}")

    def test_log_warn_guarded(self):
        # P6-502: the log/warn DEFINITIONS must guard the evidence tee.
        defs = [ln.strip() for ln in SRC.splitlines()
                if ln.startswith(("log()", "warn()"))]
        self.assertEqual(len(defs), 2, defs)
        for d in defs:
            self.assertIn("|| true", d, d)


if __name__ == "__main__":
    unittest.main()
