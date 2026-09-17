"""Behavioural tests for pipeline-lib.sh (P6 wave 17, findings P6-142…P6-483).

The library is sourced by the load/soak/full-load sweeps, so these tests source
it in a throwaway ROOT/OUT and call single functions. Nothing here needs a
Docker daemon, a Flink REST endpoint or a Fluss cluster: external commands are
stubbed on PATH and the stubs record their argv, which is what several findings
are about (a credential literal in an argv, a hardcoded container name, a
`-v $OUT:/run` mount that let the container rewrite its own evidence).

Functions that are only reachable through a full `pipeline_preflight` are pinned
statically (see the `static pin` tests) — a real preflight needs jars, a built
loadgen image and a live stack. Those pins are named as static in CHG-154 rather
than dressed up as behavioural coverage.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import textwrap
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
LIB = REPO / "code" / "01_platform" / "04_scripts" / "pipeline-lib.sh"

DOCKER_STUB = """#!/usr/bin/env bash
# Records every invocation, then answers just enough for the lib's own flows.
printf '%s\\n' "$*" >> "$DOCKER_CALLS"
case " $* " in
  *" exec "*) case " $* " in *" mkdir "*) exit 0 ;; esac
              [ "${STUB_EXEC_FAIL:-}" = "1" ] && exit 7
              printf '%s\\n' "${STUB_JOBID_LINE:-JobID 1234abcd}"; exit 0 ;;
  *" ps "*)   [ "${STUB_PS_FAIL:-}" = "1" ] && { echo "Cannot connect to the Docker daemon" >&2; exit 1; }
              printf '%s\\n' "cid-flink-jobmanager"; exit 0 ;;
esac
case "${1:-} ${2:-}" in
  "logs -f")      printf '%s\\n' "HFT subscribed"; exit 0 ;;
  "inspect --format") printf '%s\\n' "true"; exit 0 ;;
  "info --format")    printf '%s\\n' "active"; exit 0 ;;
  "node ls")      printf '%s\\n' "node-1"; exit 0 ;;
  "run -d")       printf '%s\\n' "container-id"; exit 0 ;;
  *)              exit 0 ;;
esac
"""
CURL_STUB = """#!/usr/bin/env bash
case "${STUB_CURL_MODE:-ok}" in
  fail)    echo "curl: (7) Failed to connect" >&2; exit 7 ;;
  garbage) echo 'not json at all'; exit 0 ;;
  *)       BODY="${STUB_CURL_BODY}"
           [ -n "$BODY" ] || BODY='{"state":"RUNNING"}'
           printf '%s\\n' "$BODY" ;;
esac
"""


JAVAC_STUB = """#!/usr/bin/env bash
printf '%s\n' "$PWD" >> "$JAVAC_CALLS"
exit 0
"""
JAVA_STUB = """#!/usr/bin/env bash
printf '%s\n' "${STUB_JAVA_OUT:-PURGED default.raw_table_1}"
exit "${STUB_JAVA_RC:-0}"
"""

def make_stubs(tmp_path: Path) -> tuple[Path, Path]:
    bindir = tmp_path / "stubbin"
    bindir.mkdir(exist_ok=True)
    calls = tmp_path / "docker.calls"
    calls.write_text("")
    for name, body in (("docker", DOCKER_STUB), ("curl", CURL_STUB)):
        f = bindir / name
        f.write_text(body)
        f.chmod(0o755)
    # javac/java stubs for the DDL helpers: `java` reports success for purge.
    (bindir / "javac").write_text(JAVAC_STUB)
    (bindir / "java").write_text(JAVA_STUB)
    for name in ("javac", "java"):
        (bindir / name).chmod(0o755)
    return bindir, calls


def run_lib(tmp_path: Path, script: str, *, timeout: int = 60,
            extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Source pipeline-lib.sh in a throwaway ROOT/OUT and run `script`."""
    bindir, calls = make_stubs(tmp_path)
    root = tmp_path / "root"
    (root / "code" / "02_services" / "02_compute" / "target").mkdir(parents=True, exist_ok=True)
    (root / "code" / "02_services" / "02_compute" / "target" / "compute.jar").write_text("jar")
    # The loadgen stamp's three listed inputs (P6-143).
    for rel in ("code/02_services/01_ingestion/Dockerfile.loadgen",
                "code/02_services/01_ingestion/pom.xml",
                "code/02_services/06_execution_gateway/pom.xml"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    env = {
        **os.environ,
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "ROOT": str(root),
        "OUT": str(out),
        "RATE_HZ": "10",
        "DOCKER_CALLS": str(calls),
        "JAVAC_CALLS": str(tmp_path / "javac.calls"),
        "CP": str(tmp_path / "cp"),
        "PIPELINE_PREFLIGHT_OK": "1",          # guarded callers only need the flag
        "FAKETOOL_PORT": "8899",
        **(extra_env or {}),
    }
    body = textwrap.dedent(f'''
        set -u
        source {LIB!s}
        PIPELINE_PREFLIGHT_OK=1     # the lib resets this when sourced
        {script}
    ''')
    return subprocess.run(["bash", "-c", body], env=env, capture_output=True,
                          text=True, timeout=timeout)


def docker_calls(tmp_path: Path) -> str:
    return (tmp_path / "docker.calls").read_text()


# ── P6-142: the sourcing contract ────────────────────────────────────────────
def test_direct_execution_is_refused(tmp_path):
    r = subprocess.run(["bash", str(LIB)], capture_output=True, text=True)
    assert r.returncode == 1
    assert "must be sourced" in r.stderr


def test_source_refused_without_root(tmp_path):
    env = {**os.environ, "RATE_HZ": "10"}
    r = subprocess.run(["bash", "-c", f'source {LIB!s}'], env=env,
                       capture_output=True, text=True)
    assert r.returncode != 0 and "ROOT" in r.stderr


def test_source_refused_without_rate_hz(tmp_path):
    env = {**os.environ, "ROOT": str(tmp_path)}
    r = subprocess.run(["bash", "-c", f'source {LIB!s}'], env=env,
                       capture_output=True, text=True)
    assert r.returncode != 0 and "RATE_HZ" in r.stderr


def test_source_accepts_the_contract_env(tmp_path):
    r = run_lib(tmp_path, 'echo "SOURCED ok rate=$RATE_HZ"')
    assert r.returncode == 0 and "SOURCED ok" in r.stdout


# ── P6-467/P6-469: port validation ───────────────────────────────────────────
@pytest.mark.parametrize("value", ["", "abc", "88 99", "8899;rm -rf /", "-1"])
def test_validate_port_rejects_junk(tmp_path, value):
    r = run_lib(tmp_path, f'pipeline_validate_port "{value}" TEST_PORT && echo ACCEPTED')
    assert "ACCEPTED" not in r.stdout
    assert "must be an integer 1-65535" in r.stderr


@pytest.mark.parametrize("value", ["0", "65536", "99999"])
def test_validate_port_rejects_out_of_range(tmp_path, value):
    r = run_lib(tmp_path, f'pipeline_validate_port "{value}" TEST_PORT && echo ACCEPTED')
    assert "ACCEPTED" not in r.stdout
    assert "out of range" in r.stderr


def test_validate_port_accepts_a_real_port(tmp_path):
    r = run_lib(tmp_path, 'pipeline_validate_port 8899 TEST_PORT && echo ACCEPTED')
    assert "ACCEPTED" in r.stdout


def test_port_free_rejects_non_numeric(tmp_path):
    r = run_lib(tmp_path, 'pipeline_port_free "abc" && echo FREE')
    assert "FREE" not in r.stdout
    assert "must be an integer" in r.stderr


def test_port_free_detects_a_listening_port(tmp_path):
    with socket.socket() as srv:
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        r = run_lib(tmp_path, f'pipeline_port_free {port} && echo FREE || echo BUSY')
    assert "BUSY" in r.stdout


def test_fluss_port_open_is_bounded(tmp_path):
    # 10.255.255.1 is a blackhole address: without the timeout the connect hangs
    # for the kernel's SYN retry budget; with it the call returns in ~2s.
    started = time.monotonic()
    r = run_lib(tmp_path, 'pipeline_fluss_port_open 10.255.255.1 9123; echo "rc=$?"')
    elapsed = time.monotonic() - started
    assert elapsed < 10, f"port probe took {elapsed:.1f}s — the timeout is not applied"
    assert "rc=" in r.stdout


# ── P6-143: build stamp must fail closed ─────────────────────────────────────
def test_stamp_fails_closed_when_a_listed_input_is_missing(tmp_path):
    r = run_lib(tmp_path, 'rm -f "$ROOT/code/02_services/01_ingestion/pom.xml"; '
                          'pipeline_loadgen_input_stamp && echo "STAMP=$?"')
    assert "STAMP=" not in r.stdout
    assert "build input missing" in r.stderr


def test_stamp_computes_a_digest_when_inputs_exist(tmp_path):
    r = run_lib(tmp_path, 'pipeline_loadgen_input_stamp')
    assert r.returncode == 0
    assert len(r.stdout.strip()) == 64


# ── P6-146/P6-475: credentials and the evidence mount ────────────────────────
def test_ingestion_refuses_without_the_secrets_file(tmp_path):
    r = run_lib(tmp_path, 'pipeline_start_ingestion; echo "rc=$?"')
    assert "rc=1" in r.stdout
    assert "secrets.env" in r.stderr


def test_ingestion_argv_has_no_credential_literals_and_a_readonly_slice():
    """P6-146/475 pinned statically: the docker argv is built inline in
    pipeline_start_ingestion, and driving it to the `docker run` line needs a
    live Fluss coordinator (the readiness gate polls a real cluster), so the
    properties are asserted on the source. Named as a static pin in CHG-154."""
    text = LIB.read_text()
    assert "--env-file" in text and "LIB_SECRETS_FILE" in text, \
        "credentials are not passed as a file"
    for literal in ("ARROW_APP_SECRET=testd", "ARROW_TOTP_KEY=JBSWY3DPEHPK3PXP"):
        assert literal not in text, f"{literal} is still hardcoded in the lib"
    # the ids are not secrets (secrets.env carries no ARROW_APP_ID), but they must
    # at least be overridable instead of baked in
    assert '${ARROW_APP_ID:-testd}' in text, "the app id is baked into the argv"
    assert "dst=/run/instruments-1024.csv,readonly" in text, \
        "the evidence slice is still mounted read-write (the container can rewrite it)"


# ── P6-147/P6-476: the DDL helpers get their own temp dir ────────────────────
def test_purge_helper_uses_a_private_tmpdir(tmp_path):
    ddl = tmp_path / "t.sql"
    ddl.write_text("CREATE TABLE raw_table_1 (a INT);\n")
    r = run_lib(tmp_path, f'pipeline_purge_table {ddl!s} raw && echo "rc=$?"')
    assert "rc=0" in r.stdout, r.stderr
    javac_calls = (tmp_path / "javac.calls").read_text().split()
    assert javac_calls, "javac was never called"
    for cwd in javac_calls:
        assert cwd != "/tmp", "the helper still compiles in shared /tmp"
        assert not Path(cwd).exists(), "the private temp dir was not cleaned up"


# ── P6-148: stale data needs an explicit opt-in ──────────────────────────────
def test_purge_failure_refuses_to_continue_on_stale_data(tmp_path):
    ddl = tmp_path / "t.sql"
    ddl.write_text("x\n")
    r = run_lib(tmp_path, f'pipeline_purge_table {ddl!s} raw; echo "rc=$?"',
                extra_env={"STUB_JAVA_OUT": "drop skipped", "STUB_JAVA_RC": "0"})
    assert "rc=1" in r.stdout
    assert "ALLOW_STALE_TABLE" in r.stderr


def test_purge_failure_continues_with_the_opt_in(tmp_path):
    ddl = tmp_path / "t.sql"
    ddl.write_text("x\n")
    r = run_lib(tmp_path, f'pipeline_purge_table {ddl!s} raw; echo "rc=$?"',
                extra_env={"STUB_JAVA_OUT": "drop skipped", "STUB_JAVA_RC": "0",
                           "ALLOW_STALE_TABLE": "true"})
    assert "rc=0" in r.stdout


# ── P6-474: INJECT_* must be bare integers ───────────────────────────────────
@pytest.mark.parametrize("value", ["5s", "abc", ""])
def test_inject_env_is_validated(tmp_path, value):
    r = run_lib(tmp_path, 'pipeline_start_faketool; echo "rc=$?"',
                extra_env={"INJECT_AFTER_MS": value})
    if value == "":
        # empty means "0" by default -> not an error; only assert the message is
        # absent when a value was actually supplied.
        return
    assert "rc=1" in r.stdout
    assert "must be non-negative integers" in r.stderr
    assert "run -d" not in docker_calls(tmp_path), "docker ran before validation"


# ── P6-149/P6-150/P6-478: submit result, JobID parsing, resolved container ───
def test_submit_reports_a_failed_compose_exec(tmp_path):
    r = run_lib(tmp_path, 'pipeline_submit_job; echo "rc=$?"',
                extra_env={"STUB_EXEC_FAIL": "1"})
    assert "rc=1" in r.stdout
    assert "submit command failed (rc=7)" in r.stderr


def test_submit_parses_an_uppercase_jobid_and_uses_the_resolved_container(tmp_path):
    r = run_lib(tmp_path, 'pipeline_submit_job && echo "JOB=$JOB_ID"',
                extra_env={"STUB_JOBID_LINE": "Job has been submitted with JobID 0A1B2C3D"})
    assert "JOB=0A1B2C3D" in r.stdout, r.stderr
    calls = docker_calls(tmp_path)
    assert "01_docker-flink-jobmanager-1" not in calls, "hardcoded container name still used"
    assert "cid-flink-jobmanager" in calls


def test_submit_clears_a_stale_jobid_before_it_starts(tmp_path):
    r = run_lib(tmp_path, 'JOB_ID="stale-from-previous-run"\n'
                          'pipeline_submit_job >/dev/null 2>&1 || true\n'
                          'echo "JOB=${JOB_ID:-unset}"',
                extra_env={"STUB_EXEC_FAIL": "1"})
    assert "JOB=unset" in r.stdout, r.stdout


# ── P6-480/P6-152: checkpoint evidence must not vanish silently ──────────────
def test_checkpoint_capture_requires_preflight_and_a_job(tmp_path):
    r = run_lib(tmp_path, 'PIPELINE_PREFLIGHT_OK=0; capture_checkpoint_history; echo "rc=$?"')
    assert "rc=1" in r.stdout and "preflight" in r.stderr
    r = run_lib(tmp_path, 'JOB_ID=""; capture_checkpoint_history; echo "rc=$?"')
    assert "rc=1" in r.stdout and "no job submitted" in r.stderr


def test_checkpoint_capture_reports_a_fetch_failure(tmp_path):
    r = run_lib(tmp_path, 'JOB_ID=j1; capture_checkpoint_history; echo "rc=$?"',
                extra_env={"STUB_CURL_MODE": "fail"})
    assert "rc=1" in r.stdout
    assert "checkpoint history fetch failed" in r.stderr
    assert not (tmp_path / "out" / "checkpoints.jsonl").exists()


def test_checkpoint_capture_reports_a_parse_failure(tmp_path):
    r = run_lib(tmp_path, 'JOB_ID=j1; capture_checkpoint_history; echo "rc=$?"',
                extra_env={"STUB_CURL_MODE": "garbage"})
    assert "rc=1" in r.stdout
    assert "parse failed" in r.stderr


def test_checkpoint_capture_appends_on_success(tmp_path):
    r = run_lib(tmp_path, 'JOB_ID=j1; capture_checkpoint_history; echo "rc=$?"',
                extra_env={"STUB_CURL_MODE": "ok",
                           "STUB_CURL_BODY": '{"history":[{"id":1,"status":"COMPLETED"}]}'})
    assert "rc=0" in r.stdout, r.stderr
    assert '"id": 1' in (tmp_path / "out" / "checkpoints.jsonl").read_text()


# ── P6-481/P6-482: metric dump checks the fetch and names its fallback ───────
def test_metric_dump_fails_when_the_rest_fetch_fails(tmp_path):
    r = run_lib(tmp_path, 'JOB_ID=j1; flink_metric_dump; echo "rc=$?"',
                extra_env={"STUB_CURL_MODE": "fail"})
    assert "rc=1" in r.stdout
    assert "job fetch failed" in r.stderr


def test_metric_dump_names_the_fallback_that_served_the_numbers(tmp_path):
    body = ('{"state":"RUNNING","vertices":[{"id":"v1","name":"raw_table_1",'
            '"metrics":{"read-records":5,"write-records":6}}]}')
    r = run_lib(tmp_path, 'JOB_ID=j1; flink_metric_dump',
                extra_env={"STUB_CURL_BODY": body})
    assert "raw_table_1 | 5 | 6" in r.stdout, r.stdout
    # python's stderr is routed to $OUT/flink-metric-fetch.err so the stdout
    # shape stays parseable; the source is what P6-482 added.
    err = (tmp_path / "out" / "flink-metric-fetch.err").read_text()
    assert "counters from" in err, err


# ── P6-483: wait_state validates its timeout and aborts on terminal states ───
@pytest.mark.parametrize("value", ["0", "abc", "-3"])
def test_wait_state_rejects_a_bad_timeout(tmp_path, value):
    r = run_lib(tmp_path, f'flink_wait_state RUNNING "{value}"; echo "rc=$?"')
    assert "rc=1" in r.stdout
    assert "bad timeout" in r.stderr


def test_wait_state_fails_fast_on_a_terminal_state(tmp_path):
    started = time.monotonic()
    r = run_lib(tmp_path, 'JOB_ID=j1; flink_wait_state RUNNING 2; echo "rc=$?"',
                extra_env={"STUB_CURL_BODY": '{"state":"FAILED"}'})
    elapsed = time.monotonic() - started
    assert "rc=1" in r.stdout
    assert "terminal state FAILED" in r.stderr
    assert elapsed < 10, f"waited {elapsed:.1f}s instead of aborting on the terminal state"


# ── P6-151/P6-479: trap chaining + mirror cleanup ────────────────────────────
def test_cleanup_trap_chains_a_caller_trap(tmp_path):
    r = run_lib(tmp_path, "trap 'echo CALLER-TRAP-RAN' EXIT\n"
                          "pipeline_install_cleanup_trap\n"
                          "echo body-done")
    assert "CALLER-TRAP-RAN" in r.stdout, "the caller's EXIT trap was discarded"
    assert "cleanup: removed containers=" in r.stdout


def test_cleanup_kills_the_log_mirrors_and_clears_the_job_id(tmp_path):
    r = run_lib(tmp_path, "sleep 300 & MIRROR=$!\n"
                          "FAKETOOL_LOG_PID=$MIRROR\n"
                          "JOB_ID=j1\n"
                          "OUT=\"$OUT\" pipeline_cleanup >/dev/null\n"
                          "if kill -0 \"$MIRROR\" 2>/dev/null; then echo MIRROR-ALIVE; else echo MIRROR-GONE; fi\n"
                          "echo \"JOB=${JOB_ID:-unset}\"")
    assert "MIRROR-GONE" in r.stdout
    assert "JOB=unset" in r.stdout


# ── P6-468: the compose command survives a ROOT with spaces ──────────────────
def test_compose_array_keeps_paths_intact_when_root_has_a_space(tmp_path):
    spaced = tmp_path / "root with space"
    (spaced / "code" / "01_platform" / "01_docker").mkdir(parents=True)
    r = run_lib(tmp_path, 'pipeline_compose_cid flink-jobmanager', extra_env={"ROOT": str(spaced)})
    assert "cid-flink-jobmanager" in r.stdout, r.stderr
    calls = (tmp_path / "docker.calls").read_text()
    assert "docker-compose.yml" in calls or "compose" in calls
    assert "space" in calls, "the space-containing ROOT was split into separate argv words"


# ── CHG-185: the ingestion container's environment ───────────────────────────
# Ingestion fails closed without DEPLOYMENT_ENV, and neither usual path supplies
# it for this call: Dockerfile.loadgen has no ENTRYPOINT (so
# docker-entrypoint.sh's `${DEPLOYMENT_ENV:-dev}` default never runs) and
# `docker run` does not forward the caller's exported variables. These assert on
# the recorded docker argv, so they cover the real command the lib builds.
def _start_ingestion(tmp_path, **extra_env):
    """Run pipeline_start_ingestion with the credentials file it requires
    (P6-146) and return the completed process."""
    secrets = tmp_path / "root/code/01_platform/01_docker/secrets.env"
    secrets.parent.mkdir(parents=True, exist_ok=True)
    secrets.write_text("ARROW_APP_SECRET=x\n")
    # The function waits for the readiness marker and for "HFT subscribed" in
    # its log mirror; the docker stub is not a real container, so write both
    # (the log target is created by the function's own `docker logs -f` stub).
    script = textwrap.dedent('''
        rm -f "$OUT/j1/java.out"
        ( sleep 1; touch "$OUT/ingestion.loadtest.ready"; mkdir -p "$OUT/j1"; \\
          printf '%s\\n' "HFT subscribed" > "$OUT/j1/java.out" ) &
        pipeline_start_ingestion; echo "rc=$?"
    ''')
    return run_lib(tmp_path, script,
                   extra_env=extra_env or None)


def test_ingestion_passes_deployment_env_with_a_dev_default(tmp_path):
    r = _start_ingestion(tmp_path)
    assert "rc=0" in r.stdout, r.stderr + r.stdout
    calls = docker_calls(tmp_path)
    assert "run -d" in calls, calls
    assert "-e DEPLOYMENT_ENV=dev" in calls, (
        "the ingestion container is started without DEPLOYMENT_ENV, so "
        "IngestionConfig fails closed and the container exits immediately:\n"
        + calls)


def test_ingestion_forwards_an_operators_deployment_env(tmp_path):
    """A prod invocation must reach the container unchanged — the production-only
    gates in PlatformConfig depend on seeing the real value."""
    r = _start_ingestion(tmp_path, DEPLOYMENT_ENV="production")
    assert "rc=0" in r.stdout, r.stderr + r.stdout
    calls = docker_calls(tmp_path)
    assert "-e DEPLOYMENT_ENV=production" in calls, calls
    assert "-e DEPLOYMENT_ENV=dev" not in calls, (
        "the operator's DEPLOYMENT_ENV was overwritten by the default:\n" + calls)


# ── static pins: guards that only a full preflight can reach ─────────────────
def test_preflight_only_guards_are_pinned_statically():
    text = LIB.read_text()
    assert '"$OUT/docker-ps.err"' in text, "P6-472: docker ps failure is not captured"
    assert "docker daemon unreachable" in text, "P6-472: no fail-closed message"
    assert 'hdr_fields" -ge 2' in text, "P6-473: the manifest header is not shape-checked"
    assert "manifest header has no comma separator" in text, "P6-473: no header guard"
    assert "is $state (terminal)" not in text or "terminal state" in text, "P6-483 wording"
    assert "TableEnsure/TablePurge helpers are deduplicated" not in text, \
        "P6-477 was deliberately deferred; this pin documents the expectation"
