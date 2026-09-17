"""Wave-38 commit-1 pins for stage-a2-baseline.sh header region (P6-552/553/554).

Strategy: run the real script with stubbed PATH deps (curl/sleep/docker).
All cases exit BEFORE preflight/purge/submit — nothing touches the cluster:
  - huge MIN_UPTIME_S fatals at the stability gate (dirs created, then fatal)
  - abc MIN_UPTIME_S fatals at validation (before any docker/REST use)
  - stubbed-curl-down reaches bring-up, records the compose argv, then
    fatals at the still-down check (stub sleep keeps the 40x loop instant)
Created PHASE_OUT dirs are parsed from `out=` and removed afterwards.

HERMETIC RULE (learned live 2026-09-17): EVERY test runs with stub
curl/sleep/docker on PATH — including tests that fatal before the REST
check on the NEW code. A failing-first run against OLD code skips the gate
and would otherwise reach real preflight (it restarted the live TM once).
With stubs, the worst old-code path is bring-up-then-still-down fatal.
"""
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

TESTS = Path(__file__).resolve().parent
SCRIPTS = TESTS.parent
SCRIPT = SCRIPTS / "stage-a2-baseline.sh"


def _run(env_extra, stubs=None, timeout=60):
    env = dict(os.environ)
    env.update(env_extra)
    if stubs is not None:
        env["PATH"] = str(stubs) + os.pathsep + env["PATH"]
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True,
                          text=True, timeout=timeout)


def _out_dir(proc):
    m = re.search(r"out=(\S+)", proc.stdout + proc.stderr)
    assert m, f"no out= in output:\n{proc.stdout}\n{proc.stderr}"
    return Path(m.group(1))


def _stub_bin(tmp_path, log_names=()):
    """curl always-down, sleep instant, docker record-and-OK. Hermetic."""
    d = tmp_path / "stubs"
    d.mkdir()
    (d / "curl").write_text('#!/usr/bin/env bash\n[ -n "${CURL_LOG:-}" ] && echo "curl $@" >> "$CURL_LOG"\nexit 1\n')
    (d / "sleep").write_text('#!/usr/bin/env bash\nexit 0\n')
    (d / "docker").write_text('#!/usr/bin/env bash\n[ -n "${DOCKER_LOG:-}" ] && echo "docker $@" >> "$DOCKER_LOG"\nexit 0\n')
    for f in d.iterdir():
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return d


def test_run_dirs_unique_and_created(tmp_path):
    stubs = _stub_bin(tmp_path)
    outs = []
    try:
        for _ in range(2):
            p = _run({"MIN_UPTIME_S": "99999999"}, stubs=stubs)
            assert p.returncode == 1, p.stdout + p.stderr
            assert "refusing a measurement start right after a reboot" in p.stdout + p.stderr
            d = _out_dir(p)
            assert d.is_dir(), f"run dir not created: {d}"
            assert (d / "FAILURE.txt").is_file()
            outs.append(d)
        assert outs[0] != outs[1], "concurrent runs share one PHASE_OUT (P6-552)"
    finally:
        for d in outs:
            shutil.rmtree(d, ignore_errors=True)


def test_min_uptime_noninteger_fails_fast(tmp_path):
    stubs = _stub_bin(tmp_path)
    d = None
    try:
        p = _run({"MIN_UPTIME_S": "abc"}, stubs=stubs)
        assert p.returncode == 1, p.stdout + p.stderr
        curled = "starting flink containers" in p.stdout + p.stderr
        assert not curled, "gate skipped — measuring on invalid input"
        assert "MIN_UPTIME_S must be non-negative integer" in p.stdout + p.stderr
        d = _out_dir(p)
    finally:
        if d is not None:
            shutil.rmtree(d, ignore_errors=True)


def test_proc_uptime_guard_in_source():
    # /proc absence is untestable live on Linux CI — pin the guard by source
    # text instead (same precedent as the P6-863 WARN-stream guard).
    src = SCRIPT.read_text()
    assert "[ -r /proc/uptime ]" in src, "missing-/proc fail-closed guard absent"


def test_bring_up_uses_lib_compose(tmp_path):
    stubs = _stub_bin(tmp_path)
    curl_log = tmp_path / "curl.log"
    docker_log = tmp_path / "docker.log"
    d = None
    try:
        p = _run({"MIN_UPTIME_S": "0", "CURL_LOG": str(curl_log),
                  "DOCKER_LOG": str(docker_log)}, stubs=stubs)
        assert p.returncode == 1, p.stdout + p.stderr
        assert "Flink REST still down after 200s" in p.stdout + p.stderr
        assert curl_log.is_file(), "REST check never attempted"
        calls = docker_log.read_text().strip().split("\n")
        # (The EXIT-trap cleanup also calls stub docker with `rm -f` — ignore it.)
        compose_calls = [c for c in calls if c.startswith("docker compose ")]
        assert len(compose_calls) == 1, f"bring-up must be one compose call, got: {calls}"
        parts = compose_calls[0].split()
        # lib $COMPOSE array shape (pipeline-lib.sh B1): both env files + -f,
        # then the service bring-up — scalar $COMPOSE would be bare `docker up`.
        assert parts[0] == "docker" and parts[1] == "compose", argv
        assert "-f" in parts and parts[parts.index("-f") + 1].endswith("docker-compose.yml"), argv
        assert parts.count("--env-file") == 2, argv
        assert parts[-3:] == ["up", "-d", "flink-jobmanager"] or parts[-4:] == ["up", "-d", "flink-jobmanager", "flink-taskmanager"], argv
        d = _out_dir(p)
    finally:
        if d is not None:
            shutil.rmtree(d, ignore_errors=True)


# ---------------- wave-38 commit 2 (P6-210/211/212) ----------------
ROOT = TESTS.parent.parent.parent.parent
STUBS = TESTS / "stubs"
LIB = SCRIPTS / "pipeline-lib.sh"
FAKE_JOB = "abcdef0123456789abcdef0123456789"


def _g24_run(tmp_path, inspect, exec_rc="0"):
    """Source the real lib, run only pipeline_g24_rocksdb_check. Hermetic:
    stub docker answers ps/inspect/exec, stub sleep keeps the wait instant."""
    d = tmp_path / "g24stubs"
    d.mkdir()
    shutil.copy(STUBS / "wave38_docker.py", d / "docker")
    sl = d / "sleep"
    sl.write_text("#!/usr/bin/env bash\nexit 0\n")
    sl.chmod(0o755)
    env = dict(os.environ)
    env.update({"ROOT": str(ROOT), "RATE_HZ": "10", "STATE_BACKEND": "rocksdb",
                "PATH": str(d) + os.pathsep + env["PATH"],
                "STUB_DOCKER_PS": "fakecid123",
                "STUB_DOCKER_INSPECT": inspect,
                "STUB_DOCKER_EXEC_RC": exec_rc})
    return subprocess.run(
        ["bash", "-c", 'source "$ROOT/code/01_platform/04_scripts/pipeline-lib.sh"; '
                       'JOB_ID="$FAKE_JOB"; pipeline_g24_rocksdb_check'],
        env=env, capture_output=True, text=True, timeout=60)


def test_g24_unmounted_fails_fast(tmp_path):
    p = _g24_run(tmp_path, inspect="")
    assert p.returncode == 1, p.stdout + p.stderr
    assert "NOT MOUNTED" in p.stdout + p.stderr


def test_g24_bind_mount_fails(tmp_path):
    p = _g24_run(tmp_path, inspect="/data/rocksdb")
    assert p.returncode == 1, p.stdout + p.stderr
    assert "BIND MOUNT" in p.stdout + p.stderr


def test_g24_named_volume_passes(tmp_path):
    p = _g24_run(tmp_path, inspect="01_docker_flink-rocksdb", exec_rc="0")
    assert p.returncode == 0, p.stdout + p.stderr
    assert "G24 OK" in p.stdout + p.stderr


def test_jobid_gate_present_and_ordered():
    # P6-210's path is unreachable via real submit (the lib already guarantees
    # non-empty) — pin the defense-in-depth guard by source text + position.
    src = (SCRIPTS / "stage-a2-baseline.sh").read_text()
    i_submit = src.index("pipeline_submit_job || fatal")
    i_gate = src.index("empty/malformed JOB_ID")
    # (the same loop shape appears earlier in the bring-up block — search after the gate)
    i_wait = src.index("for _ in $(seq 1 40)", i_gate)
    assert i_submit < i_gate < i_wait, "210 guard must sit between submit and wait"


def test_handoff_forwards_env():
    # P6-212 (env half): the capture child must see the run's own knobs.
    src = (SCRIPTS / "stage-a2-baseline.sh").read_text()
    handoff = src.index('bash "$SCRIPT_DIR/stage-capture.sh"')
    head = src[:handoff]
    for assign in ('RATE_HZ="$RATE_HZ"', 'MIN_UPTIME_S="$MIN_UPTIME_S"',
                   'STATE_BACKEND="${STATE_BACKEND:-rocksdb}"',
                   'SOURCE_RATE_FLOOR_PCT="${SOURCE_RATE_FLOOR_PCT:-80}"'):
        assert assign in head, f"handoff drops {assign} (P6-212)"


# ---------------- wave-38 commit 3 (P6-555/213/556 + manifest) ----------------
def _lib_run(command, env_extra=None, timeout=60):
    env = dict(os.environ)
    env.update({"ROOT": str(ROOT), "RATE_HZ": "10"})
    if env_extra:
        env.update(env_extra)
    return subprocess.run(["bash", "-c", f'source "{LIB}"; {command}'],
                          env=env, capture_output=True, text=True, timeout=timeout)


def test_knob_validation_fails_fast(tmp_path):
    # P6-555: bad knobs fatal BEFORE mkdir (no out=) and before any cluster
    # use. Stubs stay on PATH so a pre-fix script can only reach the
    # still-down fatal, never live preflight (hermetic rule).
    stubs = _stub_bin(tmp_path)
    cases = [({"RATE_HZ": "abc"}, "RATE_HZ must be a positive integer"),
             ({"RATE_HZ": "0"}, "RATE_HZ must be a positive integer"),
             ({"DURATION_S": "abc"}, "DURATION_S must be a positive integer"),
             ({"DURATION_S": "0"}, "DURATION_S must be a positive integer"),
             ({"SOURCE_RATE_FLOOR_PCT": "abc"}, "SOURCE_RATE_FLOOR_PCT must be an integer 1-100"),
             ({"SOURCE_RATE_FLOOR_PCT": "0"}, "SOURCE_RATE_FLOOR_PCT must be an integer 1-100"),
             ({"SOURCE_RATE_FLOOR_PCT": "101"}, "SOURCE_RATE_FLOOR_PCT must be an integer 1-100")]
    for env_extra, msg in cases:
        d = None
        try:
            p = _run(env_extra, stubs=stubs)
            out = p.stdout + p.stderr
            assert p.returncode == 1, f"{env_extra}: rc={p.returncode}\n{out}"
            assert msg in out, f"{env_extra}: missing {msg!r}\n{out}"
            assert "out=" not in out, f"{env_extra}: created a run dir before validating"
        finally:
            m = re.search(r"out=(\S+)", p.stdout + p.stderr)
            if m:
                shutil.rmtree(m.group(1), ignore_errors=True)


def _verdict_tsv(tmp_path, name, lines):
    tsv = tmp_path / name
    tsv.write_text("".join(lines))
    return tsv


def test_g25_healthy(tmp_path):
    tsv = _verdict_tsv(tmp_path, "ok.tsv",
                       ["epoch\tw\traw-validation\tx\t0\n",
                        "epoch\tw\traw-validation\tx\t200000\n"])
    # fix epochs: first col must be ints 100 -> 110 (avg 20000/s)
    tsv.write_text("100\tw\traw-validation\tx\t0\n110\tw\traw-validation\tx\t200000\n")
    p = _lib_run(f'pipeline_g25_floor_verdict "{tsv}" 20480 80')
    assert p.returncode == 0, p.stdout + p.stderr
    assert "HEALTHY" in p.stdout


def test_g25_poisoned(tmp_path):
    tsv = _verdict_tsv(tmp_path, "low.tsv",
                       ["100\tw\traw-validation\tx\t0\n",
                        "110\tw\traw-validation\tx\t34000\n"])
    p = _lib_run(f'pipeline_g25_floor_verdict "{tsv}" 20480 80')
    assert p.returncode == 1, p.stdout + p.stderr
    assert "POISONED" in p.stdout


def test_g25_insufficient(tmp_path):
    tsv = _verdict_tsv(tmp_path, "empty.tsv",
                       ["epoch\tvert\tmetric\tsub\tcount\n",
                        "garbage,not,a,tsv,row\n",
                        "100\tw\traw-validation\tx\t5\n"])
    p = _lib_run(f'pipeline_g25_floor_verdict "{tsv}" 20480 80')
    assert p.returncode == 2, p.stdout + p.stderr
    assert "INSUFFICIENT DATA" in p.stdout


def test_manifest_helper(tmp_path):
    m = tmp_path / "manifest.tsv"
    # NOTE: the lib owns JOB_ID (top-level JOB_ID="" at source) — set it after.
    p = _lib_run(f'JOB_ID="{FAKE_JOB}"; gate_manifest_add "{m}" "extra_line=1"',
                 {"PHASE_OUT": "/tmp/run-1", "DURATION_S": "720",
                  "STATE_BACKEND": "rocksdb",
                  "SOURCE_RATE_FLOOR_PCT": "80"})
    assert p.returncode == 0, p.stdout + p.stderr
    lines = m.read_text().strip().split("\n")
    assert lines[0] == "run_dir=/tmp/run-1", lines
    assert "rate_hz=10" in lines and "duration_s=720" in lines
    assert f"job_id={FAKE_JOB}" in lines
    assert "state_backend=rocksdb" in lines and "floor_pct=80" in lines
    assert any(l.startswith("git_sha=") for l in lines)
    assert "extra_line=1" in lines


def test_g25_callsite_and_tail_gate():
    src = (SCRIPTS / "stage-a2-baseline.sh").read_text()
    assert "pipeline_g25_floor_verdict" in src, "G25 must call the lib verdict (P6-213)"
    assert 'G25: $g25_verdict' in src, "both verdicts must fatal loud"
    assert 'ls -la "$PHASE_OUT/stages"' not in src, "success-by-ls must go (P6-556)"
    assert 'submission-ids.txt' in src, "submit receipt missing (P6-556)"
    assert 'gate_manifest_add "$PHASE_OUT/stage-manifest.tsv"' in src, "manifest call missing (P6-212)"
    assert "INSUFFICIENT" in LIB.read_text(), "lib must distinguish INSUFFICIENT (P6-213)"
