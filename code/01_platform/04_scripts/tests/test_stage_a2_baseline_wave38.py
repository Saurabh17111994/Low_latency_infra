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
