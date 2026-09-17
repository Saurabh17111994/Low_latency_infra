"""Wave-38 capture tests (commits 4-7): real stage-capture.sh + stubbed deps.

Sandbox = the real script with stub curl/docker/sleep/iostat on PATH and a
short DURATION_S (8s — under every timed gate: 25s freshness, t+30 legs,
t+40 bridge, t+65 bridge). FLUSS_PROBE_CP / INGESTION_JAVA_OUT stay unset so
the JVM/file legs are skipped; JOB_ID comes from the stubbed overview
(test-jid). Hermetic rule: no stub may touch the cluster (all exit from
fixtures), so pre-fix runs can only fail fast, never write live state.
"""
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

TESTS = Path(__file__).resolve().parent
SCRIPTS = TESTS.parent
STUBS = TESTS / "stubs"
SCRIPT = SCRIPTS / "stage-capture.sh"


def _sandbox(tmp_path):
    d = tmp_path / "bin"
    d.mkdir()
    shutil.copy(STUBS / "wave38_curl.py", d / "curl")
    shutil.copy(STUBS / "wave38_docker.py", d / "docker")
    shutil.copy(STUBS / "wave24_iostat.py", d / "iostat")
    (d / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
    for f in d.iterdir():
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return d


def _run(tmp_path, curl_mode="stay-running", queue="", duration="8"):
    out = tmp_path / "out"
    out.mkdir()
    state = tmp_path / "curl-state.json"
    env = dict(os.environ)
    env.update({"PATH": str(_sandbox(tmp_path)) + os.pathsep + env["PATH"],
                "OUT_DIR": str(out), "DURATION_S": duration,
                "W38_CURL_STATE": str(state), "W38_CURL_MODE": curl_mode,
                "W38_CURL_QUEUE": queue})
    proc = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True,
                          text=True, timeout=120)
    return proc, out, state


def _exact_job_hits(state):
    counts = json.loads(state.read_text()) if state.exists() else {}
    return sum(n for u, n in counts.items()
               if "/jobs/" in u and "overview" not in u and "checkpoints" not in u
               and u.rstrip("/").rsplit("/", 1)[-1] == "test-jid")


# ---------------- commit 4 (P6-214/215/216) ----------------
def test_scrape_empty_warns_not_dies(tmp_path):
    # P6-214: alive-but-empty metrics warn, keep the raw scrape, finish rc 0.
    proc, out, _ = _run(tmp_path, curl_mode="scrape-empty")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    assert "zero matching series" in text
    assert "scrape dead" not in text
    proms = sorted(out.glob("prom-*.txt"))
    assert proms, "no prom scrape landed"
    assert any("up 1" in p.read_text() for p in proms), "raw scrape not kept"


def test_scrape_dead_tick_resumes(tmp_path):
    # P6-215: a dead scrape fails the tick (return 2), the loop continues.
    proc, out, _ = _run(tmp_path, curl_mode="scrape-dead")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    assert "scrape dead" in text, "expected per-tick FAIL lines"
    assert "warn: one sample tick failed (continuing)" in text


def test_single_blip_survives(tmp_path):
    # P6-216: one FAILED poll then healthy — the run survives.
    proc, out, _ = _run(tmp_path, queue="FAILED")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    assert "failing closed" not in text


def test_three_consecutive_fail_loud(tmp_path):
    # P6-216 posture: persistent failure still exits 2 WITH diagnostics —
    # and the stub counts prove 3 polls happened (debounce, not hair-trigger).
    proc, out, state = _run(tmp_path,
                            queue="FAILED,FAILED,FAILED,FAILED,FAILED,FAILED")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 2, text
    assert "failing closed" in text
    assert list(out.rglob("00-reason.txt")), "stall diagnostics never fired"
    assert _exact_job_hits(state) >= 5, "guard must poll 3x before failing"
