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


def _run(tmp_path, curl_mode="stay-running", queue="", duration="8", extra_env=None):
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)  # tests may pre-plant fixtures (e.g. a corrupt offset)
    state = tmp_path / "curl-state.json"
    env = dict(os.environ)
    env.update({"PATH": str(_sandbox(tmp_path)) + os.pathsep + env["PATH"],
                "OUT_DIR": str(out), "DURATION_S": duration,
                "W38_CURL_STATE": str(state), "W38_CURL_MODE": curl_mode,
                "W38_CURL_QUEUE": queue})
    if extra_env:
        env.update(extra_env)
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


OTLP_LINE = ('2026-09-17T00:00:00Z otlp-metrics-payload: '
             '{"resourceMetrics": [{"scopeMetrics": [{"metrics": ['
             '{"name": "feed.ack", "sum": {"dataPoints": [{"asInt": 42}]}}'
             ']}]}]}\n')


def _java_out(tmp_path, name="java.out", mode=0o644):
    j = tmp_path / name
    j.write_text(OTLP_LINE)
    j.chmod(mode)
    return j


# ---------------- commit 5 (P6-558/559) ----------------
def test_missing_java_out_uses_sentinel(tmp_path):
    # P6-558: file present at preflight, vanishing mid-run. The new separate
    # sentinel warns exactly once; the offset file is never touched-empty
    # (old code warned never — its sentinel was the offset file, already
    # created by the early successful ticks — and poisoned resume).
    j = _java_out(tmp_path)
    deleter = subprocess.Popen(
        ["python3", "-c", "import time,os; time.sleep(3); os.remove(r'%s')" % j])
    try:
        proc, out, _ = _run(tmp_path, extra_env={"INGESTION_JAVA_OUT": str(j)})
    finally:
        deleter.wait(timeout=30)
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    assert text.count("set but file missing") == 1, text
    assert (out / ".ingestion-warned").is_file(), "warned-once sentinel missing"
    off = out / ".ingestion-java.out.offset"
    if off.exists():
        assert off.read_text().strip().isdigit(), "offset must stay numeric"


def test_corrupt_offset_defaults_zero(tmp_path):
    # P6-559: garbage offset + present file parses from 0 and heals the file
    # (old code crashed int() that tick and wrote zero rows).
    out = tmp_path / "out"
    out.mkdir()
    (out / ".ingestion-java.out.offset").write_text("garbage!!\n")
    j = _java_out(tmp_path)
    proc, out, _ = _run(tmp_path, extra_env={"INGESTION_JAVA_OUT": str(j)})
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    tsv = out / "ingestion.tsv"
    assert tsv.is_file(), "no rows parsed despite offset defaulting to 0"
    assert any("feed.ack\t42" in line for line in tsv.read_text().splitlines())
    off = out / ".ingestion-java.out.offset"
    assert off.read_text().strip().isdigit(), "offset file not healed to numeric"


def test_parse_failure_holds_offset(tmp_path):
    # P6-559: unreadable java.out fails the parse — the offset must NOT
    # advance (old code wrote fsize unconditionally, skipping bytes forever).
    # Per-tick resilience still ends loud: the end-of-run evidence gate fails
    # the run (rc 2) because the enabled leg produced zero rows.
    j = _java_out(tmp_path, mode=0o000)
    try:
        proc, out, _ = _run(tmp_path, extra_env={"INGESTION_JAVA_OUT": str(j)})
    finally:
        j.chmod(0o644)
    text = proc.stdout + proc.stderr
    assert proc.returncode == 2, text
    assert "offset held at 0" in text
    assert "ingestion.tsv" in text and "NO data rows" in text
    assert not (out / ".ingestion-java.out.offset").exists(), \
        "offset advanced despite failed parse"
