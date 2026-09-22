"""Wave-38 capture tests (commits 4-7): real stage-capture.sh + stubbed deps.

Sandbox = the real script with stub curl/docker/sleep/iostat on PATH and a
short DURATION_S (8s — under every timed gate: 25s freshness, t+30 legs,
t+40 bridge, t+65 bridge). FLUSS_PROBE_CP / INGESTION_JAVA_OUT stay unset so
the JVM/file legs are skipped; JOB_ID comes from the stubbed overview
(test-jid). Hermetic rule: no stub may touch the cluster (all exit from
fixtures), so pre-fix runs can only fail fast, never write live state.

Two cadences, chosen by what a test asserts. Tests that assert a COUNT of
polls or ticks (the P6-216 debounce, 5 consecutive checkpoint failures, the
t+5 bridge check, the 3s file deletion) keep the 8s / interval-5 default —
their assertions are about how many times the loop ran. Tests that assert only
an ARTIFACT (a parsed row, a healed offset, a probe argv, appended ids) run 3s
at CAPTURE_INTERVAL_S=1: faster, and MORE sampled (3 ticks, not 1-2).
"""
import json
import os
import shutil
import stat
import subprocess
import re
import ast
from pathlib import Path

TESTS = Path(__file__).resolve().parent
SCRIPTS = TESTS.parent
STUBS = TESTS / "stubs"
SCRIPT = SCRIPTS / "stage-capture.sh"


JAVA_STUB = """#!/usr/bin/env bash
echo "java $@" >> "$JAVA_ARGV"
# Hang only the read-lag probe: all three probes share this stub, and each
# has its own 20s timeout — hanging all three would (correctly) cost 60s.
case "$*" in
  *FlussReadLagProbe*) [ "${JAVA_HANG:-0}" = "1" ] && /bin/sleep 60 ;;
esac
exit "${JAVA_RC:-0}"
"""


def _sandbox(tmp_path, extra_bins=None):
    d = tmp_path / "bin"
    d.mkdir()
    shutil.copy(STUBS / "wave38_curl.py", d / "curl")
    shutil.copy(STUBS / "wave38_docker.py", d / "docker")
    shutil.copy(STUBS / "wave24_iostat.py", d / "iostat")
    (d / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
    (d / "java").write_text(JAVA_STUB)
    (d / "javac").write_text("#!/usr/bin/env bash\nexit 0\n")
    for name, body in (extra_bins or {}).items():
        (d / name).write_text(body)
    for f in d.iterdir():
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return d


def _run(tmp_path, curl_mode="stay-running", queue="", duration="8", extra_env=None,
         extra_bins=None):
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)  # tests may pre-plant fixtures (e.g. a corrupt offset)
    state = tmp_path / "curl-state.json"
    env = dict(os.environ)
    env.update({"PATH": str(_sandbox(tmp_path, extra_bins)) + os.pathsep + env["PATH"],
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
    proc, out, _ = _run(tmp_path, duration="3",
                        extra_env={"INGESTION_JAVA_OUT": str(j),
                                   "CAPTURE_INTERVAL_S": "1"})
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
        proc, out, _ = _run(tmp_path, duration="3",
                            extra_env={"INGESTION_JAVA_OUT": str(j),
                                       "CAPTURE_INTERVAL_S": "1"})
    finally:
        j.chmod(0o644)
    text = proc.stdout + proc.stderr
    assert proc.returncode == 2, text
    assert "offset held at 0" in text
    assert "ingestion.tsv" in text and "NO data rows" in text
    assert not (out / ".ingestion-java.out.offset").exists(), \
        "offset advanced despite failed parse"


# ---------------- commit 6 (P6-560/561) ----------------
def test_probe_db_table_params(tmp_path):
    # P6-561: probe 1 must take PROBE_DB/PROBE_RAW_TABLE (old: hardcoded
    # `default raw_table_1`, incomparable with the KV legs); the KV probes
    # keep their own tables (guards against over-editing the block).
    argv = tmp_path / "java-argv.txt"
    extra = {"FLUSS_PROBE_CP": "/tmp/fake-cp", "JAVA_ARGV": str(argv),
             "PROBE_DB": "mydb", "PROBE_RAW_TABLE": "myraw",
             "CAPTURE_INTERVAL_S": "1"}
    proc, out, _ = _run(tmp_path, duration="3", extra_env=extra)
    text = proc.stdout + proc.stderr
    # The stub probes emit no rows, so the end-gate fails the run on the
    # rowless read-lag leg (rc 2) — expected: it proves the probes actually
    # ran under FLUSS_PROBE_CP. The 561 pin is the argv below.
    assert proc.returncode == 2, text
    assert "read-lag.tsv(header-only)" in text, text
    lines = argv.read_text().splitlines()
    lag = [l for l in lines if "FlussReadLagProbe" in l]
    assert lag, "read-lag probe never ran"
    assert any("mydb myraw" in l for l in lag), lines
    assert not any("default raw_table_1" in l for l in lag), lines
    kv = [l for l in lines if "FlussKvProbe" in l]
    assert kv and any("candle_live" in l for l in kv), lines


def test_probe_hang_bounded(tmp_path):
    # P6-560: a hung probe RPC must die at 20s (timeout), keep stderr in the
    # per-tick err file, and WARN — never wedge the run. The stub hangs the
    # read-lag probe 60s; without the fix that tick blocks ~60s (wall >= 50).
    import time
    argv = tmp_path / "java-argv.txt"
    extra = {"FLUSS_PROBE_CP": "/tmp/fake-cp", "JAVA_ARGV": str(argv),
             "JAVA_HANG": "1"}
    start = time.monotonic()
    proc, out, _ = _run(tmp_path, duration="25", extra_env=extra)
    elapsed = time.monotonic() - start
    text = proc.stdout + proc.stderr
    assert "FlussReadLagProbe failed this tick" in text, text
    assert (out / "probe-read-lag.err").is_file(), "probe stderr not kept"
    assert elapsed < 50, f"probe hang not bounded (wall {elapsed:.0f}s)"


# ---------------- commit 7 (P6-790/563/564/565) ----------------
def test_bad_durations_fail_fast(tmp_path):
    # P6-790: non-numeric cadences die at the header with one clear line
    # (old: cryptic mid-run `[ -ge ]` / `sleep` errors).
    cases = ({"DURATION_S": "abc"}, {"CAPTURE_INTERVAL_S": "1.5"},
             {"DURATION_S": "0"})
    for i, bad in enumerate(cases):
        sub = tmp_path / f"case{i}"
        sub.mkdir()
        proc, _, _ = _run(sub, extra_env=bad)
        text = proc.stdout + proc.stderr
        assert proc.returncode == 1, text
        assert "must be a positive integer" in text, text


def test_freshness_scales_with_interval(tmp_path):
    # P6-565: scrapes die ~2s in; at interval 30 the 5x budget (150s) holds
    # the run green with a ~26s-old last scrape. Old hardcoded 25s fails it
    # spuriously. Duration stays under the t+30 leg gate (which owns dead-leg
    # detection) so this pins exactly the end-gate budget.
    extra = {"CAPTURE_INTERVAL_S": "30", "W38_CURL_MAX_ALIVE": "5"}
    proc, _, _ = _run(tmp_path, duration="28", extra_env=extra)
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    assert "last prom scrape" not in text, text


def test_checkpoints_append_only_new_ids(tmp_path):
    # P6-564: cumulative history appends each id exactly once (old: full
    # history every tick, O(ticks x checkpoints) with duplicates).
    import json
    proc, out, _ = _run(tmp_path, duration="3",
                        extra_env={"W38_CURL_CP_MODE": "grow",
                                   "CAPTURE_INTERVAL_S": "1"})
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    lines = (out / "flink-checkpoints.jsonl").read_text().splitlines()
    assert len(lines) >= 2, "expected several tick summaries"
    ids = [e["id"] for line in lines for e in json.loads(line)["events"]]
    assert len(ids) == len(set(ids)), f"duplicate checkpoint ids: {ids}"


def test_checkpoints_dead_rest_fails_closed(tmp_path):
    # P6-564: 5 consecutive fetch failures exit 2 (old: `|| true` silence).
    proc, _, _ = _run(tmp_path, extra_env={"W38_CURL_CP_MODE": "dead"})
    text = proc.stdout + proc.stderr
    assert proc.returncode == 2, text
    assert "checkpoint fetch failed" in text, text
    assert "5 consecutive ticks" in text, text


def _arrow_run(tmp_path, arrow_line, tag):
    j = tmp_path / f"java-{tag}.out"
    j.write_text(OTLP_LINE + arrow_line)
    extra = {"INGESTION_JAVA_OUT": str(j), "BRIDGE_CHECK_AT_S": "5"}
    return _run(tmp_path, extra_env=extra)


def test_bridge_full_date_stale_fails(tmp_path):
    # P6-563: a stale FULL-date report fails (old: `cut -c1-12` mangled it to
    # `2026-09-17 0`, the parse failed, and the guard skipped fail-open).
    arrow = "2020-01-01 00:00:00.000 arrow-tick-counts: total=5\n"
    proc, _, _ = _arrow_run(tmp_path, arrow, "fulldate")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 2, text
    assert "arrow-tick-counts report" in text and "old at t+" in text, text


def test_bridge_future_bare_time_rolls_to_yesterday(tmp_path):
    # P6-563: a bare clock-time 1h in the future belongs to yesterday
    # (midnight rollover) — age ~23h, FAIL. Old: negative age, missed stall.
    import datetime
    future = (datetime.datetime.now(datetime.timezone.utc)
              + datetime.timedelta(hours=1)).strftime("%H:%M:%S.000")
    proc, _, _ = _arrow_run(tmp_path, f"{future} arrow-tick-counts: total=5\n",
                            "future")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 2, text
    assert "arrow-tick-counts report" in text and "old at t+" in text, text


def test_bridge_fresh_report_passes(tmp_path):
    # P6-563 guard: a fresh report must NOT trip the check (no false stall
    # from the UTC / rollover handling).
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S.000")
    proc, _, _ = _arrow_run(tmp_path, f"{now} arrow-tick-counts: total=5\n",
                            "fresh")
    text = proc.stdout + proc.stderr
    assert proc.returncode == 0, text
    assert "arrow-tick-counts report" not in text, text


# ---------------- P6-557 (remainder): the leg must run on python 3.8 ---------

def _python_heredocs(script: Path) -> list[tuple[int, str]]:
    """Every `python3 ... <<'DELIM'` body in the script, with its first line number.

    The capture legs are embedded python; the host's python version is not
    pinned anywhere, so the source is the only thing a test can inspect.
    """
    lines = script.read_text(encoding="utf-8").splitlines()
    blocks: list[tuple[int, str]] = []
    i = 0
    while i < len(lines):
        # Only a real invocation: the line must START with `python3` (an optional
        # `if` in front), never a comment that merely mentions the form.
        m = re.match(r"\s*(?:if\s+)?python3\b[^\n]*<<'([A-Za-z_][A-Za-z0-9_]*)'",
                     lines[i])
        if not m:
            i += 1
            continue
        delim, body = m.group(1), []
        # The body starts after the WHOLE command: a heredoc command may be
        # continued over several lines (`... <<'X' | \` + `python3 ... || \`).
        while i < len(lines) and lines[i].rstrip().endswith("\\"):
            i += 1
        start = i + 2          # 1-based line number of the first body line
        i += 1
        while i < len(lines) and lines[i].strip() != delim:
            body.append(lines[i])
            i += 1
        blocks.append((start, "\n".join(body)))
        i += 1
    return blocks


def _pep585_annotations(src: str) -> list[str]:
    """Annotation nodes that subscript a builtin (`dict[str, str]`)."""
    builtins = {"dict", "set", "list", "tuple", "frozenset", "type"}
    hits = []
    for node in ast.walk(ast.parse(src)):
        ann = getattr(node, "annotation", None)
        if isinstance(ann, ast.Subscript) and isinstance(ann.value, ast.Name) \
                and ann.value.id in builtins:
            hits.append(f"L{ann.lineno}: {ann.value.id}[...]")
    return hits


def test_capture_python_is_version_neutral():
    # P6-557: `pairs: dict[str, str]` / `present: set[str]` are PEP 585 — they
    # need python >=3.9, and on 3.8 the annotation is EVALUATED at runtime, so
    # the whole leg dies with TypeError and the capture loses its custom-metric
    # rows. Nothing in the repo pins the host's python, so the legs must stay
    # version-neutral. Syntax-level features (match, parenthesized context
    # managers) are covered by feature_version.
    blocks = _python_heredocs(SCRIPT)
    assert blocks, "no python heredoc found in stage-capture.sh — extractor is stale"
    problems = []
    for start, src in blocks:
        try:
            ast.parse(src, feature_version=(3, 8))
        except SyntaxError as exc:
            problems.append(f"heredoc at L{start}: not 3.8 syntax: {exc}")
        for hit in _pep585_annotations(src):
            problems.append(f"heredoc at L{start}: PEP 585 annotation {hit}")
    assert not problems, "\n".join(problems)
