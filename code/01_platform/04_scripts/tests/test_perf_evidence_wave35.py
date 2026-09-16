#!/usr/bin/env python3
"""Wave 35 — the perf-evidence collector must survive a flaky cluster (P6-011..466).

The collector runs for the whole of PERF-AUDIT-001 alongside the load generator.
Its value is that it keeps recording while something is going wrong, and its
failure mode was the opposite: `set -euo pipefail` ended the entire capture on
the first transient. Proved on 2026-09-16 — a `docker` that failed (daemon down,
container gone) made iteration 1 the last one, so the run produced no evidence
at all at exactly the moment evidence mattered.

These tests run the REAL script with `docker` replaced by a shim, plus direct
tests of the companion module that owns the arithmetic:

  * P6-011 — a failing `docker` must not end the capture, and the run must say
    how much of it failed rather than exiting 0 on an empty result.
  * P6-463 — interval validation, snapshot index continuation on rerun.
  * P6-136 — the aggregate response form is summed for throughput; the old
    code reported a per-subtask MEAN and always n_subtasks=1.
  * P6-465 — memory is normalized to bytes, so MiB and GiB are comparable.
  * P6-466 — the error row has exactly the header's field count, and a
    truncated snapshot cannot be left behind.
  * P6-137 — the latency category the header advertises is actually recorded,
    and its absence is explained.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]
SCRIPTS = REPO / "code/01_platform/04_scripts"
COLLECTOR = SCRIPTS / "perf-evidence-collector.sh"

sys.path.insert(0, str(SCRIPTS))
import perf_evidence_parse as P  # noqa: E402


# Flink's real aggregate response: ONE object per metric carrying the
# cross-subtask min/max/avg/sum. pipeline-lib.sh records the same shape as
# live-verified on Flink 2.2.1 (CHG-120).
AGGREGATE_RESPONSE = [
    {"id": "busyTimeMsPerSecond", "min": 10.0, "max": 900.0,
     "avg": 400.0, "sum": 3200.0},
    {"id": "numRecordsInPerSecond", "min": 90.0, "max": 110.0,
     "avg": 100.0, "sum": 800.0},
    {"id": "numRecordsOutPerSecond", "min": 70.0, "max": 90.0,
     "avg": 80.0, "sum": 640.0},
]

# The shim answers the small set of Flink REST URLs the collector uses, so the
# script's own request construction and the module's parsing are both exercised.
DOCKER_SHIM = '''
import json, sys
url = sys.argv[-1] if sys.argv[1:] else ""
if "jobs/overview" in url:
    print(json.dumps({"jobs": [{"jid": "abc123", "state": "RUNNING",
                                "name": "signal-pipeline"}]}))
elif url.endswith("/jobs/abc123"):
    print(json.dumps({"vertices": [{"id": "v1", "name": "compute",
                                    "parallelism": 8}]}))
elif "subtasks/metrics" in url:
    print(json.dumps(%s))
elif url.endswith("/checkpoints"):
    print(json.dumps({"counts": {"completed": 7},
                      "latest": {"completed": {"checkpointed_size": 4096,
                                               "end_to_end_duration": 120}}}))
else:
    print("{}")
''' % json.dumps(AGGREGATE_RESPONSE)

FAILING_SHIM = '''
import sys
sys.stderr.write("Cannot connect to the Docker daemon\\n")
sys.exit(1)
'''


def _docker_shim(dirpath: Path, body: str) -> None:
    """Install a `docker` executable that answers however this test needs."""
    p = dirpath / "docker"
    p.write_text("#!/usr/bin/env python3\n" + body)
    p.chmod(0o755)


def _run_collector(out_dir: Path, env: dict, interval: str = "1",
                   timeout: int = 90) -> subprocess.CompletedProcess:
    e = dict(os.environ)
    e.update(env)
    # A 1s interval keeps the suite fast. The loop's arithmetic does not depend
    # on the interval, so the behaviour under test is the same at 30s.
    return subprocess.run(
        ["bash", str(COLLECTOR), str(out_dir), interval],
        capture_output=True, text=True, env=e, timeout=timeout,
    )


def _summary_rows(out_dir: Path):
    lines = [ln for ln in (out_dir / "summary.tsv").read_text().splitlines()
             if ln.strip()]
    if not lines:
        return [], []
    return lines[0].split("\t"), [ln.split("\t") for ln in lines[1:]]


@pytest.fixture
def sandbox(tmp_path):
    """A tmp dir with a `bin/` containing the docker shim, prepended to PATH."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    return {"root": tmp_path, "bin": bindir,
            "env": {"PATH": f"{bindir}:{os.environ['PATH']}"}}


# ── P6-011: survive a transient, and report it ───────────────────────────────


def test_failing_docker_does_not_end_the_capture(sandbox):
    """A dead daemon must not turn the whole capture into one iteration.

    Before the fix this exited 1 on iteration 1 with no snapshot written at all.

    Note on scope: this asserts the OUTCOME (>=2 snapshots despite a dead
    docker), verified to fail against the pre-fix script. It does not assert
    that `-e` is absent from the `set` line: the collector is now structured so
    no single command can abort it, so re-adding `-e` alone would not change
    this result. The guard is the behaviour, not the spelling.
    """
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"], FAILING_SHIM)
    env = {**sandbox["env"], "RUN_FOR_S": "2", "O2_ENV_FILE": "/nonexistent"}
    r = _run_collector(out, env)

    snaps = sorted((out / "snapshots").glob("snap-*.json"))
    assert len(snaps) >= 2, (
        "capture did not survive a failing docker: exit=%s stdout=%r stderr=%r"
        % (r.returncode, r.stdout, r.stderr)
    )
    # Each snapshot records WHY it is thin, rather than looking like a
    # successfully-quiet cluster.
    d = json.loads(snaps[0].read_text())
    assert d["errors"], "a snapshot with no data recorded no reason"
    assert "docker stats" in d["errors"]


def test_a_missing_companion_module_is_fatal(sandbox):
    """Dropping `set -e` must not make a total failure look like success."""
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"], "import sys\nsys.exit(0)\n")
    hidden = SCRIPTS / "perf_evidence_parse.py.hidden"
    got = {"rc": None, "err": ""}
    try:
        os.rename(SCRIPTS / "perf_evidence_parse.py", hidden)
        r = subprocess.run(["bash", str(COLLECTOR), str(out), "1"],
                           capture_output=True, text=True,
                           env={**os.environ, **sandbox["env"]}, timeout=60)
        got["rc"], got["err"] = r.returncode, r.stderr
    finally:
        os.rename(hidden, SCRIPTS / "perf_evidence_parse.py")
    assert got["rc"] != 0, "missing companion was not fatal"
    assert "missing companion" in got["err"]


def test_phrases_that_match_no_container_do_not_kill_the_capture(sandbox):
    """The old `docker stats | grep -E 'flink|fluss|ingestion'` died on no-match.

    Under pipefail a grep matching nothing exited 1 and took the loop with it.
    Filtering now happens in Python, where no-match is an empty list.
    """
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"],
                 'print("unrelated-container|200MiB / 15GiB|0.0%")')
    env = {**sandbox["env"], "RUN_FOR_S": "1", "O2_ENV_FILE": "/nonexistent"}
    r = _run_collector(out, env)
    assert r.returncode == 0, r.stderr
    snaps = sorted((out / "snapshots").glob("snap-*.json"))
    assert snaps, "no snapshot written"
    d = json.loads(snaps[0].read_text())
    # The unrelated container is still recorded, under role "other": the capture
    # is not silently empty just because no phrase matched.
    assert [s["role"] for s in d["docker_stats"]] == ["other"]


# ── P6-463: argument validation and rerun safety ─────────────────────────────


def test_non_numeric_interval_is_rejected(sandbox):
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"], "import sys\nsys.exit(0)\n")
    r = subprocess.run(["bash", str(COLLECTOR), str(out), "abc"],
                       capture_output=True, text=True,
                       env={**os.environ, **sandbox["env"]}, timeout=30)
    assert r.returncode == 2
    assert "interval must be a positive integer" in r.stderr


@pytest.mark.parametrize("bad", ["0", "-5"])
def test_zero_or_negative_interval_is_rejected(sandbox, bad):
    """`0` used to busy-loop and `-5` used to abort inside sleep."""
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"], "import sys\nsys.exit(0)\n")
    r = subprocess.run(["bash", str(COLLECTOR), str(out), bad],
                       capture_output=True, text=True,
                       env={**os.environ, **sandbox["env"]}, timeout=30)
    assert r.returncode == 2
    assert "interval must be" in r.stderr


def test_rerun_continues_the_snapshot_index_instead_of_overwriting(sandbox):
    """A rerun used to restart at n=1 and destroy the previous capture."""
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"], "import sys\nsys.exit(0)\n")
    env = {**sandbox["env"], "RUN_FOR_S": "1", "O2_ENV_FILE": "/nonexistent"}
    _run_collector(out, env)
    first = sorted(p.name for p in (out / "snapshots").glob("snap-*.json"))
    assert first, "first run wrote nothing"
    _run_collector(out, env)
    second = sorted(p.name for p in (out / "snapshots").glob("snap-*.json"))
    assert len(second) > len(first), (
        "rerun overwrote instead of continuing: %s -> %s" % (first, second))
    for name in first:
        assert name in second


# ── P6-136: throughput aggregation ───────────────────────────────────────────


def test_aggregate_form_uses_sum_for_throughput_not_the_mean():
    """The core of P6-136: job throughput is the SUM across subtasks.

    The old code appended each entry's `avg` to a list and averaged those, so
    it reported 100.0 records/s for a job that was doing 800/s.
    """
    agg = P._aggregate_from(AGGREGATE_RESPONSE)
    assert agg["numRecordsInPerSecond"]["sum"] == 800.0
    assert agg["numRecordsInPerSecond"]["avg"] == 100.0
    per = P.parse_vertex_metrics(
        {"vertices": [{"id": "v1", "name": "compute"}]},
        {"v1": AGGREGATE_RESPONSE})
    totals = P.job_totals(per)
    assert totals["numRecordsInPerSecond"] == 800.0, (
        "throughput used the per-subtask mean instead of the job total")
    assert totals["numRecordsOutPerSecond"] == 640.0


def test_skew_is_preserved_so_data_distribution_can_be_read():
    """Category 6 needs the spread; the old code collapsed it to one number."""
    agg = P._aggregate_from(AGGREGATE_RESPONSE)
    busy = agg["busyTimeMsPerSecond"]
    assert busy["min"] == 10.0 and busy["max"] == 900.0
    per = P.parse_vertex_metrics(
        {"vertices": [{"id": "v1", "name": "compute"}]},
        {"v1": AGGREGATE_RESPONSE})
    # The busiest subtask is the one that bounds throughput.
    assert P.job_totals(per)["busy_max_ms_per_s"] == 900.0


def test_subtask_count_is_not_always_the_default():
    """The old loop always yielded n_subtasks=1 for the aggregate form."""
    raw = [{"id": "numRecordsInPerSecond", "min": 1.0, "max": 9.0,
            "avg": 5.0, "sum": 40.0, "subtaskCount": 8}]
    assert P._aggregate_from(raw)["numRecordsInPerSecond"]["n_subtasks"] == 8


def test_per_subtask_form_is_still_accepted_and_summed():
    """A deployment answering the older per-subtask shape must still work."""
    raw = [{"id": "0.numRecordsInPerSecond", "value": "300"},
           {"id": "1.numRecordsInPerSecond", "value": "500"}]
    agg = P._aggregate_from(raw)
    assert agg["numRecordsInPerSecond"]["sum"] == 800.0
    assert agg["numRecordsInPerSecond"]["n_subtasks"] == 2
    assert agg["numRecordsInPerSecond"]["max"] == 500.0


def test_two_vertices_are_summed_at_job_level():
    """Job throughput is the sum across vertices, not one vertex's value."""
    a = [{"id": "numRecordsInPerSecond", "avg": 100.0, "sum": 800.0}]
    b = [{"id": "numRecordsInPerSecond", "avg": 50.0, "sum": 400.0}]
    per = P.parse_vertex_metrics(
        {"vertices": [{"id": "v1", "name": "a"}, {"id": "v2", "name": "b"}]},
        {"v1": a, "v2": b})
    assert P.job_totals(per)["numRecordsInPerSecond"] == 1200.0


# ── P6-465: memory normalization ─────────────────────────────────────────────


def test_memory_is_normalized_to_bytes_across_units():
    """'1.5GiB' and '900MiB' must be comparable, not string-compared."""
    used, limit = P.parse_mem("1.5GiB / 15.46GiB")
    assert used == int(1.5 * 1024 ** 3)
    assert limit == int(15.46 * 1024 ** 3)
    big, _ = P.parse_mem("900MiB / 1GiB")
    small, _ = P.parse_mem("1.5GiB / 15GiB")
    assert small > big, "a GiB reading must compare above a MiB reading"


def test_unparseable_memory_is_none_not_a_crash():
    assert P.parse_mem("") == (None, None)
    assert P.parse_mem("garbage") == (None, None)
    assert P.parse_mem("1.2ZiB / 2ZiB") == (None, None)


def test_every_role_is_kept_not_just_taskmanager_and_ingestion():
    """The summary used to drop jobmanager and the Fluss containers entirely."""
    text = (
        "01_docker-flink-taskmanager-1|200MiB / 15GiB|0.88%\n"
        "01_docker-flink-jobmanager-1|246MiB / 15GiB|0.70%\n"
        "01_docker-fluss-tablet-1|310MiB / 15GiB|0.61%\n"
        "01_docker-fluss-coordinator-1|72MiB / 15GiB|0.17%\n"
        "01_docker-ingestion-1|500MiB / 15GiB|9.0%\n"
    )
    rows = P.parse_docker_stats(text)
    roles = {r["role"] for r in rows}
    assert roles == {"tm", "jm", "fluss_tablet", "fluss_coordinator", "ingestion"}
    for r in rows:
        assert isinstance(r["mem_used_bytes"], int)
        assert isinstance(r["cpu_perc"], float)


def test_cpu_percent_is_a_number_and_bad_values_are_none():
    rows = P.parse_docker_stats("c1|1MiB / 2GiB|12.5%\nc2|1MiB / 2GiB|--")
    assert rows[0]["cpu_perc"] == 12.5
    assert rows[1]["cpu_perc"] is None


# ── P6-466: the summary schema cannot drift ──────────────────────────────────


def test_error_row_has_exactly_the_header_field_count():
    """Header 8 fields vs error row 2 was the bug; a 5-tab 'fix' was still short."""
    # Keyed by column NAME, not by index: an index here would silently assert
    # the wrong thing the next time a column is added.
    header = P.header_row().split("\t")
    err = dict(zip(header, P.error_row(3, "boom").split("\t")))
    assert len(err) == len(header), (
        "error row has %d fields, header has %d" % (len(err), len(header)))
    assert err["snap"] == "3"
    assert "boom" in err["errors"]


def test_every_row_shape_matches_the_header():
    header = P.header_row().split("\t")
    doc = P.build_doc({"snapshot": 1, "started_at": "2026-09-16T00:00:00Z"})
    assert len(P.summary_row(doc, 1).split("\t")) == len(header)
    assert len(P.error_row(2, "x").split("\t")) == len(header)


def test_row_values_containing_tabs_cannot_add_a_column():
    """An error message containing a tab must not shift the schema."""
    header = P.header_row().split("\t")
    assert len(P.error_row(1, "bad\tvalue\there").split("\t")) == len(header)


def test_snapshot_write_is_atomic_and_leaves_no_temp_file(tmp_path):
    """A kill mid-write must not leave a truncated JSON behind."""
    target = tmp_path / "snap-1.json"
    P.write_snapshot(str(target), {"snapshot": 1, "value": "x"})
    assert json.loads(target.read_text())["snapshot"] == 1
    assert not (tmp_path / "snap-1.json.tmp").exists()
    P.write_snapshot(str(target), {"snapshot": 1, "value": "y"})
    assert json.loads(target.read_text())["value"] == "y"
    assert not (tmp_path / "snap-1.json.tmp").exists()


def test_timestamp_is_the_callers_start_time_not_the_write_time():
    """P6-466: the stamp describes when the sample started.

    The old code stamped utcnow() after the slow queries, so a 20s-stalled
    JobManager produced a snapshot dated 20s after it was taken.
    """
    doc = P.build_doc({"snapshot": 1, "started_at": "2026-09-16T01:02:03Z"})
    assert doc["ts"] == "2026-09-16T01:02:03Z"


# ── P6-137: the latency category exists ──────────────────────────────────────


def test_latency_is_recorded_when_o2_answers():
    """The header promised category 7 and nothing implemented it."""
    lat = P.build_latency({"count": 100.0, "sum": 2500.0, "p99": None, "p50": None})
    assert lat["count"] == 100.0
    assert lat["mean_ms"] == 25.0
    assert lat["source"] == "mean_fallback"
    assert lat["p99_ms"] == 25.0, "documented p99 fallback is the mean"


def test_a_served_quantile_is_preferred_over_the_mean_fallback():
    lat = P.build_latency({"count": 100.0, "sum": 2500.0, "p99": 88.0})
    assert lat["p99_ms"] == 88.0
    assert lat["source"] == "histogram_quantile"


def test_absent_latency_says_why_it_is_absent():
    """A bare null cannot distinguish a dead O2 from an idle emitter."""
    assert P.build_latency({"reason": "unreachable"})["reason"] == "unreachable"
    assert P.build_latency(
        {"reason": "no_fresh_samples"})["reason"] == "no_fresh_samples"
    assert P.build_latency(None)["reason"] == "not_configured"


def test_missing_quantiles_do_not_mark_the_category_unavailable():
    """p50/p99 do not exist in this deployment (only histogram buckets do).

    Their absence must not hide a perfectly good count/sum pair: that would
    turn the documented mean fallback into a hole.
    """
    lat = P.build_latency({"count": 10.0, "sum": 100.0, "reason": "",
                           "quantile_reason": "no_series"})
    assert lat["source"] == "mean_fallback"
    assert lat["reason"] == ""
    assert lat["quantile_reason"] == "no_series"


def test_latency_reaching_a_snapshot_is_visible_in_the_summary_row():
    doc = P.build_doc({"snapshot": 1, "started_at": "t",
                       "o2": {"count": 10.0, "sum": 100.0, "p99": None}})
    header = P.header_row().split("\t")
    row = dict(zip(header, P.summary_row(doc, 1).split("\t")))
    assert row["latency_count"] == "10.0"
    assert row["latency_mean_ms"] == "10.0"
    assert row["latency_source"] == "mean_fallback"


def test_o2_auth_reader_strips_prefix_and_preserves_base64_padding(tmp_path):
    """P6-028's trap, which the collector's reader must not reintroduce."""
    f = tmp_path / ".env"
    f.write_text("OTHER=1\nO2_AUTH_BASIC=YWJjOmRlZg==\n")
    assert P.read_o2_auth(str(f)) == "YWJjOmRlZg=="
    f.write_text('O2_AUTH_BASIC="YWJjOmRlZg=="\r\n')
    assert P.read_o2_auth(str(f)) == "YWJjOmRlZg=="
    assert P.read_o2_auth(str(tmp_path / "absent")) == ""


def test_o2_query_reports_unreachable_for_a_missing_credential(tmp_path):
    """No credential file means we cannot ask; that is not 'no fresh data'."""
    value, reason = P.query_o2("http://127.0.0.1:1",
                               str(tmp_path / "absent"), "select 1")
    assert value is None
    assert reason == "unreachable"


# ── the end-to-end shape ─────────────────────────────────────────────────────


def test_a_healthy_cluster_produces_a_complete_snapshot(sandbox):
    """The happy path: a job, its metrics, checkpoints and stats all land."""
    out = sandbox["root"] / "out"
    _docker_shim(sandbox["bin"], DOCKER_SHIM)
    env = {**sandbox["env"], "RUN_FOR_S": "1", "O2_ENV_FILE": "/nonexistent"}
    r = _run_collector(out, env)
    assert r.returncode == 0, r.stderr
    snaps = sorted((out / "snapshots").glob("snap-*.json"))
    assert snaps
    d = json.loads(snaps[0].read_text())
    assert d["jid"] == "abc123"
    assert d["checkpoints"]["counts"]["completed"] == 7
    assert d["job_totals"]["numRecordsInPerSecond"] == 800.0
    assert d["vertices"][0]["name"] == "compute"
    assert not d["errors"], d["errors"]
    header, rows = _summary_rows(out)
    assert rows
    assert all(len(row) == len(header) for row in rows), (
        "a summary row does not match the header field count")
