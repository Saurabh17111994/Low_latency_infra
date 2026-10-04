"""Fail-fast gates in the stage-capture.sh pre-run block (2026-10-04).

Both checks run BEFORE the timed capture window, next to the existing
"fail NOW, not after DURATION_S" liveness legs:

1. requested-vs-deployed topology — the three branch flags reach only the
   `flink run` CLIENT (pipeline-lib.sh `docker compose exec -T -e ...`), so an
   unset flag silently defaults to false in SignalJobConfig and whole operator
   chains vanish. Without `STRATEGY_HOST_ENABLED=true` there is no
   candle-features-sink, candle_features stays empty, and every read probe
   reports 0 rows — which surfaces 3.5 min later as a bare
   "S8.feature_read: 0 < 1 samples" presence failure. Four runs on 2026-10-04
   hit exactly this.
2. read-path expectation — branch deployed but candle_features not readable
   NOW means the capture records an empty read leg. This one is TIMING
   dependent (a row exists only after the first candle window seals), so it
   WARNS by default and never fails the run; READ_EXPECTATION_STRICT=1 opts
   into the hard failure. The presence gate at the end stays the authority.

HERMETIC: curl/sleep/docker/java/javac are stubbed on PATH, so nothing here
reaches Flink or the cluster (pattern: test_stage_a2_baseline_wave38.py).
DURATION_S=1 keeps the post-gate path short on old code that lacks the gates.
"""

import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parent
SCRIPTS = TESTS.parent
SCRIPT = SCRIPTS / "stage-capture.sh"

STRATEGY_VERTEX = {"id": "df20787985f46dc40528902f480d9676", "name": "candle-features-sink: Writer"}
REDUCED_VERTICES = [
    {"id": "d49b076529ea4f87695ebe831a95f6bf", "name": "Source: raw-table-1 -> raw-validation"},
    {"id": "c608566043058f50e68a5930f2fe450e", "name": "fingerprint-dedup -> ingest-latency-monitor"},
    {"id": "60828025aaab0947a851fefb156ff0e6", "name": "multi-tf-aggregator"},
]
FULL_VERTICES = REDUCED_VERTICES + [
    STRATEGY_VERTEX,
    {"id": "082243312e2cb726386581b3d6fb0d38", "name": "strategy-host -> canonical-signal-filter-strategy-host"},
]


def _stub_bin(tmp_path: Path, vertices: list[dict[str, str]], java_rows: str) -> Path:
    """curl/sleep/docker/java/javac stubs — no Flink, no cluster, no real JVM."""
    d = tmp_path / "stubs"
    d.mkdir()
    job_json = tmp_path / "job.json"
    job_json.write_text(json.dumps({"state": "RUNNING", "vertices": vertices}))
    (tmp_path / "java-out.tsv").write_text(java_rows)
    (d / "curl").write_text(
        "#!/usr/bin/env bash\n"
        f'case "$*" in *"/jobs/"*) cat {job_json};; esac\n'
        "exit 0\n"
    )
    (d / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
    (d / "docker").write_text("#!/usr/bin/env bash\nexit 0\n")
    (d / "java").write_text(f"#!/usr/bin/env bash\ncat {tmp_path}/java-out.tsv\nexit 0\n")
    (d / "javac").write_text("#!/usr/bin/env bash\nexit 0\n")
    for f in d.iterdir():
        f.chmod(f.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return d


def _run(tmp_path: Path, stubs: Path, env_extra: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = str(stubs) + os.pathsep + env["PATH"]
    env.update(
        {
            "JOB_ID": "stub-job",
            "OUT_DIR": str(tmp_path / "out"),
            "DURATION_S": "1",
            "CAPTURE_INTERVAL_S": "1",
            "PROBE_TOKENS": "1,2",
            "PROBE_BOOTSTRAP": "stub:0",
        }
    )
    env.update(env_extra)
    return subprocess.run(
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120
    )


def _run_meta(tmp_path: Path) -> str:
    """The gate's own evidence file — what a later reader (and the plan) trusts.

    Asserting on run-meta.txt rather than on the process exit code matters here:
    these cases run the WHOLE capture on stub data, so a tail-leg failure (e.g.
    the prom-staleness check) can return a non-zero code of its own. The gate's
    outcome must be readable from the artifact, not inferred from rc.
    """
    path = tmp_path / "out" / "run-meta.txt"
    return path.read_text() if path.exists() else ""


@pytest.mark.parametrize("vertices", [REDUCED_VERTICES])
def test_topology_flag_true_but_vertex_missing_fails(
    tmp_path: Path, vertices: list[dict[str, str]]
) -> None:
    stubs = _stub_bin(tmp_path, vertices, java_rows="")
    proc = _run(tmp_path, stubs, {"STRATEGY_HOST_ENABLED": "true"})
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, out
    assert "STRATEGY_HOST_ENABLED" in out and "strategy-host" in out, out
    # The gate exits before run-meta.txt is opened: a hard-failed capture leaves
    # NO evidence file behind, so a later reader cannot mistake a half-run for a
    # measured one. Assert the invariant, not a line that cannot exist yet.
    assert _run_meta(tmp_path) == "", out


def test_topology_ok_but_read_leg_empty_warns_by_default(tmp_path: Path) -> None:
    stubs = _stub_bin(tmp_path, FULL_VERTICES, java_rows="")
    proc = _run(
        tmp_path,
        stubs,
        {
            "STRATEGY_HOST_ENABLED": "true",
            "FLUSS_PROBE_CP": "stub.jar",
            "READ_EXPECTATION_TIMEOUT_S": "0",
        },
    )
    out = proc.stdout + proc.stderr
    assert "no readable row" in out, out
    assert proc.returncode != 1, f"warn mode must not fail the capture:\n{out}"
    assert "read_expectation=empty" in _run_meta(tmp_path), out


def test_read_leg_empty_with_strict_knob_exits_1(tmp_path: Path) -> None:
    stubs = _stub_bin(tmp_path, FULL_VERTICES, java_rows="")
    proc = _run(
        tmp_path,
        stubs,
        {
            "STRATEGY_HOST_ENABLED": "true",
            "FLUSS_PROBE_CP": "stub.jar",
            "READ_EXPECTATION_TIMEOUT_S": "0",
            "READ_EXPECTATION_STRICT": "1",
        },
    )
    out = proc.stdout + proc.stderr
    assert proc.returncode == 1, out
    assert "no readable row" in out, out
    assert "read_expectation=empty" in _run_meta(tmp_path), out


def test_topology_ok_and_read_leg_readable_passes_gate(tmp_path: Path) -> None:
    stubs = _stub_bin(tmp_path, FULL_VERTICES, java_rows="1\t0\t0\t15000\t0\t0\n")
    proc = _run(
        tmp_path,
        stubs,
        {"STRATEGY_HOST_ENABLED": "true", "FLUSS_PROBE_CP": "stub.jar"},
    )
    out = proc.stdout + proc.stderr
    assert "read-path expectation satisfied" in out, out
    assert "no readable row" not in out, out
    assert "read_expectation=satisfied" in _run_meta(tmp_path), out


def test_reduced_topology_stays_legal(tmp_path: Path) -> None:
    """An unset flag means a deliberately reduced arm — the gates must not fire."""
    stubs = _stub_bin(tmp_path, REDUCED_VERTICES, java_rows="")
    proc = _run(tmp_path, stubs, {})
    out = proc.stdout + proc.stderr
    assert "STRATEGY_HOST_ENABLED=true but" not in out, out
    assert "no readable row" not in out, out
    # No branch requested ⇒ no read expectation was formed, so that line is
    # absent; the flags line is always written and reports every branch off.
    assert "read_expectation=" not in _run_meta(tmp_path), out
    assert (
        "topology_flags_requested= STRATEGY_HOST_ENABLED=off MULTITF_ENABLED=off "
        "EXECUTION_INTENT_ENABLED=off" in _run_meta(tmp_path)
    ), out
