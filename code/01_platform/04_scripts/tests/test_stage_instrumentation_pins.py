"""Static pins (gate step 3, CHG-451/W3-i): the stage profiler carries JVM
instrumentation for the residual-stall attribution.

Design note: ``docs/plans/2026-09-30-w3-w4-design-note.md`` (W3-i). The
large-tail outliers (200-430 ms) are not explained by the TM JVM (gc.log/JFR
exonerated it); the remaining suspects are the ingestion-writer JVM, the Fluss
tablet and host scheduling. The profiler must therefore:

1. launch every per-run ingestion container with ``JAVA_TOOL_OPTIONS`` carrying
   a GC+safepoint log and a bounded JFR recording (both branches: faketool and
   real);
2. pull those files into the round evidence at teardown, together with the TM
   ``gc.log``/``tm-diag.jfr``;
3. sample per-container CPU/memory at ~1 s during each phase;
4. run the GC/safepoint summariser after each phase.

Anything that silently drops one of these leaves the next round unable to
attribute the outliers -- the same silent-gap class the W1 fetch-wait pin
(CHG-449) and the STRATEGY_CONTEXT_* pins guard.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PROFILER = (
    REPO / "code" / "01_platform" / "06_stage_profiler" / "stage-profile.sh"
)
COMPOSE = REPO / "code" / "01_platform" / "01_docker" / "docker-compose.yml"


def test_changelog_base_path_wiped_per_phase() -> None:
    """CHG-458: the FS changelog base path is a persistent shared volume; without
    a per-phase wipe, post-restart runs see stale files and the path grows
    unbounded (1.9 GB/day). The profiler preflight must wipe it, fail-closed.
    (The 457-465/round "state is not in tracking" WARNs proved intra-run and
    are NOT the stale-file artifact; the wipe's rationale is hygiene.)"""
    src = PROFILER.read_text(encoding="utf-8")
    assert "/checkpoints/changelog && mkdir -p /checkpoints/changelog" in src, (
        "the per-phase changelog wipe is gone from stage-profile.sh preflight"
    )


def test_changelog_preemptive_persist_threshold_stays_at_default() -> None:
    """CHG-459 (revert of the CHG-458 tuning): the 1 MB preemptive-persist
    threshold showed no KPI win, a checkpoint-e2e median regression (57 vs
    37 ms) and a registry-WARN amplification (1297 vs 457-465/round) in
    logs/chg458-main-20260930-135319. The key must stay unset — the pinned
    2.2.1 default (5 MB) applies — unless a future CHG brings its own
    evidence. Option verified in the pinned flink-dstl-dfs-2.2.1.jar bytecode
    (FsStateChangelogOptions)."""
    src = COMPOSE.read_text(encoding="utf-8")
    assert "state.changelog.dstl.dfs.preemptive-persist-threshold:" not in src, (
        "a preemptive-persist-threshold override is back in docker-compose.yml "
        "without CHG evidence"
    )


def test_tm_g1_pause_target_is_pinned() -> None:
    """W3-a (CHG-453): the TM runs a fixed 2.15 GiB heap with G1's default
    200 ms pause target; the instrumented round measured 40-48 ms pauses at the
    tick spike windows. ``-XX:MaxGCPauseMillis=20`` is the approved lever — pin
    it here so a compose edit cannot silently revert the tuning. Applying it
    needs a container recreate (`docker compose up -d flink-taskmanager`); the
    profiler's per-phase `restart` alone would keep the old env."""
    src = COMPOSE.read_text(encoding="utf-8")
    assert "-XX:MaxGCPauseMillis=20" in src, (
        "the TM G1 pause target (W3-a) is gone from docker-compose.yml"
    )


def _src() -> str:
    return PROFILER.read_text(encoding="utf-8")


def test_ingestion_jvm_instrumentation_defined_and_applied() -> None:
    src = _src()
    assert "INGESTION_JAVA_TOOL_OPTIONS=" in src, (
        "the ingestion JVM instrumentation variable is gone"
    )
    assert "-Xlog:gc*,safepoint:file=/tmp/gc.log" in src, (
        "the ingestion GC log flag is gone (W3-i needs it for the outliers)"
    )
    applied = src.count('-e "JAVA_TOOL_OPTIONS=$INGESTION_JAVA_TOOL_OPTIONS"')
    assert applied == 2, (
        "JAVA_TOOL_OPTIONS must be applied to BOTH ingestion launch branches "
        f"(faketool + real); found {applied}"
    )


def test_ingestion_jvm_flags_are_disableable_with_an_empty_value() -> None:
    """CHG-548: ``${VAR:-default}`` treats an explicitly empty value as unset, so
    ``INGESTION_JAVA_TOOL_OPTIONS=""`` silently re-enabled the GC log and the JFR
    recording. The 2026-10-04 JFR A/B only proved its treatment by inspecting the
    live containers, which is exactly the failure this pin removes: it evaluates the
    profiler's real assignment, so the three states stay correct — unset -> the
    default flags, empty -> disabled, explicit value -> passed through."""
    line = next(
        l for l in _src().splitlines() if l.startswith("INGESTION_JAVA_TOOL_OPTIONS=")
    )

    def resolved(value: str | None) -> str:
        env = {
            k: v for k, v in os.environ.items()
            if k != "INGESTION_JAVA_TOOL_OPTIONS"
        }
        if value is not None:
            env["INGESTION_JAVA_TOOL_OPTIONS"] = value
        out = subprocess.run(
            ["bash", "-c", f'{line}\nprintf %s "$INGESTION_JAVA_TOOL_OPTIONS"'],
            capture_output=True, text=True, env=env, check=True,
        )
        return out.stdout

    default = resolved(None)
    assert "-Xlog:gc*,safepoint:file=/tmp/gc.log" in default
    assert "-XX:StartFlightRecording=settings=profile" in default
    assert resolved("") == "", "an empty value must disable the instrumentation"
    assert resolved("-XX:+UseG1GC") == "-XX:+UseG1GC"


def test_ingestion_and_tm_jvm_artifacts_are_pulled_into_evidence() -> None:
    src = _src()
    assert 'docker cp "$c:/tmp/gc.log"' in src, (
        "teardown no longer copies the per-container ingestion gc.log"
    )
    assert 'docker cp "$c:/tmp/ing-diag.jfr"' in src, (
        "teardown no longer copies the per-container ingestion JFR"
    )
    assert 'docker cp "$FLINK_TM_CONTAINER:/opt/flink/log/gc.log"' in src, (
        "teardown no longer copies the TM gc.log"
    )


def test_container_stats_sampler_is_wired_for_each_phase() -> None:
    src = _src()
    assert "start_stats_sampler()" in src, "the stats sampler function is gone"
    assert "start_stats_sampler \"" in src, (
        "run_phase no longer starts the per-container stats sampler"
    )
    assert "STATS_PID" in src, (
        "the stats sampler has no PID variable (it cannot be stopped)"
    )
    # stop_fleet must kill it, exactly like the other background log processes.
    assert 'kill "$STATS_PID"' in src, (
        "stop_fleet no longer kills the stats sampler"
    )
    # Host CPU/IO/memory pressure (PSI) rides along in the same sample: it is
    # the only host-level signal that separates "a container was preempted"
    # from "a container's JVM stalled" (2026-09-30: PSI-cpu 8% at the 354 ms
    # outlier vs 0-4% elsewhere).
    assert "/proc/pressure/" in src, (
        "the stats sampler no longer records host pressure (PSI)"
    )


def test_ingestion_jfr_is_materialized_by_graceful_stop() -> None:
    src = _src()
    # A JFR recording configured with filename+maxsize writes its file when the
    # recording ENDS (duration expiry or JVM exit) -- a docker rm -f (SIGKILL)
    # in teardown loses it. The writers run 1800 s recordings, longer than any
    # phase, so teardown must stop them gracefully before the copy.
    assert 'docker stop -t 10 "$c"' in src, (
        "stop_fleet no longer stops the ingestion container gracefully — the "
        "per-round JFR would silently stay 0 bytes"
    )


def test_gc_summary_runs_after_each_phase() -> None:
    src = _src()
    assert "stage_gc_summary.py" in src, (
        "the GC/safepoint summariser is no longer called by the profiler"
    )
