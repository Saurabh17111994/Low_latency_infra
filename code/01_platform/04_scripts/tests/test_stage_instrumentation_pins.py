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

from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PROFILER = (
    REPO / "code" / "01_platform" / "06_stage_profiler" / "stage-profile.sh"
)
COMPOSE = REPO / "code" / "01_platform" / "01_docker" / "docker-compose.yml"


def test_changelog_materialization_interval_is_pinned() -> None:
    """W3-c (CHG-454): the changelog's background materialization produces the
    mid-spike class (91-216 ms per event, 1-2 staggered rounds/subtask per
    900 s at the 10 min default). The interval key must stay in the shared
    FLINK_PROPERTIES block (a TASKMANAGER-read cluster key, CHG-444) or the
    10 min default silently returns. Production adoption additionally needs
    the restore drill (design-note W3-c guardrail)."""
    src = COMPOSE.read_text(encoding="utf-8")
    assert "state.changelog.periodic-materialize.interval: 30 min" in src, (
        "the W3-c changelog materialization interval is gone from docker-compose.yml"
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
