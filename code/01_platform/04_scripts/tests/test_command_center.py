"""COMMAND - Command Center contract: plain words, honest darkness, fail-fast.

The captain's screen must never show a bare 0/1 the user has to decode, and
must never fake-green an unwired execution path. These tests pin:

  - status tiles (feed link / feed safety / pipeline) are SQL *text* tables
    (UP-or-DOWN words), not promql 0/1 gauges;
  - no COMMAND title carries a numeric "(1 = yes, 0 = no)" legend;
  - every [NOT LIVE YET] panel points at a stream in the declared
    KNOWN_COMMAND_MISSING_STREAMS set (and vice versa - a live stream must
    never wear the prefix);
  - promql panels whose query has no task_name label carry an explicit
    legend (else O2 renders the raw {{task_name}} template, as seen live
    2026-09-05 on the disk/health/worker tiles).

Spec-only: imports o2-provision.py for its DASHBOARDS corpus, makes no API
calls. A dummy O2_AUTH_BASIC satisfies the module's import-time guard.
"""
import importlib.util
import os
import re
from pathlib import Path

os.environ.setdefault("O2_AUTH_BASIC", "ZHVtbXk6ZHVtbXk=")  # dummy, never used

PROV = Path(__file__).resolve().parents[1] / "o2-provision.py"
_spec = importlib.util.spec_from_file_location("o2provision_cmd", PROV)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

DASHBOARDS = _mod.DASHBOARDS


def _command():
    cmd = next(
        (d for d in DASHBOARDS if d.get("title") == "COMMAND - Command Center"),
        None,
    )
    assert cmd is not None, "COMMAND - Command Center missing from DASHBOARDS"
    return cmd


# Panels identified by stream (stable across title renames).
STATUS_TILES = {
    "bridge_connected": ("UP", "DOWN"),
    "bridge_slot_safety_state": ("SAFE", "STOP"),
    "ingestion_ready": ("READY", "NOT READY"),
}

NUMERIC_LEGEND = re.compile(r"\(1\s*=\s*(yes|no|safe|stop|ready)", re.I)


def test_command_titles_have_no_numeric_legend():
    bad = [p[0] for p in _command()["panels"] if NUMERIC_LEGEND.search(p[0])]
    assert not bad, f"numeric 0/1 decode legends must become words: {bad}"


def test_command_status_tiles_are_text_tables():
    panels = {p[3]: p for p in _command()["panels"]}
    for stream, words in STATUS_TILES.items():
        assert stream in panels, f"status tile for {stream} missing from COMMAND"
        title, ptype, query = panels[stream][0], panels[stream][1], panels[stream][2]
        assert ptype == "table", (
            f"{title!r}: status tile must be a SQL text table, got {ptype!r}"
        )
        assert "CASE WHEN" in query.upper(), (
            f"{title!r}: status query must map the gauge to words with CASE WHEN"
        )
        for w in words:
            assert f"'{w}'" in query, (
                f"{title!r}: status query must render the word {w!r}"
            )


def test_command_text_tables_omit_placeholder_where():
    """O2 does NOT substitute '{start_time}' in SQL (verified live 2026-09-05):
    a string-vs-BIGINT comparison silently returns no rows. SQL panels must
    rely on the request body window instead."""
    for p in _command()["panels"]:
        if p[1] == "promql":
            continue
        assert "{start_time}" not in p[2] and "{end_time}" not in p[2], (
            f"{p[0]!r}: SQL must not carry {{start_time}}/{{end_time}} "
            "placeholders (never substituted; silently empty)"
        )


def test_command_text_tables_select_latest_single_row():
    panels = {p[3]: p for p in _command()["panels"]}
    for stream in STATUS_TILES:
        title, query = panels[stream][0], panels[stream][2]
        assert "desc limit 1" in query.lower(), (
            f"{title!r}: text tile must select the latest single row so the "
            "UI fires the SQL request"
        )


def test_validate_command_spec_flags_bad_spec():
    assert callable(getattr(_mod, "validate_command_spec", None)), (
        "o2-provision.py must expose validate_command_spec() (fail-fast guard)"
    )
    bad = [
        {
            "title": "COMMAND - Command Center",
            "description": "bad",
            "panels": [
                (
                    "Feed link up? (1 = yes, 0 = no)",
                    "promql",
                    "max(bridge_connected)",
                    "bridge_connected",
                )
            ],
        }
    ]
    errors = _mod.validate_command_spec(bad)
    assert errors, "validate_command_spec() must flag the numeric-legend panel"
    assert any("Feed link up?" in e for e in errors), (
        f"errors must name the panel: {errors}"
    )


def test_validate_command_spec_passes_on_current_spec():
    assert _mod.validate_command_spec() == [], (
        f"current COMMAND spec must validate clean: "
        f"{_mod.validate_command_spec()}"
    )


def test_not_live_yet_matches_declared_missing_streams():
    missing = set(_mod.KNOWN_COMMAND_MISSING_STREAMS)
    assert missing == {
        "execution_orders_submitted",
        "execution_fills_confirmed",
        "execution_gate_denied",
    }, f"missing-stream set drifted: {sorted(missing)}"
    panels = _command()["panels"]
    dark = {p[3] for p in panels if "[NOT LIVE YET]" in p[0]}
    # O2 v0.91.5 cannot draw an honest placeholder: any panel on a missing
    # stream (SQL or promql) stalls the OTHER SQL panels in the dashboard
    # (verified live 2026-09-05), so the dark tiles are ABSENT by design and
    # the missing-stream set is the contract C2 must light up.
    assert dark == set(), (
        f"[NOT LIVE YET] tiles must be absent (missing streams stall the "
        f"dashboard SQL batch): {sorted(dark)}"
    )
    live = set(_mod.KNOWN_COMMAND_LIVE_STREAMS)
    for p in panels:
        assert p[3] in live, (
            f"{p[0]!r}: stream {p[3]!r} belongs to no KNOWN_COMMAND_* set"
        )


def test_command_promql_without_taskname_has_explicit_legend():
    bare = [
        p[0]
        for p in _command()["panels"]
        if p[1] == "promql"
        and "task_name" not in p[2]
        and (len(p) < 5 or not p[4])
    ]
    assert not bare, (
        "O2 renders raw {{task_name}} on these (seen live 2026-09-05): "
        f"{bare}"
    )


def test_command_sql_panels_carry_no_placeholder_where():
    """All SQL panels must omit {start_time}/{end_time} — O2 never
    substitutes them (string-vs-BIGINT -> silently empty)."""
    bad = [
        p[0]
        for p in _command()["panels"]
        if p[1] != "promql"
        and ("{start_time}" in p[2] or "{end_time}" in p[2])
    ]
    assert not bad, f"SQL panels must not carry time placeholders: {bad}"


def test_command_not_live_yet_tiles_are_promql_empty():
    """C2 contract: the execution-path streams are declared missing and NO
    panel may reference them (any panel on a missing stream — SQL or promql —
    stalls the other SQL panels in the dashboard, verified live 2026-09-05)."""
    missing = set(_mod.KNOWN_COMMAND_MISSING_STREAMS)
    for p in _command()["panels"]:
        assert p[3] not in missing, (
            f"{p[0]!r}: must not reference missing stream {p[3]!r} "
            "(stalls the dashboard SQL batch)"
        )


def test_command_streams_are_underscore_names():
    dotted = [p[0] for p in _command()["panels"] if "." in p[3]]
    assert not dotted, f"O2 metric streams use underscores, not dots: {dotted}"
