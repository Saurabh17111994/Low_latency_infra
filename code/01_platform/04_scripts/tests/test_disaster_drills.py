"""Unit tests for the Item F disaster-drill runner (no real faults here —
probes and docker calls are mocked; the live drills run separately with
--approve against the local compose stack)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import disaster_drills as dd


REQUIRED_KEYS = {
    "id", "title", "fault_class", "documented_expectation", "pre", "fault",
    "during", "recovery", "post", "bound_s",
}


def test_all_drills_have_required_spec_keys():
    for d in dd.DRILLS:
        missing = REQUIRED_KEYS - set(d)
        assert not missing, f"{d['id']} missing {missing}"
        assert d["bound_s"] > 0
        assert d["fault"], f"{d['id']} must inject at least one fault step"


def test_drill_ids_unique_and_probes_registered():
    ids = [d["id"] for d in dd.DRILLS]
    assert len(ids) == len(set(ids))
    for d in dd.DRILLS:
        for role in ("pre", "during", "post"):
            for item in d[role]:
                probe = item[0]
                assert probe in dd.PROBES, f"{d['id']} {role} uses unknown probe {probe}"


def test_evidence_rendered_with_all_required_headings(tmp_path):
    d = dd.drill_by_id("DR-001")
    record = {
        "id": d["id"], "title": d["title"], "fault_class": d["fault_class"],
        "fault_time_utc": "2026-08-21T00:00:00Z", "suite": "TEST",
        "approve": True, "verdict": "PASS", "path": "logs/disaster-drills/DR-001-x.md",
        "sections": ["s%d" % i for i in range(len(dd.EVIDENCE_HEADINGS))],
    }
    text = dd.render_evidence(d, record)
    for heading in dd.EVIDENCE_HEADINGS:
        assert heading in text
    assert "Verdict: PASS" in text
    # P6-619: the line here was `assert "log" not in text.lower() or True` — always
    # true, and it could never have worked anyway: the record's own path contains
    # "log". No structural rule was ever specified, so the line is gone rather than
    # guessed at (see CHG-243, observations).


def test_redact_strips_secret_shapes():
    dirty = (
        "password=Dev-o2-local!2026 AWS_SECRET_ACCESS_KEY=hunter2 "
        "Authorization: Basic dXNlcjpwYXNz secret_token=abc"
    )
    clean = dd.redact(dirty)
    assert "Dev-o2-local!2026" not in clean
    assert "hunter2" not in clean
    assert "dXNlcjpwYXNz" not in clean
    assert "REDACTED" in clean


def test_dry_run_touches_nothing_and_exits_zero():
    assert dd.main(["--dry-run"]) == 0
    assert dd.main(["--dry-run", "--drill", "DR-001", "--drill", "DR-004"]) == 0


def test_fault_injection_refused_without_approve():
    assert dd.main(["--drill", "DR-001"]) == 3


def test_unknown_drill_is_usage_error():
    assert dd.main(["--dry-run", "--drill", "DR-999"]) == 2


def test_drive_precondition_failure_aborts(monkeypatch, tmp_path):
    """A failed pre-check must make the drill FAIL, never fault blindly."""
    captured = []

    def fake_probe(_name, *_):
        return False, "forced down"

    monkeypatch.setattr(dd, "PROBES",
                        {k: lambda n=k: fake_probe(n) for k in dd.PROBES})
    monkeypatch.setattr(dd, "docker", lambda *a, **k: {"rc": 0, "out": "true 0"})

    verdict, record = dd.drive(dd.drill_by_id("DR-001"), "TEST", True,
                               str(tmp_path), False)
    assert verdict == "FAIL"
    assert record["verdict"] == "FAIL"


def test_probe_fluss_success_tokens(monkeypatch):
    """The success line is 'no DDL drift detected' — a naive 'DDL drift not
    in output' check must never regress (2026-08-21 regression)."""
    real_out = (
        "Manifest is current; no DDL drift detected.\n"
        "/app/code/01_platform/02_sql/ddl/schema_manifest.json unchanged (26 tables)."
    )
    monkeypatch.setattr(dd, "compose",
                        lambda *a, **k: {"rc": 0, "out": real_out})
    ok, detail = dd.probe_fluss()
    assert ok, detail

    stale_out = real_out.replace("no DDL drift", "DDL drift")
    monkeypatch.setattr(dd, "compose",
                        lambda *a, **k: {"rc": 0, "out": stale_out})
    ok, _ = dd.probe_fluss()
    assert not ok


def test_o2_auth_header_built_without_leak(monkeypatch):
    """The O2 probe must authenticate from the compose .env and never print
    the credential (2026-08-21 regression: pre-auth probe always saw 401)."""
    header = dd._o2_auth_header()
    assert header.startswith("Authorization: Basic ")
    assert " " not in header[len("Authorization: Basic "):]
    monkeypatch.setattr(dd, "run",
                        lambda cmd, timeout=30: {"rc": 0, "out": "200"})
    ok, detail = dd.probe_o2()
    assert ok, detail
    assert "Basic" not in detail


def test_drive_passes_and_writes_evidence(monkeypatch, tmp_path):
    """Happy-path drive: all probes green, evidence file written, verdict PASS."""
    calls = []

    def fake_runner(cmd, timeout=60):
        calls.append(cmd)
        return {"rc": 0, "out": "ok"}

    def green(*_a, **_k):
        return True, "ok"

    monkeypatch.setattr(dd, "run", fake_runner)
    monkeypatch.setattr(
        dd, "docker",
        lambda *a, **k: {"rc": 0, "out": "01_docker_trading-net "})
    monkeypatch.setattr(dd, "PROBES", {k: green for k in dd.PROBES})

    verdict, record = dd.drive(dd.drill_by_id("DR-006"), "TEST", True,
                               str(tmp_path), False)
    assert verdict == "PASS"
    assert record["verdict"] == "PASS"
    # RPO/RTO and approvals must be recorded in the evidence sections.
    joined = "\n".join(record["sections"])
    assert "RPO = 0" in joined
    assert "RTO =" in joined
    assert "approval" in joined.lower() or "approve" in joined.lower()
    ev_file = next(p for p in os.listdir(str(tmp_path)) if p.startswith("DR-006-"))
    content = open(os.path.join(str(tmp_path), ev_file), encoding="utf-8").read()
    assert "## Verdict" in content and "PASS" in content
    assert calls, "fault/recovery steps must actually be executed via run()"


def test_resolve_steps_replaces_network_sentinel(monkeypatch):
    monkeypatch.setattr(
        dd, "docker",
        lambda *a, **k: {"rc": 0, "out": "01_docker_trading-net "})
    steps, err = dd.resolve_steps([["docker", "network", "disconnect", "{{TRADING_NET}}"]],
                                  False)
    assert err is None
    assert steps == [["docker", "network", "disconnect", "01_docker_trading-net"]]


def test_resolve_steps_fails_when_net_unresolvable(monkeypatch):
    monkeypatch.setattr(dd, "docker", lambda *a, **k: {"rc": 0, "out": ""})
    _steps, err = dd.resolve_steps([["docker", "network", "disconnect", "{{TRADING_NET}}"]],
                                   False)
    assert err and "cannot resolve" in err


def test_recovery_step_failure_fails_drill(monkeypatch, tmp_path):
    """A failing recovery step must make the drill FAIL — never PASS with a
    broken recover step (2026-08-21 honesty gap: recovery rc was ignored)."""
    monkeypatch.setattr(
        dd, "run",
        lambda cmd, timeout=60: {"rc": 0, "out": "x"})
    monkeypatch.setattr(dd, "reconnect_tablet",
                        lambda retries=5: (1, "forced heal failure"))
    monkeypatch.setattr(
        dd, "docker",
        lambda *a, **k: {"rc": 0, "out": "01_docker_trading-net "})
    monkeypatch.setattr(dd, "PROBES",
                        {k: (lambda: (True, "ok")) for k in dd.PROBES})

    verdict, record = dd.drive(dd.drill_by_id("DR-006"), "TEST", True,
                               str(tmp_path), False)
    assert verdict == "FAIL"
    assert record["verdict"] == "FAIL"


def test_records_rpo_zero_and_measured_rto():
    for d in dd.DRILLS:
        joined = dd.render_evidence(d, {
            "id": d["id"], "title": d["title"], "fault_class": d["fault_class"],
            "fault_time_utc": "2026-08-21T00:00:00Z", "suite": "TEST",
            "approve": True, "verdict": "PASS", "path": "x",
            "sections": ["—"] * len(dd.EVIDENCE_HEADINGS),
        })
        assert "RPO / RTO" in joined
