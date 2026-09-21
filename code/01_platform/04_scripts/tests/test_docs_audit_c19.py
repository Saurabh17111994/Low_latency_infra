"""C19 — the alert catalogue agrees with the provisioners (plan task T11, area B6a).

The dossier's "Alert catalogue" table states, per security area, the mechanism
that holds it and whether a runtime alert exists. That statement is about code,
so it is checked in both directions, and the table's own measured sentence
("provisions 47 rules — 20 `SIGNAL-*`, …") is checked against the provisioners
because it is the evidence the eight areas rest on.

Plain functions, not unittest.TestCase: the documented runner passes
`-p no:unittest`, so a TestCase class here would be skipped and certify nothing.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import docs_audit  # noqa: E402


def _catalogue(runtime_cell, area="Credential expiry, revocation"):
    return (
        "#### Alert catalogue\n\n"
        "| Area | Mechanism that holds it today | Where | Runtime alert |\n"
        "| --- | --- | --- | --- |\n"
        f"| {area} | the rotation log | `05_deployment/04-secrets-rotation.md` | {runtime_cell} |\n"
    )


def _count_doc(total, signal, ing, infra):
    return (
        "#### Alert catalogue\n\n"
        f"`o2-provision.py` provisions {total} rules — {signal} `SIGNAL-*`, "
        f"{ing} `ING-*`, {infra} `INFRA-*` — and no rule names a security fact.\n"
    )


def _names(signal=20, ing=18, infra=9, extra=()):
    names = [f"SIGNAL-rule-{i}" for i in range(signal)]
    names += [f"ING-rule-{i}" for i in range(ing)]
    names += [f"INFRA-rule-{i}" for i in range(infra)]
    return names + list(extra)


# --- the two controls the plan names -------------------------------------


def test_a_row_claiming_a_runtime_alert_with_no_rule_is_flagged():
    doc = _catalogue("yes — SEC-credential-expiry, armed on the broker counter")
    problems = docs_audit.alert_catalogue_problems(doc, ["ING-warn-capacity-80"])
    assert any("no provisioner defines" in p for p in problems), problems


def test_a_sec_rule_with_no_table_row_is_flagged():
    problems = docs_audit.alert_catalogue_problems(
        _catalogue("no — nothing emits the fact"), ["SEC-credential-expiry"]
    )
    assert any("no catalogue row names it" in p for p in problems), problems


# --- the other direction and the denials ---------------------------------


def test_a_sec_rule_named_by_a_row_passes():
    doc = _catalogue("SEC-credential-expiry — armed on the broker counter")
    assert docs_audit.alert_catalogue_problems(doc, ["SEC-credential-expiry"]) == []


def test_a_claim_naming_a_provisioned_rule_passes():
    doc = _catalogue("ING-warn-capacity-80")
    assert docs_audit.alert_catalogue_problems(doc, ["ING-warn-capacity-80"]) == []


def test_denial_cells_are_not_claims():
    # Measured over all eight real rows on 2026-09-21: every cell is one of these.
    for cell in ("no", "no — nothing emits the fact", "build-time, not runtime"):
        problems = docs_audit.alert_catalogue_problems(_catalogue(cell), ["ING-warn-capacity-80"])
        assert problems == [], (cell, problems)


def test_a_near_miss_rule_name_is_not_a_match():
    # `ING-warn-capacity-80-extra` must not satisfy a row for `ING-warn-capacity-80`.
    doc = _catalogue("ING-warn-capacity-80-extra")
    problems = docs_audit.alert_catalogue_problems(doc, ["ING-warn-capacity-80"])
    assert any("no provisioner defines" in p for p in problems), problems


# --- vacuity and readability ---------------------------------------------


def test_a_missing_catalogue_section_is_flagged():
    problems = docs_audit.alert_catalogue_problems("# nothing here\n", ["ING-x"])
    assert any("Alert catalogue" in p for p in problems), problems


def test_a_catalogue_with_no_rows_is_flagged():
    doc = (
        "#### Alert catalogue\n\n"
        "| Area | Mechanism that holds it today | Where | Runtime alert |\n"
        "| --- | --- | --- | --- |\n"
    )
    problems = docs_audit.alert_catalogue_problems(doc, ["ING-x"])
    assert any("no security-area rows" in p for p in problems), problems


def test_an_empty_provisioner_set_is_flagged():
    problems = docs_audit.alert_catalogue_problems(_catalogue("no"), [])
    assert any("no provisioned alert rule" in p for p in problems), problems


# --- the measured sentence in the same section ----------------------------


def test_the_documents_count_claim_is_checked():
    assert docs_audit.alert_rule_count_problems(_count_doc(47, 20, 18, 9), _names()) == []


def test_a_wrong_total_is_flagged():
    problems = docs_audit.alert_rule_count_problems(_count_doc(47, 20, 18, 8), _names(infra=8))
    assert any("claims 47 rules" in p for p in problems), problems


def test_a_wrong_per_prefix_count_is_flagged():
    problems = docs_audit.alert_rule_count_problems(_count_doc(47, 19, 19, 9), _names())
    assert any("SIGNAL-*" in p for p in problems), problems


def test_a_missing_count_claim_is_flagged():
    problems = docs_audit.alert_rule_count_problems("#### Alert catalogue\n", _names())
    assert problems, "the measured-coverage claim vanished; the check must say so"


# --- the check itself -----------------------------------------------------


def test_the_real_dossier_agrees_with_the_real_provisioners():
    doc = docs_audit.safe_read(docs_audit.OBSERVABILITY_DOC)
    names = docs_audit.provisioned_alert_names()
    o2_names = docs_audit.o2_provision_alert_names()
    assert doc is not None, docs_audit.OBSERVABILITY_DOC
    assert len(o2_names) == 47, f"expected 47 rules in o2-provision.py, found {len(o2_names)}"
    assert len(names) == 50, f"expected 47 + 3 provisioned rules, found {len(names)}"
    assert docs_audit.alert_catalogue_problems(doc, names) == []
    # The sentence names o2-provision.py, so it is counted against that file.
    assert docs_audit.alert_rule_count_problems(doc, o2_names) == []


def test_the_count_sentence_is_scoped_to_the_provisioner_it_names():
    # "o2-provision.py provisions 47 rules" is about that file; adding rules to
    # the position-state JSON must not make it look wrong.
    problems = docs_audit.alert_rule_count_problems(
        _count_doc(47, 20, 18, 9), docs_audit.provisioned_alert_names()
    )
    assert any("claims 47 rules" in p for p in problems), "the scope guard is the point"


def test_the_check_records_a_failure(tmp_path, monkeypatch):
    saved = list(docs_audit.failures)
    try:
        bad = tmp_path / "10-observability.md"
        bad.write_text(_catalogue("yes — SEC-credential-expiry"))
        monkeypatch.setattr(docs_audit, "OBSERVABILITY_DOC", str(bad))
        monkeypatch.setattr(docs_audit, "provisioned_alert_names", lambda: ["ING-warn-capacity-80"])
        docs_audit.failures[:] = []
        docs_audit.c19_alert_catalogue()
        assert docs_audit.failures, "the check must record the failure, not only print it"
    finally:
        docs_audit.failures[:] = saved


def test_the_check_fails_when_the_dossier_is_unreadable(monkeypatch):
    saved = list(docs_audit.failures)
    try:
        monkeypatch.setattr(docs_audit, "OBSERVABILITY_DOC", "/nonexistent/10-observability.md")
        docs_audit.failures[:] = []
        docs_audit.c19_alert_catalogue()
        assert docs_audit.failures, "an unreadable dossier must not pass silently"
    finally:
        docs_audit.failures[:] = saved
