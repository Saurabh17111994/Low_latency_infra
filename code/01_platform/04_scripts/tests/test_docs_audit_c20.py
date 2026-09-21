"""C20 — the deck, the rotation log and the bootstrap name the same secrets (T13).

T13 asks whether the secrets the deploy actually demands are the secrets the
rotation log tells the operator to rotate. Three artifacts answer it: the deck's
`secrets:` block, `secrets-bootstrap.sh --print-names`, and the rotation table in
`04-secrets-rotation.md`. Drift in any direction is the finding — a secret
declared but never rotatable, or rotatable but never declared.

Measured 2026-09-21 before the check was written: the deck declares nine secrets
and the bootstrap names the same nine, so this check starts at zero problems
rather than at a backlog. The plan row describes the invariant as "the union of
`secrets:` plus `${VAR:?}`", which is not literally true and never was: the deck's
16 required variables are mostly image tags and paths, and only `O2_PASSWORD` of
them names a secret. The check therefore holds the two sets that can be equal and
counts the rest instead of pretending it can classify them.

Plain functions, not unittest.TestCase: the documented runner passes
`-p no:unittest`, so a TestCase class here would be skipped and certify nothing.
"""

import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import docs_audit  # noqa: E402

# A small stand-in for the canonical nine. Fixture cases pass it explicitly so
# they never depend on the real script's output.
BOOTSTRAP = ["aws_access_key_id", "o2_password", "arrow_password"]


def _deck(declared=(), required=(), with_secrets_block=True):
    env = "".join(
        f"      - {name}=${{{name}:?operator sets this}}\n" for name in required
    ) or "      - LOG_LEVEL=INFO\n"
    body = "".join(f"  {name}:\n    external: true\n" for name in declared)
    block = f"secrets:\n{body}" if with_secrets_block else ""
    return (
        "version: \"3.8\"\n"
        "services:\n"
        "  ingestion:\n"
        "    environment:\n"
        f"{env}"
        f"{block}"
        "volumes:\n"
        "  flink-data:\n"
    )


def _doc(names=(), extra_rows=""):
    rows = "".join(f"| `{name}` | rotate quarterly |\n" for name in names)
    return f"## Rotation\n\n| Secret | Cadence |\n| --- | --- |\n{rows}{extra_rows}"


def test_the_real_deck_the_real_doc_and_the_bootstrap_agree():
    deck = docs_audit.safe_read(docs_audit.STACK_DECK)
    doc = docs_audit.safe_read(docs_audit.ROTATION_DOC)
    assert deck is not None, docs_audit.STACK_DECK
    assert doc is not None, docs_audit.ROTATION_DOC
    names = docs_audit.bootstrap_secret_names()
    assert len(names) == 9, names
    assert len(docs_audit.deck_secret_names(deck)) == 9
    assert docs_audit.rotation_coverage_problems(deck, doc, names) == []


def test_a_declared_secret_the_bootstrap_does_not_name_is_flagged():
    deck = _deck(declared=[*BOOTSTRAP, "ghost_secret"])
    problems = docs_audit.rotation_coverage_problems(deck, _doc(BOOTSTRAP), BOOTSTRAP)
    assert any("ghost_secret" in p and "bootstrap" in p for p in problems), problems


def test_a_bootstrap_name_the_deck_does_not_declare_is_flagged():
    deck = _deck(declared=["o2_password", "arrow_password"])
    problems = docs_audit.rotation_coverage_problems(deck, _doc(BOOTSTRAP), BOOTSTRAP)
    assert any("aws_access_key_id" in p and "deck" in p for p in problems), problems


def test_a_bootstrap_name_absent_from_the_rotation_rows_is_flagged():
    problems = docs_audit.rotation_coverage_problems(
        _deck(declared=BOOTSTRAP), _doc(["o2_password"]), BOOTSTRAP
    )
    assert any("aws_access_key_id" in p and "rotation" in p for p in problems), problems


def test_a_name_inside_a_longer_word_does_not_count_as_named():
    doc = _doc(["o2_password", "arrow_password"]) + "| `aws_access_key_id_backup` | no |\n"
    problems = docs_audit.rotation_coverage_problems(_deck(declared=BOOTSTRAP), doc, BOOTSTRAP)
    assert any("aws_access_key_id" in p and "rotation" in p for p in problems), problems


def test_a_required_variable_that_is_a_secret_must_be_declared():
    deck = _deck(declared=["arrow_password"], required=["O2_PASSWORD"])
    problems = docs_audit.rotation_coverage_problems(
        deck, _doc(["o2_password", "arrow_password"]), ["o2_password", "arrow_password"]
    )
    assert any("O2_PASSWORD" in p for p in problems), problems


def test_a_required_variable_that_is_not_a_secret_is_not_flagged():
    deck = _deck(declared=BOOTSTRAP, required=["FLINK_IMAGE", "CHECKPOINT_DIR"])
    assert docs_audit.rotation_coverage_problems(deck, _doc(BOOTSTRAP), BOOTSTRAP) == []


def test_a_deck_with_no_secrets_block_is_flagged():
    problems = docs_audit.rotation_coverage_problems(
        _deck(with_secrets_block=False), _doc(BOOTSTRAP), BOOTSTRAP
    )
    assert any("no secrets: block" in p for p in problems), problems


def test_an_empty_bootstrap_list_is_flagged():
    problems = docs_audit.rotation_coverage_problems(_deck(declared=BOOTSTRAP), _doc(BOOTSTRAP), [])
    assert any("no secret names" in p for p in problems), problems


def test_the_demanded_variables_outside_the_bootstrap_are_counted_not_failed():
    # Reported, never silently dropped: the deck legitimately demands image tags
    # and paths as well, and classifying those by name shape is the guesswork
    # this check exists to avoid.
    deck = _deck(declared=BOOTSTRAP, required=["ARROW_APP_ID", "ARROW_USER_ID", "FLINK_IMAGE"])
    assert docs_audit.rotation_coverage_problems(deck, _doc(BOOTSTRAP), BOOTSTRAP) == []
    assert docs_audit.demanded_names_not_in_bootstrap(deck, BOOTSTRAP) == {
        "ARROW_APP_ID", "ARROW_USER_ID", "FLINK_IMAGE",
    }


def test_the_check_is_registered_in_main():
    assert "c20_rotation_coverage()" in inspect.getsource(docs_audit.main)


def test_the_check_fails_closed_when_a_file_is_unreadable(monkeypatch):
    monkeypatch.setattr(docs_audit, "STACK_DECK", "/nonexistent/docker-stack.yml")
    docs_audit.failures.clear()
    try:
        docs_audit.c20_rotation_coverage()
        assert docs_audit.failures, "an unreadable deck must fail, not pass quietly"
    finally:
        docs_audit.failures.clear()
