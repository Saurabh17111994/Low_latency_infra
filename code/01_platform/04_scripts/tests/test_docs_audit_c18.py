"""C18 — runbook executability (plan task T17, runbook B7).

The operator pastes these blocks, so a block that cannot run is a defect. Each
case pins something a census of the real runbooks (2026-09-21: 10 files, 33
shell-tagged blocks, 54 distinct command words) showed the checker has to get
right: continuations, heredocs, quoted separators, `console` blocks and `$VAR`
first words. A checker that got any of them wrong would either cry wolf on the
real documents or miss the control the plan asks for.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import docs_audit  # noqa: E402


def _doc(tmp_path, body):
    path = tmp_path / "RUNBOOK.md"
    path.write_text(body)
    return str(path)


def _problems(tmp_path, body):
    return docs_audit.runbook_problems(_doc(tmp_path, body))


# --- the two controls the plan names -------------------------------------


def test_a_bogus_repository_path_is_flagged(tmp_path):
    problems = _problems(tmp_path, "```bash\ncat code/01_platform/04_scripts/nope.sh\n```\n")
    assert any("nope.sh" in p for p in problems), problems


def test_a_bogus_command_is_flagged(tmp_path):
    problems = _problems(tmp_path, "```bash\nnot-a-real-tool --go\n```\n")
    assert any("not-a-real-tool" in p for p in problems), problems


# --- the rest of what the check claims ------------------------------------


def test_a_block_that_is_not_shell_syntax_is_flagged(tmp_path):
    problems = _problems(tmp_path, "```bash\nif [ 1 = 1 ; then\n```\n")
    assert any("bash -n" in p for p in problems), problems


def test_a_placeholder_in_a_shell_block_is_flagged(tmp_path):
    problems = _problems(tmp_path, "```bash\nrclone copy <bucket> dest\n```\n")
    assert any("placeholder" in p for p in problems), problems


# --- shapes the census found in the real runbooks -------------------------


def test_a_continuation_line_is_not_read_as_a_command(tmp_path):
    problems = _problems(
        tmp_path, "```bash\ndocker run --rm \\\n  --name x \\\n  alpine true\n```\n"
    )
    assert problems == [], problems


def test_a_heredoc_body_is_not_read_as_shell(tmp_path):
    problems = _problems(tmp_path, "```bash\npython3 - <<'PY'\nimport os\nprint('hi')\nPY\n```\n")
    assert problems == [], problems


def test_a_quoted_separator_is_not_a_command_boundary(tmp_path):
    problems = _problems(tmp_path, "```bash\ngrep -c '^O2_PASSWORD=|^O2_USER=' .env\n```\n")
    assert problems == [], problems


def test_a_variable_first_word_is_accepted(tmp_path):
    problems = _problems(tmp_path, '```bash\n"$DDL_APPLY_IMAGE" --apply\n```\n')
    assert problems == [], problems


def test_a_trailing_comment_is_not_a_command(tmp_path):
    # Guide line 644: `docker logout ghcr.io   # the push is done; the token …`
    # — the `;` lives in the comment, so it must not start a second command.
    problems = _problems(
        tmp_path, "```bash\ndocker logout ghcr.io   # the push is done; the token is not needed\n```\n"
    )
    assert problems == [], problems


def test_a_quoted_assignment_value_is_one_word(tmp_path):
    # Guide line 654: `IMAGE="${IMAGE_NAME:?set the locally built image name}"`
    # — the `:?` message holds spaces, so a plain split() reads "the" as a tool.
    problems = _problems(
        tmp_path,
        '```bash\nIMAGE="${IMAGE_NAME:?set the locally built project image name}"\n'
        'REF="ghcr.io/${OWNER:?set the owner}/${IMAGE}"\n```\n',
    )
    assert problems == [], problems


def test_a_command_substitution_is_not_split(tmp_path):
    # Alert-routing runbook line 50: the `|` sits inside `$( … )`, so the whole
    # assignment is one command and the tool word is the one after it.
    problems = _problems(
        tmp_path,
        "```bash\nO2_PASSWORD=$(grep ^O2_PASSWORD= .env | cut -d= -f2) \\\n  make alert-routing-test\n```\n",
    )
    assert problems == [], problems


def test_an_operator_supplied_inventory_path_is_allowed(tmp_path):
    # The guide tells the operator to write prod_vms.json at S1; it is an input,
    # not a repository file, so its absence is not a defect.
    problems = _problems(
        tmp_path,
        "```bash\npython3 code/01_platform/04_scripts/cluster_check.py \\\n"
        "    --expect code/01_platform/04_scripts/prod_vms.json --out ~/readiness\n```\n",
    )
    assert problems == [], problems


def test_a_redirection_is_not_a_command_boundary(tmp_path):
    # Runbook 01-runbooks.md line 103: `docker logs … 2>&1 | grep -E "…"` — the
    # `&` belongs to the redirection, so this is one pipeline, not a command "1".
    problems = _problems(
        tmp_path,
        '```bash\ndocker logs x 2>&1 | grep -E "a|b"\necho done >&2\n```\n',
    )
    assert problems == [], problems


def test_a_console_block_is_not_shell(tmp_path):
    # console blocks hold the transcript, commands and output together; running
    # `bash -n` over them would fail on the output lines. A runbook with no
    # shell block at all is legitimate — six of the ten real ones have none.
    problems = _problems(tmp_path, "```console\n$ docker ps\nSTATUS  Up\n```\n")
    assert problems == [], problems


def test_a_clean_runbook_reports_nothing(tmp_path):
    problems = _problems(
        tmp_path,
        "```bash\nset -e\nmkdir -p /tmp/x\npython3 -m pytest -q\n```\n```text\nnot shell\n```\n",
    )
    assert problems == [], problems


# --- the check itself, not just its core ----------------------------------


def test_the_check_fails_when_a_doc_has_problems(tmp_path, monkeypatch, capsys):
    saved = list(docs_audit.failures)
    try:
        monkeypatch.setattr(
            docs_audit, "runbook_docs", lambda: [_doc(tmp_path, "```bash\nnot-a-real-tool\n```\n")]
        )
        docs_audit.failures[:] = []
        docs_audit.c18_runbook_executability()
        out = capsys.readouterr().out
        assert "[FAIL]" in out and "C18" in out, out
        assert docs_audit.failures, "the check must record the failure, not only print it"
    finally:
        docs_audit.failures[:] = saved


def test_the_check_fails_when_the_doc_set_is_empty(monkeypatch):
    saved = list(docs_audit.failures)
    try:
        monkeypatch.setattr(docs_audit, "runbook_docs", list)
        docs_audit.failures[:] = []
        docs_audit.c18_runbook_executability()
        assert docs_audit.failures, "an empty document set must not pass silently"
    finally:
        docs_audit.failures[:] = saved


def test_the_check_fails_when_no_shell_block_exists_anywhere(tmp_path, monkeypatch):
    # The vacuity guard is per set, not per file: if every document lost its
    # shell blocks the check would pass while measuring nothing.
    saved = list(docs_audit.failures)
    try:
        monkeypatch.setattr(
            docs_audit,
            "runbook_docs",
            lambda: [_doc(tmp_path, "```console\n$ docker ps\n```\n")],
        )
        docs_audit.failures[:] = []
        docs_audit.c18_runbook_executability()
        assert docs_audit.failures, "a set with no shell block at all must not pass silently"
    finally:
        docs_audit.failures[:] = saved
