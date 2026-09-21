"""The rendered compose config keeps its shape and loses its values.

Why this exists: `docker compose config` interpolates, so logging its output
writes every resolved value to disk — measured 2026-09-21, when 15 soak logs held
the live Arrow password and TOTP key. The gate's compose-config step still has to
run the render (that is how a `${VAR:?}` gap is caught), so the fix is a filter
between the render and the log rather than dropping either.

Plain functions, not unittest.TestCase: the documented runner passes
`-p no:unittest`, so a TestCase class here would be skipped and certify nothing.
"""

import io
import os
import pathlib
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import compose_config_redact as redact  # noqa: E402

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]


def test_a_secret_from_the_environment_is_replaced(monkeypatch):
    secret = "env-secret-abcdef123456"
    monkeypatch.setenv("ARROW_APP_SECRET", secret)
    out = redact.redact(f"ARROW_APP_SECRET: {secret}\n", redact.candidate_values([]))
    assert secret not in out
    assert "ARROW_APP_SECRET: REDACTED" in out


def test_a_secret_from_an_env_file_is_replaced(tmp_path, monkeypatch):
    secret = "file-secret-abcdef123456"
    env_file = tmp_path / ".env"
    env_file.write_text(f"# comment\nARROW_TOTP_KEY={secret}\n")
    monkeypatch.delenv("ARROW_TOTP_KEY", raising=False)
    out = redact.redact(f"ARROW_TOTP_KEY: {secret}\n", redact.candidate_values([str(env_file)]))
    assert secret not in out
    assert "ARROW_TOTP_KEY: REDACTED" in out


def test_the_config_shape_survives(monkeypatch):
    secret = "a-real-password-1234"
    monkeypatch.setenv("ARROW_PASSWORD", secret)
    text = (
        "services:\n"
        "  ingestion:\n"
        "    environment:\n"
        f"      ARROW_PASSWORD: {secret}\n"
        "      LOG_LEVEL: INFO\n"
    )
    out = redact.redact(text, redact.candidate_values([]))
    for kept in ("services:", "  ingestion:", "    environment:", "ARROW_PASSWORD:", "LOG_LEVEL: INFO"):
        assert kept in out, kept


def test_short_and_boolean_values_are_not_redacted(monkeypatch):
    monkeypatch.setenv("TOO_SHORT", "abc123")
    monkeypatch.setenv("FLAG", "true")
    monkeypatch.setenv("PORT", "5432")
    values = redact.candidate_values([])
    assert not any(v in values for v in ("abc123", "true", "5432"))
    text = "FLAG: true\nPORT: 5432\n"
    assert redact.redact(text, values) == text


def test_a_standard_shell_variable_is_not_a_candidate(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/local/bin:/usr/bin:/bin")
    monkeypatch.setenv("HOSTNAME", "workstation-with-a-long-name")
    values = redact.candidate_values([])
    assert "/usr/local/bin:/usr/bin:/bin" not in values
    assert "workstation-with-a-long-name" not in values


def test_overlapping_values_leave_no_fragment(monkeypatch):
    monkeypatch.setenv("SHORTER", "abcdefgh1234")
    monkeypatch.setenv("LONGER", "abcdefgh12345678")
    out = redact.redact("x=abcdefgh12345678\n", redact.candidate_values([]))
    assert "abcdefgh" not in out, "the longest value must be replaced first"
    assert out.strip() == "x=REDACTED"


def test_an_unreadable_env_file_is_not_fatal():
    assert isinstance(redact.candidate_values(["/nonexistent/.env"]), list)


def test_main_redacts_stdin(monkeypatch, capsys):
    secret = "stdin-secret-abcdef123456"
    monkeypatch.setenv("ARROW_TOTP_KEY", secret)
    monkeypatch.setattr("sys.stdin", io.StringIO(f"ARROW_TOTP_KEY: {secret}\n"))
    assert redact.main([]) == 0
    out = capsys.readouterr().out
    assert secret not in out
    assert "REDACTED" in out


def test_the_control_without_the_redactor_the_secret_is_visible(monkeypatch):
    # The must-fail control: the same text the redactor cleans is detectable when
    # nothing cleans it, so a broken filter cannot pass as a passing test.
    secret = "control-secret-abcdef123456"
    monkeypatch.setenv("ARROW_APP_SECRET", secret)
    raw = f"ARROW_APP_SECRET: {secret}\n"
    assert secret in raw
    assert secret not in redact.redact(raw, redact.candidate_values([]))


def test_the_gate_renders_through_the_redactor():
    # Wiring, pinned: the render is piped through the filter, and the step judges
    # the *render's* status out of PIPESTATUS — a pipeline's status is the last
    # command's, so reading the wrong slot would silently stop catching a config
    # that does not render. Both slots are captured in one command because under
    # `set -u` a second assignment resets PIPESTATUS and slot 1 vanishes.
    script = (SCRIPTS / "run-monday-gates.sh").read_text()
    assert "compose_config_redact.py" in script
    assert "config 2>&1" in script
    assert 'COMPOSE_RCS=("${PIPESTATUS[@]}")' in script
    assert "${COMPOSE_RCS[0]:-0}" in script
    assert "${COMPOSE_RCS[1]:-0}" in script
