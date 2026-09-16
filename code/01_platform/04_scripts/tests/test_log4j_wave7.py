"""Wave 7 log4j guards — P1-278/279 — static, no JVM."""

import re

LOG4J = __import__("pathlib").Path(__file__).parents[4] / "code/02_services/01_ingestion/src/main/resources/log4j2.xml"

# P1-278 follow-up (c387109a) made immediateFlush/bufferedIO overridable as
# ${env:NAME:-DEFAULT} so the bench overlay can switch the buffering off
# (compose.bench.yml sets LOG_IMMEDIATE_FLUSH=true / LOG_BUFFERED_IO=false).
# The guard is about what those two values DEFAULT to, so each assertion reads
# the fallback out of the seam instead of matching the old bare literal.
_ENV_DEFAULT = re.compile(r"\$\{env:[^:}]+:-([^}]*)\}")


def _attr(text: str, name: str) -> str:
    """The value of `name="..."` as written in the config."""
    match = re.search(rf'\b{name}="([^"]*)"', text)
    assert match, f"log4j2.xml has no {name}= attribute on the JSON_FILE appender"
    return match.group(1)


def _default(value: str) -> str:
    """The fallback of an ${env:NAME:-DEFAULT} seam; the literal when there is none.

    A bare ${env:NAME} (no `:-`) is returned unchanged on purpose: it resolves
    to unset, which drops through to log4j2's own default rather than the value
    this guard wants, so the caller's assertion fails instead of passing a
    setting nobody chose.
    """
    match = _ENV_DEFAULT.search(value)
    return match.group(1) if match else value


def test_P1_278_buffered_file_log_no_async_wrapper():
    """P1-278: file log buffered; NO Async wrapper (proven 2026-09-08 that
    AsyncAppender.start() throws when the file appender is absent, turning
    degraded logging into fatal boot death). Console stays sync."""
    text = LOG4J.read_text()
    flush = _attr(text, "immediateFlush")
    buffered = _attr(text, "bufferedIO")
    assert _default(flush) == "false", (
        f"P1-278: immediateFlush must default to false (got {flush!r})"
    )
    assert _default(buffered) == "true", (
        f"P1-278: bufferedIO must default to true (got {buffered!r})"
    )
    assert "<Async " not in text, "P1-278: Async wrapper must NOT be present (fatal when file appender absent)"
    assert '<AppenderRef ref="CONSOLE"/>' in text, "P1-278: console ref must survive"


def test_P1_279_bounded_retention_with_startup_trigger():
    """P1-279: 7d + 5GB cap + startup trigger; no 30d/57GB retention."""
    text = LOG4J.read_text()
    assert 'age="7d"' in text, "P1-279: 7d cap missing"
    assert 'exceeds="5GB"' in text, "P1-279: 5GB cap missing"
    assert "OnStartupTriggeringPolicy" in text, "P1-279: idle never cleans without startup trigger"
    assert 'age="30d"' not in text, "P1-279: 30d retention must go"
