"""Wave 7 log4j guards — P1-278/279 — static, no JVM."""

LOG4J = __import__("pathlib").Path(__file__).parents[4] / "code/02_services/01_ingestion/src/main/resources/log4j2.xml"


def test_P1_278_buffered_file_log_no_async_wrapper():
    """P1-278: file log buffered; NO Async wrapper (proven 2026-09-08 that
    AsyncAppender.start() throws when the file appender is absent, turning
    degraded logging into fatal boot death). Console stays sync."""
    text = LOG4J.read_text()
    assert 'immediateFlush="false"' in text, "P1-278: immediateFlush must be false"
    assert 'bufferedIO="true"' in text, "P1-278: bufferedIO missing"
    assert "<Async " not in text, "P1-278: Async wrapper must NOT be present (fatal when file appender absent)"
    assert '<AppenderRef ref="CONSOLE"/>' in text, "P1-278: console ref must survive"


def test_P1_279_bounded_retention_with_startup_trigger():
    """P1-279: 7d + 5GB cap + startup trigger; no 30d/57GB retention."""
    text = LOG4J.read_text()
    assert 'age="7d"' in text, "P1-279: 7d cap missing"
    assert 'exceeds="5GB"' in text, "P1-279: 5GB cap missing"
    assert "OnStartupTriggeringPolicy" in text, "P1-279: idle never cleans without startup trigger"
    assert 'age="30d"' not in text, "P1-279: 30d retention must go"
