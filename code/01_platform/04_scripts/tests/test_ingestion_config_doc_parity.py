#!/usr/bin/env python3
"""L4-1: the ingestion dossier's Configuration contract matches the code.

WHY THIS EXISTS
---------------
The dossier's config table had drifted from the code: `DRAIN_DEADLINE_SECONDS`
was documented as "default 30 s" long after B130 ruled the 2 s total close
budget, `DEPLOY_ENV` was documented as the key while the code reads the
canonical `DEPLOYMENT_ENV` first, and the ING-FAIL-010/ING-TCP-003 citations
named values and tests that no longer existed. Docs-audit C16 only checks that
each documented key is *mentioned* somewhere in ingestion code — a value can
drift silently under it.

WHAT IS CHECKED
---------------
A fail-closed registry maps every key documented in the dossier's
"Configuration contract" table to a code regex (which must match an ingestion
source, tests excluded) and, where the row states a value, the doc substrings
that must stay in the row:

* a documented key with no registry entry fails (new key = add the mapping);
* a registry key no longer documented fails (stale entry);
* every code regex must match an ingestion source;
* every doc pin must appear in the key's row;
* the L4-1 specifics are pinned directly: the 2 s/B130 drain row and call, the
  canonical `DEPLOYMENT_ENV` + alias + fail-closed blank, the ING-FAIL-010
  default, the surviving Go `tickcounts*_test.go` citations, and the dated
  configuration audit's "superseded by B130" note.

WHY A REGEX AND NOT A LINE NUMBER
---------------------------------
Audit line numbers drift; the registry anchors on the call text (`intRange(env,
"KEY", default, min, max, errors)`), which is the statement of record.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
INGESTION = ROOT / "code/02_services/01_ingestion"
DOSSIER = ROOT / "docs/08_implementation/03-ingestion.md"
TESTING = ROOT / "docs/08_implementation/11-testing-and-release.md"
AUDIT = ROOT / "docs/08_implementation/24-configuration-audit.md"
GO_BRIDGE = INGESTION / "go-bridge"
CONFIG = INGESTION / "src/main/java/com/trading/ingestion/config/IngestionConfig.java"
PLATFORM_CONFIG = ROOT / "code/common/src/main/java/com/trading/common/config/PlatformConfig.java"


def _sources() -> dict[str, str]:
    """Every file that reads ingestion env keys: main Java + Go, tests excluded."""
    out: dict[str, str] = {}
    for root, _, files in os.walk(INGESTION / "src" / "main" / "java"):
        for f in files:
            if f.endswith(".java"):
                p = Path(root) / f
                out[str(p.relative_to(ROOT))] = p.read_text()
    for root, _, files in os.walk(GO_BRIDGE):
        for f in files:
            if f.endswith(".go") and not f.endswith("_test.go"):
                p = Path(root) / f
                out[str(p.relative_to(ROOT))] = p.read_text()
    return out


SOURCES = _sources()
ALL_SOURCE_TEXT = re.sub(r"\s+", " ", "\n".join(SOURCES.values()))


def documented_rows() -> dict[str, str]:
    txt = DOSSIER.read_text()
    m = re.search(r"### Configuration contract\n(.*?)(?=\n### |\Z)", txt, re.S)
    assert m, "the Configuration contract section is gone from 03-ingestion.md"
    rows = dict(re.findall(r"\| `([A-Z][A-Z0-9_]*)` \| (?:Yes|No) \| ([^\n]*?) \|", m.group(1)))
    assert rows, "no documented keys parsed from the Configuration contract table"
    return rows


# ---------------------------------------------------------------------------
# The registry: every documented key -> (code regex, required doc substrings).
# The regex runs against whitespace-normalized ingestion sources. Keys whose
# row states a numeric/boolean value carry the exact call as the code pin and
# the stated value as doc pins; prose-only keys keep the quoted-key presence
# pin (the same floor C16 enforces, kept here so the registry is complete).
# ---------------------------------------------------------------------------
WEAK = '"%s"'
REGISTRY: dict[str, tuple[str, tuple[str, ...]]] = {
    # ---- secrets / auth (presence only; values live in .env) ----
    "ARROW_APP_ID": (r'"ARROW_APP_ID"', ()),
    "ARROW_APP_SECRET": (r'"ARROW_APP_SECRET"', ()),
    "ARROW_TOKEN": (r'"ARROW_TOKEN"', ()),
    "ARROW_USER_ID": (r'"ARROW_USER_ID"', ()),
    "ARROW_PASSWORD": (r'"ARROW_PASSWORD"', ()),
    "ARROW_TOTP_KEY": (r'"ARROW_TOTP_KEY"', ()),
    # ---- HFT policy (Java-owned pins) ----
    "ARROW_HFT_LATENCY_MS": (
        r'intRange\(env, "ARROW_HFT_LATENCY_MS", 50, 50, 60_000, errors\)',
        ("default 50, range 50-60000",),
    ),
    "ARROW_HFT_CONNECTIONS": (
        r'intRange\(env, "ARROW_HFT_CONNECTIONS", 1, 1, 3, errors\)',
        ("`1`..`3` (default `1`)",),
    ),
    "ARROW_HFT_MAX_TOKENS_PER_CONNECTION": (
        r'exactInt\(env, "ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024, errors\)',
        ("must equal `1024`",),
    ),
    "ARROW_HFT_MAX_TOKENS_PER_REQUEST": (
        r'exactInt\(env, "ARROW_HFT_MAX_TOKENS_PER_REQUEST", 512, errors\)',
        ("must equal `512`",),
    ),
    "ARROW_HFT_HEARTBEAT_SECONDS": (
        r'exactInt\(env, "ARROW_HFT_HEARTBEAT_SECONDS", 3, errors\)',
        ("must equal `3`",),
    ),
    "ARROW_HFT_STALL_TIMEOUT_SECONDS": (
        r'intRange\(env, "ARROW_HFT_STALL_TIMEOUT_SECONDS", 15, 5, 60, errors\)',
        ("default 15, range 5-60",),
    ),
    "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS": (
        r'intRange\(env, "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", 10, 1, 60, errors\)',
        ("default 10, range 1-60",),
    ),
    "ARROW_HFT_RECONNECT_BASE_SECONDS": (
        r'exactInt\(env, "ARROW_HFT_RECONNECT_BASE_SECONDS", 1, errors\)',
        ("must equal `1`",),
    ),
    "ARROW_HFT_RECONNECT_MAX_SECONDS": (
        r'exactInt\(env, "ARROW_HFT_RECONNECT_MAX_SECONDS", 30, errors\)',
        ("must equal `30`",),
    ),
    "ARROW_HFT_AUTH_REFRESH_ATTEMPTS": (
        r'exactInt\(env, "ARROW_HFT_AUTH_REFRESH_ATTEMPTS", 3, errors\)',
        ("must equal `3`",),
    ),
    "ARROW_HFT_MIN_ACTIVE_SLOTS": (
        r'exactInt\(env, "ARROW_HFT_MIN_ACTIVE_SLOTS", 1, errors\)',
        ("must equal `1`",),
    ),
    "ARROW_HFT_MULTI_CONNECTION_APPROVED": (
        r'boolEnv\(env, "ARROW_HFT_MULTI_CONNECTION_APPROVED", false, errors\)',
        ("default false",),
    ),
    "ARROW_FEED": (
        r'"ARROW_FEED"',
        ("`token`", "`hft`", "ARROW_HFT_URL"),
    ),
    # ---- event-age gates (no default; must be set) ----
    "ARROW_MAX_EVENT_AGE_MS": (
        r'requiredLong\(env, "ARROW_MAX_EVENT_AGE_MS", errors\)',
        ("no default",),
    ),
    "ARROW_MAX_FUTURE_EVENT_SKEW_MS": (
        r'requiredLong\(env, "ARROW_MAX_FUTURE_EVENT_SKEW_MS", errors\)',
        ("no default",),
    ),
    # ---- connection / tables / manifest ----
    "FLUSS_BOOTSTRAP": (r'"FLUSS_BOOTSTRAP"', ()),
    "RAW_TABLE_NAME": (r'"RAW_TABLE_NAME"', ()),
    "INSTRUMENT_MANIFEST_PATH": (r'"INSTRUMENT_MANIFEST_PATH"', ()),
    "INSTRUMENT_MANIFEST_MIN_COUNT": (
        r'intRange\(env, "INSTRUMENT_MANIFEST_MIN_COUNT", 1, 1, 1_000_000, errors\)',
        ("default `1`", "1024"),
    ),
    "ARROW_INSTRUMENT_TOKENS": (r'"ARROW_INSTRUMENT_TOKENS"', ()),
    "ARROW_TICK_COUNTS": (r'"ARROW_TICK_COUNTS"', ("default 60",)),
    "GO_ARROW_SDK_VERSION": (r'"GO_ARROW_SDK_VERSION"', ("v0.2.0",)),
    # ---- batching / pending limits (Java-owned pins) ----
    "INGESTION_MAX_BATCH_RECORDS": (
        r'intRange\(env, "INGESTION_MAX_BATCH_RECORDS",\s*PlatformConfig\.INGESTION_MAX_BATCH_RECORDS, 1, 1000, errors\)',
        ("`1..1000` (default `1`)",),
    ),
    "INGESTION_MAX_BATCH_WAIT_MS": (
        r'intRange\(env, "INGESTION_MAX_BATCH_WAIT_MS",\s*PlatformConfig\.INGESTION_MAX_BATCH_WAIT_MS, 0, 100, errors\)',
        ("`0..100` (default `0`)",),
    ),
    "MAX_PENDING_APPEND_RECORDS": (
        r'intRangeWithAlias\(env,\s*"MAX_PENDING_APPEND_RECORDS", "PENDING_MAX_RECORDS",\s*\(int\) MAX_PENDING_RECORDS, 100, 1_000_000, errors\)',
        ("`100..1000000` (default `150000`",),
    ),
    "MAX_PENDING_APPEND_BYTES": (
        r'longRangeWithAlias\(env,\s*"MAX_PENDING_APPEND_BYTES", "PENDING_MAX_BYTES",\s*MAX_PENDING_BYTES, 1_048_576L, Long\.MAX_VALUE, errors\)',
        ("201326592",),
    ),
    "PENDING_APPEND_WARNING_PERCENT": (
        r'doubleRangeWithAlias\(env,\s*"PENDING_APPEND_WARNING_PERCENT", "PENDING_WARNING_PERCENT",\s*WARNING_PERCENT, 0\.10, 0\.99, errors\)',
        ("0.80 (range 0.10-0.99)",),
    ),
    # ---- timing ----
    "APPEND_TIMEOUT_SECONDS": (
        r'intRange\(env, "APPEND_TIMEOUT_SECONDS", 5, 1, 30, errors\)',
        ("default 5 s, range 1-30",),
    ),
    "DRAIN_DEADLINE_SECONDS": (
        r'intRange\(env, "DRAIN_DEADLINE_SECONDS", 2, 1, 300, errors\)',
        ("default 2 s, range 1-300", "B130", "prior 30 s default is historical"),
    ),
    "CLOCK_OFFSET_LIMIT_MS": (
        r'longRange\(env, "CLOCK_OFFSET_LIMIT_MS",\s*CLOCK_OFFSET_LIMIT_MS, 10L, 60_000L, errors\)',
        ("2000 ms (2s) code default",),
    ),
    "FLUSS_STARTUP_WAIT_MS": (
        r'intRange\(\s*env, "FLUSS_STARTUP_WAIT_MS", 3_600_000, 0, 7_200_000, errors\)',
        ("default 3600000, range 0-7200000",),
    ),
    "INGESTION_WRITE_STARTUP_GRACE_MS": (
        r'longRange\(env, "INGESTION_WRITE_STARTUP_GRACE_MS",\s*3_600_000L, 0L, 7_200_000L, errors\)',
        ("default 3600000, range 0-7200000",),
    ),
    # ---- environment / flags ----
    "DEPLOYMENT_ENV": (
        r'env\.getOrDefault\("DEPLOYMENT_ENV", env\.getOrDefault\("DEPLOY_ENV", ""\)\)',
        ("Canonical deployment environment", "legacy alias", "fail-closed"),
    ),
    "INGESTION_ALLOW_DEGRADED": (
        r'boolEnv\(env, "INGESTION_ALLOW_DEGRADED", false, errors\)',
        ("default false",),
    ),
    "ALLOW_RUNTIME_DDL": (
        r'boolEnv\(env, "ALLOW_RUNTIME_DDL", false, errors\)',
        ("default `false`",),
    ),
    "CLOCK_CHECK_REQUIRED": (
        r'boolEnv\(env, "CLOCK_CHECK_REQUIRED", false, errors\)',
        ("default `false`",),
    ),
    # ---- paths / misc (presence only) ----
    "UNCERTAINTY_JOURNAL_PATH": (r'"UNCERTAINTY_JOURNAL_PATH"', ()),
    "ACCOUNT_SCOPE_ID": (r'"ACCOUNT_SCOPE_ID"', ("QP3796",)),
    "OTEL_COLLECTOR_HOST": (r'"OTEL_COLLECTOR_HOST"', ("otel-collector:4318",)),
    "READINESS_FILE_PATH": (r'"READINESS_FILE_PATH"', ()),
    "ARROW_BRIDGE_BIN": (r'"ARROW_BRIDGE_BIN"', ("/app/arrow-bridge",)),
    "NTP_SERVER": (r'"NTP_SERVER"', ("ntp.ubuntu.com",)),
}

# Defaults that live in PlatformConfig (the batch keys' code pin names the
# constant); the doc states the value, so the constant is compared to it.
PLATFORM_DEFAULTS = {
    "INGESTION_MAX_BATCH_RECORDS": 1,
    "INGESTION_MAX_BATCH_WAIT_MS": 0,
}


def test_every_documented_key_is_registered():
    unknown = sorted(set(documented_rows()) - set(REGISTRY))
    assert not unknown, (
        "a key was documented without a parity entry — the drift class L4-1 "
        "found returns silently. Add it to REGISTRY (code regex + doc pins): "
        + ", ".join(unknown))


def test_every_registry_key_is_still_documented():
    rows = documented_rows()
    stale = sorted(set(REGISTRY) - set(rows))
    assert not stale, (
        "REGISTRY has entries whose key is no longer documented (deleted or "
        "renamed row?) — update the registry with the row: " + ", ".join(stale))


def test_every_code_pin_matches_an_ingestion_source():
    missing = []
    for key, (code_re, _) in REGISTRY.items():
        if not re.search(code_re, ALL_SOURCE_TEXT):
            missing.append(key)
    assert not missing, (
        "these documented keys have no matching code statement (the code moved, "
        "renamed, or the value changed — fix the code or the registry): "
        + ", ".join(sorted(missing)))


def test_every_doc_pin_stays_in_its_row():
    rows = documented_rows()
    drift = []
    for key, (_, pins) in REGISTRY.items():
        row = rows.get(key, "")
        for pin in pins:
            if pin not in row:
                drift.append(f"{key}: missing {pin!r}")
    assert not drift, "documented values drifted from their registry pins:\n" + "\n".join(drift)


def test_platform_batch_defaults_match_the_documented_values():
    txt = PLATFORM_CONFIG.read_text()
    for key, value in PLATFORM_DEFAULTS.items():
        m = re.search(rf"int {key} = (\d+);", txt)
        assert m, f"{key} constant not found in PlatformConfig"
        assert int(m.group(1)) == value, (
            f"PlatformConfig.{key} is {m.group(1)} but the dossier documents {value}")


def test_drain_deadline_is_the_b130_two_second_budget():
    call = re.search(
        r'intRange\(env, "DRAIN_DEADLINE_SECONDS", 2, 1, 300, errors\)',
        re.sub(r"\s+", " ", CONFIG.read_text()))
    assert call, "the drain deadline call no longer pins the 2 s default / 1-300 range"
    row = documented_rows()["DRAIN_DEADLINE_SECONDS"]
    assert "default 2 s" in row and "B130" in row, row
    assert "default 30 s" not in row, "the retired 30 s default must stay marked historical"


def test_deployment_env_is_canonical_with_alias_and_fail_closed_blank():
    src = CONFIG.read_text()
    assert 'env.getOrDefault("DEPLOYMENT_ENV", env.getOrDefault("DEPLOY_ENV", ""))' in src
    assert 'errors.add("DEPLOYMENT_ENV/DEPLOY_ENV is required but not set"' in src, \
        "a blank environment must stay fail-closed"
    row = documented_rows()["DEPLOYMENT_ENV"]
    assert "Canonical deployment environment" in row and "legacy alias" in row
    assert "fail-closed" in row


def test_ing_fail_010_row_carries_the_b130_default():
    row = TESTING.read_text()
    m = re.search(r"\| `ING-FAIL-010` \|.*", row)
    assert m, "the ING-FAIL-010 row is gone"
    assert "default 2, range 1-300" in m.group(0), m.group(0)
    assert "B130" in m.group(0), "the ruling behind the 2 s default must be named"


def test_ing_tcp_003_cites_the_surviving_go_tests():
    row = re.search(r"\| `ING-TCP-003` \|.*", TESTING.read_text())
    assert row, "the ING-TCP-003 row is gone"
    text = row.group(0)
    go_tests = {
        "TestReportTickCountsNonEmpty": GO_BRIDGE / "tickcounts_test.go",
        "TestReportTickCountsEmptyMap": GO_BRIDGE / "tickcounts_test.go",
        "TestReportTickCountsConcurrent": GO_BRIDGE / "tickcounts_concurrent_test.go",
        "TestReportTickCountsConcurrentReports": GO_BRIDGE / "tickcounts_concurrent_test.go",
    }
    for name, path in go_tests.items():
        assert name in text, f"the ING-TCP-003 row must cite the surviving {name}"
        assert f"func {name}(" in path.read_text(), f"{name} no longer exists in {path.name}"
    for deleted in ("TestIngTcp003TickCountReportChunkedFileAndStderrMirror",
                    "TestIngTcp003ReportFilePersistsWhenStderrDies"):
        assert deleted not in text, (
            f"{deleted} was deleted with the NDJSON-era path; the row must not cite it")


def test_configuration_audit_notes_the_b130_supersession():
    audit = AUDIT.read_text()
    m = re.search(r"\| M13 \|.*", audit)
    assert m, "the M13 row is gone from the configuration audit"
    assert "superseded by B130" in m.group(0), (
        "the dated audit row still states the retired 30 s default without the note")
    assert "2 s" in m.group(0), m.group(0)
