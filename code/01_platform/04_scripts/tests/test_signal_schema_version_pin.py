#!/usr/bin/env python3
"""M5-1: the Signal_Candidates row schema_version is "2" everywhere, or nothing agrees.

WHY THIS EXISTS
---------------
The writer sets `SignalCandidatesTableColumns.SCHEMA_VERSION_V2 = "2"`, but DDL 05's header
said 3 (the table-kind history number leaking into the row contract), DDL 23's said 1, and
`schema_manifest.json` copied both wrong numbers. Three sources, three answers: a reader
pinning the manifest would reject real rows, and a DDL edit could silently rename the row
contract.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* The Java constant is "2" and both consumers write it (no bare literal).
* DDL 05 and DDL 23 headers say 2; both domain lines say `writer-set '2'`.
* Both `schema_manifest.json` entries say "2" (regenerated with `ddl_apply.py --force`).
* `18_safety_halt_requests.sql` keeps its own contract at 3 — the pin must not touch it.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
DDL = ROOT / "code/01_platform/02_sql/ddl"
COMPUTE = ROOT / "code/02_services/02_compute/src/main/java/com/trading/compute/signaljob"
COLUMNS = COMPUTE / "SignalCandidatesTableColumns.java"
N7 = COMPUTE / "N7RangeBreakoutStrategy.java"
FILTER = COMPUTE / "CanonicalSignalFilterFunction.java"

LOG_DDL = "05_signal_candidates.sql"
KV_DDL = "23_signal_candidates_current.sql"


def _java_constant() -> str:
    match = re.search(r'SCHEMA_VERSION_V2\s*=\s*"(\d+)"', COLUMNS.read_text())
    assert match, "SCHEMA_VERSION_V2 not found in SignalCandidatesTableColumns"
    return match.group(1)


def _header_version(text: str) -> str:
    match = re.search(r"^--\s*Schema version:\s*(\d+)", text, re.MULTILINE)
    assert match, "DDL must declare '-- Schema version: <n>'"
    return match.group(1)


def _writer_version(text: str) -> str:
    match = re.search(r"schema_version writer-set '(\d+)'", text)
    assert match, "DDL must declare schema_version writer-set '<n>'"
    return match.group(1)


def test_java_constant_is_two_and_consumers_use_it():
    assert _java_constant() == "2"
    for path in (N7, FILTER):
        assert "SCHEMA_VERSION_V2" in path.read_text(), \
            f"{path.name} must write the constant, never a literal version"


def test_both_ddl_headers_and_domain_lines_say_two():
    log = (DDL / LOG_DDL).read_text()
    kv = (DDL / KV_DDL).read_text()
    assert _header_version(log) == "2", "DDL 05 header must carry the row contract version"
    assert _header_version(kv) == "2", "DDL 23 header must carry the row contract version"
    assert _writer_version(log) == "2", "DDL 05 domain line must say writer-set '2'"
    assert _writer_version(kv) == "2", "DDL 23 domain line must say writer-set '2'"


def test_manifest_entries_say_two():
    manifest = json.loads((DDL / "schema_manifest.json").read_text())
    entries = {table["table_name"]: table for table in manifest["tables"]}
    assert entries["Signal_Candidates"]["schema_version"] == "2", \
        "the manifest must mirror the DDL header the writer follows"
    assert entries["Signal_Candidates_current"]["schema_version"] == "2"


def test_safety_halt_contract_stays_at_three():
    text = (DDL / "18_safety_halt_requests.sql").read_text()
    assert _header_version(text) == "3", \
        "18 has its own contract at 3 — the signal pin must not drag it"
