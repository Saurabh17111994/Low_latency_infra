#!/usr/bin/env python3
"""M6-1: schema_manifest.json must equal a fresh parse of the DDL corpus, every entry.

WHY THIS EXISTS
---------------
`parse_with_options` used a non-greedy regex that stopped at the first `)` followed by
`;` — including one inside a `--` comment. `02_raw_table_1.sql`'s 'table.log.ttl' line
ends with "... (was 7d); keep > num-retention + EOD SLA", so every option after it was
dropped and the manifest recorded `raw_table_1.lake_policy = "off"` while the DDL
enables the datalake. Nothing compared the committed manifest against a fresh parse, so
the wrong value survived every audit.

WHAT IS CHECKED (offline; reads files, runs nothing)
----------------------------------------------------
* Every committed manifest entry equals a fresh `compute_manifest_entries()` result
  (name, kind, keys, checksum, and the four header/WITH-derived fields).
* The real `raw_table_1` DDL parses `lake_policy` enabled (the truncation regression).
* A `);` inside a comment cannot truncate the option list.
* `--` inside a quoted value is not treated as a comment.
* A comment's `PRIMARY KEY (...)` is not a primary key.
* A DDL that names 'table.datalake.enabled' without a readable option refuses.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[4]
SCRIPTS = ROOT / "code/01_platform/04_scripts"
sys.path.insert(0, str(SCRIPTS))
import ddl_apply  # noqa: E402

DDL_DIR = ROOT / "code/01_platform/02_sql/ddl"
MANIFEST = DDL_DIR / "schema_manifest.json"


def test_every_manifest_entry_equals_a_fresh_parse():
    committed = json.loads(MANIFEST.read_text())["tables"]
    computed = ddl_apply.compute_manifest_entries()
    by_name = {entry["table_name"]: entry for entry in computed}
    assert len(by_name) == len(computed), "duplicate table names in the corpus"
    assert {entry["table_name"] for entry in committed} == set(by_name), \
        "the manifest's table set differs from the corpus"
    for entry in committed:
        fresh = by_name[entry["table_name"]]
        assert entry == fresh, (
            f"manifest entry for {entry['table_name']} differs from a fresh parse — "
            f"regenerate with ddl_apply.py --force (never apply DDL)")


def test_raw_table_1_lake_policy_is_enabled():
    options = ddl_apply.parse_with_options((DDL_DIR / "02_raw_table_1.sql").read_text())
    assert options.get("table.datalake.enabled") == "true"
    assert ddl_apply.lake_policy_of(options).startswith("enabled=true")


def test_close_paren_semicolon_in_a_comment_cannot_truncate():
    ddl = """
CREATE TABLE t (
    a BIGINT NOT NULL
) WITH (
    'bucket.num' = '16',
    'table.log.ttl' = '9d', -- keep > retention (was 7d); and the rest
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg'
);
"""
    options = ddl_apply.parse_with_options(ddl)
    assert options["table.log.ttl"] == "9d"
    assert options["table.datalake.enabled"] == "true"
    assert options["table.datalake.format"] == "iceberg"


def test_close_paren_semicolon_inside_a_quoted_value_cannot_truncate():
    ddl = """
CREATE TABLE t (a BIGINT NOT NULL) WITH (
    'note' = 'a);b',
    'table.datalake.enabled' = 'true'
);
"""
    options = ddl_apply.parse_with_options(ddl)
    assert options["note"] == "a);b"
    assert options["table.datalake.enabled"] == "true"


def test_double_dash_inside_a_quoted_value_is_not_a_comment():
    ddl = """
CREATE TABLE t (a BIGINT NOT NULL) WITH (
    'note' = 'a--b',
    'table.datalake.enabled' = 'true'
);
"""
    options = ddl_apply.parse_with_options(ddl)
    assert options["note"] == "a--b"
    assert options["table.datalake.enabled"] == "true"


def test_primary_key_inside_a_comment_is_not_a_primary_key():
    ddl = """
-- PRIMARY KEY (fake_column)
CREATE TABLE t (a BIGINT NOT NULL) WITH ('bucket.key' = 'a');
"""
    assert ddl_apply.parse_primary_key(ddl) is None


def test_refusal_when_datalake_is_named_but_the_option_is_unreadable():
    ddl = """
CREATE TABLE t (a BIGINT NOT NULL) WITH (
    'bucket.key' = 'a',
    'table.datalake.enabled' = true
);
"""
    with pytest.raises(ValueError, match="datalake"):
        ddl_apply.parse_with_options(ddl)
