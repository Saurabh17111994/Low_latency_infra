#!/usr/bin/env bash
# =============================================================================
# import_instruments.sh — RETIRED 2026-09-19 (wave 40, CHG-229)
#
# This script generated one positional 12-value statement per manifest row
# against the pre-v3 table shape. It cannot work against the live table:
#
#   * 20_instruments.sql has 14 columns in a different order (the script emitted
#     12), and `manifest_version` + `schema_version` are NOT NULL;
#   * the table is KV — PRIMARY KEY (instrument_token, manifest_version),
#     bucket.key=instrument_token, table.kv.format-version=2 — so a loader must
#     upsert by the composite PK and keep one is_active=TRUE row per token;
#   * the prerequisite this script documented (11_instruments.sql) no longer
#     exists — the file is 20_instruments.sql.
#
# The instruments table has ONE loader, and the loader contract lives in it:
# `InstrumentManifestWriter.ManifestEntry` (compact-ctor + unit tests) enforces
# lot_size > 0; tick_size_paise NULL-or->0; option_type CE|PE iff
# instrument_type=OPT; expiry NULL iff EQUITY; single active row per token
# (deactivate-prior load sequencing); monotonic manifest_version;
# duplicate-composite-key rejection; and a KV-shape preflight before any write.
# 20_instruments.sql states this explicitly: "enforced in
# InstrumentManifestWriter.ManifestEntry compact-ctor + unit tests, never in
# DDL". Bash cannot enforce the sequencing rules, so repairing this script would
# have produced a second loader with weaker guarantees than the documented one.
#
# USE INSTEAD — the ingestion service loads the approved manifest:
#   com.trading.ingestion.InstrumentManifestWriter
#   INSTRUMENT_MANIFEST_PATH=<approved manifest CSV>   (manifest version: loader
#   parameter, default 1)
#   docs/08_implementation/03-ingestion.md — "Writes" row; ING-INT-004 live proof
#   (2026-08-15); unit tests ING-SCHEMA-002.
#
# Why a stub and not a deletion: docs/commands/COMMANDS.md and
# code/01_platform/05_instruments/README.md documented this command, so a reader
# who follows them must learn the real path rather than a 404. The stub fails
# closed — it never writes SQL that could be partially applied.
# =============================================================================

set -euo pipefail

cat >&2 <<'MSG'
ERROR: import_instruments.sh is RETIRED (wave 40, CHG-229) and does nothing.
       It cannot write the live `instruments` table: it emitted a 12-column
       positional statement for a 14-column KV table whose loader contract is
       enforced in code, not in SQL.

       Use the contract-enforcing loader instead:

         com.trading.ingestion.InstrumentManifestWriter
         (ingestion service; INSTRUMENT_MANIFEST_PATH=<approved manifest CSV>)

       Docs: docs/08_implementation/03-ingestion.md ("Writes" row, ING-INT-004)
             docs/05_deployment/change-records/CHG-229.md
             docs/plans/2026-09-19-wave40-instrument-import-and-corpus-pin.md

       Exit 2 = retired tool. Nothing was imported.
MSG
exit 2
