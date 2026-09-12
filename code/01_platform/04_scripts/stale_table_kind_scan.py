#!/usr/bin/env python3
"""Stale table-kind scanner for the 2026-08-13 table-kind re-scope.

Default mode — docs/08_implementation dossiers: detects claims that
contradict the re-scope and reports the hits that are NOT already annotated
as historical/superseded:

  * feature_candles_15s described as a LOG       -> it is the KV upsert table
                                                    (PK (instrument_token, window_start));
                                                    the table is now candle_live
                                                    (DDL 32_candle_live.sql) with its
                                                    twin candle_closed
                                                    (DDL 33_candle_closed.sql). The old
                                                    name stays a rule on purpose: dossiers
                                                    written before the rename still use it
  * Signal_Candidates described as a KV table    -> it is the append-only LOG;
                                                    Signal_Candidates_current is the KV
  * feature_candles_15s_current mentioned at all -> the table is deleted (DDL 22 removed)

Phase-status claims (same annotation tiers, narrower marker set — e.g. a bare
"2026-08-13" or "RE-SCOPED" annotates table-kind history but NOT a stale
status claim):

  * forming-bar-postponed         -> forming bar described as postponed/pending/
                                     upcoming (the handoff + KV persistence landed
                                     2026-08-16; only the full Business Logic
                                     internals remain pending)
  * ranking-reservation-postponed -> ranking/reservation described as postponed
                                     (Slice 3 was REMOVED from scope by CHG-005,
                                     2026-08-15 — not deferred)
  * trade-decisions-active        -> Trade_Decisions output described as
                                     produced/emitted/streamed (it is gated off:
                                     TRADE_DECISIONS_ENABLED=false, no producer
                                     in scope)

Numeric-drift claims (same annotation tiers):

  * tables-count-stale     -> a table count of 20-23 (the manifest and
                              DdlBootstrap.ALL_TABLES now carry 24 tables)
  * acceptance-count-stale -> "151 acceptance IDs" (the acceptance matrix
                              carries 152 unique rows)

Counts equal to the current truth (24 tables, 152 acceptance) never match;
only stale values are reported. A date or historical marker within
NUMERIC_WINDOW chars of the count token annotates the claim as "the count at
that time" (e.g. "(2026-08-10: ... 21 tables ...)") — a distant date on the
same line does NOT hide a stale code description.

Test-count drift claims (same tiers, truth from the docs-audit C6 line,
01-foundation.md L3 — TEST_COUNT_TRUTH below, currently 489/285/419):

  * test-count-stale -> "common N" / "ingestion N" / "compute N" (or
                        "N common/ingestion/compute tests") claims where N
                        differs from the C6 truth. The correct values
                        (TEST_COUNT_TRUTH) are filtered in code.
  * c6-triple-stale   -> "docs-audit C6 line N/N/N" citations where any of
                        the three counts (common/ingestion/compute) differs
                        from the C6 truth.

Annotation classification (per hit):
  UNANNOTATED        no historical marker anywhere near the claim — the line
                     reads as current design (these are the findings to review)
  SECTION-ANNOTATED  the enclosing # heading, or a supersession banner
                     blockquote in the same H2 section, declares the content
                     historical/re-scoped
  LINE-ANNOTATED     the claim span itself (or the whole line, for deleted-table
                     mentions) carries a marker
  DOC-ANNOTATED      the whole document is banner-declared a historical record

Heuristic rules that keep this precise:
  * markers are only trusted within a short claim span (table name .. kind
    token), so an unrelated marker elsewhere on the line cannot hide a claim;
  * a "kind-change" marker (was LOG, R-084, converted, re-scoped, back to LOG)
    within KIND_WINDOW characters of the table name annotates the claim;
  * for mentions of the deleted feature_candles_15s_current, any marker on the
    same line is trusted (nearly every such mention is contextualized);
  * supersession banner blockquotes are scoped to their H2 section and must
    declare content superseded (not merely scoping which rows are historical).

`--upstream` mode — adds the authoritative upstream layers to the scan
(docs/01_project, docs/02_requirements, docs/03_architecture,
docs/04_contracts, docs/05_deployment) alongside the dossiers; `--dir` accepts
several directories. change-records/ subdirectories are never scanned — they
are immutable point-in-time records whose counts ("rollback to 21 tables")
are historical by construction. Claims are the same table-kind, phase-status,
and numeric-drift patterns everywhere.

Exit status: 0 when no stale hits (markdown) / no drift (--ddl); 1 when
findings exist (usable as a gate, e.g. `make stale-tables`).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DOSSIER_DIR = ROOT / "docs" / "08_implementation"

# Authoritative upstream layers (added by --upstream): the docs that sit above
# the implementation dossiers in the authority hierarchy.
UPSTREAM_DIRS = (
    ROOT / "docs" / "01_project",
    ROOT / "docs" / "02_requirements",
    ROOT / "docs" / "03_architecture",
    ROOT / "docs" / "04_contracts",
    ROOT / "docs" / "05_deployment",
)

# ---------------------------------------------------------------------------
# Claim patterns (per line). The kind token must appear within CLAIM_SPAN_MAX
# characters after the table name, with no mention of the sibling current-state
# table in between (so "Signal_Candidates LOG + Signal_Candidates_current KV"
# is not misread as "Signal_Candidates is KV") and no `;` clause break in
# between (so "... on `Signal_Candidates`); (c) KV upserts ..." — where the KV
# belongs to the next clause's table — is not misread either).
# ---------------------------------------------------------------------------
CLAIM_SPAN_MAX = 60

CLAIM_TYPES = (
    (
        "feature_candles_15s-as-LOG",
        re.compile(
            r"feature_candles_15s(?!_current)"
            r"(?:(?!feature_candles_15s_current)[^;]){0,%d}\bLOG\b" % CLAIM_SPAN_MAX
        ),
    ),
    (
        "Signal_Candidates-as-KV",
        re.compile(
            r"Signal_Candidates(?!_current)"
            r"(?:(?!Signal_Candidates_current)[^;]){0,%d}\bKV\b" % CLAIM_SPAN_MAX
        ),
    ),
    (
        "feature_candles_15s_current-mentioned",
        re.compile(r"feature_candles_15s_current"),
    ),
)

# Phase-status claim patterns. These target claims whose CURRENT phase status
# is stale (forming bar postponed vs 2026-08-16 handoff; ranking/reservation
# postponed vs CHG-005 removed-from-scope; Trade_Decisions active vs gated off).
STATUS_CLAIM_TYPES = (
    (
        "forming-bar-postponed",
        re.compile(
            r"forming[- ]bar[^.\n]{0,150}\b(postponed|pending|upcoming|"
            r"not implemented|to be implemented|deferred|future work)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "ranking-reservation-postponed",
        re.compile(
            r"(?:\branking\b|\breservation\b)[^.\n]{0,90}\bpostponed\b"
            r"|\bpostponed\b[^.\n]{0,90}(?:\branking\b|\breservation\b)",
            re.IGNORECASE,
        ),
    ),
    (
        "trade-decisions-active",
        re.compile(
            r"trade[_ ]decisions[^.\n]{0,50}\b"
            r"(produced|emitted|streamed|written|sinked|flowing|enabled)\b",
            re.IGNORECASE,
        ),
    ),
)

# Numeric-drift claim patterns: hard-coded counts that drifted from the
# current truth (verified against code + manifest). Only stale values match —
# "24 tables" and "152 acceptance" never do.
NUMERIC_CLAIM_TYPES = (
    (
        "tables-count-stale",
        re.compile(r"\b2[0-3]\s+tables?\b", re.IGNORECASE),
        "24 tables (schema_manifest.json + DdlBootstrap.ALL_TABLES)",
    ),
    (
        "acceptance-count-stale",
        re.compile(r"\b15[01]\s+acceptance\b", re.IGNORECASE),
        "152 acceptance IDs (acceptance matrix)",
    ),
)

# Test-count drift: "common N" / "ingestion N" / "compute N" (or "N
# common/ingestion/compute tests") claims whose count differs from the
# current docs-audit C6 truth (01-foundation.md L3: unit suites green
# 489/285/419 — common/ingestion/compute; 2026-09-01 C2 hardening added
# two DDL partition-preservation guard tests; 2026-08-26 stale-code audit
# compute 388→386 (ActiveSignalFilterFunctionTest deleted −4, feedback
# test +2); 2026-08-25 CHG-102 re-verified
# 466/247/388 (surefire 469 minus 3 gated FlussBundleReader* reports — C6
# counts only classes present in src/test; the C6 gate reads live surefire);
# prior 2026-08-25 CHG-100 +2 net (DdlSmokeTwinSweepUnitTest
# +2 run everywhere; live half class-level env-gated, skipped in plain runs);
# prior 2026-08-24 bump common 437→464 compute 383→387 for execution-core stubs
# + docs_audit alignment; prior 341/236/319 2026-08-18).
# Historical evolution retained in comment: ingestion +2 2026-08-18 (ING-UNIT-023
# CHG-032 + ING-UNIT-024 CHG-033), common +1 2026-08-18 SlotAssignmentResolver, compute −19
# DEC-038, −10/−2/−11 CHG-023, +2 forming-bar CHG-030; 2026-08-24 bump to 464/247/387,
# 2026-08-25 CHG-100 to 466/247/387.
# counts fire.
TEST_COUNT_TRUTH = {"common": 489, "ingestion": 285, "compute": 419}
TEST_COUNT_CLAIM_TYPES = (
    (
        "test-count-stale",
        re.compile(
            r"\b(common|ingestion|compute)\s+\*{0,2}(\d{2,4})\b(?!\s+tables?)"
            r"|\b(\d{2,4})\s+(common|ingestion|compute)\b",
            re.IGNORECASE,
        ),
    ),
)

# docs-audit C6 triple citations: "docs-audit C6 line N/N/N" where the three
# counts (common/ingestion/compute) differ from the current C6 truth.
C6_TRIPLE_TRUTH = (489, 285, 419)

# Current suite triples per module ("N run / 0 failures / M skips") — bare
# N/0/M-skips claims carry no module word, so they need their own truth.
SUITE_TRIPLE_TRUTH = {"common": (489, 0, 2), "ingestion": (285, 0, 8), "compute": (419, 0, 22)}

# A "now/current N" count claim reads as CURRENT state regardless of any
# nearby date marker (2026-08-18 masking class, CHG-033 follow-up): the
# date-window annotation let stale live counts like "now 234 /0/8-skips,
# common 340/0/1-skip" ride on an adjacent 2026-08-15 marker. A live marker
# within LIVE_WINDOW_CHARS before a claim forces the failing LIVE-STALE tier.
LIVE_WINDOW_CHARS = 60
LIVE_MARKER = re.compile(r"\b(?:now|currently|current)\b(?!\s+that)", re.IGNORECASE)

# "current" used as a STATUS word ("manifest is current", "the manifest is
# current") is not a live-count modifier — the count claim it sits near is
# dated by its own "as of"/date clause and must keep the date annotation.
LIVE_MARKER_BAD = re.compile(
    r"\b(?:is|are|was|were|as|manifest|remain|remains|stays)\s+current\b",
    re.IGNORECASE,
)


def live_marker_in(text: str) -> bool:
    """True when a genuine live-count marker ("now/current N") appears in
    ``text`` — "current" as a predicate/status word is not one."""
    for m in LIVE_MARKER.finditer(text):
        if m.group(0).lower() == "current" and LIVE_MARKER_BAD.search(
                text[max(0, m.start() - 24): m.end()]):
            continue
        return True
    return False

# Bare "now N/M/K" suite-triple claims (no module word) — the 234-masking
# class. "C6 line N/N/N" citations are checked by C6_TRIPLE_CLAIM_TYPES.
LIVE_SUITE_TRIPLE_CLAIM_TYPES = (
    (
        "live-count-stale",
        re.compile(
            r"\b(?:now|currently|current)\b(?!\s+that)[^.,;)\n]{0,25}?"
            r"(\d{2,4})\s*/\s*(\d{1,2})\s*/\s*(\d{1,2})\b",
            re.IGNORECASE,
        ),
    ),
)
C6_TRIPLE_CLAIM_TYPES = (
    (
        "c6-triple-stale",
        re.compile(r"docs-audit C6 line\s+(\d{2,4})/(\d{2,4})/(\d{2,4})",
                   re.IGNORECASE),
    ),
)

# Numeric-claim annotations: a date or historical marker near the count reads
# as "the count at that time". Only trusted within NUMERIC_WINDOW chars of the
# count token, so a stale code description elsewhere on the line is not hidden
# by an unrelated date far away.
NUMERIC_WINDOW = 80
NUMERIC_MARKER = re.compile(
    r"historical|superseded|retired|legacy|formerly|no longer|as built|as-built|"
    r"removed from scope|out of scope|not in scope|de-scop|"
    r"deleted|removed|dropped|converted|conversion|re-scop|"
    r"\bCHG-\d{3}\b|\bDEC-\d{3}\b|"
    r"2026-08-\d{2}",
    re.IGNORECASE,
)

# Subdirectories whose contents are immutable point-in-time records: change
# records describe a change and its rollback at application time, so counts
# inside them are historical by construction, never current claims.
EXCLUDE_DIR_PARTS = ("change-records",)

# ---------------------------------------------------------------------------
# Annotation markers.
# ---------------------------------------------------------------------------
CONTEXT_BEFORE = 25          # chars before the table name included in the claim span
KIND_WINDOW = 80             # chars either side of the table name for kind-change markers
ADJACENT_LINES = 1           # neighboring lines checked for deleted-table mentions in prose

# Nouns that make a nearby marker refer to something OTHER than the table kind
# (e.g. "the candle [LOG + KV] facility RETIRED" — RETIRED modifies the facility,
# not a claim that feature_candles_15s is a LOG).
BLOCKER_NOUN = re.compile(r"\b(facility|machinery|plan|design|era|twin|pair|feature|banner)\b", re.IGNORECASE)

MARKER = re.compile(
    r"historical|superseded|retired|\bretire\b|re-scop|legacy|formerly|"
    r"was (log|kv)|no longer|decommissioned|delete|deleted|remove|removed|"
    r"drop|dropped|as built|as-built|kept for the record|record of what was built|"
    r"pre-dec-038|pre-change|pre-conversion|do not read|not the current|"
    r"out of scope|requirement change|user decision|see banner|per banner|"
    r"converted|conversion|era\b|2026-08-1[34]",
    re.IGNORECASE,
)

# Status-claim annotations: markers that acknowledge the CURRENT phase status.
# Deliberately narrower than MARKER — a bare "2026-08-13" or "RE-SCOPED"
# annotates table-kind history but NOT a stale "forming bar postponed" claim.
STATUS_MARKER = re.compile(
    r"superseded|handoff|handed off|placeholder|scaffold|"
    r"removed from scope|out of scope|not in scope|de-scop|\bCHG-005\b|not deferred|"
    r"gated|disabled|not enabled|no producer|inactive|\bfalse\b|\boff\b|2026-08-1[56]",
    re.IGNORECASE,
)

# Markers that signal the table KIND changed (vs. the table was removed) —
# trusted within KIND_WINDOW chars of the table name even when outside the span.
KIND_CHANGE = re.compile(
    r"was (log|kv)|back to (log|kv)|re-scop|converted|conversion|reversed|"
    r"reverted|\br-084\b|re-target|rework|flips? to|moved to",
    re.IGNORECASE,
)

# Whole-document "this file is historical record" declarations (e.g. tracker 13).
DOC_HISTORICAL = re.compile(
    r"accurate historical record|do not read this (tracker|document) as the current|"
    r"scope superseded|further superseded|historical record of the implemented",
    re.IGNORECASE,
)

# Supersession banner blockquotes: declare the content below retired/superseded.
# Deliberately does NOT match scoping notes like "rows marked HISTORICAL record
# the retired candle KV projection" or "SIGNAL-* rows are the re-scoped gates".
BANNER_MARKER = re.compile(
    r"is (now )?(retired|superseded|deleted|removed)|"
    r"are (now )?(retired|superseded|deleted|removed)|"
    r"(partially|fully|further|scope) superseded|superseded by|retired per|"
    r"do not read|accurate historical record|historical record of the",
    re.IGNORECASE,
)

# Section headings of the form "### <LOG|KV> contract" that introduce a table
# in the following lines with a contradictory current kind (e.g. doc 14 P1.1
# "LOG contract" still describing feature_candles_15s).
HEADING_KIND = re.compile(r"^#{1,6}\s+(LOG|KV)\s+contract\b", re.IGNORECASE)
HEADING_LOOKAHEAD = 6

TIER_RANK = {"LIVE-STALE": 0, "UNANNOTATED": 1, "SECTION-ANNOTATED": 2,
             "LINE-ANNOTATED": 3, "DOC-ANNOTATED": 4}

# ---------------------------------------------------------------------------
# DDL + manifest verification (--ddl): the 2026-08-13 re-scope table kinds as
# carried by code/01_platform/02_sql/ddl/*.sql and schema_manifest.json.
# The candle KV pair replaced feature_candles_15s when a329247 retired that DDL
# (multi-TF aggregator Phase 0): the re-scope's KV current-state kind now lives
# on candle_live, with candle_closed as its immutable closed-history twin.
# ---------------------------------------------------------------------------
DEFAULT_DDL_DIR = ROOT / "code" / "01_platform" / "02_sql" / "ddl"

# Expected kind and exact primary key per re-scope table.
DDL_INVARIANTS = {
    "candle_live": {"kind": "KV", "pk": ["instrument_token", "tf", "window_start"]},
    "candle_closed": {"kind": "KV", "pk": ["instrument_token", "tf", "window_start"]},
    "Signal_Candidates": {"kind": "LOG", "pk": []},
    "Signal_Candidates_current": {"kind": "KV", "pk": ["instrument_token"]},
}
RETIRED_TABLE = "feature_candles_15s_current"

RE_CREATE = re.compile(r"CREATE TABLE\s+(\S+)\s*\(", re.IGNORECASE)
RE_PK = re.compile(r"PRIMARY KEY\s*\(([^)]*)\)", re.IGNORECASE)
RE_BUCKET_KEY = re.compile(r"'bucket\.key'\s*=\s*'([^']*)'")
RE_BUCKET_NUM = re.compile(r"'bucket\.num'\s*=\s*'(\d+)'")
RE_TYPE_HEADER = re.compile(r"--\s*Type:\s*(LOG|KV)\b", re.IGNORECASE)


def parse_ddl(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    m = RE_CREATE.search(text)
    pk_m = RE_PK.search(text)
    pk = [c.strip().lower() for c in pk_m.group(1).split(",")] if pk_m else []
    bkey = RE_BUCKET_KEY.search(text)
    hdr = RE_TYPE_HEADER.search(text)
    return {
        "name": m.group(1) if m else None,
        "kind": "KV" if pk else "LOG",
        "pk": pk,
        "bucket_key": bkey.group(1) if bkey else None,
        "header_kind": hdr.group(1).upper() if hdr else None,
    }


def normalize_pk(s) -> list[str]:
    if not s:
        return []
    return [c.strip().lower() for c in s.split(",") if c.strip()]


def run_ddl_check(ddl_dir: Path) -> int:
    files = sorted(ddl_dir.glob("*.sql"))
    manifest_path = ddl_dir / "schema_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = {t["table_name"]: t for t in manifest["tables"]}

    checks: list[tuple[bool, str]] = []
    issues: list[str] = []

    # Invariant A: the retired candle-KV table is gone everywhere.
    retired_file = ddl_dir / "22_feature_candles_15s_current.sql"
    checks.append((not retired_file.exists(),
                   "no 22_feature_candles_15s_current.sql (" + ("PRESENT" if retired_file.exists() else "absent") + ")"))
    checks.append((RETIRED_TABLE not in entries,
                   f"no {RETIRED_TABLE} manifest entry (" + (f"PRESENT" if RETIRED_TABLE in entries else "absent") + ")"))

    parsed: dict[str, tuple[Path, dict]] = {}
    for f in files:
        p = parse_ddl(f)
        if p["name"]:
            parsed[p["name"]] = (f, p)

    # Invariants B-E: the four re-scope tables carry the exact current kind.
    for tname, want in DDL_INVARIANTS.items():
        if tname not in parsed:
            checks.append((False, f"{tname}: DDL file missing"))
            continue
        f, p = parsed[tname]
        ent = entries.get(tname)
        ok = (
            p["kind"] == want["kind"]
            and p["pk"] == want["pk"]
            and ent is not None
            and ent.get("table_kind") == want["kind"]
            and normalize_pk(ent.get("primary_key")) == want["pk"]
            and (p["bucket_key"] == "instrument_token" or want["kind"] != "KV")
        )
        detail = (
            f"{tname}: {f.name} -> {p['kind']} PK {tuple(p['pk']) or 'none'} | "
            f"manifest {ent.get('table_kind') if ent else 'MISSING'} PK {ent.get('primary_key') if ent else '-'} | "
            f"bucket.key={p['bucket_key']}"
        )
        checks.append((ok, detail))
        if p["header_kind"] and p["header_kind"] != p["kind"]:
            issues.append(f"{f.name}: header claims Type {p['header_kind']} but the parsed kind is {p['kind']}")

    # Parity: every DDL file <-> exactly one manifest entry, kinds and PKs agree.
    missing_entries = sorted(n for n in parsed if n not in entries)
    orphan_entries = sorted(n for n in entries if n not in parsed and n != RETIRED_TABLE)
    kind_mismatch = sorted(n for n in parsed
                           if entries.get(n) and entries[n].get("table_kind") != parsed[n][1]["kind"])
    pk_mismatch = sorted(n for n in parsed
                         if entries.get(n) and normalize_pk(entries[n].get("primary_key")) != parsed[n][1]["pk"])
    checks.append((not missing_entries,
                   f"every DDL file has a manifest entry ({len(parsed)}/{len(files)}" +
                   (f" missing: {', '.join(missing_entries)}" if missing_entries else "") + ")"))
    checks.append((not orphan_entries,
                   f"no manifest entries without a DDL file ({len(entries)}" +
                   (f" orphans: {', '.join(orphan_entries)}" if orphan_entries else "") + ")"))
    checks.append((not kind_mismatch,
                   f"file kind == manifest kind ({', '.join(kind_mismatch) if kind_mismatch else 'all'})"))
    checks.append((not pk_mismatch,
                   f"file PK == manifest PK ({', '.join(pk_mismatch) if pk_mismatch else 'all'})"))

    print(f"ddl-table-kind: scanned {len(files)} DDL files + schema_manifest.json under {ddl_dir}")
    for ok, label in checks:
        print(f"  [{'OK' if ok else 'DRIFT'}] {label}")
    for issue in issues:
        print(f"  [DRIFT] {issue}")
    drift = not all(ok for ok, _ in checks) or bool(issues)
    print(f"VERDICT: {'DDL/manifest drift found (exit 1)' if drift else 'no DDL/manifest drift (exit 0)'}")
    return 1 if drift else 0


def marker_in_paren_unblocked(line: str) -> bool:
    """True if a marker appears inside an open parenthetical without a blocker
    noun before it (e.g. "(HISTORICAL — pre-Stage-2; ...)" annotates the row,
    while "LOG (candle [LOG + KV] facility RETIRED)" does not)."""
    for m in MARKER.finditer(line):
        before = line[:m.start()]
        if before.count("(") <= before.count(")"):
            continue  # marker not inside an open parenthetical
        pre = line[max(0, m.start() - 40):m.start()]
        if not BLOCKER_NOUN.search(pre):
            return True
    return False


def classify_status(lines: list[str], idx: int, claim_type: str, span: str,
                    heading_marker: bool, banner_ctx: bool, doc_hist: bool) -> str:
    """Classification for phase-status claims: a STATUS_MARKER on the line (or a
    section banner / whole-doc historical declaration) annotates the claim."""
    if STATUS_MARKER.search(lines[idx]):
        return "LINE-ANNOTATED"
    if heading_marker or banner_ctx:
        return "SECTION-ANNOTATED"
    if doc_hist:
        return "DOC-ANNOTATED"
    return "UNANNOTATED"


def classify_numeric(lines: list[str], idx: int, claim_type: str, m: re.Match,
                     heading_marker: bool, banner_ctx: bool, doc_hist: bool) -> str:
    """Classification for numeric-drift claims: a date/historical marker within
    NUMERIC_WINDOW chars of the count token annotates it as 'at that time'."""
    line = lines[idx]
    # A "now/current N" claim is a live current-state claim: a live marker
    # before the count overrides the date-window annotation (the masking
    # class — a stale live count parked next to an unrelated date was read as
    # "at that time").
    before = line[max(0, m.start() - LIVE_WINDOW_CHARS): m.start()]
    if live_marker_in(before):
        return "LIVE-STALE"
    window = line[max(0, m.start() - NUMERIC_WINDOW): m.end() + NUMERIC_WINDOW]
    if NUMERIC_MARKER.search(window):
        return "LINE-ANNOTATED"
    if heading_marker or banner_ctx:
        return "SECTION-ANNOTATED"
    if doc_hist:
        return "DOC-ANNOTATED"
    return "UNANNOTATED"


def classify(lines: list[str], idx: int, claim_type: str, span: str,
             heading_marker: bool, banner_ctx: bool, doc_hist: bool,
             kind_window: str = "") -> str:
    line = lines[idx]
    if claim_type == "feature_candles_15s_current-mentioned":
        # Mentions of the deleted table: a marker on the same line, or on an
        # adjacent line when the hit is prose/blockquote (not a self-contained
        # table row), contextualizes the mention.
        if MARKER.search(line):
            return "LINE-ANNOTATED"
        if not line.lstrip().startswith("|"):
            for j in range(max(0, idx - ADJACENT_LINES),
                           min(len(lines), idx + ADJACENT_LINES + 1)):
                if j != idx and MARKER.search(lines[j]):
                    return "LINE-ANNOTATED"
    else:
        if MARKER.search(span):
            return "LINE-ANNOTATED"
        if kind_window and KIND_CHANGE.search(kind_window):
            return "LINE-ANNOTATED"
        if marker_in_paren_unblocked(line):
            return "LINE-ANNOTATED"
    if heading_marker or banner_ctx:
        return "SECTION-ANNOTATED"
    if doc_hist:
        return "DOC-ANNOTATED"
    return "UNANNOTATED"


def scan_file(path: Path) -> list[tuple[int, int, str, str, str]]:
    """Return (tier_rank, lineno, claim_type, line_text, note) for every hit."""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    doc_hist = bool(DOC_HISTORICAL.search("\n".join(lines[:80])))
    hits: list[tuple[int, int, str, str, str]] = []
    seen: set[tuple[int, str, str]] = set()

    heading_marker = False
    banner_ctx = False
    for i, line in enumerate(lines):
        if re.match(r"^##\s", line):
            banner_ctx = False
            heading_marker = bool(MARKER.search(line))
        elif re.match(r"^#{1,6}\s", line):
            heading_marker = bool(MARKER.search(line))
        elif line.startswith(">") and BANNER_MARKER.search(line):
            banner_ctx = True
        for claim_type, rx in CLAIM_TYPES:
            for m in rx.finditer(line):
                span = line[max(0, m.start() - CONTEXT_BEFORE): m.end()]
                kind_window = ""
                if claim_type != "feature_candles_15s_current-mentioned":
                    kind_window = line[max(0, m.start() - KIND_WINDOW): m.start() + KIND_WINDOW]
                tier = classify(lines, i, claim_type, span, heading_marker, banner_ctx,
                                doc_hist, kind_window)
                key = (i + 1, claim_type, "")
                if key in seen:
                    continue
                seen.add(key)
                hits.append((TIER_RANK[tier], i + 1, claim_type, line.strip(), ""))

        # Phase-status claims (forming-bar / ranking-reservation postponed,
        # trade-decisions active) — same line loop, status-specific markers.
        for claim_type, rx in STATUS_CLAIM_TYPES:
            for m in rx.finditer(line):
                span = line[max(0, m.start() - CONTEXT_BEFORE): m.end()]
                tier = classify_status(lines, i, claim_type, span, heading_marker,
                                       banner_ctx, doc_hist)
                key = (i + 1, claim_type, "")
                if key in seen:
                    continue
                seen.add(key)
                hits.append((TIER_RANK[tier], i + 1, claim_type, line.strip(), ""))

        # Numeric-drift claims (hard-coded counts that drifted from the current
        # truth: 24 tables / 152 acceptance IDs) — same line loop; a count with
        # a nearby date/historical marker reads as "at that time".
        for claim_type, rx, truth in NUMERIC_CLAIM_TYPES:
            for m in rx.finditer(line):
                tier = classify_numeric(lines, i, claim_type, m, heading_marker,
                                        banner_ctx, doc_hist)
                # the tier is part of the key: a dated measurement and a live
                # count for the same module on one line are distinct claims
                key = (i + 1, claim_type, "", tier)
                if key in seen:
                    continue
                seen.add(key)
                hits.append((TIER_RANK[tier], i + 1, claim_type, line.strip(),
                             f"truth: {truth}"))

        # Test-count drift (common/compute counts vs the docs-audit C6 truth)
        # — same line loop; the truth-filter skips the correct values (TEST_COUNT_TRUTH).
        for claim_type, rx in TEST_COUNT_CLAIM_TYPES:
            for m in rx.finditer(line):
                if m.group(1):
                    mod, num = m.group(1).lower(), m.group(2)
                else:
                    mod, num = m.group(4).lower(), m.group(3)
                if int(num) == TEST_COUNT_TRUTH[mod]:
                    continue
                tier = classify_numeric(lines, i, claim_type, m, heading_marker,
                                        banner_ctx, doc_hist)
                # per-module dedup with tier: a line may carry several module
                # counts ("common 112, compute 184"), and a dated measurement
                # must not hide a live ("now/current N") count for the same
                # module on the same line
                key = (i + 1, claim_type, mod, tier)
                if key in seen:
                    continue
                seen.add(key)
                hits.append((TIER_RANK[tier], i + 1, claim_type, line.strip(),
                             f"truth: {TEST_COUNT_TRUTH[mod]} {mod} tests (docs-audit C6 line)"))

        # docs-audit C6 triple citations ("C6 line N/N/N" — common/ingestion/
        # compute) — flag any component that drifted from the current truth.
        for claim_type, rx in C6_TRIPLE_CLAIM_TYPES:
            for m in rx.finditer(line):
                nums = tuple(int(m.group(i)) for i in (1, 2, 3))
                if nums == C6_TRIPLE_TRUTH:
                    continue
                tier = classify_numeric(lines, i, claim_type, m, heading_marker,
                                        banner_ctx, doc_hist)
                key = (i + 1, claim_type, "", tier)
                if key in seen:
                    continue
                seen.add(key)
                hits.append((TIER_RANK[tier], i + 1, claim_type, line.strip(),
                             f"truth: {C6_TRIPLE_TRUTH[0]}/{C6_TRIPLE_TRUTH[1]}/{C6_TRIPLE_TRUTH[2]} common/ingestion/compute (docs-audit C6)"))

        # Bare "now/current N/M/K" suite-triple claims (no module word) — a
        # live current-state claim; always the failing LIVE-STALE tier (there
        # is no date-marker annotation for a bare "now N" clause).
        for claim_type, rx in LIVE_SUITE_TRIPLE_CLAIM_TYPES:
            for m in rx.finditer(line):
                nums = tuple(int(m.group(i)) for i in (1, 2, 3))
                if nums == C6_TRIPLE_TRUTH:
                    continue  # a "C6 line N/N/N" citation, checked separately
                if nums in SUITE_TRIPLE_TRUTH.values():
                    continue
                key = (i + 1, claim_type, "", "LIVE-STALE")
                if key in seen:
                    continue
                seen.add(key)
                st_com = SUITE_TRIPLE_TRUTH["common"]
                st_ing = SUITE_TRIPLE_TRUTH["ingestion"]
                st_cpt = SUITE_TRIPLE_TRUTH["compute"]
                hits.append((TIER_RANK["LIVE-STALE"], i + 1, claim_type, line.strip(),
                             f"truth: current suite triples common {st_com[0]}/0/{st_com[2]}, "
                             f"ingestion {st_ing[0]}/0/{st_ing[2]}, compute {st_cpt[0]}/0/{st_cpt[2]} "
                             "(docs-audit C6)"))

    # Section-heading kind assertions: "### LOG/KV contract" headings that
    # introduce a now-contradictory table within the following few lines.
    for i, line in enumerate(lines):
        hm = HEADING_KIND.match(line)
        if not hm:
            continue
        kind = hm.group(1).upper()
        lookahead = " ".join(lines[i + 1:i + 1 + HEADING_LOOKAHEAD])
        claims = []
        if kind == "LOG" and re.search(r"feature_candles_15s(?!_current)", lookahead):
            claims.append(("feature_candles_15s-as-LOG",
                           "section heading asserts LOG kind and introduces feature_candles_15s"))
        if kind == "KV" and re.search(r"Signal_Candidates(?!_current)", lookahead):
            claims.append(("Signal_Candidates-as-KV",
                           "section heading asserts KV kind and introduces Signal_Candidates"))
        for claim_type, note in claims:
            tier = classify(lines, i, claim_type, line, heading_marker_at(lines, i),
                            banner_ctx_at(lines, i), doc_hist)
            key = (i + 1, claim_type, note)
            if key in seen:
                continue
            seen.add(key)
            hits.append((TIER_RANK[tier], i + 1, claim_type, line.strip(), note))
    return hits


def heading_marker_at(lines: list[str], idx: int) -> bool:
    for j in range(idx, -1, -1):
        if re.match(r"^#{1,6}\s", lines[j]):
            return bool(MARKER.search(lines[j]))
    return False


def banner_ctx_at(lines: list[str], idx: int) -> bool:
    """True if a supersession banner blockquote appears after the enclosing H2."""
    for j in range(idx, -1, -1):
        if re.match(r"^##\s", lines[j]):
            return False
        if lines[j].startswith(">") and BANNER_MARKER.search(lines[j]):
            return True
    return False


def clip(text: str, width: int = 200) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= width else text[: width - 1] + "\u2026"


def collect_files(dirs: list[str]) -> list[Path] | None:
    files: list[Path] = []
    for d in dirs:
        p = Path(d)
        if not p.is_dir():
            print(f"stale-table-kind: no such directory: {p}", file=sys.stderr)
            return None
        for f in sorted(p.rglob("*.md")):
            if any(part in EXCLUDE_DIR_PARTS for part in f.parts):
                continue
            files.append(f)
    if not files:
        print("stale-table-kind: no *.md files found under the given directories", file=sys.stderr)
        return None
    seen: set[str] = set()
    out: list[Path] = []
    for f in files:
        r = str(f.resolve())
        if r not in seen:
            seen.add(r)
            out.append(f)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dir", nargs="+", default=[], metavar="DIR",
                    help="doc directories to scan (repeatable; default: docs/08_implementation)")
    ap.add_argument("--upstream", action="store_true",
                    help="also scan the authoritative upstream layers: docs/01_project, "
                         "docs/02_requirements, docs/03_architecture, docs/04_contracts, "
                         "docs/05_deployment (change-records excluded) (in addition to the "
                         "dossiers)")
    ap.add_argument("--ddl", nargs="?", const=str(DEFAULT_DDL_DIR), default=None, metavar="DIR",
                    help="verify DDL files + schema_manifest.json table kinds instead of dossiers "
                         "(default dir: code/01_platform/02_sql/ddl)")
    ap.add_argument("--all", action="store_true",
                    help="also print LINE-ANNOTATED and DOC-ANNOTATED hits (default: counts only)")
    args = ap.parse_args()

    if args.ddl:
        return run_ddl_check(Path(args.ddl))

    dirs = list(args.dir)
    if args.upstream:
        dirs.append(str(DEFAULT_DOSSIER_DIR))
        dirs.extend(str(d) for d in UPSTREAM_DIRS)
    if not dirs:
        dirs = [str(DEFAULT_DOSSIER_DIR)]
    files = collect_files(dirs)
    if files is None:
        return 2

    all_hits: list[tuple[int, str, int, str, str, str]] = []  # rank, file, line, type, text, note
    doc_hist_files: list[str] = []
    for f in files:
        lines = f.read_text(encoding="utf-8", errors="replace").splitlines()
        if DOC_HISTORICAL.search("\n".join(lines[:80])):
            doc_hist_files.append(f.name)
        for rank, lineno, claim_type, text, note in scan_file(f):
            try:
                shown = str(f.relative_to(ROOT))
            except ValueError:
                shown = str(f)
            all_hits.append((rank, shown, lineno, claim_type, text, note))

    all_hits.sort(key=lambda h: (h[0], h[1], h[2]))

    counts: dict[int, int] = {}
    for rank, *_ in all_hits:
        counts[rank] = counts.get(rank, 0) + 1

    print(f"stale-table-kind: scanned {len(files)} markdown files under: {', '.join(dirs)}")
    print("  claims: table kinds (feature_candles_15s-as-LOG | Signal_Candidates-as-KV |")
    print("           feature_candles_15s_current-mentioned) | phase status (forming-bar-postponed |")
    print("           ranking-reservation-postponed | trade-decisions-active) | numeric drift")
    print("           (tables-count-stale | acceptance-count-stale | test-count-stale | c6-triple-stale |")
    print("            live-count-stale)")
    if doc_hist_files:
        print("  whole-doc historical banners: " + ", ".join(doc_hist_files))

    labels = {0: "LIVE-STALE", 1: "UNANNOTATED", 2: "SECTION-ANNOTATED",
              3: "LINE-ANNOTATED", 4: "DOC-ANNOTATED"}
    for rank, label in labels.items():
        tier_hits = [h for h in all_hits if h[0] == rank]
        if not tier_hits:
            continue
        if rank >= 3 and not args.all:
            continue
        print()
        if rank == 0:
            print("== LIVE-STALE — 'now/current' count claim reads as current state, review ==")
        elif rank == 1:
            print("== UNANNOTATED — reads as current design, review ==")
        elif rank == 2:
            print("== SECTION-ANNOTATED — covered by a section heading/banner marker, spot-check ==")
        else:
            print("== LINE/DOC-ANNOTATED — explicitly marked, informational ==")
        for _, f, lineno, claim_type, text, note in tier_hits:
            suffix = f"  [{note}]" if note else ""
            print(f"{f}:{lineno}  {claim_type}  | {clip(text)}{suffix}")

    n_fail = counts.get(0, 0) + counts.get(1, 0)
    print()
    print(f"SUMMARY: {counts.get(0, 0)} LIVE-STALE | {counts.get(1, 0)} UNANNOTATED | "
          f"{counts.get(2, 0)} SECTION-ANNOTATED | {counts.get(3, 0)} LINE-ANNOTATED | "
          f"{counts.get(4, 0)} DOC-ANNOTATED")
    if n_fail:
        print("VERDICT: live/un-annotated stale claims found (exit 1)")
    else:
        print("VERDICT: no stale table-kind, phase-status, or numeric-drift claims (exit 0)")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
