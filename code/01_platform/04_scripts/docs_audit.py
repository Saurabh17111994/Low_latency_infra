#!/usr/bin/env python3
"""
docs_audit.py — doc-vs-code truth gate (docs/08_implementation/01-foundation.md L388
"no dossier silently contradicts an upstream document").

Checks the concrete, machine-verifiable invariants that the 2026-08-13 ground-truth
audit established. Failures are exit-code 1 with a named message; run as
`make docs-audit` from the repo root. Intentionally NOT exhaustive — it pins the
checks that have already caught real drift, so the same class of mistake fails fast.

Checks:
  C1  schema_manifest.json: 23 tables; every entry has non-null ddl_sha256,
      compatibility_class; LOG entries have non-null bucket_key.
  C2  OwnershipMatrix.java encodes exactly the 12 doc rows; test pins it.
  C3  Foundation schema-state diagram is the 5-state enum (no RECONCILED/...).
  C4  CompatibilityClass enum names == version_matrix.yaml header comment.
  C5  No known-stale phrases anywhere in docs/.
  C6  Test counts in 01-foundation.md L42 match current surefire totals.
  C7  versions.pin holds FLINK/FLUSS pins; no 'latest' / placeholder.
  C8  Acceptance matrix (09-acceptance-matrix.md): AC/EB/NI totals match the
      pinned baseline (152/13/139, DEC-039); coverage + summary tables equal
      the actual rows; every defined REQ maps to an AC row (no unmapped, no
      phantom refs); NFR rows resolve to real sections in 03-non-functional.md.
  C9  DEC-039 invariants: no stale 4-mode / seconds-timestamp / checkpoint-
      durability / LOG-control / ledger-skipped claims outside the dated
      decisions index; HFT frame sizes 40/196 B and ltpc+full-only mode switch;
      bridge ns->ms conversion; ledger + halt tables KV in manifest and DDL;
      ledger live-in-dev evidence; 27 DDLs on file (29 minus the four retired by
      a329247, plus the two candle KV DDLs 32/33) incl. the DEC-038 dedup DDL
      (24_fingerprint_dedup.sql) + SCH-19 instruction-index DDL
      (25_trade_instruction_state.sql) + SCH-23 EOD offload-state DDL
      (26_eod_offload_state.sql)
      (25_trade_instruction_state.sql); dedup marked implemented in
      observability + runbooks; forming_bar + ingestion_quarantine in all four
      inventories.
  C10 Dossier traceability (2026-08-14 five-fix traceability work): the
      11-testing-and-release.md acceptance-criteria coverage table lists every
      matrix AC domain with an owning dossier and ranges equal to the matrix;
      dossier status rows (02-10) cite only real, owned AC ids; every matrix
      AC id is covered by a dossier or the coverage table; REQ13-* ids appear
      only in the master signal-job dossier (04) and are all defined in its
      requirement traceability section.
  C11 Verification mapping: every functional dossier (01-10) has a
      "## Verification mapping" section, and every link in those sections to
      11-testing-and-release.md#anchor resolves (GitHub-slugged) to a heading
      under its "## Component test designs" section — no dead test-design links.
  C12 WORM/Object-Lock statements name the R2 mechanism (bucket locks) or the
      S3 Object Lock API limitation, or are marked evidence-gated — no bare
      'WORM/Object Lock' claim without a mechanism or gate (2026-08-14 sweep).
  C13 Runbook index <-> requirements sync: every runbook_id in
      docs/06_operations front matter appears in the §6.9 runbook coverage
      table of 06-operational.md, and no phantom id sits in the table.
  C14 Change-control reconciliation: every change record in
      docs/05_deployment/change-records/ names the six required fields
      (affected artifacts, compatibility class, savepoint impact, test
      updates, rollback behavior, plan tasks) with a valid compatibility
      class (01-foundation.md "Change control", orig L205).
  C15 Non-root evidence ownership contract: every apply.json the ddl-apply
      CONTAINER wrote (owner == the engine user, DDL_APPLY_UID) must be
      group-writable (setgid 2775 evidence root + umask 002 -> 664), and no
      record may be root-owned (the engine never runs as root). Host-owned
      records (host-side make ddl) are out of scope.
  C16 Env-key doc drift (ING-UNIT-019): every env key in the ingestion
      dossier's "Configuration contract" table is read by IngestionConfig,
      the Go bridge, or another ingestion service source.
"""

import glob
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

import change_control_check  # same directory; C14 record validator
import evidence_ownership_check  # same directory; C15 non-root evidence gate

ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
DDL_DIR = os.path.join(ROOT, "code", "01_platform", "02_sql", "ddl")
SCRIPTS_DIR = os.path.join(ROOT, "code", "01_platform", "04_scripts")
DOCS_DIR = os.path.join(ROOT, "docs")
COMMON_DIR = os.path.join(ROOT, "code", "common")
INGEST_DIR = os.path.join(ROOT, "code", "02_services", "01_ingestion")

failures = []


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}{(' — ' + detail) if detail and not ok else ''}")
    if not ok:
        failures.append(name)


def safe_read(path):
    """Return file text, or None when unreadable (missing file is a check failure)."""
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return fh.read()
    except (OSError, ValueError):
        return None


def safe_read_json(path):
    try:
        return json.loads(safe_read(path) or "null")
    except (ValueError, TypeError):
        return None


def _ddl_table_name(filename):
    """`29_position_state.sql` -> `position_state`.

    The manifest keys tables by name, the DDL directory by ordinal prefix; this
    is the one mapping between the two that both C1 and C9 rely on.
    """
    base = filename[:-4]
    head, sep, rest = base.partition("_")
    return (rest if sep and head.isdigit() else base).lower()


def c1_manifest():
    p = os.path.join(DDL_DIR, "schema_manifest.json")
    m = safe_read_json(p)
    if m is None:
        return check("C1 manifest readable", False, p)
    tables = m.get("tables", [])
    # 27 since a329247 retired four signal-cutover DDLs (03, 04, 30, 31) and the
    # two candle KV tables (32, 33) landed. The pin is deliberate and is NOT made
    # redundant by the agreement checks below: only the pin notices a table being
    # dropped from BOTH lists at once.
    check("C1 manifest has 27 tables", len(tables) == 27, f"got {len(tables)}")
    # The literal above went stale (29 held for four retired tables) because
    # nothing tied the manifest to the DDL directory. These two do.
    ddl_names = {_ddl_table_name(f) for f in os.listdir(DDL_DIR) if f.endswith(".sql")}
    manifest_names = {t["table_name"].lower() for t in tables}
    check(
        "C1 every manifest table has a DDL file",
        not (manifest_names - ddl_names),
        f"{sorted(manifest_names - ddl_names)}",
    )
    check(
        "C1 every DDL file has a manifest table",
        not (ddl_names - manifest_names),
        f"{sorted(ddl_names - manifest_names)}",
    )
    bad_sha = [t["table_name"] for t in tables if not t.get("ddl_sha256")]
    bad_compat = [t["table_name"] for t in tables if not t.get("compatibility_class")]
    bad_routing = [
        t["table_name"]
        for t in tables
        if str(t.get("table_kind", "")).upper() == "LOG" and not t.get("bucket_key")
    ]
    check("C1 all entries have ddl_sha256", not bad_sha, f"{bad_sha}")
    check("C1 all entries have compatibility_class", not bad_compat, f"{bad_compat}")
    check("C1 LOG entries have bucket_key", not bad_routing, f"{bad_routing}")


def c2_ownership_matrix():
    p = os.path.join(
        COMMON_DIR,
        "src",
        "main",
        "java",
        "com",
        "trading",
        "common",
        "ownership",
        "OwnershipMatrix.java",
    )
    src = safe_read(p)
    if src is None:
        return check("C2 OwnershipMatrix readable", False, p)
    # "new Rule(" also matches the logRule() helper's dynamic instantiation —
    # the 12 encoded rows are exactly the string-literal-target rules.
    n = len(re.findall(r'new Rule\("', src))
    check("C2 ownership matrix = 12 rows", n == 12, f"found {n} Rule( instantiations")
    test_p = os.path.join(
        COMMON_DIR,
        "src",
        "test",
        "java",
        "com",
        "trading",
        "common",
        "ownership",
        "OwnershipMatrixTest.java",
    )
    check("C2 OwnershipMatrixTest pins matrix", os.path.exists(test_p))


def c3_schema_state_diagram():
    p = os.path.join(DOCS_DIR, "08_implementation", "01-foundation.md")
    txt = safe_read(p)
    if txt is None:
        return check("C3 foundation doc readable", False, p)
    five = all(
        s in txt for s in ["PROPOSED", "APPROVED", "APPLYING", "OBSERVED", "REJECTED"]
    )
    stale = any(
        s in txt for s in ["RECONCILED", "DIALECT_VALIDATED", "INTEGRATION_VALIDATED"]
    )
    check("C3 schema-state diagram = 5-state enum", five and not stale)


def c4_compat_vocabulary():
    p = os.path.join(
        COMMON_DIR,
        "src",
        "main",
        "java",
        "com",
        "trading",
        "common",
        "version",
        "CompatibilityClass.java",
    )
    enum_src = safe_read(p)
    yaml = safe_read(os.path.join(SCRIPTS_DIR, "version_matrix.yaml"))
    if enum_src is None or yaml is None:
        return check("C4 enum/yaml readable", False)
    header = "COMPATIBLE_WITH_LIMITATION" in yaml and "NOT_APPLICABLE" in yaml
    enum_has = "COMPATIBLE_WITH_LIMITATION" in enum_src and "NOT_APPLICABLE" in enum_src
    no_old = "LIMITED," not in enum_src and "\n    NA" not in enum_src
    check("C4 enum vocabulary matches yaml header", header and enum_has and no_old)


STALE_PHRASES = [
    "fingerprint-based LRU idempotency cache",
    "fingerprint idempotency cache",
    "RECONCILED",
    "no Flink jobs built yet",
    "MAX_PENDING_APPEND_RECORDS = 10000",
]


def c5_stale_phrases():
    hits = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if not f.endswith(".md"):
                continue
            path = os.path.join(root, f)
            txt = safe_read(path)
            if txt is None:
                continue
            for phrase in STALE_PHRASES:
                if phrase in txt:
                    hits.append(f"{os.path.relpath(path, ROOT)}: '{phrase}'")
    check("C5 no stale phrases in docs", not hits, "; ".join(hits[:5]))


def _is_gated_class(test_src_dir, report_path):
    """True when the report is stale — its class no longer exists in the
    current test source tree (renamed/deleted, e.g. the candle->signal KV
    conversion). Gated classes DO count: surefire's module headline
    "Tests run: N, Skipped: M" includes skipped tests, and the doc count
    follows that headline convention. Reports for gated classes are written
    by the normal suite (as skips) exactly like any other class."""
    if not test_src_dir:
        return False
    name = os.path.basename(report_path)  # TEST-com.trading.compute.X.xml
    cls = name[len("TEST-") : -len(".xml")].rsplit(".", 1)[-1]
    return all(cls + ".java" not in files for _, _, files in os.walk(test_src_dir))


def surefire_total(module_dir, test_src_dir=None):
    # Only classes the plain `mvn test` would select may be counted. A report
    # whose class surefire never picks up (an *IT held for a targeted run)
    # otherwise sits in the total until someone notices: that is how a
    # 2026-09-05 report for DedupRocksDbThroughputMemoryIT made compute read
    # 539 against a 538-test run. No module here overrides the defaults
    # (Test*, *Test, *Tests, *TestCase); nested classes report under their outer.
    plain_includes = re.compile(r"^(?:Test.*|.*Tests?|.*TestCase)$")
    total = 0
    for p in glob.glob(
        os.path.join(module_dir, "target", "surefire-reports", "TEST-*.xml")
    ):
        if _is_gated_class(test_src_dir, p):
            continue
        simple = os.path.basename(p)[len("TEST-") : -len(".xml")].rsplit(".", 1)[-1]
        if not plain_includes.match(simple.split("$", 1)[0]):
            continue
        try:
            root = ET.parse(p).getroot()
            total += int(root.get("tests", 0))
        except (ET.ParseError, OSError, ValueError):
            continue  # stale/corrupt report dirs are ignored — totals come from mtime-clean runs
    return total


def c6_test_counts():
    p = os.path.join(DOCS_DIR, "08_implementation", "01-foundation.md")
    txt = safe_read(p)
    if txt is None:
        return check("C6 foundation doc readable", False, p)
    m = re.search(r"unit suites green (\d+)/(\d+)/(\d+)", txt)
    if not m:
        return check("C6 doc test-count line found", False)
    doc_c, doc_i, doc_comp = map(int, m.groups())
    c = surefire_total(COMMON_DIR, os.path.join(COMMON_DIR, "src", "test", "java"))
    i = surefire_total(INGEST_DIR, os.path.join(INGEST_DIR, "src", "test", "java"))
    # compute module target/ is often absent or root-owned — and the Monday
    # gate never runs compute tests — so compare its count ONLY when compute
    # surefire reports actually exist. A fresh checkout must not spuriously
    # fail C6 on a module that was not run (common/ingestion are still strict).
    comp_dir = os.path.join(ROOT, "code", "02_services", "02_compute")
    comp_reports = glob.glob(os.path.join(comp_dir, "target", "surefire-reports", "TEST-*.xml"))
    check("C6 common count matches doc", c == doc_c, f"surefire={c} doc={doc_c}")
    check("C6 ingestion count matches doc", i == doc_i, f"surefire={i} doc={doc_i}")
    if comp_reports:
        # One measurement, used by both the check and its message: a second call
        # re-globbed and re-parsed every compute report and re-walked the compute
        # test tree per report (P6-724).
        comp_total = surefire_total(comp_dir, os.path.join(comp_dir, "src", "test", "java"))
        check(
            "C6 compute count matches doc",
            comp_total == doc_comp,
            f"surefire={comp_total} doc={doc_comp}",
        )
    else:
        check("C6 compute count (not built here — skipped)", True)


def c7_version_pins():
    p = os.path.join(SCRIPTS_DIR, "versions.pin")
    txt = safe_read(p)
    if txt is None:
        return check("C7 versions.pin readable", False, p)
    flink = re.search(r"FLINK_VERSION=(\S+)", txt)
    fluss = re.search(r"FLUSS_VERSION=(\S+)", txt)
    ok = bool(flink and fluss)
    bad = any(
        v in (flink.group(1) if flink else "", fluss.group(1) if fluss else "")
        for v in ("latest", "TO_BE_PINNED")
    )
    check(
        "C7 FLINK/FLUSS pinned",
        ok and not bad,
        f"flink={flink.group(1) if flink else '?'} fluss={fluss.group(1) if fluss else '?'}",
    )


# ---------------------------------------------------------------------------
# C8 — acceptance-matrix integrity (docs/02_requirements/09-acceptance-matrix.md)
# ---------------------------------------------------------------------------

MATRIX_PATH = os.path.join(DOCS_DIR, "02_requirements", "09-acceptance-matrix.md")
NFR_PATH = os.path.join(DOCS_DIR, "02_requirements", "03-non-functional.md")
DECISIONS_PATH = os.path.join(DOCS_DIR, "01_project", "04-decisions.md")

# Baseline agreed 2026-08-14 (DEC-039) — bump these when the matrix
# legitimately grows and update the doc's Coverage summary / Summary tables.
# 2026-08-21: EB 13 -> 10 — AC-ING-001/003/005 proven PASSED (ING-E2E-001
# full fake-broker acceptance + live Fluss round-trip; reconciliation of the
# Summary table rows to match; NI unchanged 139).
EXPECTED_AC_TOTAL = 152
EXPECTED_EB_TOTAL = 10
EXPECTED_NI_TOTAL = 139

REQ_FILES = {  # domain (as used in the matrix tables) -> (prefix, requirement file)
    "Ingestion": ("REQ-ING", "02-functional/01-ingestion.md"),
    "Storage": ("REQ-FLS", "02-functional/02-storage.md"),
    "Compute": ("REQ-FC", "02-functional/03-compute.md"),
    "Business Logic": ("REQ-SS", "02-functional/04-business-logic.md"),
    "Ranking": ("REQ-RNK", "02-functional/10-ranking.md"),
    "Action Capture": ("REQ-AC", "02-functional/06-action-capture.md"),
    "Executor": ("REQ-EXE", "02-functional/07-executor.md"),
    "Babysitter": ("REQ-BB", "02-functional/05-babysitter.md"),
    "Observability": ("REQ-OBS", "02-functional/08-observability.md"),
    "Platform": ("REQ-PF", "02-functional/09-platform-runtime.md"),
}
NFR_DOMAIN = "Non-functional"

INVENTORY_FILES = [  # all four table inventories must list both tables (DEC-039)
    "02_requirements/04-data.md",
    "03_architecture/00-arch-overview.md",
    "03_architecture/02-data-pipeline.md",
    "03_architecture/01-technology-choices.md",
]


def _ac_rows(section):
    """Return (rows, malformed) for every AC row in a matrix section.

    Every row must have the 8-column header shape (Acceptance ID | Requirement |
    Coverage type | Uncovered criteria | Fixture / Workload | Threshold |
    Evidence Artifact | Status). Malformed rows (wrong cell count) are returned
    separately so C8 can fail on them; requirement is always cell 1 and status
    is always the last cell's token."""
    rows, malformed = [], []
    for line in section.splitlines():
        if not line.startswith("| `AC-"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 8:
            malformed.append(line.strip()[:90])
            continue
        m = re.search(r"`([A-Z_]+)`", cells[-1])  # last cell: status token
        rows.append((cells[1], m.group(1) if m else None))
    return rows, malformed


def _matrix_sections(txt):
    """domain (as used in the tables) -> section body. Normalizes
    'Platform / Runtime' (section header) to 'Platform' (table rows)."""
    sections = {}
    for part in re.split(r"^### ", txt, flags=re.M)[1:]:
        name = part.splitlines()[0].strip().replace(" / Runtime", "")
        sections[name] = part
    return sections


def _section_under(txt, header):
    """Text of the section whose `## header` line starts the section."""
    m = re.search(rf"^## {re.escape(header)}\s*\n(.*?)(?=\n## |\Z)", txt, flags=re.M | re.S)
    return m.group(1) if m else ""


def _coverage_map(section):
    """Domain -> (requirements, acceptance_ids) from the Coverage summary table.
    Tolerates `**bold**` cells (the Total row is fully bold)."""
    out = {}
    for m in re.finditer(
        r"^\| \*{0,2}([\w /-]+?)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \|",
        section,
        flags=re.M,
    ):
        out[m.group(1)] = (int(m.group(2)), int(m.group(3)))
    return out


def _summary_map(section):
    """Domain -> (total, passed, failed, evidence_blocked, not_implemented)
    from the Summary table. Tolerates `**bold**` cells (the Total row is bold)."""
    out = {}
    for m in re.finditer(
        r"^\| \*{0,2}([\w /-]+?)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \| \*{0,2}(\d+)\*{0,2} \|",
        section,
        flags=re.M,
    ):
        out[m.group(1)] = tuple(map(int, m.groups()[1:]))
    return out


def _defined_reqs():
    """domain -> set of `## REQ-XXX-NNN` headers defined in its requirement file."""
    defined = {}
    for domain, (prefix, f) in REQ_FILES.items():
        body = safe_read(os.path.join(DOCS_DIR, "02_requirements", f)) or ""
        defined[domain] = set(
            re.findall(rf"^#+\s*({re.escape(prefix)}-\d+)", body, flags=re.M)
        )
    return defined


def c8_acceptance_matrix():
    txt = safe_read(MATRIX_PATH)
    if txt is None:
        return check("C8 matrix readable", False, MATRIX_PATH)
    sections = _matrix_sections(txt)
    coverage = _coverage_map(_section_under(txt, "Coverage summary"))
    summary = _summary_map(_section_under(txt, "Summary"))

    row_n, eb_n, ni_n = {}, {}, {}
    refs, nfr_cells, malformed_all = set(), [], []
    for domain, sec in sections.items():
        rows, malformed = _ac_rows(sec)
        malformed_all += [f"{domain}: {m}" for m in malformed]
        row_n[domain] = len(rows)
        eb_n[domain] = sum(1 for _, s in rows if s == "EVIDENCE_BLOCKED")
        ni_n[domain] = sum(1 for _, s in rows if s == "NOT_IMPLEMENTED")
        for req_cell, _ in rows:
            refs |= set(re.findall(r"REQ-[A-Z0-9-]+", req_cell))
            if domain == NFR_DOMAIN:
                nfr_cells.append(req_cell.strip())

    tot_rows, tot_eb, tot_ni = sum(row_n.values()), sum(eb_n.values()), sum(ni_n.values())
    check("C8 every AC row has 8 columns", not malformed_all, "; ".join(malformed_all[:4]))
    check(
        "C8 AC rows match baseline",
        tot_rows == EXPECTED_AC_TOTAL,
        f"rows={tot_rows} expected={EXPECTED_AC_TOTAL}",
    )
    check(
        "C8 evidence-blocked match baseline",
        tot_eb == EXPECTED_EB_TOTAL,
        f"eb={tot_eb} expected={EXPECTED_EB_TOTAL}",
    )
    check(
        "C8 not-implemented match baseline",
        tot_ni == EXPECTED_NI_TOTAL,
        f"ni={tot_ni} expected={EXPECTED_NI_TOTAL}",
    )

    cov_bad = [
        d for d, (_, acs) in coverage.items() if d != "Total" and acs != row_n.get(d)
    ]
    check("C8 coverage table matches rows", not cov_bad, f"{cov_bad}")

    sum_bad = [
        d
        for d, (t, _p, _f, eb, ni) in summary.items()
        if d != "Total" and (t, eb, ni) != (row_n.get(d), eb_n.get(d), ni_n.get(d))
    ]
    check("C8 summary table matches rows", not sum_bad, f"{sum_bad}")
    check(
        "C8 summary totals sum",
        summary.get("Total") == (tot_rows, 0, 0, tot_eb, tot_ni),
        f"total={summary.get('Total')} computed=({tot_rows},0,0,{tot_eb},{tot_ni})",
    )

    defined = _defined_reqs()
    all_defined = set().union(*defined.values()) if defined else set()
    unmapped = sorted(all_defined - refs)
    phantom = sorted(refs - all_defined)
    check("C8 every defined REQ mapped", not unmapped, f"unmapped={unmapped}")
    check("C8 no phantom REQ refs", not phantom, f"phantom={phantom}")
    req_bad = [
        d
        for d, (reqs, _) in coverage.items()
        if d != "Total" and d != NFR_DOMAIN and reqs != len(defined.get(d, ()))
    ]
    check("C8 coverage requirements == defined", not req_bad, f"{req_bad}")
    check(
        "C8 coverage requirements total == 132",
        coverage.get("Total", (0, 0))[0] == 132,
        f"total={coverage.get('Total')}",
    )

    nfr = safe_read(NFR_PATH) or ""
    unresolved = []
    for cell in nfr_cells:
        if re.fullmatch(r"NFR-PERF-\d+", cell):
            if not re.search(rf"^#+\s*{re.escape(cell)}\b", nfr, flags=re.M):
                unresolved.append(cell)
        else:
            m = re.fullmatch(r"NFR (\d+\.\d+(?:\.\d+)?)", cell)
            if m and not re.search(
                rf"^#+\s*{re.escape(m.group(1))}\b", nfr, flags=re.M
            ):
                unresolved.append(cell)
    check("C8 NFR rows resolve to sections", not unresolved, f"{unresolved}")


# ---------------------------------------------------------------------------
# C9 — DEC-039 invariants (2026-08-14 doc-consistency reconciliation)
# ---------------------------------------------------------------------------

DEC039_DOC_PHRASES = {
    "no 4-mode HFT claims": [
        "LTP/LTPC/Quote/Full",
        "all four modes",
        "four modes",
        "4 modes",
        "LTP, LTPC, QUOTE, FULL",
    ],
    "no seconds timestamps": ["int32 epoch seconds", "epoch seconds"],
    "no checkpoint-durability claim": ["durability via Flink checkpoints"],
}
DEC039_LINE_PAIRS = {
    "no Safety_Halt_Requests LOG/control": ("Safety_Halt_Requests", "LOG/control"),
    "no ledger skipped in MVP": ("Postback_Projection_Ledger", "skipped"),
    "no ledger absent from MVP": ("Postback_Projection_Ledger", "in MVP"),
}


def c9_dec039_invariants():
    # --- doc scans (04-decisions.md is the dated record: DEC-012/018/039) ---
    hits = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if not f.endswith(".md"):
                continue
            path = os.path.join(root, f)
            if path == DECISIONS_PATH:
                continue
            txt = safe_read(path)
            if txt is None:
                continue
            for ln in txt.splitlines():
                if "removed 2026-08-14" in ln:
                    continue  # dated Standard-feed history annotations are allowed
                for label, phrases in DEC039_DOC_PHRASES.items():
                    if any(phrase in ln for phrase in phrases):
                        hits.append(f"{os.path.relpath(path, ROOT)}: {label}")
                        break
                for label, (a, b) in DEC039_LINE_PAIRS.items():
                    if a in ln and b in ln:
                        hits.append(f"{os.path.relpath(path, ROOT)}: {label}")
    check("C9 no stale DEC-039 doc claims", not hits, "; ".join(hits[:6]))

    # --- code: HFT modes ltpc (40 B) + full (196 B), nothing else accepted ---
    hft = safe_read(
        os.path.join(
            INGEST_DIR, "go-bridge", "third_party", "go-arrow", "arrow", "hft_stream.go"
        )
    )
    if hft is None:
        check("C9 hft_stream readable", False)
    else:
        check(
            "C9 HFT frame sizes 40/196 B",
            "hftSizeLTP      = 40" in hft and "hftSizeFull     = 196" in hft,
        )
        m = re.search(r"func normalizeHFTMode.*?\n}", hft, flags=re.S)
        fn = m.group(0) if m else ""
        check(
            "C9 HFT modes ltpc+full only",
            'case "l", "ltpc":' in fn
            and 'case "f", "full":' in fn
            and '"quote"' not in fn
            and '"ltp"' not in fn,
        )

    # --- code: bridge converts ns -> ms ---
    main_go = safe_read(os.path.join(INGEST_DIR, "go-bridge", "main.go")) or ""
    check("C9 bridge TS ns->ms", "/ 1_000_000" in main_go)

    # --- manifest/DDL: ledger + halt kinds KV; ledger live in dev ---
    manifest = safe_read_json(os.path.join(DDL_DIR, "schema_manifest.json")) or {}
    kinds = {
        t["table_name"]: (str(t.get("table_kind", "")), t.get("primary_key"))
        for t in manifest.get("tables", [])
    }
    check(
        "C9 ledger manifest KV",
        kinds.get("Postback_Projection_Ledger") == ("KV", "postback_event_id"),
        f"got {kinds.get('Postback_Projection_Ledger')}",
    )
    check(
        "C9 halt manifest KV",
        kinds.get("Safety_Halt_Requests") == ("KV", "halt_request_id"),
        f"got {kinds.get('Safety_Halt_Requests')}",
    )
    check(
        "C9 ledger DDL exists",
        os.path.exists(os.path.join(DDL_DIR, "17_postback_projection_ledger.sql")),
    )
    ddl18 = safe_read(os.path.join(DDL_DIR, "18_safety_halt_requests.sql")) or ""
    check("C9 halt DDL is KV control", "KV control table" in ddl18 and "was LOG" in ddl18)
    foundation = safe_read(
        os.path.join(DOCS_DIR, "08_implementation", "01-foundation.md")
    ) or ""
    check("C9 ledger live-in-dev evidence", "Postback_Projection_Ledger 705" in foundation)

    # --- DEC-038 dedup + SCH-19 index + SCH-23 EOD + REQ-EXE-004 intent DDL; 26 DDLs ---
    sqls = sorted(f for f in os.listdir(DDL_DIR) if f.endswith(".sql"))
    # 27: see the C1 pin. Both numbers are the same fact counted from either side.
    check("C9 DDL count = 27", len(sqls) == 27, f"got {len(sqls)}")
    check(
        "C9 dedup DDL on file",
        os.path.exists(os.path.join(DDL_DIR, "24_fingerprint_dedup.sql")),
    )
    check(
        "C9 instruction-index DDL on file",
        os.path.exists(os.path.join(DDL_DIR, "25_trade_instruction_state.sql")),
    )
    check(
        "C9 eod offload-state DDL on file",
        os.path.exists(os.path.join(DDL_DIR, "26_eod_offload_state.sql")),
    )
    check(
        "C9 execution-intent DDL on file",
        os.path.exists(os.path.join(DDL_DIR, "27_execution_intent.sql")),
    )
    obs = safe_read(os.path.join(DOCS_DIR, "08_implementation", "10-observability.md")) or ""
    rb = safe_read(os.path.join(DOCS_DIR, "06_operations", "01-runbooks.md")) or ""
    check(
        "C9 dedup DDL implemented (observability)",
        "fingerprint_dedup" in obs and "24_fingerprint_dedup.sql" in obs,
    )
    check(
        "C9 dedup DDL implemented (runbooks)",
        "fingerprint_dedup" in rb and "24_fingerprint_dedup.sql" in rb,
    )

    # --- inventories include forming_bar + ingestion_quarantine ---
    missing_inv = []
    for f in INVENTORY_FILES:
        body = safe_read(os.path.join(DOCS_DIR, f)) or ""
        for tbl in ("forming_bar", "ingestion_quarantine"):
            if tbl not in body:
                missing_inv.append(f"{f}: {tbl}")
    check("C9 inventories include both tables", not missing_inv, f"{missing_inv}")


# ---------------------------------------------------------------------------
# C10 — dossier traceability (2026-08-14 five-fix traceability work)
# ---------------------------------------------------------------------------

IMPLEMENTATION_DIR = os.path.join(DOCS_DIR, "08_implementation")
DOSSIER_TRACE_PATH = os.path.join(IMPLEMENTATION_DIR, "11-testing-and-release.md")
# REQ13-* ids are scoped to the master signal-job dossier (04); the retired
# candle-era dossier (13) was deleted 2026-08-17 with its traceability table
# absorbed into 04 §5.1.
REQ13_HOME_PATH = os.path.join(IMPLEMENTATION_DIR, "04-signal-job.md")

# Functional dossiers whose status tables carry Requirements/Acceptance rows.
COVERAGE_DOSSIERS = [
    "02-schema-storage.md",
    "03-ingestion.md",
    "04-signal-job.md",
    "05-execution-core.md",  # integrated Action Capture + Babysitter + Executor (2026-08-18)
    "08-local-compose.md",
    "09-production-swarm.md",
    "10-observability.md",
]
COVERAGE_ROW_CELLS = (
    "Requirements",
    "Acceptance criteria",
    "Source requirements",
    "Sources",
)

AC_RANGE_RE = re.compile(r"`?(AC-[A-Z]+-\d+)`?\s*[\u2013\u2014-]\s*`?(AC-[A-Z]+-\d+)`?")
AC_SINGLE_RE = re.compile(r"`(AC-[A-Z]+-\d+)`")
REQ13_RE = re.compile(r"REQ13-[A-Z]+-\d+")


def _ac_prefix(ac_id):
    return ac_id.rsplit("-", 1)[0]


def _expand_ac(text):
    """Set of AC ids mentioned in text, expanding `AC-X-a`–`AC-X-b` ranges
    (same domain only; cross-domain ranges keep both endpoints)."""
    ids = set()
    for m in AC_RANGE_RE.finditer(text):
        a, b = m.group(1), m.group(2)
        if _ac_prefix(a) == _ac_prefix(b):
            lo, hi = int(a.rsplit("-", 1)[1]), int(b.rsplit("-", 1)[1])
            for n in range(lo, hi + 1):
                ids.add(f"{_ac_prefix(a)}-{n:03d}")
        else:
            ids.update((a, b))
    ids.update(AC_SINGLE_RE.findall(text))
    return ids


def _matrix_ac_by_prefix():
    """AC prefix (AC-ING, …) -> set of matrix AC ids (backticks stripped).
    Reads the AC id from the first cell of each `| `AC-X-NNN` |` row (the
    C8 _ac_rows helper returns the requirement cell, which is cells[1])."""
    txt = safe_read(MATRIX_PATH)
    if txt is None:
        return {}
    prefix_of = {
        domain: "AC-" + prefix[4:] for domain, (prefix, _) in REQ_FILES.items()
    }
    prefix_of[NFR_DOMAIN] = "AC-NFR"
    out = {}
    for domain, sec in _matrix_sections(txt).items():
        prefix = prefix_of.get(domain)
        if not prefix:
            continue
        ids = set()
        for line in sec.splitlines():
            if not line.startswith("| `AC-"):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if cells and cells[0].startswith("`AC-"):
                ids.add(cells[0].strip("`"))
        out[prefix] = ids
    return out


def c10_dossier_traceability():
    # --- coverage table in 11-testing-and-release.md ---
    tt = safe_read(DOSSIER_TRACE_PATH)
    if tt is None:
        return check("C10 traceability doc readable", False, DOSSIER_TRACE_PATH)
    m = re.search(
        r"^### Acceptance criteria coverage\s*\n(.*?)(?=\n### |\Z)",
        tt,
        flags=re.M | re.S,
    )
    sec = m.group(1) if m else ""
    if not sec:
        return check("C10 acceptance-criteria coverage section found", False)

    table_ids, owner = {}, {}
    for ln in sec.splitlines():
        if not ln.startswith("| `AC-"):
            continue
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        if len(cells) < 4:
            continue
        dom = cells[0].strip("`").rstrip("-*")
        table_ids[dom] = _expand_ac(cells[1])
        owner[dom] = set(re.findall(r"[0-9]{2}-[a-z0-9-]+\.md", cells[2]))

    matrix = _matrix_ac_by_prefix()
    check(
        "C10 coverage table lists every AC domain",
        not (set(matrix) - set(table_ids)),
        f"missing={sorted(set(matrix) - set(table_ids))}",
    )
    check(
        "C10 no phantom AC domain rows",
        not (set(table_ids) - set(matrix)),
        f"phantom={sorted(set(table_ids) - set(matrix))}",
    )
    check(
        "C10 every domain has an owning dossier",
        not [d for d, ds in owner.items() if not ds],
        f"{sorted(d for d, ds in owner.items() if not ds)}",
    )
    bad_range = sorted(d for d, ids in table_ids.items() if ids != matrix.get(d, set()))
    check("C10 coverage ranges match matrix", not bad_range, f"{bad_range}")

    # --- dossier status rows (02-10) ---
    row_ids = {}
    for f in COVERAGE_DOSSIERS:
        body = safe_read(os.path.join(IMPLEMENTATION_DIR, f)) or ""
        ids = set()
        for ln in body.splitlines():
            if ln.startswith("| ") and ln.split("|")[1].strip() in COVERAGE_ROW_CELLS:
                ids |= _expand_ac(ln)
        if ids:
            row_ids[f] = ids

    all_matrix_ids = set().union(*matrix.values()) if matrix else set()
    phantom_ids = sorted(
        {ac for ids in row_ids.values() for ac in ids} - all_matrix_ids
    )
    check("C10 dossier rows cite real AC ids", not phantom_ids, f"{phantom_ids}")

    bad_owner = []
    for f, ids in row_ids.items():
        for ac in sorted(ids):
            owned = owner.get(_ac_prefix(ac), set())
            if f not in owned:
                bad_owner.append(f"{f}: {ac} (domain owned by {sorted(owned)})")
    check("C10 dossiers claim only owned AC domains", not bad_owner, "; ".join(bad_owner[:6]))

    covered = set().union(*row_ids.values()) | set().union(*table_ids.values())
    orphan = sorted(all_matrix_ids - covered)
    check("C10 no orphan AC id", not orphan, f"{orphan}")

    # --- REQ13-* ids are scoped to the master dossier (04) and defined there ---
    leaks = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if not f.endswith(".md"):
                continue
            path = os.path.join(root, f)
            if os.path.abspath(path) == os.path.abspath(REQ13_HOME_PATH):
                continue
            if REQ13_RE.search(safe_read(path) or ""):
                leaks.append(os.path.relpath(path, ROOT))
    check("C10 REQ13 ids stay in the master dossier 04", not leaks, f"{leaks}")

    d04 = safe_read(REQ13_HOME_PATH) or ""
    m13 = re.search(
        r"^## 5\.1 Requirement traceability[^\n]*\n(.*?)(?=\n## |\Z)",
        d04,
        flags=re.M | re.S,
    )
    defined13 = set(REQ13_RE.findall(m13.group(1) if m13 else ""))
    undefined13 = sorted(set(REQ13_RE.findall(d04)) - defined13)
    check("C10 REQ13 ids defined in 04", not undefined13, f"{undefined13}")


# ---------------------------------------------------------------------------
# C11 — verification-mapping links resolve to real test-design sections
# ---------------------------------------------------------------------------

VERIFICATION_DOSSIERS = COVERAGE_DOSSIERS + ["01-foundation.md"]


def _gh_slug(heading):
    """GitHub-style heading anchor: lowercase; drop punctuation; spaces -> '-'."""
    s = re.sub(r"[^\w\s-]", "", heading.strip().lower())
    return re.sub(r"\s+", "-", s)


TEST_DESIGN_SECTIONS = ("Component test designs", "Shared testing rules")


def _test_design_section_anchors(txt):
    """GitHub anchors for every heading under the master catalog's two
    test-design regions ('## Component test designs' and
    '## Shared testing rules' — the latter holds the performance-benchmark
    and security/CI procedures that verification mappings also link to)."""
    anchors = set()
    inside = None
    for line in txt.splitlines():
        m = re.match(r"^(#{2,4}) (.+)$", line)
        if not m:
            continue
        level, heading = len(m.group(1)), m.group(2).strip()
        if level == 2:
            inside = heading if heading in TEST_DESIGN_SECTIONS else None
            continue
        if inside:
            anchors.add(_gh_slug(heading))
    return anchors


def c11_verification_mapping():
    tt = safe_read(DOSSIER_TRACE_PATH)
    if tt is None:
        return check("C11 traceability doc readable", False, DOSSIER_TRACE_PATH)
    anchors = _test_design_section_anchors(tt)

    missing_secs = []
    for f in VERIFICATION_DOSSIERS:
        body = safe_read(os.path.join(IMPLEMENTATION_DIR, f)) or ""
        if not re.search(r"^## Verification mapping\s*$", body, flags=re.M):
            missing_secs.append(f)
    check("C11 all dossiers have a Verification mapping", not missing_secs, f"{missing_secs}")

    dead = []
    for f in sorted(os.listdir(IMPLEMENTATION_DIR)):
        if not f.endswith(".md"):
            continue
        body = safe_read(os.path.join(IMPLEMENTATION_DIR, f)) or ""
        m = re.search(
            r"^## Verification mapping\s*\n(.*?)(?=\n## |\Z)", body, flags=re.M | re.S
        )
        if not m:
            continue
        for link in re.finditer(
            r"\]\((?:\\./)?[^)#]*11-testing-and-release\.md#([^)]+)\)", m.group(1)
        ):
            anchor = link.group(1)
            if anchor not in anchors:
                dead.append(f"{f}: #{anchor}")
    check(
        "C11 verification links resolve to test-design sections",
        not dead,
        "; ".join(dead[:6]),
    )


# ---------------------------------------------------------------------------
# C12 — WORM/Object-Lock statements must name the R2 mechanism or be gated
# (2026-08-14 repo-wide sweep: every WORM/Object Lock statement either names
#  the R2 'bucket locks' mechanism or the S3 Object Lock API limitation, or is
#  marked evidence-gated; a bare claim with neither is drift reintroduced.)
# ---------------------------------------------------------------------------

WORM_TRIGGER_RE = re.compile(r"\b(worm|object[ _-]lock|write-once)\b", re.I)
WORM_MECHANISM_RE = re.compile(r"bucket[ _-]lock", re.I)  # the R2 WORM mechanism
WORM_API_LIMIT_RE = re.compile(r"object[ _-]lock api", re.I)  # the S3 API R2 doesn't implement
WORM_CTRL_REF_RE = re.compile(r"AC-NFR-005|NFR 3\.4\.1")  # IDs that pin the mechanism
WORM_GATE_RES = [
    re.compile(r"evidence[ -]gated", re.I),
    re.compile(r"evidence[ -]blocked", re.I),
    re.compile(r"deferred", re.I),
    re.compile(r"pending", re.I),
    re.compile(r"not implemented", re.I),
    re.compile(r"not-implemented", re.I),
    re.compile(r"not yet", re.I),
    re.compile(r"to be determined", re.I),
    re.compile(r"\btbd\b", re.I),
    re.compile(r"\btodo\b", re.I),
    re.compile(r"\bopen\b", re.I),
]


def c12_worm_statements():
    hits = []
    for root, _, files in os.walk(DOCS_DIR):
        for f in files:
            if not f.endswith(".md"):
                continue
            path = os.path.join(root, f)
            txt = safe_read(path)
            if txt is None:
                continue
            lines = txt.splitlines()
            # The doc names the mechanism somewhere -> headings within it are grounded.
            doc_mechanism = bool(WORM_MECHANISM_RE.search(txt))
            for i, ln in enumerate(lines):
                if not WORM_TRIGGER_RE.search(ln):
                    continue
                # Trigger only inside a backticked identifier (e.g. `audit-worm-indefinite`)
                # is a name, not a statement.
                if not WORM_TRIGGER_RE.search(re.sub(r"`[^`]*`", "", ln)):
                    continue
                if ln.lstrip().startswith("#") and doc_mechanism:
                    continue  # heading in a doc that establishes the mechanism
                # A statement may wrap across lines; judge the 3-line window.
                window = " ".join(lines[max(0, i - 1) : i + 2])
                if WORM_MECHANISM_RE.search(window):
                    continue
                if WORM_API_LIMIT_RE.search(window):
                    continue
                if WORM_CTRL_REF_RE.search(window):
                    continue
                if any(g.search(window) for g in WORM_GATE_RES):
                    continue
                rel = os.path.relpath(path, ROOT)
                hits.append(f"{rel}:{i + 1}: {ln.strip()[:100]}")
    check(
        "C12 WORM statements name R2 bucket locks or are evidence-gated",
        not hits,
        "; ".join(hits[:6]),
    )


# ---------------------------------------------------------------------------
# C13 — runbook index <-> requirements sync: every runbook_id in
# docs/06_operations front matter must appear in the §6.9 runbook coverage
# table of 06-operational.md (and no phantom id may sit in the table).
# ---------------------------------------------------------------------------

OPS_DIR = os.path.join(DOCS_DIR, "06_operations")
OPERATIONAL_REQ_PATH = os.path.join(DOCS_DIR, "02_requirements", "06-operational.md")
RUNBOOK_ID_RE = re.compile(r"^runbook_id:\s*([A-Z][A-Z0-9-]*)", re.M)


def c13_runbook_coverage():
    ids = {}
    for f in sorted(os.listdir(OPS_DIR)):
        if not f.endswith(".md"):
            continue
        txt = safe_read(os.path.join(OPS_DIR, f)) or ""
        m = RUNBOOK_ID_RE.search(txt)
        if m:
            ids[m.group(1)] = f
    if not ids:
        return check("C13 runbook ids found in front matter", False, "none found")

    req = safe_read(OPERATIONAL_REQ_PATH) or ""
    m = re.search(
        r"^## 6\.9 Observability and alert response\s*\n(.*?)(?=\n## |\Z)",
        req,
        flags=re.M | re.S,
    )
    sec = m.group(1) if m else ""
    if not sec:
        return check("C13 §6.9 coverage section found", False, OPERATIONAL_REQ_PATH)

    missing = sorted(rb_id for rb_id in ids if rb_id not in sec)
    check(
        "C13 every runbook_id appears in §6.9 coverage table",
        not missing,
        f"missing={missing}",
    )

    table_ids = set(
        t for t in re.findall(r"`([^`]+)`", sec) if t.startswith("OPS-")
    )
    phantom = sorted(table_ids - set(ids))
    check(
        "C13 no phantom runbook ids in §6.9 table",
        not phantom,
        f"phantom={phantom}",
    )


# ---------------------------------------------------------------------------
# C14 — change-control reconciliation records (01-foundation.md "Change
# control", orig L205): every record in docs/05_deployment/change-records/
# must name the six required fields with a valid compatibility class.
# ---------------------------------------------------------------------------

CHANGE_RECORDS_DIR = os.path.join(DOCS_DIR, "05_deployment", "change-records")


def c14_change_control():
    files, issues, missing = change_control_check.scan_records(CHANGE_RECORDS_DIR)
    if missing:
        return check(
            "C14 change-records directory exists", False, CHANGE_RECORDS_DIR
        )
    if not files:
        return check(
            "C14 change records on file", True, "none — nothing to validate"
        )
    for f in files:
        iss = issues[f]
        check(
            f"C14 {f} names all required fields",
            not iss,
            "; ".join(iss),
        )
    bad = [f for f, iss in issues.items() if iss]
    check(
        "C14 all change records complete",
        not bad,
        f"incomplete={bad}",
    )


def c15_evidence_ownership():
    problems, (checked, cw, host) = evidence_ownership_check.check_evidence_dir(
        evidence_ownership_check.EVIDENCE_DIR,
        evidence_ownership_check.CONTAINER_UID)
    detail = (f"{checked} record(s), {cw} container-written, "
              f"{host} host-owned (out of scope)")
    check("C15 container-written evidence group-writable",
          not problems, detail + (("; " + "; ".join(problems[:3])) if problems else ""))


def _ingestion_sources():
    """Every file that reads ingestion env keys: main Java sources and the Go
    bridge (test files excluded — a key only mentioned in a test would be
    drift)."""
    for root, _, files in os.walk(os.path.join(INGEST_DIR, "src", "main", "java")):
        for f in files:
            if f.endswith(".java"):
                yield os.path.join(root, f)
    bridge = os.path.join(INGEST_DIR, "go-bridge")
    if os.path.isdir(bridge):
        for root, _, files in os.walk(bridge):
            for f in files:
                if f.endswith(".go") and not f.endswith("_test.go"):
                    yield os.path.join(root, f)


def c16_env_key_drift():
    """C16 (ING-UNIT-019): every env key documented in the ingestion dossier's
    "Configuration contract" table must be read by IngestionConfig, the Go
    bridge, or another ingestion service source. A documented key nobody reads
    is doc drift; an undocumented key nobody can set is the mirror-image trap.
    """
    p = os.path.join(DOCS_DIR, "08_implementation", "03-ingestion.md")
    txt = safe_read(p)
    if txt is None:
        return check("C16 ingestion dossier readable", False, p)
    m = re.search(r"### Configuration contract\n(.*?)(?=\n### |\Z)", txt, re.S)
    if not m:
        return check("C16 config contract section found", False)
    keys = re.findall(r"\| `([A-Z][A-Z0-9_]*)` \| (?:Yes|No) \|", m.group(1))
    if not keys:
        return check("C16 config contract env keys found", False)
    sources = "".join(safe_read(s) or "" for s in _ingestion_sources())
    missing = sorted(k for k in keys if f'"{k}"' not in sources)
    check("C16 every documented env key is read by ingestion code",
          not missing,
          f"{len(keys)} documented keys; unread: {', '.join(missing) if missing else 'none'}")


def main():
    c1_manifest()
    c2_ownership_matrix()
    c3_schema_state_diagram()
    c4_compat_vocabulary()
    c5_stale_phrases()
    c6_test_counts()
    c7_version_pins()
    c8_acceptance_matrix()
    c9_dec039_invariants()
    c10_dossier_traceability()
    c11_verification_mapping()
    c12_worm_statements()
    c13_runbook_coverage()
    c14_change_control()
    c15_evidence_ownership()
    c16_env_key_drift()
    if failures:
        print(f"\ndocs-audit: {len(failures)} check(s) FAILED — fix before proceeding")
        return 1
    print("\ndocs-audit: all checks pass — docs agree with code")
    return 0


if __name__ == "__main__":
    sys.exit(main())
