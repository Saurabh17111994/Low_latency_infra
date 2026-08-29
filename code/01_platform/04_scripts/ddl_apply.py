#!/usr/bin/env python3
"""
DDL application contract
(docs/08_implementation/01-foundation.md -> "DDL application contract", orig L420).

Workflow (gated):
  1. Load version pins from versions.pin (sibling of this script).
  2. Enforce VersionGate on PLATFORM versions only (FLINK_VERSION, FLUSS_VERSION):
     any absent / 'latest' / placeholder -> FAIL (no apply).
     External/verification versions (broker protocol, Arrow API, schema
     lifecycle) may remain TO_BE_VERIFIED and do NOT block DDL emission.
  3. Compute SHA-256 of every DDL file; detect whether the committed
     schema_manifest.json is stale (names, paths, or checksums differ).
     Every entry also emits the four designed per-entry fields
     (schema_version, writer_owner, retention_policy, lake_policy) parsed
     from the DDL header comments and WITH options — a manifest missing or
     stale on those fields is drift and must be regenerated with --force.
  4. Without --force, refuse to overwrite an existing manifest that differs
     from the computed one. Print a diff and require an explicit decision.
  5. Parse each DDL for its routing identity (bucket.key); LOG tables must have a non-null one.
  6. Apply to Fluss ONLY when --apply-verified AND --matrix-evidence (capability
     evidence, e.g. the COMPAT-FLUSS-* / COMPAT-FLINK-* records) are given.
     The apply executes the full 9-step contract through the Java engine
     com.trading.common.schema.ddl.DdlApplyTool (see 02-schema-storage.md):
     empty-catalog precondition, the COMPAT-FLUSS-005 raw-client composite-PK
     matrix re-verified IN-BAND (scratch tables, dropped after; a deviation
     refuses the apply — the matrix is never just referenced as evidence),
     deterministic apply, effective-schema inspection + parity, write/read
     smoke, and an evidence record carrying the applied manifest id + the
     matrix outcome. Exit codes: 0 full PASS; 6 PASS_WITH_LIMITATION
     (acknowledged partial apply — composite-PK limitation, Flink-only design;
     distinct from PASS so automation can branch); 1 apply/parity/smoke/matrix
     failure OR an unacknowledged composite-PK limitation; 2 usage/classpath;
     3 empty-catalog violation (refused); 4/5 gate refusals. The tool prints a
     machine-readable DDL-APPLY-RESULT: <STATUS> exit=<code> sentinel line.

Dev verification (never production): set DDL_APPLY_TABLE_PREFIX=<p> to apply to
scratch tables (dropped after the run), DDL_APPLY_SKIP_SMOKE=1 to skip the
write/read smoke, DDL_APPLY_ACK_LIMITATIONS=auto (or <tables>) to acknowledge
the documented composite-PK raw-client limitation (COMPAT-FLUSS-005): 'auto'
prefills the exact tables detected from the manifest (composite PK + default
bucket key) so the operator only confirms, never guesses; without an
acknowledgment an apply with limited tables refuses with exit 1. The smoke
(CHG-100) runs against a scratch twin (<prefix>smoke_twin_<table>) created from
the same descriptor and dropped immediately — fixture rows never reach real
tables (LOG fixture rows are undeletable). DDL_APPLY_ALLOW_LIVE_SMOKE=1 restores
the old live-table smoke (debug/evidence only). Leftover fixture rows from the
old smoke path are detected and repaired with DDL_APPLY_SWEEP=1
(DDL_APPLY_SWEEP_FIX_KV=1 deletes KV fixtures; LOG fixtures are reported with
exit 3); DDL_APPLY_SWEEP_TABLES=name1,name2 scopes the sweep. Application
requires capability evidence instead of the historical reconciliation blocker
(superseded 2026-08-10, removed 2026-08-15): versions are pinned and live
dev-cluster applies have run.

Every run also echoes the non-root ownership contract the gate enforces — host
parity with the ddl-apply container wrapper's emit — so operators see the same
expectation (engine uid/gid, evidence root setgid 2775, container-written
records group-writable 664) whether the contract runs on the host or in the
container (see evidence_ownership_check.py).
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
DDL_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "02_sql", "ddl")
VERSIONS_PIN = os.path.join(SCRIPT_DIR, "versions.pin")
MANIFEST_PATH = os.path.join(DDL_DIR, "schema_manifest.json")
# Java apply-engine classpath (com.trading.common.schema.ddl.DdlApplyTool).
# Versions mirror the parent pom properties (code/pom.xml jackson.version /
# slf4j.version); fluss-client is the pinned 0.9.1-incubating shaded artifact.
# DDL_APPLY_M2_REPO overrides the default ~/.m2/repository — the ddl-apply
# container runs the engine as a non-root user (no root home) and bakes the
# jars at /opt/ddl-apply/m2/repository via this env.
M2_REPO = os.environ.get("DDL_APPLY_M2_REPO") or os.path.join(
    os.path.expanduser("~"), ".m2", "repository")
COMMON_CLASSES = os.path.join(REPO_ROOT, "code", "common", "target", "classes")
JACKSON_VERSION = "2.16.1"
SLF4J_VERSION = "2.0.9"
# DDL_APPLY_EVIDENCE_DIR overrides the default repo-root logs/ddl-apply — the
# ddl-apply container entrypoint repairs ownership on the SAME path, so an
# operator can redirect evidence to any mounted dir without editing the image.
EVIDENCE_ROOT = os.environ.get("DDL_APPLY_EVIDENCE_DIR") or os.path.join(
    REPO_ROOT, "logs", "ddl-apply")
PLACEHOLDERS = {
    "BROKER_MARKET_DATA_PROTOCOL_TO_BE_PINNED",
    "FLINK_VERSION_TO_BE_PINNED",
    "FLUSS_VERSION_TO_BE_PINNED",
    "ARROW_API_CONTRACT_TO_BE_VERIFIED",
    "OPENALGO_API_CONTRACT_TO_BE_VERIFIED",
    "SCHEMA_LIFECYCLE_TO_BE_VERIFIED",
}
REQUIRED_VERSIONS = [
    # Platform versions: required for Fluss DDL definition / schema.
    # External/verification versions (broker/Arrow/schema-lifecycle) are NOT
    # required here; they remain TO_BE_VERIFIED and do not block DDL emission.
    "FLINK_VERSION",
    "FLUSS_VERSION",
]


def sha256_of(path):
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                digest.update(chunk)
    except OSError as exc:
        raise RuntimeError(f"cannot read {path}: {exc}") from exc
    return digest.hexdigest()


def load_versions(pin_path):
    values = {}
    if not os.path.exists(pin_path):
        return values
    try:
        with open(pin_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                values[key.strip()] = val.strip()
    except OSError as exc:
        raise RuntimeError(f"cannot read {pin_path}: {exc}") from exc
    return values


def enforce_version_gate(versions):
    problems = []
    for key in REQUIRED_VERSIONS:
        val = versions.get(key)
        if not val:
            problems.append(f"{key} is absent")
        elif val.lower() == "latest":
            problems.append(f"{key} is 'latest'")
        elif val in PLACEHOLDERS:
            problems.append(f"{key} is a placeholder ({val})")
    return problems


def parse_bucket_key(ddl_text):
    match = re.search(r"'bucket\.key'\s*=\s*'([^']*)'", ddl_text)
    return match.group(1) if match else None


def parse_primary_key(ddl_text):
    match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl_text)
    return match.group(1).strip() if match else None


def parse_table_name(ddl_text):
    match = re.search(r"CREATE TABLE (\w+)", ddl_text)
    return match.group(1) if match else None


def parse_with_options(ddl_text):
    """Extract the WITH (...) options block as an ordered dict.

    Our DDLs write options as 'key' = 'value' pairs (single-quoted, dotted
    keys). Everything between the closing paren of the column list and the
    final ';' is the WITH block.
    """
    match = re.search(r"\)\s*WITH\s*\((.*?)\)\s*;", ddl_text, re.DOTALL)
    if not match:
        return {}
    options = {}
    # Keys are dotted option names and may contain hyphens
    # (e.g. 'table.datalake.auto-compaction'); values are single-quoted.
    for key, value in re.findall(r"'([a-zA-Z0-9_.-]+)'\s*=\s*'([^']*)'", match.group(1)):
        options[key] = value
    return options


def parse_owner(ddl_text):
    """Writer owner from the DDL header comment: '-- Owner: <name>'.

    Header prose sometimes continues on the same line ("Signal job (sole
    writer). Executor never mutates this table."); the manifest value is the
    first sentence/token so downstream consumers compare stable owner names.
    """
    match = re.search(r"^--\s*Owner:\s*(.+)$", ddl_text, re.MULTILINE)
    if not match:
        return ""
    return re.split(r"[.;]", match.group(1).strip(), maxsplit=1)[0].strip()


def parse_schema_version(ddl_text):
    """Table contract version from the DDL header: '-- Schema version: <n>'."""
    match = re.search(r"^--\s*Schema version:\s*(\d+)", ddl_text, re.MULTILINE)
    return match.group(1) if match else ""


def retention_policy_of(options):
    """Live retention: the table.log.ttl value, or 'none' when the DDL sets no TTL."""
    return options.get("table.log.ttl") or "none"


def lake_policy_of(options):
    """Offload/audit behavior compactly from the datalake options, or 'off'."""
    enabled = options.get("table.datalake.enabled")
    if enabled is None:
        return "off"
    parts = [f"enabled={enabled}"]
    if options.get("table.datalake.format"):
        parts.append(f"format={options['table.datalake.format']}")
    if options.get("table.datalake.freshness"):
        parts.append(f"freshness={options['table.datalake.freshness']}")
    if options.get("table.datalake.auto-compaction"):
        parts.append(f"auto-compaction={options['table.datalake.auto-compaction']}")
    return ",".join(parts)


# Per-entry fields emitted from the DDLs on top of the core identity set.
# The manifest contract (docs/08_implementation/02-schema-storage.md "Schema
# manifest") defines 12 per-entry fields; these four were designed but not
# emitted before 2026-08-15 — the gap is closed by this script.
EMITTED_FIELDS = ["schema_version", "writer_owner", "retention_policy", "lake_policy"]


def matrix_boundary(table_name):
    """Map a table to its version-matrix boundary id (version_matrix.yaml).

    Candle/signal tables ride the Fluss-Flink connector boundary (they are
    written by the Signal job); every other table rides the Fluss server
    boundary. Both are seeded UNKNOWN — classification moves to COMPATIBLE
    only via capability evidence (docs/08_implementation/01-foundation.md
    L848: "All DDLs have checksums and compatibility classes").
    """
    if table_name in (
        "feature_candles_15s",
        # Low-latency candles Phase 1 (2026-08-29): the preview table is
        # written by the Signal job's Fluss Flink connector sink — same
        # connector boundary as the final candle table.
        "feature_candles_15s_preview",
        "Signal_Candidates",
        "Signal_Candidates_current",
        # Signal-job connector-written state tables (DEC-038 dedup index +
        # SCH-19 instruction-hash index) ride the same connector boundary —
        # their writers are the Fluss Flink connector, not the raw client.
        "fingerprint_dedup",
        "trade_instruction_state",
    ):
        return "VM-FLUSS-CONN-007"
    return "VM-FLUSS-SRV-005"


def load_existing_manifest():
    """Return the committed schema_manifest.json as a dict, or None if absent/unreadable."""
    if not os.path.exists(MANIFEST_PATH):
        return None
    try:
        with open(MANIFEST_PATH, encoding="utf-8") as fh:
            raw = fh.read()
        manifest = json.loads(raw)
        # R-147: normalize the structure so a malformed-but-valid-JSON
        # manifest fails with the clear DDL-contract diagnostic, not a
        # KeyError/TypeError deep in diff_manifests.
        if not isinstance(manifest, dict) or not isinstance(
            manifest.get("tables"), list
        ):
            raise RuntimeError(
                f"{MANIFEST_PATH} has no 'tables' list — malformed manifest structure"
            )
        return manifest
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARNING: cannot parse existing manifest ({exc}); treating as absent")
        return None


def compute_manifest_entries():
    """Parse all DDL files and return list of manifest entries."""
    try:
        ddl_files = sorted(
            name for name in os.listdir(DDL_DIR) if name.endswith(".sql")
        )
    except OSError as exc:
        raise RuntimeError(f"cannot list DDL directory {DDL_DIR}: {exc}") from exc

    entries = []
    for name in ddl_files:
        path = os.path.join(DDL_DIR, name)
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
        except OSError as exc:
            raise RuntimeError(f"cannot read {path}: {exc}") from exc

        # Non-table SQL (catalog/database bootstrap) is not subject to
        # the routing-key rule.
        if "CREATE TABLE" not in text:
            continue

        table_name = parse_table_name(text)
        options = parse_with_options(text)
        emitted = {
            "schema_version": parse_schema_version(text),
            "writer_owner": parse_owner(text),
            "retention_policy": retention_policy_of(options),
            "lake_policy": lake_policy_of(options),
        }

        primary_key = parse_primary_key(text)
        if primary_key:
            # KV / primary-key table: routed by primary key, not bucket.key.
            entries.append(
                {
                    "table_name": table_name,
                    "ddl_path": name,
                    "ddl_sha256": sha256_of(path),
                    "table_kind": "KV",
                    "primary_key": primary_key,
                    "bucket_key": None,
                    "compatibility_class": "UNKNOWN",
                    "validated_matrix": matrix_boundary(table_name),
                    **emitted,
                }
            )
            continue

        # LOG table: must carry a non-null routing identity (bucket.key).
        bucket_key = parse_bucket_key(text)
        if not bucket_key or not bucket_key.strip():
            raise RuntimeError(
                f"ROUTING-KEY VIOLATION: {name} (LOG table) has no non-null "
                "bucket.key (routing identity)"
            )
        entries.append(
            {
                "table_name": table_name,
                "ddl_path": name,
                "ddl_sha256": sha256_of(path),
                "table_kind": "LOG",
                "bucket_key": bucket_key,
                "primary_key": None,
                "compatibility_class": "UNKNOWN",
                "validated_matrix": matrix_boundary(table_name),
                **emitted,
            }
        )
    return entries


def build_tool_classpath(fluss_version, m2_repo=M2_REPO):
    """Classpath for the Java DDL apply engine, or None with a diagnostic."""
    # (version, groupId path, artifactId)
    jars = [
        (fluss_version, "org/apache/fluss", "fluss-client"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-databind"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-core"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-annotations"),
        (SLF4J_VERSION, "org/slf4j", "slf4j-api"),
    ]
    entries = [COMMON_CLASSES]
    missing = []
    for version, group, name in jars:
        path = os.path.join(
            m2_repo, *group.split("/"), name, version, f"{name}-{version}.jar"
        )
        if os.path.isfile(path):
            entries.append(path)
        else:
            missing.append(path)
    if not os.path.isfile(os.path.join(COMMON_CLASSES, "com/trading/common/schema/ddl/DdlApplyTool.class")):
        missing.append(COMMON_CLASSES + " (run `cd code && mvn -o compile -pl common` first)")
    if missing:
        print("TOOL CLASSPATH INCOMPLETE:")
        for m in missing:
            print("  - " + m)
        return None
    return os.pathsep.join(entries)


def enrich_evidence(evidence_path, matrix_evidence):
    """Attach the capability-evidence artifact + its sha256 to the apply record."""
    try:
        with open(evidence_path, encoding="utf-8") as fh:
            record = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARNING: cannot read apply evidence {evidence_path}: {exc}")
        return
    record["matrix_evidence"] = matrix_evidence
    record["matrix_evidence_sha256"] = sha256_of(matrix_evidence)
    try:
        with open(evidence_path, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2)
    except OSError as exc:
        print(f"WARNING: cannot enrich apply evidence {evidence_path}: {exc}")


def run_sweep_tool():
    """CHG-100: detect/repair smoke-fixture rows (see DdlApplyTool --sweep).

    Env: DDL_APPLY_SWEEP=1, DDL_APPLY_SWEEP_FIX_KV=1 (delete KV fixtures),
    DDL_APPLY_SWEEP_TABLES=name1,name2 (scope; default = all live tables).
    """
    try:
        versions = load_versions(VERSIONS_PIN)
    except OSError as exc:
        print(f"cannot read {VERSIONS_PIN}: {exc}")
        return 2
    classpath = build_tool_classpath(versions.get("FLUSS_VERSION", "unknown"))
    if classpath is None:
        return 2
    cmd = [
        "java",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-cp", classpath,
        "com.trading.common.schema.ddl.DdlApplyTool",
        "--bootstrap", os.environ.get("FLUSS_BOOTSTRAP", "localhost:9123"),
        "--sweep",
    ]
    if os.environ.get("DDL_APPLY_SWEEP_FIX_KV") == "1":
        cmd.append("--sweep-fix-kv")
    tables = os.environ.get("DDL_APPLY_SWEEP_TABLES", "")
    for t in tables.split(","):
        if t.strip():
            cmd += ["--sweep-table", t.strip()]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="")
    return result.returncode


def run_apply_tool(versions, matrix_evidence):
    """Execute the 9-step DDL application contract via the Java engine."""
    fluss_version = versions.get("FLUSS_VERSION", "unknown")
    classpath = build_tool_classpath(fluss_version)
    if classpath is None:
        return 2
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = os.path.join(EVIDENCE_ROOT, f"ddl-apply-{stamp}")
    try:
        os.makedirs(out_dir, exist_ok=True)
    except OSError as exc:
        print(f"cannot create evidence dir {out_dir}: {exc}")
        return 2
    evidence = os.path.join(out_dir, "apply.json")

    cmd = [
        "java",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",  # Arrow MemoryUtil (fluss client)
        "-cp", classpath,
        "com.trading.common.schema.ddl.DdlApplyTool",
        "--ddl-dir", DDL_DIR,
        "--bootstrap", os.environ.get("FLUSS_BOOTSTRAP", "localhost:9123"),
        "--evidence-out", evidence,
        "--flink-version", versions.get("FLINK_VERSION", "unknown"),
        "--fluss-version", fluss_version,
    ]
    prefix = os.environ.get("DDL_APPLY_TABLE_PREFIX")
    if prefix:
        cmd += ["--table-prefix", prefix]
    if os.environ.get("DDL_APPLY_SKIP_SMOKE") == "1":
        cmd.append("--skip-smoke")
    if os.environ.get("DDL_APPLY_ALLOW_LIVE_SMOKE") == "1":
        # CHG-100 default is twin-only smoke (fixtures to scratch twins, never
        # real tables). This escape hatches the OLD behavior with fixture rows
        # in REAL tables — LOG rows undeletable; do NOT use on consumer catalogs.
        cmd.append("--allow-live-smoke")
    ack = os.environ.get("DDL_APPLY_ACK_LIMITATIONS") or ""
    if ack.strip():
        # Operator acknowledgment of the documented composite-PK raw-client
        # limitation (COMPAT-FLUSS-005): names EXACTLY the limited tables,
        # e.g. DDL_APPLY_ACK_LIMITATIONS=Order_Lifecycle,Order_Correlation.
        cmd += ["--ack-limitations", ack]

    print(f"Applying DDL to Fluss ({'dev scratch prefix ' + prefix if prefix else 'acceptance catalog'})...")
    print(f"  evidence: {evidence}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="")
    if result.returncode == 6:
        # Acknowledged partial apply (composite-PK limitation, Flink-only
        # design): the tables are applied, but this is NOT a full PASS.
        # Dedicated exit code + sentinel so downstream automation can branch
        # (0 = full PASS, 6 = acknowledged partial, != 0/6 = failure/refusal).
        enrich_evidence(evidence, matrix_evidence)
        try:
            with open(evidence, encoding="utf-8") as fh:
                record = json.load(fh)
            print("DDL-APPLY-RESULT: PASS_WITH_LIMITATION exit=6")
            print(f"APPLIED manifest id: {record.get('applied_manifest_id')} "
                  f"(status PASS_WITH_LIMITATION)")
            print(f"  evidence record: {evidence}")
        except (OSError, json.JSONDecodeError) as exc:
            print(f"WARNING: cannot read apply evidence {evidence}: {exc}")
        return 6
    if result.returncode != 0:
        print(f"DDL APPLY FAILED (exit {result.returncode}) — see {evidence}")
        return result.returncode

    enrich_evidence(evidence, matrix_evidence)
    try:
        with open(evidence, encoding="utf-8") as fh:
            record = json.load(fh)
        print("DDL-APPLY-RESULT: PASS exit=0")
        print(f"APPLIED manifest id: {record.get('applied_manifest_id')}")
        print(f"  evidence record: {evidence} (status {record.get('status')})")
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARNING: cannot read apply evidence {evidence}: {exc}")
    return 0


def diff_manifests(existing, computed):
    """Return list of human-readable diff lines, or empty list if identical."""
    existing_entries = {t["ddl_path"]: t for t in existing.get("tables", [])}
    computed_entries = {t["ddl_path"]: t for t in computed["tables"]}
    diffs = []

    for path in sorted(set(existing_entries) | set(computed_entries)):
        e = existing_entries.get(path)
        c = computed_entries.get(path)
        if e is None and c is not None:
            diffs.append(f"  + ADDED:   {path} ({c['table_name']})")
        elif c is None and e is not None:
            diffs.append(f"  - REMOVED: {path} ({e['table_name']})")
        elif e is not None and c is not None and e["ddl_sha256"] != c["ddl_sha256"]:
            diffs.append(f"  ~ CHANGED: {path} ({c['table_name']}) checksum differs")
        elif (
            e is not None
            and c is not None
            and e.get("table_kind") != c.get("table_kind")
        ):
            diffs.append(
                f"  ~ KIND:    {path} ({c['table_name']}) "
                f"{e.get('table_kind')} -> {c.get('table_kind')}"
            )
        elif (
            e is not None
            and c is not None
            and e.get("compatibility_class") != c.get("compatibility_class")
        ):
            diffs.append(
                f"  ~ COMPAT:  {path} ({c['table_name']}) "
                f"compatibility_class {e.get('compatibility_class')} -> "
                f"{c.get('compatibility_class')}"
            )
        elif (
            e is not None
            and c is not None
            and (
                e.get("primary_key") != c.get("primary_key")
                or e.get("bucket_key") != c.get("bucket_key")
            )
        ):
            diffs.append(
                f"  ~ FIELD:   {path} ({c['table_name']}) primary_key/bucket_key "
                "differs from DDL (manifest vs generator)"
            )
        elif e is not None and c is not None:
            # The four designed-but-unemitted per-entry fields
            # (schema_version, writer_owner, retention_policy, lake_policy —
            # see docs/08_implementation/02-schema-storage.md "Schema
            # manifest") are now generated from the DDLs. A committed
            # manifest that predates the emission (fields missing) or carries
            # stale values is drift and must be regenerated with --force.
            missing = [f for f in EMITTED_FIELDS if e.get(f) is None and c.get(f) is not None]
            stale = [f for f in EMITTED_FIELDS
                     if e.get(f) is not None and e.get(f) != c.get(f)]
            if missing:
                diffs.append(
                    f"  ~ FIELDS:  {path} ({c['table_name']}) missing emitted "
                    f"fields: {', '.join(missing)}"
                )
            elif stale:
                diffs.append(
                    f"  ~ FIELDS:  {path} ({c['table_name']}) emitted fields differ: "
                    + ", ".join(f"{f} {e.get(f)!r} -> {c.get(f)!r}" for f in stale)
                )

    return diffs


def echo_ownership_contract():
    """Print the non-root ownership contract the gate will enforce.

    Host parity with the ddl-apply container wrapper's emit (which prints the
    APPLIED owner/mode after its repair): this host-side run performs no repair,
    and the records it writes are host-owned — OUT of the gate's scope
    (evidence_ownership_check.py scopes "container-written" to owner == the
    engine uid). The line mirrors the wrapper's tokens (engine uid/gid, evidence
    root setgid 2775, records group-writable 664) so operators see the same
    expectation whether the contract runs on the host or in the container.
    Suppressed in-container (DDL_APPLY_IN_CONTAINER, set by the image's runner):
    the entrypoint wrapper has already emitted the APPLIED contract — one
    contract line per run, never two.
    """
    if os.environ.get("DDL_APPLY_IN_CONTAINER"):
        return
    engine_uid = os.environ.get("DDL_APPLY_UID", "10001")
    engine_gid = os.environ.get("DDL_APPLY_GID", "10001")
    print(f"ddl-apply: evidence root {EVIDENCE_ROOT} — host run "
          f"(uid {os.getuid()}, no wrapper repair); gate enforces uid "
          f"{engine_uid} gid {engine_gid}, evidence root setgid 2775, "
          f"container-written records group-writable 664 "
          f"(host-owned records out of scope)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        action="store_true",
        help="allow overwriting schema_manifest.json even when the DDLs have changed",
    )
    parser.add_argument(
        "--apply-verified",
        action="store_true",
        help="attempt real application (still gated on evidence)",
    )
    parser.add_argument(
        "--matrix-evidence", help="path to capability-evidence artifact"
    )
    args = parser.parse_args()
    echo_ownership_contract()

    # CHG-100: fixture-pollution sweep — bypasses the apply contract entirely.
    if os.environ.get("DDL_APPLY_SWEEP") == "1":
        return run_sweep_tool()

    try:
        versions = load_versions(VERSIONS_PIN)
        gate_problems = enforce_version_gate(versions)
        if gate_problems:
            print("VERSION GATE FAILED (refusing to apply DDL):")
            for problem in gate_problems:
                print("  - " + problem)
            print(f"Pin versions in {VERSIONS_PIN} — see docs/08_implementation/"
                  "12-version-compatibility-evidence.md for the gate conditions.")
            return 2

        entries = compute_manifest_entries()
        computed = {"schema_manifest_version": "1", "tables": entries}

        # --- Manifest staleness detection (R-014: never return before the
        # apply step runs — a synced manifest must still reach the gated
        # apply/refusal handling below) ---
        existing = load_existing_manifest()
        needs_write = False
        if existing is not None:
            diffs = diff_manifests(existing, computed)
            if diffs:
                print(
                    "MANIFEST IS STALE — DDLs differ from committed "
                    "schema_manifest.json:"
                )
                for d in diffs:
                    print(d)
                if not args.force:
                    print(
                        "Re-run with --force to regenerate schema_manifest.json "
                        "and accept these changes."
                    )
                    return 5
                print("--force: regenerating schema_manifest.json despite drift.")
                needs_write = True
            else:
                print("Manifest is current; no DDL drift detected.")
        else:
            needs_write = True

        # Write the (new or force-accepted) manifest.
        if needs_write:
            try:
                with open(MANIFEST_PATH, "w", encoding="utf-8") as fh:
                    json.dump(computed, fh, indent=2)
            except OSError as exc:
                print(f"cannot write {MANIFEST_PATH}: {exc}")
                return 2
            print(f"Wrote {MANIFEST_PATH} ({len(entries)} tables).")
        else:
            print(f"{MANIFEST_PATH} unchanged ({len(entries)} tables).")

        # --- Contract check on the final on-disk manifest (post-regen):
        # every entry must carry a compatibility class (foundation L848).
        final_manifest = load_existing_manifest()
        missing_compat = [
            e.get("table_name", "?")
            for e in (final_manifest or {}).get("tables", [])
            if not e.get("compatibility_class")
        ]
        if missing_compat:
            print(
                "MANIFEST CONTRACT VIOLATION: entries missing "
                f"compatibility_class: {missing_compat}"
            )
            return 2

        # --- Gated apply (R-014): runs, or is refused, regardless of drift
        # state. A caller that passes --apply-verified in the synced state
        # must reach this code, never a silent early return. ---
        if args.apply_verified:
            if not args.matrix_evidence:
                print(
                    "REFUSED: --apply-verified requires --matrix-evidence "
                    "(capability tests)."
                )
                return 4
            if not os.path.isfile(args.matrix_evidence):
                print(f"REFUSED: --matrix-evidence file not found: {args.matrix_evidence}")
                return 4
            return run_apply_tool(versions, args.matrix_evidence)

        print(
            "DDL NOT APPLIED: run with --apply-verified + --matrix-evidence to "
            "execute the 9-step application contract (see 02-schema-storage.md)."
        )
        return 0
    except (RuntimeError, KeyError, TypeError, UnicodeDecodeError) as exc:
        # R-147: malformed manifests, non-UTF-8 DDLs, and structurally-wrong
        # entries must surface as the clear DDL-contract diagnostic instead of
        # a raw traceback from a safety-gate script.
        print(f"DDL contract error: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
