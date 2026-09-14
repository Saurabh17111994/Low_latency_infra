#!/usr/bin/env python3
"""r2_legal_hold_check.py — R2 audit legal-hold / immutability verifier (E5b.2).

Verifies the money-path audit store on Cloudflare R2 is retention-protected and
retrievable, and that the retrieved manifest chain matches the policy hash chain
(docs/02_requirements/03-non-functional.md §3.4.1 "Reconstruction integrity";
java AuditHashChain in code/common). Two-layer honesty contract, same spirit as
audit_r2.py:

  1. R2 does NOT implement the S3 Object Lock API — the WORM-equivalent is a
     Cloudflare "bucket lock" rule on the audit prefix (NFR 3.4.1). We verify
     the *active* lock rules via the Cloudflare API, and we record that
     equivalence as a limitation rather than claiming S3 Object Lock.
  2. The hash chain check is a faithful Python port of the Java canonical
     serialization + linking, cross-verified against the actual Java output
     (see CHG-083 / tests/test_11_r2_legal_hold.py for the parity constants).

Three checks per run (plan E5b.2):
  * bucket_lock  — an enabled indefinite (or >= 1-year dated) bucket-lock rule
                   covers every configured audit prefix.
  * retrieval   — list the audit prefix, GET a sample of manifest objects, fold
                   each returned object's SHA-256 into the evidence.
  * hash_chain  — spot-check the sampled chain: parse canonical manifest text,
                   recompute links + root exactly as AuditHashChain.java, and
                   (when a `_root.txt` object exists) compare the expected root.
                   The sample holds the newest manifests of *every* table under
                   the prefix (P6-771) and is linked in (date, table) order.

Multi-table prefixes: `<prefix><table>/<YYYY-MM-DD>.manifest` means one day can
hold one manifest per table, so a chain's dates are NON-DECREASING, not strictly
increasing — the old rule rejected real same-day multi-table chains (P6-654).
The writer's within-day cross-table order is not recorded in any object, so
`(date, table)` is an assumption this checker shares with the writer; it is
noted in the evidence whenever a sample spans more than one table.

Object contract (documented here so the future EOD/offload writer and this
verifier agree): a manifest object is the canonical text of the Java Manifest
(the exact bytes AuditHashChain.Manifest.canonical() produces), stored at
`<audit_prefix><table>/<YYYY-MM-DD>.manifest`. Chain root, when published, is
one line of hex at `<audit_prefix>_root.txt`.

Usage:
  python3 r2_legal_hold_check.py --chain-dir DIR [--expected-root HEX]
      offline: verify local canonical-manifest text files (runs today, no token)
  python3 r2_legal_hold_check.py --validate [--env-file PATH] [--out DIR] [--audit-prefix PREFIX]
      full R2 check — requires E5b.1: R2 creds + CLOUDFLARE_API_TOKEN (exit 2 without)
  python3 r2_legal_hold_check.py --self-check [--out DIR]
      offline fake-cloud demo proving classification incl. a tampered chain -> FAIL

Exit: 0 = pass, 1 = drift detected, 2 = blocked (missing token/creds). Stdlib only.
================================================================================="""

import argparse
import datetime as _dt
import hashlib
import json
import os
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import audit_r2  # noqa: E402 — reuse SigV4 R2Client, CF lock helpers, env loading

REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
)
EVIDENCE_DIR_DEFAULT = os.path.join(REPO_ROOT, "logs", "nautilus-execution")
ENV_FILE_DEFAULT = audit_r2.ENV_FILE_DEFAULT

# One-year minimum audit retention (NFR 3.4.1 / DEC-020). A dated bucket-lock
# rule with condition Date/DateRange must retain at least this long from now.
MIN_RETENTION_DAYS = 365

DEFAULT_AUDIT_PREFIX = "audit/"

# Safety cap on listing pagination: 1000 pages x 1000 keys (P6-499).
MAX_LIST_PAGES = 1000


# ---------------------------------------------------------------------------
# 1. AuditHashChain port (Java-exact — do NOT "improve" the canonical format;
#    any change silently breaks chain continuity vs the Java writer).
# ---------------------------------------------------------------------------

def _sha256_hex(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def manifest_canonical(date_, table, schema, events):
    """Byte-exact port of AuditHashChain.Manifest.canonical()."""
    sb = "manifest-v1\n"
    sb += "date=" + date_ + "\n"
    sb += "table=" + table + "\n"
    sb += "schema=" + schema + "\n"
    sb += "count=" + str(len(events)) + "\n"
    for event_id, content_hash in events:
        sb += "event=" + event_id + ":" + content_hash + "\n"
    return sb


def parse_manifest(text):
    """Parse canonical manifest text back into (date, table, schema, events).
    The canonical round-trip is the integrity check: re-rendering the parsed
    fields must reproduce the input bytes."""
    lines = text.split("\n")
    if not lines or lines[0] != "manifest-v1":
        raise ValueError("not a manifest-v1 canonical object")
    date_ = table = schema = None
    count = None
    events = []
    for line in lines[1:]:
        if not line:
            continue
        key, _, value = line.partition("=")
        if key == "date":
            date_ = value
        elif key == "table":
            table = value
        elif key == "schema":
            schema = value
        elif key == "count":
            try:
                count = int(value)
            except ValueError:
                raise ValueError(f"manifest count is not an integer: {value!r}") from None
        elif key == "event":
            event_id, _, content_hash = value.partition(":")
            events.append((event_id, content_hash))
    if date_ is None or table is None or schema is None:
        raise ValueError("manifest missing date/table/schema")
    if count is None:
        raise ValueError("manifest missing count")
    if count != len(events):
        # Clearer than the round-trip mismatch below, which says "corrupt" but
        # not which field disagrees (P6-770).
        raise ValueError(f"manifest count={count} but {len(events)} event(s) are present")
    rendered = manifest_canonical(date_, table, schema, events)
    if rendered != text:
        raise ValueError("manifest canonical round-trip mismatch (corrupt object)")
    return date_, table, schema, events


def manifest_hash(text):
    """AuditHashChain.Manifest.hash() — SHA-256 of the canonical bytes."""
    return _sha256_hex(text)


def linked_hashes(manifest_texts):
    """AuditHashChain.linkedHashes() — each link commits to the previous."""
    out = []
    previous = ""
    for text in manifest_texts:
        link = _sha256_hex(text + "prev=" + previous)
        out.append(link)
        previous = link
    return out


def root_hash(manifest_texts):
    """AuditHashChain.rootHash() — last link; empty chain -> sha256("")."""
    links = linked_hashes(manifest_texts)
    return links[-1] if links else _sha256_hex("")


def verify_chain(manifest_texts, expected_root=None):
    """Port of AuditHashChain.verifyChain(): VALID / BROKEN_LINK /
    DUPLICATE_EVENT / TAMPERED. Non-decreasing dates first (a day may hold
    several tables' manifests, so equal dates are legitimate — P6-654), then no
    duplicate event id across the chain, then root equality. Java-exact: a null
    expected root is TAMPERED (unverifiable root = unverified integrity). Callers
    that have no published root yet must classify that case as UNVERIFIED-ROOT
    themselves (see check_retrieval_and_chain), never as VALID."""
    parsed = []
    for text in manifest_texts:
        parsed.append(parse_manifest(text))
    for i in range(1, len(parsed)):
        if parsed[i - 1][0] > parsed[i][0]:
            return "BROKEN_LINK"
    seen = set()
    for _, _, _, events in parsed:
        for event_id, _ in events:
            if event_id in seen:
                return "DUPLICATE_EVENT"
            seen.add(event_id)
    if expected_root is None or expected_root != root_hash(manifest_texts):
        return "TAMPERED"
    return "VALID"


def chain_dir_check(dir_path, expected_root=None):
    """Offline: verify all *.manifest files under DIR, in filename order."""
    paths = sorted(
        os.path.join(dir_path, f)
        for f in os.listdir(dir_path)
        if f.endswith(".manifest")
    )
    if not paths:
        raise ValueError(f"no *.manifest objects under {dir_path}")
    texts = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as fh:
            texts.append(fh.read())
    # Java-exact classification when a root is provided; otherwise report the
    # structural state and let the caller record UNVERIFIED-ROOT (root binding
    # requires a published _root.txt — that is a limitation, not a tamper).
    root = root_hash(texts)
    hashes = [manifest_hash(t) for t in texts]
    if expected_root is not None:
        chain = verify_chain(texts, expected_root)  # Java-exact incl. TAMPERED
    else:
        chain = _structural_state(texts)
        if chain == "VALID":
            chain = "UNVERIFIED-ROOT"  # structurally valid; root not yet bound
    return {"chain": chain, "root": root, "manifest_hashes": hashes,
            "objects": [os.path.basename(p) for p in paths]}


# ---------------------------------------------------------------------------
# 2. Bucket-lock rule verification (Cloudflare API)
# ---------------------------------------------------------------------------

def _date_ok(ymd):
    """YYYY-MM-DD (R2 Date/DateRange lock condition) at least a year out."""
    try:
        d = _dt.date.fromisoformat(ymd)
    except ValueError:
        return False
    return (d - _dt.date.today()).days >= MIN_RETENTION_DAYS


def rule_covers(rule, prefix):
    """An R2 bucket-lock rule rule covers prefix when its `prefix` field is
    empty (bucket-wide) or is a prefix of the audit prefix."""
    rule_prefix = (rule.get("prefix") or "").rstrip("/")
    return rule_prefix == "" or prefix.rstrip("/").startswith(rule_prefix)


def rule_retains_indefinitely(rule):
    cond = (rule.get("condition") or {}).get("type", "")
    return cond == "Indefinite"


def rule_retains_one_year(rule):
    cond = rule.get("condition") or {}
    if cond.get("type") == "Date":
        return _date_ok(cond.get("date", ""))
    if cond.get("type") == "DateRange":
        return _date_ok(cond.get("dateEnd", ""))
    return False


def check_bucket_lock(cf_cfg, prefixes):
    """GET the bucket-lock config; PASS when every audit prefix is covered by
    an enabled lock rule that is indefinite or retains >= 1 year."""
    data = audit_r2.cf_get_bucket_lock(cf_cfg)
    rules = (data.get("result") or {}).get("rules") or []
    notes = []
    for prefix in prefixes:
        covering = [
            r for r in rules
            if r.get("enabled") and rule_covers(r, prefix)
        ]
        retained = any(rule_retains_indefinitely(r) or rule_retains_one_year(r)
                       for r in covering)
        if retained:
            notes.append(f"{prefix}: lock rule present ({len(covering)} enabled)")
        else:
            notes.append(
                f"{prefix}: NO enabled indefinite/>=1y lock rule "
                f"({len(covering)} enabled rule(s) covering, none retaining)"
            )
    verdict = "PASS" if all(": lock rule present" in n for n in notes) else "FAIL"
    return verdict, "; ".join(notes)


# ---------------------------------------------------------------------------
# 3. Retrieval + hash-chain spot check (R2 via audit_r2.R2Client)
# ---------------------------------------------------------------------------

def _tags(root, name):
    """Every element whose local name is `name`, at any depth and with or
    without a namespace. R2 replies without the S3 namespace, and `<Key>` lives
    inside `<Contents>` — iterating the root's children matched neither, so the
    old code reported every populated prefix as empty (P6-499)."""
    return [c for c in root.iter() if c.tag.rsplit("}", 1)[-1] == name]


def list_objects(client, prefix):
    """ListObjectsV2 via the already-signed request machinery. Returns every key
    under the prefix: the listing is paginated with the continuation token until
    the response says it is complete (P6-499). A page that does not say whether
    it is complete, or is truncated without a usable token, is an error rather
    than a silent short listing — the same reasoning as r2-list.sh.
    Tag matching is namespace-agnostic: R2 replies without an S3 namespace."""
    keys = []
    next_token = None
    for page in range(MAX_LIST_PAGES):
        query = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if next_token:
            query["continuation-token"] = next_token
        _, _, body = client._request(
            "GET", client._bucket_resource(), query=query,
        )
        root = ET.fromstring(body)
        keys.extend(t.text for t in _tags(root, "Key") if t.text)
        truncated_tag = _tags(root, "IsTruncated")
        truncated = truncated_tag[0].text if truncated_tag else None
        if truncated is None:
            raise ValueError(
                "listing response has no IsTruncated flag — completeness cannot "
                "be established, refusing to continue with a partial listing"
            )
        if truncated.strip().lower() != "true":
            return keys
        tokens = _tags(root, "NextContinuationToken")
        next_token = tokens[0].text if tokens else None
        if not next_token:
            raise ValueError(
                f"listing is truncated after page {page + 1} but carries no "
                "NextContinuationToken — refusing to continue with a partial listing"
            )
    raise ValueError(f"listing exceeded {MAX_LIST_PAGES} pages — refusing to continue")


def _structural_state(manifest_texts):
    """Dates non-decreasing + no duplicate event id — the integrity half that
    does not need a published root. Returns VALID/BROKEN_LINK/DUPLICATE_EVENT."""
    parsed = [parse_manifest(t) for t in manifest_texts]
    for i in range(1, len(parsed)):
        if parsed[i - 1][0] > parsed[i][0]:
            return "BROKEN_LINK"
    seen = set()
    for _, _, _, events in parsed:
        for event_id, _ in events:
            if event_id in seen:
                return "DUPLICATE_EVENT"
            seen.add(event_id)
    return "VALID"


def _key_date(key):
    """`<prefix><table>/<YYYY-MM-DD>.manifest` -> the date part."""
    return key.rsplit("/", 1)[-1][: -len(".manifest")]


def _key_table(key):
    """`<prefix><table>/<YYYY-MM-DD>.manifest` -> the table part."""
    parts = [p for p in key.split("/") if p]
    return parts[-2] if len(parts) >= 2 else ""


def newest_per_table(keys, sample):
    """Newest manifest keys, with at least one from every table.

    Keys are `<table>/<YYYY-MM-DD>.manifest`, so plain lexicographic order is
    dominated by the table name: with several audit tables under the prefix the
    old `keys[-sample:]` sampled only the lexicographically-last table, and the
    newest manifests of the others were never checked (P6-771). Every table
    contributes its newest `sample // n_tables` keys (at least one), and the
    result is ordered by (date, table) — the order the links are computed in."""
    by_table = {}
    for key in keys:
        by_table.setdefault(_key_table(key), []).append(key)
    per_table = max(1, sample // max(1, len(by_table)))
    picked = []
    for table_keys in by_table.values():
        picked.extend(sorted(table_keys, key=_key_date)[-per_table:])
    return sorted(picked, key=lambda k: (_key_date(k), _key_table(k)))


def check_retrieval_and_chain(client, prefix, sample=8):
    """Get a sample of manifest objects, hash them, and spot-check the chain.
    Reads a published `_root.txt` when present and binds the sample root to it.
    Every sampled object is fetched exactly once: the bytes that were hashed and
    the bytes that were linked must be the same read (P6-500).
    Returns: {"retrieved", "objects", "manifest_hashes", "chain_root", "tables",
    "chain": VALID|BROKEN_LINK|DUPLICATE_EVENT|TAMPERED, "root_bound": bool,
    "sample_order_note": str}."""
    keys = [k for k in list_objects(client, prefix) if k.endswith(".manifest")]
    if not keys:
        raise ValueError(
            f"no .manifest objects under prefix {prefix!r} — nothing to spot-check "
            "(EOD/offload writer has not published manifests yet)"
        )
    sample_keys = newest_per_table(keys, sample)
    objects, texts = [], []
    for key in sample_keys:
        body = client.get_object(key)  # exactly one GET per object (P6-500)
        objects.append({"key": key, "sha256": _sha256_hex(body), "bytes": len(body)})
        texts.append(str(body, "utf-8"))
    root = root_hash(texts)
    published = None
    root_bound = False
    try:
        published = str(client.get_object(prefix + "_root.txt"), "utf-8").strip()
        root_bound = bool(published)
    except audit_r2.R2Error:
        root_bound = False
    structural = _structural_state(texts)
    if structural != "VALID":
        chain = structural
    elif root_bound:
        chain = "VALID" if published == root else "TAMPERED"
    else:
        chain = "UNVERIFIED-ROOT"
    return {
        "retrieved": len(objects),
        "objects": objects,
        "manifest_hashes": [manifest_hash(t) for t in texts],
        "chain_root": root,
        "chain": chain,
        "root_bound": root_bound,
        "tables": sorted({_key_table(k) for k in sample_keys}),
        "sample_order_note": _sample_order_note(sample_keys),
    }


def _sample_order_note(sample_keys):
    """Empty for a single-table sample; otherwise states the ordering the links
    assume, so a root mismatch on a multi-table sample is not read as pure
    tampering when it may only be a different cross-table order (P6-771)."""
    tables = sorted({_key_table(k) for k in sample_keys})
    if len(tables) <= 1:
        return ""
    return (
        f"sample spans {len(tables)} tables ({', '.join(tables)}) and was linked "
        "in (date, table) order; the writer's within-day cross-table order is not "
        "recorded in any object, so a root mismatch here can also mean a "
        "different order rather than tampering"
    )


# ---------------------------------------------------------------------------
# 4. Evidence + CLI
# ---------------------------------------------------------------------------

def build_evidence(cfg, checks, run_id, utc_now):
    bucket_ok = checks.get("bucket_lock", ("PASS", ""))[0] == "PASS"
    retrieval = checks.get("retrieval", {})
    retrieval_ok = retrieval.get("chain") in ("VALID", "UNVERIFIED-ROOT")
    chain_ok = checks.get("hash_chain", ("PASS", ""))[0] == "PASS"
    all_pass = bucket_ok and retrieval_ok and chain_ok
    return {
        "work_item_id": f"E5B-LEGALHOLD-{run_id}",
        "requirement_ids": ["NFR 3.4.1", "DEC-020"],
        "artifact": f"logs/nautilus-execution/{run_id}-r2-legal-hold-evidence.json",
        "version": "r2_legal_hold_check.py (stdlib; AuditHashChain parity port)",
        "environment": f"Cloudflare R2 bucket={cfg['bucket']}",
        "workload": "read-only: bucket-lock rules, sampled manifest retrieval, "
                    "chain spot-check; no writes",
        "clock": "UTC",
        "result": "PASS" if all_pass else "FAIL",
        "owner": "Saurabh (DEC-044)",
        "date": utc_now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": checks,
        "limitations": [
            "R2 bucket locks != S3 Object Lock (S3 API returns "
            "ObjectLockConfigurationNotFoundError); WORM-equivalent is an "
            "indefinite/dated Cloudflare bucket-lock rule on the audit prefix — "
            "residual risk documented (E5b.3): a credential compromise with "
            "lock-admin scope could still drop the lock; mitigated by scoped "
            "token + deletion-control review",
            "hash-chain spot-check covers only the sampled manifests + "
            "recomputed root; a published _root.txt binds it to the DEC review "
            "hash when the EOD/offload writer publishes it",
            "the sample holds the newest manifests of every table under the "
            "prefix and is linked in (date, table) order; the writer's "
            "within-day cross-table order is not recorded in any object "
            "(multi-table samples carry a sample_order_note saying so)",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="R2 audit legal-hold verifier (E5b.2)")
    parser.add_argument("--chain-dir", help="offline chain check over *.manifest files")
    parser.add_argument("--expected-root", help="expected chain root hex (chain-dir mode)")
    parser.add_argument("--validate", action="store_true", help="full R2 check")
    parser.add_argument("--env-file", default=ENV_FILE_DEFAULT)
    parser.add_argument("--audit-prefix", default=DEFAULT_AUDIT_PREFIX,
                        help="comma-separated audit prefixes to validate")
    parser.add_argument("--out", default=EVIDENCE_DIR_DEFAULT)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args(argv)

    utc_now = _dt.datetime.now(_dt.timezone.utc)
    run_id = utc_now.strftime("%Y%m%d-%H%M%S")

    if args.self_check:
        return _self_check(args.out, run_id, utc_now)
    if args.chain_dir:
        try:
            res = chain_dir_check(args.chain_dir, args.expected_root)
        except (ValueError, OSError) as exc:
            print(f"chain-dir failure: {exc}", file=sys.stderr)
            return 1
        print(f"chain: {res['chain']}")
        print(f"root : {res['root']}")
        for name, h in zip(res["objects"], res["manifest_hashes"]):
            print(f"  {name:<30} {h}")
        ok = res["chain"] in ("VALID", "UNVERIFIED-ROOT")
        print(f"result: {'PASS' if ok else 'FAIL'}")
        return 0 if ok else 1

    if not args.validate:
        parser.print_help()
        return 2

    env = audit_r2.merge_env(args.env_file)  # real env wins over the file
    cf_cfg = audit_r2.cloudflare_lock_config(env)
    if not cf_cfg:
        print("blocked: CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID required "
              "(E5b.1 token not yet issued) — bucket-lock check cannot run",
              file=sys.stderr)
        return 2

    checks = {}
    try:
        cfg = audit_r2.config_from(env)
        prefixes = [p if p.endswith("/") else p + "/"
                    for p in args.audit_prefix.split(",")]
        checks["bucket_lock"] = check_bucket_lock(cf_cfg, prefixes)
        client = audit_r2.R2Client(cfg)
        checks["retrieval"] = check_retrieval_and_chain(client, prefixes[0])
        chain_ok = checks["retrieval"]["chain"] in ("VALID", "UNVERIFIED-ROOT")
        bound_note = ("; root NOT yet bound to DEC review hash (no _root.txt object)"
                      if checks["retrieval"]["chain"] == "UNVERIFIED-ROOT" else "")
        checks["hash_chain"] = (
            ("PASS", f"chain {checks['retrieval']['chain'][:16]}…, "
                     f"root {checks['retrieval']['chain_root'][:16]}…{bound_note}")
            if chain_ok
            else ("FAIL", f"chain {checks['retrieval']['chain']} — immutable "
                          "integrity broken or sample reordered")
        )
    except (audit_r2.ConfigError, audit_r2.R2Error, audit_r2.UnsupportedFeature,
            ValueError) as exc:
        print(f"validation failure: {exc}", file=sys.stderr)
        return 1

    for name, (verdict, note) in checks.items():
        print(f"{name:<12} {verdict:<4} {note}")
    evidence = build_evidence(cfg, checks, run_id, utc_now)
    os.makedirs(args.out, exist_ok=True)
    path = os.path.join(args.out, f"{run_id}-r2-legal-hold-evidence.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"\nevidence: {path}")
    print(f"result: {evidence['result']}")
    return 0 if evidence["result"] == "PASS" else 1


def _self_check(out_dir, run_id, utc_now):
    """Offline demo (no token, no R2): build a fake canonical chain, verify it
    passes, tamper one object, verify the chain now FAILs. Proves the verifier
    classifies correctly; the Java parity of the port is pinned in the tests."""
    H1 = _sha256_hex("event-one")
    H2 = _sha256_hex("event-two")
    H3 = _sha256_hex("event-three")
    m1 = manifest_canonical("2025-01-01", "Execution_Audit", "1",
                            [("ev-1", H1), ("ev-2", H2)])
    m2 = manifest_canonical("2025-01-02", "Execution_Audit", "1", [("ev-3", H3)])
    texts = [m1, m2]
    assert verify_chain(texts, "2c62d7caaf769cd4ed3ef2f09a3e9360814453c2436234c4e537aad47b19978a") == "VALID"
    assert verify_chain(texts) == "TAMPERED"  # Java-exact: no bound root = unverified
    assert root_hash(texts) == "2c62d7caaf769cd4ed3ef2f09a3e9360814453c2436234c4e537aad47b19978a"
    tampered = m1.replace(H2, _sha256_hex("event-two-x"))
    assert verify_chain([tampered, m2], "2c62d7caaf769cd4ed3ef2f09a3e9360814453c2436234c4e537aad47b19978a") == "TAMPERED"
    # A day may hold one manifest per table, so equal dates are a real chain
    # (P6-654); only going backwards is broken.
    same_day_other_table = manifest_canonical(
        "2025-01-01", "Ledger_Audit", "1", [("ev-4", H3)])
    same_day_texts = [m1, same_day_other_table, m2]
    assert verify_chain(
        same_day_texts, root_hash(same_day_texts)) == "VALID", "P6-654"
    assert verify_chain([m2, m1]) == "BROKEN_LINK"  # dates decreased
    dup = manifest_canonical("2025-01-03", "Execution_Audit", "1", [("ev-1", H1)])
    assert verify_chain([m1, m2, dup]) == "DUPLICATE_EVENT"

    checks = {
        "bucket_lock": ("PASS", "self-check: fake indefinite rule covers audit/"),
        "retrieval": {"retrieved": 2,
                      "objects": [{"key": "audit/Execution_Audit/2025-01-01.manifest",
                                   "sha256": manifest_hash(m1), "bytes": len(m1)}],
                      "manifest_hashes": [manifest_hash(m1), manifest_hash(m2)],
                      "chain_root": root_hash(texts), "chain": "VALID"},
        "hash_chain": ("PASS", "chain VALID (fake cloud)"),
    }
    evidence = build_evidence({"bucket": "audit(self-check)"}, checks, run_id, utc_now)
    assert evidence["result"] == "PASS"
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"self-check-{run_id}-r2-legal-hold-evidence.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print("[self-check] PASS — VALID/TAMPERED/BROKEN_LINK/DUPLICATE_EVENT "
          "classification + root parity confirmed; evidence at " + path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
