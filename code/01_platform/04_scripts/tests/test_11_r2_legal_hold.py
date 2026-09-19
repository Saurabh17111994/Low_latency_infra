"""E5b.2 — offline tests for r2_legal_hold_check.py (no token, no R2 needed).

Cross-language parity fixtures: every constant below was produced by *actually
running* the Java AuditHashChain on the same fixture (CHG-083 assembly, same day):
  javac AuditHashChain.java ImmutabilityProtocol.java
  java ChainParity  ->  H1/H2/H3, HASH1/HASH2, LINK1/LINK2, ROOT, EMPTY_ROOT
The Python port must reproduce those Java values exactly — that is the
trust anchor for the "hash-chain spot check against AuditHashChain" the plan
asks for; a transcription slip here would silently break chain continuity
against the Java writer.
"""

import hashlib
import importlib.util
import json
import os
import sys
import tempfile

SCRIPTS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SCRIPTS)

# R2_LEGAL_HOLD_SOURCE lets the wave's red run point the suite at the pre-fix
# file (unset in production, unread unless the suite asks for it; the copy needs
# audit_r2.py beside it because the module imports it from its own directory).
_SOURCE = os.environ.get("R2_LEGAL_HOLD_SOURCE")
if _SOURCE:
    _spec = importlib.util.spec_from_file_location("r2_legal_hold_check", _SOURCE)
    lh = importlib.util.module_from_spec(_spec)
    sys.modules["r2_legal_hold_check"] = lh
    _spec.loader.exec_module(lh)
else:
    import r2_legal_hold_check as lh  # noqa: E402

# Java-parity constants (from the real JVM run; see module docstring + CHG-083).
H1 = "cbf7f07b5859c901a6d66590bc32250d7600d29770ffbc5085f6a2e0b19f58de"
H2 = "e52c1fa7a33a3f5b4e4b08b10a6d21085bee9c96e57a98cde123b9dc03c7dfc9"
H3 = "4a4efd5e55362ed79f63e76aaed4fea2bbf08351992bd3a906fb454a1c233523"
HASH1 = "3a82bd87a2485483781f8a427ace52439a84bbfe15084cf28973557ecee25512"
HASH2 = "c94662f66a7ba3bbdc16a1ce38542ec7c39a73e0e277e2ff5f88038de6035e78"
LINK1 = "fa665b6414ec4aadcfe93f25f8335d99ba46a67b8002ab01e07391996c7dc613"
ROOT = "2c62d7caaf769cd4ed3ef2f09a3e9360814453c2436234c4e537aad47b19978a"
EMPTY_ROOT = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def fixture_manifests():
    m1 = lh.manifest_canonical("2025-01-01", "Execution_Audit", "1",
                               [("ev-1", H1), ("ev-2", H2)])
    m2 = lh.manifest_canonical("2025-01-02", "Execution_Audit", "1", [("ev-3", H3)])
    return m1, m2


def test_canonical_round_trip_matches_java():
    m1, m2 = fixture_manifests()
    assert parse_ok(m1)
    assert parse_ok(m2)
    # canonical re-render must equal the input bytes (integrity of the format)
    assert lh.parse_manifest(m1)[0] == "2025-01-01"
    assert lh.parse_manifest(m1)[3] == [("ev-1", H1), ("ev-2", H2)]


def parse_ok(text):
    try:
        lh.parse_manifest(text)
        return True
    except ValueError:
        return False


def test_java_parity_manifest_hashes():
    m1, m2 = fixture_manifests()
    assert lh.manifest_hash(m1) == HASH1
    assert lh.manifest_hash(m2) == HASH2


def test_java_parity_linked_and_root():
    m1, m2 = fixture_manifests()
    assert lh.linked_hashes([m1, m2])[0] == LINK1
    assert lh.linked_hashes([m1, m2])[1] == ROOT
    assert lh.root_hash([m1, m2]) == ROOT
    assert lh.root_hash([]) == EMPTY_ROOT  # sha256("") — Java canonicalHash("")


def test_minimal_hashes_are_sha256_of_event_names():
    assert H1 == hashlib.sha256(b"event-one").hexdigest()
    assert H2 == hashlib.sha256(b"event-two").hexdigest()
    assert H3 == hashlib.sha256(b"event-three").hexdigest()


def test_verify_chain_classifications():
    m1, m2 = fixture_manifests()
    assert lh.verify_chain([m1, m2], ROOT) == "VALID"
    # Java-exact: a null expected root is TAMPERED (unverifiable = unverified)
    assert lh.verify_chain([m1, m2]) == "TAMPERED"
    assert lh.verify_chain([m1, m2], "0" * 64) == "TAMPERED"
    assert lh.verify_chain([m2, m1]) == "BROKEN_LINK"  # dates decrease
    tampered = m1.replace(H2, hashlib.sha256(b"event-two-x").hexdigest())
    assert lh.verify_chain([tampered, m2], ROOT) == "TAMPERED"
    dup = lh.manifest_canonical("2025-01-03", "Execution_Audit", "1", [("ev-1", H1)])
    assert lh.verify_chain([m1, m2, dup]) == "DUPLICATE_EVENT"


def test_parse_rejects_corrupt_object():
    m1, _ = fixture_manifests()
    corrupt = m1.replace("count=2", "count=3")
    assert not parse_ok(corrupt)  # canonical round-trip mismatch
    assert not parse_ok("not-a-manifest\n")
    assert not parse_ok(m1.replace("table=Execution_Audit\n", ""))


def test_chain_dir_check_offline():
    m1, m2 = fixture_manifests()
    with tempfile.TemporaryDirectory() as tmp:
        for name, text in (("2025-01-01.manifest", m1), ("2025-01-02.manifest", m2)):
            with open(os.path.join(tmp, name), "w", encoding="utf-8") as fh:
                fh.write(text)
        res = lh.chain_dir_check(tmp, expected_root=ROOT)
        assert res["chain"] == "VALID"
        assert res["root"] == ROOT
        # no published root -> structurally valid but UNVERIFIED-ROOT (not FAIL)
        res0 = lh.chain_dir_check(tmp)
        assert res0["chain"] == "UNVERIFIED-ROOT"
        # wrong expected root -> TAMPERED
        res2 = lh.chain_dir_check(tmp, "0" * 64)
        assert res2["chain"] == "TAMPERED"


def test_cli_chain_dir_and_missing_validate():
    m1, m2 = fixture_manifests()
    with tempfile.TemporaryDirectory() as tmp:
        for name, text in (("2025-01-01.manifest", m1), ("2025-01-02.manifest", m2)):
            with open(os.path.join(tmp, name), "w", encoding="utf-8") as fh:
                fh.write(text)
        assert lh.main(["--chain-dir", tmp]) == 0
        assert lh.main(["--chain-dir", tmp, "--expected-root", "0" * 64]) == 1
    # --validate without a token: exit 2 (E5b.1 blocked) — no network touched
    rc = lh.main(["--validate", "--env-file", "/nonexistent.env"])
    assert rc == 2


def test_bucket_lock_rule_classification():
    indefinite = {"id": "audit-worm-indefinite", "enabled": True,
                  "prefix": "audit/", "condition": {"type": "Indefinite"}}
    assert lh.rule_retains_indefinitely(indefinite)
    assert lh.rule_covers(indefinite, "audit/")
    assert lh.rule_covers({"prefix": ""}, "audit/")  # bucket-wide
    assert not lh.rule_covers({"prefix": "other/"}, "audit/")
    assert not lh.rule_retains_indefinitely(
        {"enabled": True, "prefix": "audit/", "condition": {"type": "Date",
         "date": "2025-01-01"}})
    # a year-out Date guard passes the one-year retention test
    # P6-822: year+2 on Feb 29 raises ValueError (2030 is not a leap year), and two
    # today() calls can straddle a year boundary. Anchor to Jan 1 of the target year.
    far = lh._dt.date(lh._dt.date.today().year + 2, 1, 1)
    ruled = {"condition": {"type": "Date", "date": far.isoformat()}}
    assert lh.rule_retains_one_year(ruled)


def test_cli_self_check_exit_zero_writes_evidence():
    with tempfile.TemporaryDirectory() as out:
        assert lh.main(["--self-check", "--out", out]) == 0
        files = os.listdir(out)
        assert any(f.endswith("-r2-legal-hold-evidence.json") for f in files)
        ev_path = os.path.join(out, [f for f in files
                                     if f.endswith("-r2-legal-hold-evidence.json")][0])
        with open(ev_path, encoding="utf-8") as fh:
            evidence = json.load(fh)
        assert evidence["result"] == "PASS"


# --- Wave 11: pagination, single-fetch sampling, per-table sample, P6-654 -----

def _xml_page(keys, truncated, token=None):
    """A ListObjectsV2 page with no namespace — R2 replies without one."""
    items = "".join(f"<Contents><Key>{k}</Key></Contents>" for k in keys)
    tok = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    flag = {True: "true", False: "false", None: ""}[truncated]
    body = items
    if flag:
        body += f"<IsTruncated>{flag}</IsTruncated>"
    return f"<ListBucketResult>{body}{tok}</ListBucketResult>"


def _expect_value_error(fn, needle):
    try:
        fn()
    except ValueError as exc:
        assert needle in str(exc), f"expected {needle!r} in {exc}"
        return
    raise AssertionError(f"expected ValueError containing {needle!r}")


def _chain_order(keys):
    """The order check_retrieval_and_chain links in: (date, table)."""
    return sorted(keys, key=lambda k: (k.rsplit("/", 1)[-1][: -len(".manifest")],
                                       k.split("/")[-2]))


class StubClient:
    """R2Client stand-in: replays listing pages, serves object bodies."""

    def __init__(self, pages, bodies, root_body=None):
        self.pages = list(pages)
        self.bodies = dict(bodies)
        self.root_body = root_body
        self.queries = []
        self.gets = []

    def _bucket_resource(self):
        return "/audit"

    def _request(self, method, resource, query=None, body=b""):
        self.queries.append(dict(query or {}))
        return 200, {}, self.pages.pop(0).encode("utf-8")

    def get_object(self, key, version_id=None):
        self.gets.append(key)
        if key.endswith("_root.txt"):
            if self.root_body is None:
                raise lh.audit_r2.R2Error("NoSuchKey", "absent", 404)
            return self.root_body.encode("utf-8")
        return self.bodies[key].encode("utf-8")


def test_listing_paginates_until_it_is_complete():
    m1, m2 = fixture_manifests()
    keys = ["audit/Execution_Audit/2025-01-01.manifest",
            "audit/Execution_Audit/2025-01-02.manifest"]
    client = StubClient(
        pages=[_xml_page([keys[0]], True, "TOK"),
               _xml_page([keys[1]], False)],
        bodies=dict(zip(keys, (m1, m2))),
        root_body=ROOT,
    )
    result = lh.check_retrieval_and_chain(client, "audit/")
    assert result["retrieved"] == 2, "the second page must not be dropped"
    assert result["chain"] == "VALID"
    assert not client.queries[0].get("continuation-token")
    assert client.queries[1]["continuation-token"] == "TOK"


def test_truncated_listing_without_a_token_is_refused():
    client = StubClient(pages=[_xml_page(["a.manifest"], True)], bodies={})
    _expect_value_error(lambda: lh.list_objects(client, "audit/"),
                        "no NextContinuationToken")


def test_listing_without_a_completion_flag_is_refused():
    client = StubClient(pages=[_xml_page(["a.manifest"], None)], bodies={})
    _expect_value_error(lambda: lh.list_objects(client, "audit/"),
                        "no IsTruncated flag")


def test_each_sampled_object_is_read_exactly_once():
    m1, m2 = fixture_manifests()
    keys = ["audit/Execution_Audit/2025-01-01.manifest",
            "audit/Execution_Audit/2025-01-02.manifest"]
    client = StubClient(pages=[_xml_page(keys, False)],
                        bodies=dict(zip(keys, (m1, m2))), root_body=ROOT)
    result = lh.check_retrieval_and_chain(client, "audit/")
    manifests = [k for k in client.gets if k.endswith(".manifest")]
    assert manifests == _chain_order(keys), "hashed bytes == linked bytes, one read"
    assert result["chain"] == "VALID"
    assert result["sample_order_note"] == ""  # single table


def test_sample_covers_every_table_not_just_the_last_one_alphabetically():
    # P6-771: keys sort by table name first, so `keys[-sample:]` only ever
    # sampled Zeta and never checked Alpha's or Beta's newest manifests.
    tables = {"Alpha": "2025-01-05", "Beta": "2024-12-31", "Zeta": "2025-01-06"}
    bodies = {}
    for table, date in tables.items():
        bodies[f"audit/{table}/{date}.manifest"] = lh.manifest_canonical(
            date, table, "1", [(f"ev-{table}", H1)])
    client = StubClient(pages=[_xml_page(sorted(bodies), False)], bodies=bodies,
                        root_body=lh.root_hash(
                            [bodies[k] for k in _chain_order(bodies)]))
    result = lh.check_retrieval_and_chain(client, "audit/", sample=3)
    assert result["tables"] == ["Alpha", "Beta", "Zeta"]
    assert result["chain"] == "VALID", "links follow (date, table) order"
    assert "date, table" in result["sample_order_note"]


def test_same_day_manifests_from_different_tables_are_a_valid_chain():
    # P6-654: one manifest per table per day is the documented object contract,
    # so equal dates are a real chain — only going backwards is broken.
    m1, m2 = fixture_manifests()
    same_day = lh.manifest_canonical("2025-01-01", "Ledger_Audit", "1", [("ev-4", H3)])
    texts = [m1, same_day, m2]
    assert lh.verify_chain(texts, lh.root_hash(texts)) == "VALID"
    assert lh._structural_state(texts) == "VALID"
    assert lh.verify_chain([m2, m1], lh.root_hash([m2, m1])) == "BROKEN_LINK"


def test_manifest_count_mismatch_names_the_field():
    # P6-770: the round-trip catches it, but "corrupt object" does not say which
    # field disagrees, and `count` was never compared with the events at all.
    m1, _ = fixture_manifests()
    _expect_value_error(lambda: lh.parse_manifest(m1.replace("count=2", "count=3")),
                        "count=3 but 2 event(s) are present")
    _expect_value_error(lambda: lh.parse_manifest(m1.replace("count=2\n", "")),
                        "manifest missing count")
