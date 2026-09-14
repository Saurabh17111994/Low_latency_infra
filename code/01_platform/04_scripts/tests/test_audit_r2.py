#!/usr/bin/env python3
"""Unit tests for audit_r2.py (stdlib unittest — no third-party deps).

Run: python3 -m unittest discover -s code/01_platform/04_scripts/tests -v
"""
import datetime
import importlib.util
import os
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock

SCRIPTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, SCRIPTS)

# AUDIT_R2_SOURCE lets the wave's red run point the suite at the pre-fix file
# (unset in production, and unread unless the suite itself asks for it).
_SOURCE = os.environ.get("AUDIT_R2_SOURCE")
if _SOURCE:
    _spec = importlib.util.spec_from_file_location("audit_r2", _SOURCE)
    audit_r2 = importlib.util.module_from_spec(_spec)
    sys.modules["audit_r2"] = audit_r2
    _spec.loader.exec_module(audit_r2)
else:
    import audit_r2  # noqa: E402

CONFIG = {
    "endpoint": "https://acct.r2.cloudflarestorage.com",
    "bucket": "audit",
    "region": "auto",
    "access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
}

UTC = datetime.datetime(2026, 8, 14, 12, 0, 0, tzinfo=datetime.timezone.utc)

# AWS SigV4 golden vector (AWS docs, "Signature Calculations for the
# Authorization Header: Transferring Payload in a Single Chunk"): GET
# https://examplebucket.s3.amazonaws.com/test.txt, region us-east-1,
# date 20130524T000000Z.
GOLDEN_AMZ_DATE = "20130524T000000Z"
GOLDEN_DATE_STAMP = "20130524"
GOLDEN_REGION = "us-east-1"
GOLDEN_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
GOLDEN_PAYLOAD_HASH = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
GOLDEN_SIGNATURE = "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"


class FakeClient:
    """In-memory S3-compatible stand-in exposing the surface audit_r2 uses.
    Mirrors R2 behavior: no S3 versioning/versionId surface, and optionally
    rejects PutBucketVersioning with NotImplemented."""

    def __init__(self, object_lock_unsupported=True, lifecycle_unsupported=False,
                 versioning_unsupported=False, io_fail=False):
        self.created = False
        self.versioning = ""
        self.lifecycle = None
        self.object_lock_unsupported = object_lock_unsupported
        self.lifecycle_unsupported = lifecycle_unsupported
        self.versioning_unsupported = versioning_unsupported
        self.io_fail = io_fail
        self.objects = {}  # key -> body

    def bucket_exists(self):
        return self.created

    def create_bucket(self):
        self.created = True

    def get_bucket_versioning(self):
        return self.versioning

    def put_bucket_versioning(self, enabled=True):
        if self.versioning_unsupported:
            raise audit_r2.R2Error("NotImplemented", "PutBucketVersioning not implemented", 501)
        self.versioning = "Enabled" if enabled else "Suspended"

    def get_object_lock_configuration(self):
        if self.object_lock_unsupported:
            raise audit_r2.UnsupportedFeature("S3 Object Lock configuration not set on this endpoint (ObjectLockConfigurationNotFoundError)")
        return "<ObjectLockConfiguration/>"

    def get_bucket_lifecycle(self):
        return self.lifecycle

    def put_bucket_lifecycle(self, xml):
        if self.lifecycle_unsupported:
            raise audit_r2.UnsupportedFeature("NotImplemented: lifecycle unsupported")
        self.lifecycle = xml

    def put_object(self, key, body):
        if self.io_fail:
            raise audit_r2.R2Error("InternalError", "io failure", 500)
        self.objects[key] = body
        return None

    def get_object(self, key, version_id=None):
        return self.objects.get(key)

    def delete_object(self, key, version_id=None):
        self.objects.pop(key, None)


class SigV4Test(unittest.TestCase):
    def test_golden_aws_vector(self):
        headers = {
            "host": "examplebucket.s3.amazonaws.com",
            "range": "bytes=0-9",
            "x-amz-content-sha256": GOLDEN_PAYLOAD_HASH,
            "x-amz-date": GOLDEN_AMZ_DATE,
        }
        block, signed = audit_r2.build_canonical_headers(headers)
        self.assertEqual(signed, "host;range;x-amz-content-sha256;x-amz-date")
        creq = audit_r2.canonical_request("GET", "/test.txt", "", block, signed,
                                          GOLDEN_PAYLOAD_HASH)
        sts = audit_r2.string_to_sign(GOLDEN_AMZ_DATE, GOLDEN_DATE_STAMP,
                                      GOLDEN_REGION, creq)
        signature = audit_r2.sign(GOLDEN_SECRET, GOLDEN_DATE_STAMP,
                                  GOLDEN_REGION, sts)
        self.assertEqual(signature, GOLDEN_SIGNATURE)

    def test_signing_is_deterministic(self):
        headers = {"host": "b.example", "x-amz-content-sha256": "h",
                   "x-amz-date": "T"}
        block, signed = audit_r2.build_canonical_headers(headers)
        creq = audit_r2.canonical_request("GET", "/b", "", block, signed, "h")
        sts = audit_r2.string_to_sign("20130524T000000Z", "20130524",
                                      "auto", creq)
        a = audit_r2.sign("secret", "20130524", "auto", sts)
        b = audit_r2.sign("secret", "20130524", "auto", sts)
        self.assertEqual(a, b)

    def test_canonical_headers_are_sorted_and_lowercased(self):
        headers = {"X-Amz-Date": "T", "host": "h",
                   "x-amz-content-sha256": "hash"}
        block, signed = audit_r2.build_canonical_headers(headers)
        self.assertEqual(signed, "host;x-amz-content-sha256;x-amz-date")
        self.assertTrue(block.startswith("host:h\nx-amz-content-sha256:hash\nx-amz-date:T\n"))

    def test_object_key_keeps_slashes_unencoded(self):
        # R2 canonicalizes the path by decoding %2F — keys must quote with
        # safe="/" (verified live 2026-08-14).
        client = audit_r2.R2Client(CONFIG)
        self.assertEqual(client._object_resource("_audit_probe/run/obj"),
                         "/audit/_audit_probe/run/obj")


class ConfigTest(unittest.TestCase):
    def test_config_requires_all_keys(self):
        with self.assertRaises(audit_r2.ConfigError):
            audit_r2.config_from({"R2_ENDPOINT": "https://x", "R2_BUCKET": "b"})

    def test_config_parses_and_strips_endpoint_slash(self):
        cfg = audit_r2.config_from({
            "R2_ENDPOINT": "https://acct.r2.cloudflarestorage.com/",
            "R2_BUCKET": "audit",
            "AWS_REGION": "auto",
            "AWS_ACCESS_KEY_ID": "AKIA",
            "AWS_SECRET_ACCESS_KEY": "SECRET",
        })
        self.assertEqual(cfg["endpoint"], "https://acct.r2.cloudflarestorage.com")
        self.assertEqual(cfg["bucket"], "audit")
        self.assertEqual(cfg["region"], "auto")

    def test_cloudflare_lock_config_requires_token_and_account(self):
        self.assertIsNone(audit_r2.cloudflare_lock_config({"R2_BUCKET": "b"}))
        self.assertIsNone(audit_r2.cloudflare_lock_config(
            {"R2_BUCKET": "b", "CLOUDFLARE_API_TOKEN": "t"}))
        cfg = audit_r2.cloudflare_lock_config(
            {"R2_BUCKET": "b", "CLOUDFLARE_API_TOKEN": "t",
             "CLOUDFLARE_ACCOUNT_ID": "a"})
        self.assertEqual(cfg["account_id"], "a")
        self.assertEqual(cfg["bucket"], "b")

    def test_env_file_loading(self):
        with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
            fh.write("# comment\n\nR2_BUCKET=audit-bucket\n"
                     "R2_ENDPOINT=\"https://x.example\"\nAWS_REGION=auto\n")
            path = fh.name
        try:
            cfg = audit_r2.load_env_file(path)
        finally:
            os.unlink(path)
        self.assertEqual(cfg["R2_BUCKET"], "audit-bucket")
        self.assertEqual(cfg["R2_ENDPOINT"], "https://x.example")
        self.assertEqual(cfg["AWS_REGION"], "auto")

    def test_missing_env_file_is_empty(self):
        self.assertEqual(audit_r2.load_env_file("/nonexistent/.env"), {})

    def test_mask_secret(self):
        self.assertEqual(audit_r2.mask_secret("AKIAIOSFODNN7EXAMPLE"), "AKIA...MPLE")
        self.assertEqual(audit_r2.mask_secret(""), "")
        self.assertEqual(audit_r2.mask_secret("abcd"), "****")


class ProvisionTest(unittest.TestCase):
    def test_provision_creates_bucket_and_enables_versioning(self):
        client = FakeClient()
        steps = audit_r2.provision(CONFIG, client)
        self.assertTrue(client.created)
        self.assertEqual(client.versioning, "Enabled")
        self.assertIn(("bucket", "CREATED"), steps)
        self.assertIn(("versioning", "ENABLED"), steps)

    def test_provision_is_idempotent(self):
        client = FakeClient()
        client.created = True
        audit_r2.provision(CONFIG, client)
        self.assertTrue(client.created)
        self.assertEqual(client.versioning, "Enabled")

    def test_provision_records_unsupported_lifecycle(self):
        client = FakeClient(lifecycle_unsupported=True)
        steps = audit_r2.provision(CONFIG, client)
        self.assertTrue(any(s[0] == "lifecycle" and s[1].startswith("UNSUPPORTED")
                            for s in steps))

    def test_provision_preserves_existing_lifecycle(self):
        client = FakeClient()
        client.lifecycle = b"<LifecycleConfiguration><Rule>existing</Rule></LifecycleConfiguration>"
        steps = audit_r2.provision(CONFIG, client)
        self.assertTrue(any(s[0] == "lifecycle" and s[1].startswith("EXISTS")
                            for s in steps))
        self.assertIn(b"existing", client.lifecycle)

    def test_provision_records_versioning_manual_when_s3_api_unimplemented(self):
        client = FakeClient(versioning_unsupported=True)
        steps = audit_r2.provision(CONFIG, client)
        self.assertTrue(any(s[0] == "versioning" and s[1].startswith("NOT_SET")
                            for s in steps))

    def test_provision_does_not_set_bucket_lock_without_cf_config(self):
        client = FakeClient()
        steps = audit_r2.provision(CONFIG, client)
        lock_steps = [s for s in steps if s[0] == "bucket_lock"]
        self.assertEqual(len(lock_steps), 1)
        self.assertTrue(lock_steps[0][1].startswith("NOT_SET"))


class ProvisionLockTest(unittest.TestCase):
    CF = {"token": "t", "account_id": "a", "bucket": "b"}

    def test_indefinite_lock_rule_shape(self):
        rule = audit_r2.indefinite_lock_rule("audit")
        self.assertEqual(rule, {"id": "audit-worm-indefinite", "enabled": True,
                                "prefix": "audit/", "condition": {"type": "Indefinite"}})

    def test_reads_locks_without_set_flag(self):
        client = FakeClient()
        with mock.patch.object(audit_r2, "cf_get_bucket_lock",
                               return_value={"result": {"rules": [{"id": "other"}]}}) as get, \
             mock.patch.object(audit_r2, "cf_put_bucket_lock") as put:
            steps = audit_r2.provision(CONFIG, client, self.CF, set_lock=False)
        get.assert_called_once()
        put.assert_not_called()
        lock_steps = [s for s in steps if s[0] == "bucket_lock"]
        self.assertIn("read (not modified)", lock_steps[0][1])

    def test_sets_indefinite_lock_when_flagged(self):
        client = FakeClient()
        with mock.patch.object(audit_r2, "cf_get_bucket_lock",
                               return_value={"result": {"rules": []}}), \
             mock.patch.object(audit_r2, "cf_put_bucket_lock") as put:
            steps = audit_r2.provision(CONFIG, client, self.CF, set_lock=True)
        put.assert_called_once()
        rules = put.call_args[0][1]
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["id"], "audit-worm-indefinite")
        self.assertEqual(rules[0]["condition"], {"type": "Indefinite"})
        lock_steps = [s for s in steps if s[0] == "bucket_lock"]
        self.assertTrue(lock_steps[0][1].startswith("SET audit-worm-indefinite"))

    def test_merges_existing_rules(self):
        client = FakeClient()
        existing = [{"id": "other-rule", "enabled": True, "prefix": "logs/",
                     "condition": {"type": "Indefinite"}}]
        with mock.patch.object(audit_r2, "cf_get_bucket_lock",
                               return_value={"result": {"rules": existing}}), \
             mock.patch.object(audit_r2, "cf_put_bucket_lock") as put:
            audit_r2.provision(CONFIG, client, self.CF, set_lock=True)
        rules = put.call_args[0][1]
        self.assertEqual([r["id"] for r in rules],
                         ["other-rule", "audit-worm-indefinite"])

    def test_skips_when_rule_already_present(self):
        client = FakeClient()
        rule = audit_r2.indefinite_lock_rule("audit/")
        with mock.patch.object(audit_r2, "cf_get_bucket_lock",
                               return_value={"result": {"rules": [rule]}}), \
             mock.patch.object(audit_r2, "cf_put_bucket_lock") as put:
            steps = audit_r2.provision(CONFIG, client, self.CF, set_lock=True)
        put.assert_not_called()
        lock_steps = [s for s in steps if s[0] == "bucket_lock"]
        self.assertIn("already present", lock_steps[0][1])

    def test_set_lock_without_token_errors(self):
        with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
            fh.write("R2_ENDPOINT=https://acct.r2.cloudflarestorage.com\n"
                     "R2_BUCKET=audit\nAWS_REGION=auto\n"
                     "AWS_ACCESS_KEY_ID=AKIA\nAWS_SECRET_ACCESS_KEY=S\n")
            path = fh.name
        try:
            rc = audit_r2.main(["provision", "--set-lock", "--env-file", path])
        finally:
            os.unlink(path)
        self.assertEqual(rc, 2)


class ValidateTest(unittest.TestCase):
    def _green_bucket(self):
        client = FakeClient()
        client.created = True
        return client

    def test_validate_marks_s3_object_lock_not_set(self):
        client = self._green_bucket()
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["object_lock"], "NOT_SET")
        self.assertIn("bucket locks", evidence["checks"]["object_lock_note"])
        self.assertEqual(evidence["checks"]["object_io_probe"], "PASS")
        self.assertEqual(evidence["result"], "PASS")

    def test_validate_records_versioning_not_enabled_via_s3(self):
        client = self._green_bucket()
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["versioning"], "NOT_ENABLED_VIA_S3")
        self.assertIn("PutBucketVersioning", evidence["checks"]["versioning_note"])
        # Not enabled via the S3 API is a limitation note, not a failure.
        self.assertEqual(evidence["result"], "PASS")

    def test_validate_fails_when_object_io_probe_fails(self):
        client = FakeClient(io_fail=True)
        client.created = True
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["object_io_probe"], "FAIL")
        self.assertEqual(evidence["result"], "FAIL")

    def test_validate_purges_probe_objects(self):
        client = self._green_bucket()
        audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(client.objects, {})

    def test_validate_records_bucket_lock_not_checked(self):
        client = self._green_bucket()
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["bucket_lock"], "NOT_CHECKED")
        self.assertIn("CLOUDFLARE_API_TOKEN", evidence["checks"]["bucket_lock_note"])

    def test_evidence_record_has_evidence_fields(self):
        client = self._green_bucket()
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        for field in ("work_item_id", "requirement_ids", "artifact", "version",
                      "environment", "workload", "clock", "result", "owner",
                      "date", "limitations"):
            self.assertIn(field, evidence)
        self.assertEqual(evidence["clock"], "UTC")
        self.assertEqual(evidence["workload"], "probe objects only; no production data touched")

    def test_provision_then_validate_green_path(self):
        client = FakeClient()
        audit_r2.provision(CONFIG, client)
        evidence = audit_r2.validate(CONFIG, client, "run-2", UTC)
        self.assertEqual(evidence["result"], "PASS")
        self.assertEqual(evidence["checks"]["bucket_exists"], "PASS")


class ProbeCleanupTest(unittest.TestCase):
    """P6-307 — cleanup is a claim about what happened, not a constant."""

    class _BadDeleteClient(FakeClient):
        def __init__(self, code="AccessDenied", message="denied"):
            super().__init__()
            self.created = True
            self.code, self.message = code, message
            self.deleted = []

        def delete_object(self, key, version_id=None):
            self.deleted.append(key)
            raise audit_r2.R2Error(self.code, self.message, 403)

    def test_failed_delete_is_reported_not_attested_as_clean(self):
        client = self._BadDeleteClient()
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["object_io_probe"], "PASS")
        self.assertEqual(evidence["checks"]["probe_cleanup"], "FAIL")
        self.assertIn("could NOT be deleted",
                      evidence["checks"]["probe_cleanup_note"])
        self.assertIn("run-1", client.deleted[0])  # the probe key, not another

    def test_missing_probe_object_is_not_a_leftover(self):
        client = self._BadDeleteClient(code="NoSuchKey", message="absent")
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["probe_cleanup"], "PASS")
        self.assertNotIn("probe_cleanup_note", evidence["checks"])

    def test_cleanup_is_attempted_even_when_the_probe_read_fails(self):
        client = FakeClient()
        client.created = True
        client.get_object = lambda key, version_id=None: b"not-what-we-wrote"
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["object_io_probe"], "FAIL")
        self.assertEqual(evidence["checks"]["probe_cleanup"], "PASS")
        self.assertEqual(client.objects, {}, "the probe object must be deleted")

    def test_put_failure_leaves_nothing_and_still_reports_clean(self):
        client = FakeClient(io_fail=True)
        client.created = True
        evidence = audit_r2.validate(CONFIG, client, "run-1", UTC)
        self.assertEqual(evidence["checks"]["object_io_probe"], "FAIL")
        self.assertEqual(evidence["checks"]["probe_cleanup"], "PASS")


class EnvOverlayTest(unittest.TestCase):
    """P6-308 — the documented contract is real environment over the file."""

    def _merge(self, file_text, environ):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, ".env")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(file_text)
            with mock.patch.dict(os.environ, environ):
                return audit_r2.merge_env(path)

    def test_environment_wins_over_the_file(self):
        env = self._merge("R2_BUCKET=from-file\n", {"R2_BUCKET": "from-env"})
        self.assertEqual(env["R2_BUCKET"], "from-env")

    def test_file_value_is_used_when_the_shell_has_none(self):
        # clear=True: the developer's own shell must not be able to decide this
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, ".env")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("R2_BUCKET=from-file\n")
            with mock.patch.dict(os.environ, {}, clear=True):
                self.assertEqual(audit_r2.merge_env(path)["R2_BUCKET"], "from-file")

    def test_cloudflare_vars_from_the_shell_reach_the_lock_config(self):
        # These two are the ones the old REQUIRED_KEYS-only overlay dropped, so
        # `provision --set-lock` refused with "missing credentials" while both
        # were exported, and validate recorded bucket_lock=NOT_CHECKED.
        env = self._merge("", {"CLOUDFLARE_API_TOKEN": "tok",
                               "CLOUDFLARE_ACCOUNT_ID": "acct",
                               "R2_BUCKET": "audit"})
        self.assertEqual(
            audit_r2.cloudflare_lock_config(env),
            {"token": "tok", "account_id": "acct", "bucket": "audit"},
        )


class CloudflareTransportTest(unittest.TestCase):
    """P6-709 — transport failures degrade into the evidence, not a traceback."""

    CFG = {"token": "tok", "account_id": "acct", "bucket": "audit"}

    def test_unreachable_api_is_an_unsupported_feature(self):
        with mock.patch("urllib.request.urlopen",
                        side_effect=urllib.error.URLError("dns failure")):
            with self.assertRaises(audit_r2.UnsupportedFeature):
                audit_r2.cf_get_bucket_lock(self.CFG)
            with self.assertRaises(audit_r2.UnsupportedFeature):
                audit_r2.cf_put_bucket_lock(self.CFG, [])

    def test_timeout_is_an_unsupported_feature(self):
        with mock.patch("urllib.request.urlopen", side_effect=TimeoutError("idle")):
            with self.assertRaises(audit_r2.UnsupportedFeature):
                audit_r2.cf_get_bucket_lock(self.CFG)

    def test_http_error_still_reports_the_api_code(self):
        err = urllib.error.HTTPError("https://api.cloudflare.com/x", 403,
                                     "Forbidden", {}, None)
        err.read = lambda: b'{"errors":[{"code":10000}]}'
        with mock.patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(audit_r2.UnsupportedFeature) as ctx:
                audit_r2.cf_get_bucket_lock(self.CFG)
        self.assertIn("403", str(ctx.exception))

    def test_validate_records_a_transport_failure_as_error(self):
        client = FakeClient()
        client.created = True
        with mock.patch("urllib.request.urlopen",
                        side_effect=urllib.error.URLError("dns failure")):
            evidence = audit_r2.validate(CONFIG, client, "run-1", UTC, self.CFG)
        self.assertEqual(evidence["checks"]["bucket_lock"], "ERROR")
        self.assertIn("bucket-lock GET failed",
                      evidence["checks"]["bucket_lock_note"])


class SignedHeadersTest(unittest.TestCase):
    """P6-710 — every header in the signature must reach the wire."""

    class _Resp:
        status = 200
        headers = {}

        def read(self):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def test_request_signs_exactly_what_it_sends(self):
        import inspect
        params = inspect.signature(audit_r2.R2Client._request).parameters
        self.assertNotIn("extra_headers", params)
        seen = {}

        def fake_urlopen(req, timeout=None):
            seen.update({k.lower(): v for k, v in req.header_items()})
            return self._Resp()

        with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            audit_r2.R2Client(CONFIG)._request("GET", "/audit")
        signed = (seen["authorization"].split("SignedHeaders=")[1]
                  .split(",")[0].split(";"))
        self.assertIn("x-amz-date", signed)
        for name in signed:
            if name == "host":
                # urllib adds Host at send time from the URL, which is built from
                # the same endpoint netloc the signature used.
                continue
            self.assertIn(name, seen, f"{name} is signed but never sent")


if __name__ == "__main__":
    unittest.main()
