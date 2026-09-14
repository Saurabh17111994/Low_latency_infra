#!/usr/bin/env python3
# =============================================================================
# audit_r2.py — Cloudflare R2 audit-store provisioning + validation (stdlib only)
#
# Provisioning (idempotent, non-destructive):
#   - ensure the R2 bucket exists
#   - attempt to enable bucket versioning — R2 does NOT implement the S3
#     PutBucketVersioning API (verified live 2026-08-14), so this is recorded
#     as a manual dashboard/Wrangler/Cloudflare-API step, not a hard failure
#   - apply a lifecycle rule for the probe scratch prefix ONLY — the audit
#     prefix is never expired by this tool, and EXISTING lifecycle rules are
#     never clobbered
#
# Validation produces an evidence record (EvidenceRecord shape, foundation
# docs/08_implementation/01-foundation.md L159) under logs/audit-r2/:
#   - connectivity / bucket existence / versioning / lifecycle
#   - S3 Object Lock API probe: R2 returns ObjectLockConfigurationNotFoundError
#     (no S3-style lock config). R2's WORM-equivalent is "bucket locks" —
#     prefix retention rules (duration / until-date / indefinite) set via the
#     Cloudflare API / dashboard / Wrangler, NOT the S3 Object Lock API. An
#     indefinite bucket-lock rule on the audit prefix satisfies the NFR 3.4.1
#     WORM control on R2.
#   - Cloudflare bucket-lock state (read-only) when CLOUDFLARE_API_TOKEN +
#     CLOUDFLARE_ACCOUNT_ID are present; recorded as NOT_CHECKED otherwise
#   - object I/O probe: put -> get (content verified) -> delete round-trip on
#     _audit_probe/. (R2 does NOT implement ListObjectVersions or the S3
#     versioning/versionId surface, so immutability on R2 is provided by
#     'bucket locks' via the Cloudflare API — not by S3 probing.)
#
# Config: reads code/01_platform/01_docker/.env (R2_ENDPOINT, R2_BUCKET,
# AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY; optional
# CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID for bucket-lock state); real
# environment variables take precedence over the file. Secrets are never printed.
#
# Usage:
#   python3 audit_r2.py provision [--env-file PATH] [--set-lock] [--audit-prefix PREFIX]
#   python3 audit_r2.py validate  [--env-file PATH] [--out DIR]
#
# SigV4 is implemented here (urllib only) — no boto3, no third-party deps.
# =============================================================================
import argparse
import datetime as _dt
import hashlib
import hmac
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import quote

ENV_FILE_DEFAULT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "01_docker", ".env"
)

REQUIRED_KEYS = (
    "R2_ENDPOINT",
    "R2_BUCKET",
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)

ALGORITHM = "AWS4-HMAC-SHA256"
SERVICE = "s3"
CLOUDFLARE_API = "https://api.cloudflare.com/client/v4"

# Lifecycle rule: probe scratch cleanup only. The audit prefix is NEVER expired
# by this tool — audit retention is a policy contract, not a TTL.
SCRATCH_LIFECYCLE_XML = """\
<LifecycleConfiguration>
  <Rule>
    <ID>audit-r2-probe-scratch-cleanup</ID>
    <Filter><Prefix>_audit_probe/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>1</Days></Expiration>
  </Rule>
</LifecycleConfiguration>
"""


class ConfigError(Exception):
    """Missing or malformed R2 configuration."""


class R2Error(Exception):
    """S3-compatible endpoint error (status + <Error><Code>)."""

    def __init__(self, code, message, status):
        super().__init__(f"{status} {code}: {message}")
        self.code = code
        self.status = status


class UnsupportedFeature(Exception):
    """The endpoint rejected a capability (e.g. ListObjectVersions on R2)."""


def _parse_error(status, body):
    """Best-effort S3 <Error><Code>..</Code><Message>..</Message> parse."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return R2Error("Unknown", body[:200], status)
    code = (root.findtext("Code") or "Unknown").strip()
    message = (root.findtext("Message") or "").strip()
    return R2Error(code, message, status)


# ---------------------------------------------------------------------------
# SigV4 (AWS Signature Version 4) — stdlib implementation
# ---------------------------------------------------------------------------
def _hmac(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret_access_key, date_stamp, region):
    k_date = _hmac(("AWS4" + secret_access_key).encode("utf-8"), date_stamp)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, SERVICE)
    return _hmac(k_service, "aws4_request")


def build_canonical_headers(headers):
    """headers: dict of header names -> values. Header names are lowercased
    (SigV4 canonical form) and sorted. Returns
    (canonical_headers_block, signed_header_list)."""
    items = sorted((k.lower(), v.strip()) for k, v in headers.items())
    block = "".join(f"{k}:{v}\n" for k, v in items)
    signed = ";".join(k for k, _ in items)
    return block, signed


def canonical_request(method, canonical_uri, canonical_query, canonical_headers,
                      signed_headers, payload_hash):
    return "\n".join(
        [method, canonical_uri, canonical_query, canonical_headers,
         signed_headers, payload_hash]
    )


def string_to_sign(amz_date, date_stamp, region, canonical_request):
    scope = f"{date_stamp}/{region}/{SERVICE}/aws4_request"
    return "\n".join(
        [ALGORITHM, amz_date, scope,
         hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()]
    )


def sign(secret_access_key, date_stamp, region, string_to_sign):
    key = signing_key(secret_access_key, date_stamp, region)
    return hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


def authorization_header(access_key_id, date_stamp, region, signed_headers, signature):
    scope = f"{date_stamp}/{region}/{SERVICE}/aws4_request"
    return (
        f"{ALGORITHM} Credential={access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )


# ---------------------------------------------------------------------------
# Config + env loading
# ---------------------------------------------------------------------------
def load_env_file(path):
    """Parse a KEY=VALUE env file (comments/blank lines ignored, quotes
    stripped). A missing file yields an empty dict."""
    cfg = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                cfg[key.strip()] = value.strip().strip('"').strip("'")
    except FileNotFoundError:
        return {}
    return cfg


def merge_env(env_file):
    """The recorded config contract: a KEY=VALUE file, with real environment
    variables taking precedence over it. Every key is overlaid, not just the
    five R2 ones — `CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID` exported in
    the shell used to be ignored, so bucket-lock state was recorded NOT_CHECKED
    and `provision --set-lock` failed while both were set (P6-308)."""
    return {**load_env_file(env_file), **os.environ}


def config_from(env):
    missing = [k for k in REQUIRED_KEYS if not env.get(k)]
    if missing:
        raise ConfigError(
            "missing env keys: " + ", ".join(missing)
            + " (R2_ENDPOINT, R2_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID, "
              "AWS_SECRET_ACCESS_KEY — see code/01_platform/01_docker/.env)"
        )
    return {
        "endpoint": env["R2_ENDPOINT"].rstrip("/"),
        "bucket": env["R2_BUCKET"],
        "region": env["AWS_REGION"],
        "access_key_id": env["AWS_ACCESS_KEY_ID"],
        "secret_access_key": env["AWS_SECRET_ACCESS_KEY"],
    }


def cloudflare_lock_config(env):
    """R2 'bucket locks' (the WORM-equivalent) are set via the Cloudflare API,
    not the S3 Object Lock API. Returns the Cloudflare config when the API
    token + account id are present, else None."""
    token = env.get("CLOUDFLARE_API_TOKEN", "")
    account = env.get("CLOUDFLARE_ACCOUNT_ID", "")
    if not token or not account:
        return None
    return {"token": token, "account_id": account, "bucket": env.get("R2_BUCKET", "")}


def mask_secret(value):
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return value[:4] + "..." + value[-4:]


def cf_get_bucket_lock(cfg):
    """Read the R2 bucket-lock configuration via the Cloudflare API."""
    url = (f"{CLOUDFLARE_API}/accounts/{cfg['account_id']}/r2/buckets/"
           f"{cfg['bucket']}/lock")
    req = urllib.request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {cfg['token']}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:300]
        raise UnsupportedFeature(f"Cloudflare API bucket-lock GET failed: {exc.code} {body}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        # Transport failures (DNS/TLS/connect/idle timeout) are recorded as an
        # unsupported-unverifiable check instead of aborting the whole run with
        # a traceback (P6-709).
        raise UnsupportedFeature(f"Cloudflare API bucket-lock GET failed: {exc}") from exc


def cf_put_bucket_lock(cfg, rules):
    """Set the R2 bucket-lock configuration via the Cloudflare API. The PUT
    REPLACES the whole configuration, so callers MUST merge existing rules
    first (see provision --set-lock)."""
    url = (f"{CLOUDFLARE_API}/accounts/{cfg['account_id']}/r2/buckets/"
           f"{cfg['bucket']}/lock")
    body = json.dumps({"rules": rules}).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Authorization", f"Bearer {cfg['token']}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:300]
        raise UnsupportedFeature(f"Cloudflare API bucket-lock PUT failed: {exc.code} {body}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise UnsupportedFeature(f"Cloudflare API bucket-lock PUT failed: {exc}") from exc


def indefinite_lock_rule(prefix):
    """An R2 'bucket lock' rule retaining the audit prefix indefinitely — the
    WORM-equivalent on R2 (NFR 3.4.1)."""
    normalized = prefix if prefix.endswith("/") else prefix + "/"
    return {"id": "audit-worm-indefinite", "enabled": True,
            "prefix": normalized, "condition": {"type": "Indefinite"}}


# ---------------------------------------------------------------------------
# R2Client — minimal S3-compatible client (path-style URLs, SigV4, stdlib only)
# ---------------------------------------------------------------------------
class R2Client:
    def __init__(self, config):
        self.config = config
        self.endpoint = config["endpoint"].rstrip("/")

    def _request(self, method, resource, query=None, body=b""):
        now = _dt.datetime.now(_dt.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest()

        host = urllib.parse.urlsplit(self.endpoint).netloc
        canonical_uri = resource if resource.startswith("/") else "/" + resource
        canonical_query = ""
        if query:
            canonical_query = "&".join(
                f"{k}={quote(str(v), safe='')}" for k, v in sorted(query.items())
            )

        headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        block, signed = build_canonical_headers(headers)

        creq = canonical_request(method, canonical_uri, canonical_query, block,
                                 signed, payload_hash)
        sts = string_to_sign(amz_date, date_stamp, self.config["region"], creq)
        signature = sign(self.config["secret_access_key"], date_stamp,
                         self.config["region"], sts)
        auth = authorization_header(self.config["access_key_id"], date_stamp,
                                    self.config["region"], signed, signature)

        url = self.endpoint + canonical_uri
        if canonical_query:
            url += "?" + canonical_query
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Authorization", auth)
        req.add_header("x-amz-content-sha256", payload_hash)
        req.add_header("x-amz-date", amz_date)
        if body:
            req.add_header("Content-Type", "application/octet-stream")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as exc:
            raise _parse_error(exc.code, exc.read().decode("utf-8", "replace")) from exc

    def _bucket_resource(self):
        return "/" + self.config["bucket"]

    # -- bucket -------------------------------------------------------------
    def bucket_exists(self):
        try:
            status, _, _ = self._request("HEAD", self._bucket_resource())
            return status == 200
        except R2Error as exc:
            if exc.status == 404:
                return False
            raise

    def create_bucket(self):
        """Create the bucket. R2 accepts an empty body; falls back to an
        explicit LocationConstraint=auto when the endpoint requires it."""
        try:
            self._request("PUT", self._bucket_resource())
        except R2Error as exc:
            if exc.code == "LocationConstraintRequired":
                body = (
                    b"<CreateBucketConfiguration>"
                    b"<LocationConstraint>auto</LocationConstraint>"
                    b"</CreateBucketConfiguration>"
                )
                self._request("PUT", self._bucket_resource(), body=body)
                return
            raise

    # -- versioning ---------------------------------------------------------
    def get_bucket_versioning(self):
        _, _, body = self._request("GET", self._bucket_resource(),
                                   query={"versioning": ""})
        root = ET.fromstring(body)
        return root.findtext("Status") or ""

    def put_bucket_versioning(self, enabled=True):
        status_value = "Enabled" if enabled else "Suspended"
        body = (
            f"<VersioningConfiguration><Status>{status_value}</Status>"
            f"</VersioningConfiguration>"
        ).encode("utf-8")
        self._request("PUT", self._bucket_resource(), query={"versioning": ""},
                      body=body)

    # -- lifecycle ----------------------------------------------------------
    def get_bucket_lifecycle(self):
        try:
            _, _, body = self._request("GET", self._bucket_resource(),
                                       query={"lifecycle": ""})
            return body
        except R2Error as exc:
            if exc.code in ("NoSuchLifecycleConfiguration", "NoSuchBucketLifecycle"):
                return None
            if exc.code == "NotImplemented":
                raise UnsupportedFeature(f"lifecycle rules unsupported ({exc.code})") from exc
            raise

    def put_bucket_lifecycle(self, xml):
        try:
            self._request("PUT", self._bucket_resource(), query={"lifecycle": ""},
                          body=xml.encode("utf-8"))
        except R2Error as exc:
            if exc.code in ("NotImplemented", "InvalidRequest", "MethodNotAllowed"):
                raise UnsupportedFeature(f"lifecycle rules unsupported ({exc.code})") from exc
            raise

    # -- object lock (S3 API capability probe) ------------------------------
    def get_object_lock_configuration(self):
        """S3 Object Lock API probe. R2 returns ObjectLockConfigurationNotFoundError
        (no S3-style lock config); R2's WORM-equivalent is 'bucket locks' via the
        Cloudflare API, so this raises UnsupportedFeature with the endpoint code."""
        try:
            _, _, body = self._request("GET", self._bucket_resource(),
                                       query={"object-lock": ""})
            return body
        except R2Error as exc:
            raise UnsupportedFeature(
                f"S3 Object Lock configuration not set on this endpoint ({exc.code})"
            ) from exc

    # -- objects ------------------------------------------------------------
    def _object_resource(self, key):
        # SigV4 canonicalization on R2 decodes %2F in the path, so object keys
        # keep "/" unencoded (quote with safe="/") — verified live 2026-08-14.
        return self._bucket_resource() + "/" + quote(key, safe="/")

    def put_object(self, key, body):
        _, headers, _ = self._request("PUT", self._object_resource(key), body=body)
        return headers.get("x-amz-version-id")

    def get_object(self, key, version_id=None):
        query = {"versionId": version_id} if version_id else None
        _, _, body = self._request("GET", self._object_resource(key), query=query)
        return body

    def delete_object(self, key, version_id=None):
        query = {"versionId": version_id} if version_id else None
        self._request("DELETE", self._object_resource(key), query=query)


# ---------------------------------------------------------------------------
# Provisioning + validation
# ---------------------------------------------------------------------------
def provision(config, client, cf_lock=None, set_lock=False, audit_prefix="audit/"):
    """Idempotent provisioning. Never destructive: existing buckets, versioning
    settings, and lifecycle rules are left untouched. Bucket locks are READ
    always; they are SET only when set_lock is true (the --set-lock flag), and
    even then existing rules are merged, never clobbered."""
    steps = []
    if client.bucket_exists():
        steps.append(("bucket", "EXISTS"))
    else:
        client.create_bucket()
        steps.append(("bucket", "CREATED"))
    try:
        client.put_bucket_versioning(True)
        steps.append(("versioning", "ENABLED"))
    except R2Error as exc:
        if exc.code == "NotImplemented":
            steps.append(("versioning", "NOT_SET — R2 does not implement the S3 "
                                       "PutBucketVersioning API; enable via the Cloudflare "
                                       "dashboard/Wrangler/API"))
        else:
            raise
    try:
        existing = client.get_bucket_lifecycle()
        if existing is not None:
            steps.append(("lifecycle", "EXISTS — scratch rule NOT added (existing rules preserved; merge manually if desired)"))
        else:
            try:
                client.put_bucket_lifecycle(SCRATCH_LIFECYCLE_XML)
                steps.append(("lifecycle", "APPLIED (probe scratch cleanup only; audit prefix untouched)"))
            except UnsupportedFeature as exc:
                steps.append(("lifecycle", f"UNSUPPORTED ({exc})"))
    except UnsupportedFeature as exc:
        steps.append(("lifecycle", f"UNSUPPORTED ({exc})"))
    if cf_lock is not None:
        try:
            body = cf_get_bucket_lock(cf_lock)
            rules = list(body.get("result", {}).get("rules", []) or [])
            if set_lock:
                rule = indefinite_lock_rule(audit_prefix)
                if rule["id"] in {r.get("id") for r in rules}:
                    steps.append(("bucket_lock",
                                  f"rule {rule['id']} already present ({len(rules)} rule(s) total) "
                                  "— not modified"))
                else:
                    merged = rules + [rule]
                    cf_put_bucket_lock(cf_lock, merged)
                    steps.append(("bucket_lock",
                                  f"SET {rule['id']} (prefix={rule['prefix']}, condition=Indefinite) "
                                  f"— {len(rules)} existing rule(s) preserved, {len(merged)} total"))
            else:
                steps.append(("bucket_lock", f"{len(rules)} rule(s) read (not modified): "
                                              f"{[r.get('id') for r in rules]}"))
        except UnsupportedFeature as exc:
            steps.append(("bucket_lock", f"READ/SET FAILED ({exc})"))
    else:
        steps.append(("bucket_lock",
                      "NOT_SET (WORM-equivalent 'bucket locks' need CLOUDFLARE_API_TOKEN + "
                      "CLOUDFLARE_ACCOUNT_ID; use the Cloudflare dashboard/Wrangler)"))
    return steps


def validate(config, client, run_id, utc_now, cf_lock=None):
    """Probe the bucket and produce an evidence record. Only probe objects
    under _audit_probe/ are written and then purged — production data is never
    touched."""
    checks = {}
    checks["connectivity"] = "PASS"
    checks["bucket_exists"] = "PASS" if client.bucket_exists() else "FAIL"
    try:
        versioning = client.get_bucket_versioning()
        if versioning == "Enabled":
            checks["versioning"] = "ENABLED"
        else:
            checks["versioning"] = "NOT_ENABLED_VIA_S3"
            checks["versioning_note"] = (
                "R2 versioning is a bucket setting enabled via the dashboard / "
                "Wrangler / Cloudflare API — the S3 PutBucketVersioning API is "
                "NotImplemented on R2 (verified live 2026-08-14)"
            )
    except R2Error as exc:
        checks["versioning"] = "UNOBSERVABLE"
        checks["versioning_note"] = str(exc)[:200]

    try:
        client.get_object_lock_configuration()
        checks["object_lock"] = "SET"
        checks["object_lock_note"] = "S3 Object Lock configuration is set on this endpoint"
    except UnsupportedFeature as exc:
        checks["object_lock"] = "NOT_SET"
        checks["object_lock_note"] = (
            "S3 API Object Lock configuration is not set on R2 "
            f"({exc}). R2's WORM-equivalent is 'bucket locks' — prefix retention "
            "rules (duration / until-date / indefinite) configured via the "
            "Cloudflare API, dashboard, or Wrangler, not the S3 Object Lock API. "
            "An indefinite bucket-lock rule on the audit prefix is the NFR 3.4.1 "
            "WORM-equivalent; provisioning requires CLOUDFLARE_API_TOKEN + "
            "CLOUDFLARE_ACCOUNT_ID."
        )

    if cf_lock is not None:
        try:
            body = cf_get_bucket_lock(cf_lock)
            rules = body.get("result", {}).get("rules", [])
            checks["bucket_lock"] = "PASS" if rules else "NONE"
            checks["bucket_lock_rules"] = [r.get("id") for r in rules]
            checks["bucket_lock_note"] = (
                "R2 'bucket lock' rules read via the Cloudflare API (read-only)"
            )
        except UnsupportedFeature as exc:
            checks["bucket_lock"] = "ERROR"
            checks["bucket_lock_note"] = str(exc)[:300]
    else:
        checks["bucket_lock"] = "NOT_CHECKED"
        checks["bucket_lock_note"] = (
            "R2 WORM-equivalent 'bucket locks' are read/set via the Cloudflare "
            "API — needs CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID; the "
            "S3-compat keys cannot read them"
        )

    try:
        lifecycle = client.get_bucket_lifecycle()
        checks["lifecycle"] = "PASS" if lifecycle is not None else "ABSENT"
    except UnsupportedFeature as exc:
        checks["lifecycle"] = "UNSUPPORTED"
        checks["lifecycle_note"] = str(exc)

    # Object I/O probe: put -> get (content verified) -> delete round-trip.
    # R2 does not implement ListObjectVersions or the S3 versioning/versionId
    # surface, so immutability on R2 is provided by 'bucket locks' (Cloudflare
    # API), not by an S3 probe.
    key = f"_audit_probe/{run_id}/obj"
    probe_ok = False
    try:
        client.put_object(key, b"audit-probe")
        got = client.get_object(key)
        probe_ok = got == b"audit-probe"
    except R2Error:
        probe_ok = False
    # Cleanup is always attempted, and the claim now reflects what happened: the
    # old code hardcoded probe_cleanup=PASS right here, so a delete that failed
    # (the object most likely to fail is also the one most likely to be left
    # behind) was attested as a clean round-trip (P6-307). NoSuchKey is not a
    # leftover — it means the probe object is not there.
    cleanup_note = ""
    try:
        client.delete_object(key)
        cleanup_ok = True
    except R2Error as exc:
        cleanup_ok = getattr(exc, "code", "") == "NoSuchKey"
        if not cleanup_ok:
            cleanup_note = f"probe object {key} could NOT be deleted: {exc}"
    checks["object_io_probe"] = "PASS" if probe_ok else "FAIL"
    checks["probe_detail"] = (
        "put -> get (content verified) -> delete round-trip on _audit_probe/; "
        "R2 does not implement ListObjectVersions or the S3 versioning/versionId "
        "surface, so immutability is proven by bucket locks (Cloudflare API), "
        "not by S3 probing"
    )
    checks["probe_cleanup"] = "PASS" if cleanup_ok else "FAIL"
    if cleanup_note:
        checks["probe_cleanup_note"] = cleanup_note

    return build_evidence(config, checks, run_id, utc_now)


def build_evidence(config, checks, run_id, utc_now):
    all_pass = (
        checks.get("bucket_exists") == "PASS"
        and checks.get("object_io_probe") == "PASS"
    )
    result = "PASS" if all_pass else "FAIL"
    limitations = []
    if checks.get("object_lock") != "SET":
        limitations.append(
            "S3 Object Lock API is not implemented on R2; the WORM-equivalent is "
            "an indefinite 'bucket lock' rule on the audit prefix via the "
            "Cloudflare dashboard/Wrangler/API (CLOUDFLARE_API_TOKEN + "
            "CLOUDFLARE_ACCOUNT_ID required to read/verify it)."
        )
    if checks.get("bucket_lock") != "PASS":
        limitations.append(
            f"bucket locks: {checks.get('bucket_lock')} — "
            f"{checks.get('bucket_lock_note', '')}"
        )
    if checks.get("versioning") != "ENABLED":
        limitations.append(
            f"versioning: {checks.get('versioning')} — "
            f"{checks.get('versioning_note', 'not enabled via the S3 API')}"
        )
    if checks.get("lifecycle") != "PASS":
        limitations.append(f"bucket lifecycle rules: {checks.get('lifecycle')}")
    if checks.get("probe_cleanup") != "PASS":
        limitations.append(f"probe cleanup: {checks.get('probe_cleanup')}")
    return {
        "work_item_id": f"AUDIT-R2-{run_id}",
        "requirement_ids": ["NFR 3.4.1", "DEC-020", "REQ-FLS-*"],
        "artifact": "logs/audit-r2/<run_id>-audit-r2-evidence.json",
        "version": "audit_r2.py (stdlib SigV4)",
        "environment": (
            f"Cloudflare R2 (S3-compatible) bucket={config['bucket']} "
            f"endpoint={config['endpoint']}"
        ),
        "workload": "probe objects only; no production data touched",
        "clock": "UTC",
        "result": result,
        "owner": "Platform Team",
        "date": utc_now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "checks": checks,
        "limitations": limitations,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Cloudflare R2 audit-store provisioning + validation"
    )
    parser.add_argument("mode", choices=("provision", "validate"), nargs="?",
                        default="validate")
    parser.add_argument("--env-file", default=ENV_FILE_DEFAULT,
                        help="KEY=VALUE env file (default: code/01_platform/01_docker/.env)")
    parser.add_argument("--out", default=os.path.join(os.getcwd(), "logs", "audit-r2"),
                        help="evidence output directory (validate only)")
    parser.add_argument("--set-lock", action="store_true",
                        help="(provision) set the indefinite WORM bucket-lock rule on the "
                             "audit prefix via the Cloudflare API (merges existing rules; "
                             "requires CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID)")
    parser.add_argument("--audit-prefix", default="audit/",
                        help="(provision --set-lock) key prefix locked indefinitely "
                             "(default: audit/)")
    args = parser.parse_args(argv)

    env = merge_env(args.env_file)  # real environment variables win (P6-308)
    try:
        config = config_from(env)
    except ConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    cf_lock = cloudflare_lock_config(env)
    if args.mode == "provision" and args.set_lock and cf_lock is None:
        print("ERROR: --set-lock requires CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID "
              "(R2 bucket locks are set via the Cloudflare API, not the S3-compat keys)",
              file=sys.stderr)
        return 2
    client = R2Client(config)
    utc_now = _dt.datetime.now(_dt.timezone.utc)
    run_id = utc_now.strftime("%Y%m%dT%H%M%SZ")

    print(
        f"endpoint={config['endpoint']} bucket={config['bucket']} "
        f"region={config['region']} key_id={mask_secret(config['access_key_id'])}"
    )
    if cf_lock is None:
        print("note: CLOUDFLARE_API_TOKEN/CLOUDFLARE_ACCOUNT_ID absent — R2 "
              "'bucket lock' state will be recorded as NOT_CHECKED")
    try:
        if args.mode == "provision":
            for step, value in provision(config, client, cf_lock,
                                         args.set_lock, args.audit_prefix):
                print(f"provision {step}: {value}")
            return 0
        evidence = validate(config, client, run_id, utc_now, cf_lock)
    except R2Error as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except UnsupportedFeature as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    os.makedirs(args.out, exist_ok=True)
    evidence_path = os.path.join(args.out, f"{run_id}-audit-r2-evidence.json")
    with open(evidence_path, "w", encoding="utf-8") as fh:
        json.dump(evidence, fh, indent=2)
    print(f"result: {evidence['result']}")
    for name, value in evidence["checks"].items():
        print(f"check {name}: {value}")
    for limitation in evidence["limitations"]:
        print(f"limitation: {limitation}")
    print(f"evidence: {evidence_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
