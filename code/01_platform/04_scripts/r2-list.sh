#!/usr/bin/env bash
# r2-list.sh (2026-08-31, hardened 2026-09-14) — signed S3 ListObjectsV2 against
# Cloudflare R2. No aws cli on this host; a minimal SigV4 request answers the one
# question the tiering smoke needs: "are there lake objects under the warehouse
# prefix?" Credentials come from the SAME files compose interpolates
# (.env + secrets.env) — never hardcoded here, never in argv.
#
# Functions: r2_list_lake  (objects under the lake warehouse prefix)
#            r2_list_all  (everything in the bucket)
# Direct use: r2-list.sh lake | all      (sourced use is unchanged)
#
# PAGINATION (2026-08-31): ListObjectsV2 caps at 1000 keys per page and the
# original max-keys=100 single request SILENTLY TRUNCATED listings — a
# "no objects for table X" verdict could be pure truncation (observed:
# lake prefix returned exactly 100 rows). Now paginates via
# continuation-token until IsTruncated=false.
#
# HARDENING (2026-09-14, wave 10): no shell options are set at source time and
# nothing is loaded at source time, so `source r2-list.sh` can no longer abort or
# re-flag the caller's shell (P6-765); config is read lazily by r2_load and every
# defect is a named error instead of an empty variable (P6-766). Values are
# de-quoted/CR-stripped by r2_var (P6-485); the endpoint's host and the warehouse
# prefix are validated, so a bad S3_WAREHOUSE_PATH can no longer make "no objects"
# mean "wrong bucket" (P6-486, P6-487, P6-489); secrets are passed via the
# environment rather than argv, which /proc/<pid>/cmdline exposes to every local
# user (P6-154); HTTP errors, error documents, non-XML bodies and namespace-less
# responses are all reported instead of crashing or reading as empty (P6-155,
# P6-156, P6-488); and pagination fails fast when a truncated page carries no
# token instead of re-fetching page 1 forever (P6-157).

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
# shellcheck source=./r2-env.sh
. "$_SCRIPT_DIR/r2-env.sh"

# Seams for the tests (unset in production).
R2_ENV_FILE="${R2_ENV_FILE:-$_SCRIPT_DIR/../01_docker/.env}"
R2_SECRETS_FILE="${R2_SECRETS_FILE:-$_SCRIPT_DIR/../01_docker/secrets.env}"

r2_load() {
  # Idempotent. Returns 1 with one line on stderr for any config defect.
  [ "${_R2_LOADED:-0}" = "1" ] && return 0

  R2_ENDPOINT="$(r2_var "$R2_ENV_FILE" R2_ENDPOINT)" || return 1
  R2_BUCKET="$(r2_var "$R2_ENV_FILE" R2_BUCKET)" || return 1
  WAREHOUSE="$(r2_var "$R2_ENV_FILE" S3_WAREHOUSE_PATH)" || return 1
  R2_AK="$(r2_var "$R2_SECRETS_FILE" AWS_ACCESS_KEY_ID)" || return 1
  R2_SK="$(r2_var "$R2_SECRETS_FILE" AWS_SECRET_ACCESS_KEY)" || return 1

  case "$R2_ENDPOINT" in
    https://?*) ;;
    *) echo "r2-list: R2_ENDPOINT must be an https:// URL, got '${R2_ENDPOINT}'" >&2; return 1 ;;
  esac
  R2_HOST="${R2_ENDPOINT#https://}"
  R2_HOST="${R2_HOST%%/*}"                      # drop any path and trailing slash
  case "$R2_HOST" in
    ""|*/*) echo "r2-list: R2_ENDPOINT has no usable host: '${R2_ENDPOINT}'" >&2; return 1 ;;
  esac

  case "$WAREHOUSE" in
    "s3://$R2_BUCKET/"*) R2_PREFIX="${WAREHOUSE#s3://"$R2_BUCKET"/}" ;;
    "s3://$R2_BUCKET")   echo "r2-list: S3_WAREHOUSE_PATH needs a prefix below the bucket: '${WAREHOUSE}'" >&2; return 1 ;;
    s3://*)              echo "r2-list: S3_WAREHOUSE_PATH bucket is not R2_BUCKET (${R2_BUCKET}): '${WAREHOUSE}'" >&2; return 1 ;;
    *)                   echo "r2-list: S3_WAREHOUSE_PATH must be s3://<bucket>/<prefix>, got '${WAREHOUSE}'" >&2; return 1 ;;
  esac
  R2_PREFIX="${R2_PREFIX%/}"                    # "lake/" and "lake" are the same prefix
  [ -n "$R2_PREFIX" ] || { echo "r2-list: S3_WAREHOUSE_PATH has an empty prefix: '${WAREHOUSE}'" >&2; return 1; }

  _R2_LOADED=1
}

_r2_list() {
  # _r2_list <prefix> — TSV "key<TAB>size" on stdout, one object per line.
  local prefix="$1"
  r2_load || return 1
  # Secrets travel in the environment: argv is world-readable (P6-154). The
  # endpoint/host, bucket and prefix are not secret and stay as arguments.
  R2_AK="$R2_AK" R2_SK="$R2_SK" python3 - "$R2_HOST" "$R2_BUCKET" "$prefix" <<'PYEOF'
import datetime
import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import quote

host, bucket, prefix = sys.argv[1:4]
ak, sk = os.environ["R2_AK"], os.environ["R2_SK"]
MAX_PAGES = 1000          # 1000 keys per page; a cap turns a token bug into an error


def fail(msg):
    print(f"r2-list: {msg}", file=sys.stderr)
    sys.exit(1)


def local(tag):
    # Namespace-agnostic: R2 has returned these documents both with and without
    # the s3 namespace, and findall('s3:Contents') read that as "bucket empty"
    # (P6-488).
    return tag.rsplit("}", 1)[-1]


def enc(s):
    # R2's SigV4 canonicalizer percent-encodes "/" and other specials in
    # query VALUES (prefix=lake%2F, NOT prefix=lake/) — a raw "/" produces
    # SignatureDoesNotMatch (observed 2026-08-31). quote(safe='') matches.
    return quote(s, safe="")


def list_page(token):
    now = datetime.datetime.now(datetime.timezone.utc)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    scope = f"{datestamp}/auto/s3/aws4_request"
    payload_hash = hashlib.sha256(b"").hexdigest()
    # Canonical query: keys sorted (continuation-token < list-type <
    # max-keys < prefix).
    params = {"list-type": "2", "max-keys": "1000", "prefix": prefix}
    if token:
        params["continuation-token"] = token
    qs = "&".join(f"{k}={enc(v)}" for k, v in sorted(params.items()))
    canonical_uri = f"/{bucket}/"
    headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"GET\n{canonical_uri}\n{qs}\n{headers}\n{signed}\n{payload_hash}"
    k = hmac.new(("AWS4" + sk).encode(), datestamp.encode(), hashlib.sha256).digest()
    for step in (b"auto", b"s3", b"aws4_request"):
        k = hmac.new(k, step, hashlib.sha256).digest()
    sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        f"https://{host}/{bucket}/?{qs}",
        headers={"Authorization": f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
                 f"SignedHeaders={signed}, Signature={sig}",
                 "x-amz-date": amzdate, "x-amz-content-sha256": payload_hash})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        detail = e.read()[:1000].decode("utf-8", "replace")
        parts = []
        try:
            for el in ET.fromstring(detail).iter():
                if local(el.tag) in ("Code", "Message"):
                    parts.append(f"{local(el.tag)}={el.text}")
        except ET.ParseError:
            pass
        fail(f"ListObjectsV2 HTTP {e.code} for {host}/{bucket}: {' '.join(parts)} body={detail[:200]!r}")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        fail(f"ListObjectsV2 cannot reach {host}: {e}")
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        fail(f"ListObjectsV2 response is not XML ({e}): {body[:200]!r}")
    if local(root.tag) == "Error":
        # 403 / NoSuchBucket / SignatureDoesNotMatch arrive as a 200-with-error
        # document often enough that an absence of Contents must never be read
        # as "no keys" (P6-156).
        parts = [f"{local(el.tag)}={el.text}" for el in root.iter()
                 if local(el.tag) in ("Code", "Message")]
        fail("ListObjectsV2 returned an error document: " + " ".join(parts))

    keys, truncated, saw_truncated = [], False, False
    token_el = None
    for el in root.iter():
        tag = local(el.tag)
        if tag == "Contents":
            key = size = None
            for child in el:
                if local(child.tag) == "Key":
                    key = child.text
                elif local(child.tag) == "Size":
                    size = child.text
            if key is not None:
                keys.append((key, size if size is not None else "0"))
        elif tag == "IsTruncated":
            saw_truncated = True
            truncated = (el.text or "").strip().lower() == "true"
        elif tag == "NextContinuationToken":
            token_el = el
    if not saw_truncated:
        fail("ListObjectsV2 response has no IsTruncated element — refusing to guess "
             "whether the listing is complete")
    return keys, truncated, (token_el.text if token_el is not None else None)


token = None
for page in range(1, MAX_PAGES + 1):
    keys, truncated, token = list_page(token)
    for key, size in keys:
        print(f"{key}\t{size}")
    if not truncated:
        break
    if not token:
        fail(f"page {page} says IsTruncated=true but carries no usable "
             "NextContinuationToken — refusing to request it again")
else:
    fail(f"listing is still truncated after {MAX_PAGES} pages — refusing to continue")
PYEOF
}

# r2_load first: R2_PREFIX only exists after the config is read, and expanding it
# earlier would quietly list "/" — the "no objects" verdict this wave is about.
r2_list_lake() { r2_load || return 1; _r2_list "${R2_PREFIX}/"; }
r2_list_all()  { _r2_list ""; }

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  # Executed, not sourced: this is the only place that may touch the caller's
  # shell, and it also gives the script a real CLI instead of a silent no-op
  # (P6-767).
  set -euo pipefail
  case "${1:-}" in
    lake) r2_list_lake ;;
    all)  r2_list_all ;;
    ""|help|--help|-h)
      cat >&2 <<'USAGE'
usage: r2-list.sh lake   # objects under the lake warehouse prefix
       r2-list.sh all    # every object in the bucket
       source r2-list.sh # then r2_list_lake / r2_list_all
Prints TSV: "key<TAB>size". Config: 01_docker/.env + secrets.env
(override with R2_ENV_FILE / R2_SECRETS_FILE).
USAGE
      [ -n "${1:-}" ] || exit 2
      ;;
    *) echo "r2-list.sh: unknown command '$1' (lake|all)" >&2; exit 2 ;;
  esac
fi
