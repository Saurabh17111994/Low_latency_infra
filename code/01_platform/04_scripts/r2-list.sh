#!/usr/bin/env bash
# r2-list.sh (2026-08-31) — signed S3 ListObjectsV2 against Cloudflare R2.
# No aws cli on this host; a minimal SigV4 request answers the one question
# the tiering smoke needs: "are there lake objects under the warehouse
# prefix?" Credentials come from the SAME files compose interpolates
# (.env + secrets.env) — never hardcoded here.
#
# Functions: r2_list_lake  (objects under the lake warehouse prefix)
#            r2_list_all  (everything in the bucket)
# PAGINATION (2026-08-31): ListObjectsV2 caps at 1000 keys per page and the
# original max-keys=100 single request SILENTLY TRUNCATED listings — a
# "no objects for table X" verdict could be pure truncation (observed:
# lake prefix returned exactly 100 rows). Now paginates via
# continuation-token until IsTruncated=false.
set -euo pipefail

_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_DOCKER_DIR="$(cd "$_SCRIPT_DIR/../01_docker" && pwd)"
R2_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_DOCKER_DIR/.env" | cut -d= -f2-)"
R2_BUCKET="$(grep -E '^R2_BUCKET=' "$_DOCKER_DIR/.env" | cut -d= -f2-)"
WAREHOUSE="$(grep -E '^S3_WAREHOUSE_PATH=' "$_DOCKER_DIR/.env" | cut -d= -f2-)"
AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_DOCKER_DIR/secrets.env" | cut -d= -f2-)"
SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_DOCKER_DIR/secrets.env" | cut -d= -f2-)"

# warehouse path s3://bucket/lake -> prefix "lake/"
_PREFIX="${WAREHOUSE#s3://}"
_PREFIX="${_PREFIX#*/}"

_r2_list() {
  local prefix="$1"
  python3 - "$R2_ENDPOINT" "$R2_BUCKET" "$AK" "$SK" "$prefix" <<'PYEOF'
import hashlib, hmac, datetime, sys, urllib.request, xml.etree.ElementTree as ET
from urllib.parse import quote

endpoint, bucket, ak, sk, prefix = sys.argv[1:6]
host = endpoint.split("//", 1)[1]

def enc(s):
    # R2's SigV4 canonicalizer percent-encodes "/" and other specials in
    # query VALUES (prefix=lake%2F, NOT prefix=lake/) — a raw "/" produces
    # SignatureDoesNotMatch (observed 2026-08-31). quote(safe='') matches.
    return quote(s, safe='')

def list_page(token):
    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')
    scope = f"{datestamp}/auto/s3/aws4_request"
    payload_hash = hashlib.sha256(b'').hexdigest()
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
    body = urllib.request.urlopen(req, timeout=20).read()
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(body)
    keys = []
    for c in root.findall("s3:Contents", ns):
        keys.append((c.find('s3:Key', ns).text, c.find('s3:Size', ns).text))
    truncated = (root.find('s3:IsTruncated', ns).text or "").lower() == "true"
    next_token_el = root.find('s3:NextContinuationToken', ns)
    next_token = next_token_el.text if next_token_el is not None else None
    return keys, truncated, next_token

token = None
while True:
    keys, truncated, token = list_page(token)
    for key, size in keys:
        print(f"{key}\t{size}")
    if not truncated:
        break
PYEOF
}

r2_list_lake() { _r2_list "${_PREFIX%/}/"; }
r2_list_all()  { _r2_list ""; }
