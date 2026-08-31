#!/usr/bin/env bash
# r2-move-prefix.sh <src_prefix> <dst_prefix> (2026-08-31) — move R2 objects
# between prefixes via GET+PUT+DELETE (SigV4). R2's CopyObject canonicalizer
# rejects x-amz-copy-source header signing (observed 2026-08-31, 3 variants)
# — plain GET+PUT+DELETE is the proven path. Used to archive a stale iceberg
# table dir aside before a table recreate (T-7). Fails loudly on non-200.
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SRC="${1:?usage: r2-move-prefix.sh <src_prefix> <dst_prefix>}"
DST="${2:?usage: r2-move-prefix.sh <src_prefix> <dst_prefix>}"
_ENV="$_D/../01_docker/.env" _SEC="$_D/../01_docker/secrets.env"
R2_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_ENV" | cut -d= -f2-)"
R2_BUCKET="$(grep -E '^R2_BUCKET=' "$_ENV" | cut -d= -f2-)"
AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_SEC" | cut -d= -f2-)"
SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_SEC" | cut -d= -f2-)"
python3 - "$R2_ENDPOINT" "$R2_BUCKET" "$AK" "$SK" "$SRC" "$DST" <<'MOVEPY'
import hashlib, hmac, datetime, sys, urllib.request, urllib.error, xml.etree.ElementTree as ET
from urllib.parse import quote

endpoint, bucket, ak, sk, src, dst = sys.argv[1:7]
host = endpoint.split("//", 1)[1]

def enc(s):
    return quote(s, safe='')

def sign(method, key, payload=b'', qs='', canonical_uri=None):
    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ'); datestamp = t.strftime('%Y%m%d')
    scope = f"{datestamp}/auto/s3/aws4_request"
    payload_hash = hashlib.sha256(payload).hexdigest()
    if canonical_uri is None:
        canonical_uri = "/" + bucket + "/" + quote(key, safe='/')
    headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"{method}\n{canonical_uri}\n{qs}\n{headers}\n{signed}\n{payload_hash}"
    k = hmac.new(("AWS4" + sk).encode(), datestamp.encode(), hashlib.sha256).digest()
    for step in (b"auto", b"s3", b"aws4_request"):
        k = hmac.new(k, step, hashlib.sha256).digest()
    sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    return amzdate, {"Authorization": f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed}, Signature={sig}",
                     "x-amz-date": amzdate, "x-amz-content-sha256": payload_hash}

def req(method, key, payload=b''):
    amzdate, hdrs = sign(method, key, payload)
    r = urllib.request.Request(f"https://{host}/{bucket}/{quote(key, safe='/')}",
                               data=payload if method in ("PUT", "POST") else None,
                               headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(r, timeout=600) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()

def list_keys(prefix):
    keys = []
    token = None
    while True:
        params = {"list-type": "2", "max-keys": "1000", "prefix": prefix}
        if token: params["continuation-token"] = token
        qs = "&".join(f"{k}={enc(v)}" for k, v in sorted(params.items()))
        # ListObjectsV2 targets the BUCKET ROOT (/bucket/), prefix is a query
        # param — signing /bucket/<prefix> instead yields SignatureDoesNotMatch
        # (observed 2026-08-31, M-4).
        amzdate, hdrs = sign("GET", prefix, qs=qs, canonical_uri=f"/{bucket}/")
        r = urllib.request.Request(f"https://{host}/{bucket}/?{qs}", headers=hdrs)
        body = urllib.request.urlopen(r, timeout=600).read()
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        root = ET.fromstring(body)
        for c in root.findall("s3:Contents", ns):
            keys.append(c.find('s3:Key', ns).text)
        if (root.find('s3:IsTruncated', ns).text or "").lower() != "true":
            break
        token = root.find('s3:NextContinuationToken', ns).text
    return keys

keys = list_keys(src)
print(f"moving {len(keys)} objects: {src} -> {dst}")
import time
for i, key in enumerate(keys, 1):
    new_key = dst + key[len(src):]
    # Idempotent + retry: GET+PUT+DELETE per object; on transient URLError
    # retry up to 3x (R2 large-object transfers time out occasionally —
    # observed 2026-08-31 M-5). Objects already moved (prior interrupted run)
    # are simply absent from the listing, so re-runs are safe.
    for attempt in range(1, 4):
        try:
            status, body = req("GET", key)
            if status != 200: raise SystemExit(f"GET failed {status} for {key}: {body[:200]}")
            status, body = req("PUT", new_key, body)
            if status != 200: raise SystemExit(f"PUT failed {status} for {new_key}: {body[:200]}")
            status, body = req("DELETE", key)
            if status != 204 and status != 200: raise SystemExit(f"DELETE failed {status} for {key}: {body[:200]}")
            break
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt == 3: raise SystemExit(f"GAVE UP on {key} after 3 attempts: {e}")
            print(f"  retry {attempt} for {key}: {e}")
    if i % 50 == 0 or i == len(keys): print(f"  {i}/{len(keys)}")
left = list_keys(src)
if left: raise SystemExit(f"ARCHIVE INCOMPLETE: {len(left)} objects remain under {src}")
print(f"ARCHIVE COMPLETE: {len(keys)} objects moved")
MOVEPY
