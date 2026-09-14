#!/usr/bin/env bash
# r2-move-prefix.sh <src_prefix> <dst_prefix> (2026-08-31) — move R2 objects
# between prefixes via GET+PUT+DELETE (SigV4). R2's CopyObject canonicalizer
# rejects x-amz-copy-source header signing (observed 2026-08-31, 3 variants)
# — plain GET+PUT+DELETE is the proven path. Used to archive a stale iceberg
# table dir aside before a table recreate (T-7). Fails loudly on non-200.
#
# 2026-09-14 (P6-013/P6-158/P6-159/P6-160/P6-161/P6-490/491/492/493/768):
# the copy is verified before the source is deleted, objects stream through a
# temp file instead of RAM (honours TMPDIR — point it at real disk when moving
# large objects on a tmpfs /tmp), transient HTTP statuses are retried with backoff,
# prefixes are normalized and overlap-checked, and the credentials reach python
# through the environment rather than argv (argv is world-readable in
# /proc/<pid>/cmdline).
#
# Test seams (used by test_r2_move_prefix.py, unset in production):
#   R2_ENV_FILE / R2_SECRETS_FILE — point the config lookup at a fixture.
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
SRC="${1:?usage: r2-move-prefix.sh <src_prefix> <dst_prefix>}"
DST="${2:?usage: r2-move-prefix.sh <src_prefix> <dst_prefix>}"
_ENV="${R2_ENV_FILE:-$_D/../01_docker/.env}"
_SEC="${R2_SECRETS_FILE:-$_D/../01_docker/secrets.env}"

# Read KEY=value (also `export KEY=value`), last match wins, strip CR, spaces
# and surrounding quotes. Never fails the script on a missing key (P6-158/490).
get_kv() {
  grep -E "^(export +)?$2=" "$1" 2>/dev/null | tail -n1 | cut -d= -f2- \
    | tr -d '\r' | sed -e 's/^[[:space:]"'"'"']*//' -e 's/[[:space:]"'"'"']*$//' || true
}

[[ -n "$SRC" && -n "$DST" ]] || { echo "r2-move-prefix: prefixes must be non-empty" >&2; exit 2; }
[[ "$SRC" != "$DST" ]] || { echo "r2-move-prefix: src and dst must differ ('$SRC')" >&2; exit 2; }
[[ -r "$_ENV" && -r "$_SEC" ]] || {
  echo "r2-move-prefix: cannot read $_ENV or $_SEC" >&2; exit 2; }

R2_ENDPOINT="$(get_kv "$_ENV" R2_ENDPOINT)"
R2_BUCKET="$(get_kv "$_ENV" R2_BUCKET)"
AK="$(get_kv "$_SEC" AWS_ACCESS_KEY_ID)"
SK="$(get_kv "$_SEC" AWS_SECRET_ACCESS_KEY)"
[[ -n "$R2_ENDPOINT" && -n "$R2_BUCKET" && -n "$AK" && -n "$SK" ]] || {
  echo "r2-move-prefix: incomplete R2 config in $_ENV / $_SEC" >&2; exit 2; }

R2_ENDPOINT="$R2_ENDPOINT" R2_BUCKET="$R2_BUCKET" R2_AK="$AK" R2_SK="$SK" \
R2_SRC="$SRC" R2_DST="$DST" \
python3 - <<'MOVEPY'
import datetime, hashlib, hmac, os, random, shutil, sys, tempfile, time
import urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

endpoint = os.environ.get("R2_ENDPOINT", "")
bucket = os.environ.get("R2_BUCKET", "")
ak = os.environ.get("R2_AK", "")
sk = os.environ.get("R2_SK", "")
src = os.environ.get("R2_SRC", "")
dst = os.environ.get("R2_DST", "")


def fail(msg, code=1):
    """Print to stderr and exit. 2 = refused before touching the bucket
    (nothing was moved), 1 = an operation failed mid-run."""
    print(msg, file=sys.stderr)
    raise SystemExit(code)


# P6-491: parse the endpoint instead of splitting on "//" — a missing scheme,
# trailing whitespace, a path or a trailing slash used to raise IndexError or
# corrupt the host. The scheme is honoured (http stays http).
u = urllib.parse.urlparse(endpoint.strip())
if u.scheme not in ("http", "https") or not u.netloc:
    fail(f"bad R2_ENDPOINT {endpoint!r}: need scheme://host", 2)
BASE = f"{u.scheme}://{u.netloc}"
missing = [n for n, v in (("R2_BUCKET", bucket), ("R2_AK", ak), ("R2_SK", sk),
                          ("R2_SRC", src), ("R2_DST", dst)) if not v]
if missing:
    fail("missing " + ", ".join(missing), 2)

# P6-160: S3 prefixes are raw string prefixes, so `table` also lists
# `table_backup/...`. Normalize to a delimiter-terminated prefix and refuse
# overlapping moves: dst inside src invalidates the listing, src inside dst
# moves objects under an ancestor, src == dst self-copies then deletes.
def normalize(p, name):
    p = p.strip().lstrip("/")
    if not p:
        fail(f"{name} is empty after normalization", 2)
    return p if p.endswith("/") else p + "/"

src = normalize(src, "src")
dst = normalize(dst, "dst")
if src == dst:
    fail(f"refusing to move {src} onto itself", 2)
if dst.startswith(src) or src.startswith(dst):
    fail(f"refusing overlapping move: {src} -> {dst}", 2)

TIMEOUT = 600
RETRIES = 3
RETRY_BASE = 0.5           # first backoff ~1s: long enough to clear a throttle
RETRYABLE = {408, 429, 500, 502, 503, 504}


class Transient(RuntimeError):
    """Retryable HTTP status or transport error."""


def enc(s):
    return urllib.parse.quote(s, safe='')


def sign(method, key, payload_hash, qs='', canonical_uri=None, extra=None):
    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ'); datestamp = t.strftime('%Y%m%d')
    scope = f"{datestamp}/auto/s3/aws4_request"
    if canonical_uri is None:
        canonical_uri = "/" + bucket + "/" + urllib.parse.quote(key, safe='/')
    headers = f"host:{u.netloc}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"{method}\n{canonical_uri}\n{qs}\n{headers}\n{signed}\n{payload_hash}"
    k = hmac.new(("AWS4" + sk).encode(), datestamp.encode(), hashlib.sha256).digest()
    for step in (b"auto", b"s3", b"aws4_request"):
        k = hmac.new(k, step, hashlib.sha256).digest()
    sts = f"AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    hdrs = {"Authorization": f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
                             f"SignedHeaders={signed}, Signature={sig}",
            "x-amz-date": amzdate, "x-amz-content-sha256": payload_hash}
    if extra:
        hdrs.update(extra)
    return hdrs


def send(method, key, data=b'', payload_hash=None, content_type=None, qs='',
         canonical_uri=None, file_path=None, stream_to=None):
    """Signed request. Returns (status, headers, body); body is empty when the
    response was streamed to `stream_to` (P6-159)."""
    if payload_hash is None:
        payload_hash = hashlib.sha256(data).hexdigest()
    extra = {}
    if content_type:
        # Unsigned: the payload hash binds the bytes; this is carried metadata
        # so a Parquet/manifest keeps its type (P6-492).
        extra["Content-Type"] = content_type
    hdrs = sign(method, key, payload_hash, qs=qs, canonical_uri=canonical_uri, extra=extra)
    url = BASE + (canonical_uri if canonical_uri is not None
                  else "/" + bucket + "/" + urllib.parse.quote(key, safe='/'))
    if qs:
        url += "?" + qs
    body = data
    if file_path is not None:
        body = open(file_path, "rb")
        hdrs["Content-Length"] = str(os.path.getsize(file_path))
    try:
        r = urllib.request.Request(url, data=body if method in ("PUT", "POST") else None,
                                  headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(r, timeout=TIMEOUT) as resp:
                if stream_to is not None:
                    with open(stream_to, "wb") as out:
                        shutil.copyfileobj(resp, out, 8 * 1024 * 1024)
                    return resp.status, dict(resp.headers), b''
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers or {}), e.read()
    finally:
        if file_path is not None:
            body.close()


def check(status, body, what, ok=(200,)):
    """Fail closed on non-2xx; retryable statuses raise Transient (P6-161)."""
    if status in RETRYABLE:
        raise Transient(f"HTTP {status} for {what}: {body[:200]!r}")
    if status not in ok:
        raise SystemExit(f"{what} failed {status}: {body[:200]!r}")


def retrying(what, fn):
    for attempt in range(1, RETRIES + 1):
        try:
            return fn()
        except (Transient, urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt == RETRIES:
                raise SystemExit(f"GAVE UP on {what} after {RETRIES} attempts: {e}")
            delay = min(30.0, RETRY_BASE * 2 ** attempt) + random.uniform(0, 1)
            print(f"  retry {attempt}/{RETRIES - 1} for {what} in {delay:.1f}s: {e}",
                  flush=True)
            time.sleep(delay)


def call(what, method, ok=(200,), **request):
    """Retried signed request returning (headers, body). The status check is
    inside the retry on purpose: a 503 that surfaces outside it would abort the
    whole migration on the first throttled object (P6-161)."""
    def once():
        status, headers, body = send(method, **request)
        check(status, body, what, ok=ok)
        return headers, body
    return retrying(what, once)


def file_digest(path, algo):
    h = hashlib.new(algo)
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def list_keys(prefix):
    """One page at a time; retried, and tolerant of a missing element in the
    listing XML (a 5xx error body is not XML) (P6-493)."""
    keys, token = [], None
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    while True:
        params = {"list-type": "2", "max-keys": "1000", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        qs = "&".join(f"{k}={enc(v)}" for k, v in sorted(params.items()))
        # ListObjectsV2 targets the BUCKET ROOT (/bucket/), prefix is a query
        # param — signing /bucket/<prefix> instead yields SignatureDoesNotMatch
        # (observed 2026-08-31, M-4).
        def page():
            status, _h, body = send("GET", prefix, qs=qs, canonical_uri=f"/{bucket}/")
            check(status, body, f"LIST {prefix}", ok=(200,))
            return body
        body = retrying(f"LIST {prefix}", page)
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            raise SystemExit(f"LIST {prefix}: unparseable listing: {e}: {body[:200]!r}")
        for c in root.findall("s3:Contents", ns):
            el = c.find("s3:Key", ns)
            if el is not None and el.text:
                keys.append(el.text)
        trunc = root.find("s3:IsTruncated", ns)
        trunc_text = trunc.text if trunc is not None else ""
        if (trunc_text or "").strip().lower() != "true":
            break
        tok = root.find("s3:NextContinuationToken", ns)
        if tok is None or not tok.text:
            raise SystemExit(f"LIST {prefix}: truncated listing without a continuation "
                             f"token: {body[:200]!r}")
        token = tok.text
    return keys


def verify_copy(new_key, headers, size, md5_hex):
    """P6-013: the source is deleted only after the destination is proven to
    hold the same number of bytes (and the same MD5 when the backend publishes
    a single-PUT ETag). A mismatch keeps both copies — never a lost object."""
    declared = headers.get("Content-Length")
    if declared is None or int(declared) != size:
        raise SystemExit(f"REFUSING TO DELETE {new_key}: destination reports "
                         f"Content-Length {declared!r}, copied {size} bytes")
    etag = (headers.get("ETag") or "").strip('"').lower()
    if len(etag) == 32 and "-" not in etag and etag != md5_hex:
        raise SystemExit(f"REFUSING TO DELETE {new_key}: destination ETag {etag} "
                         f"!= md5 {md5_hex} of the copied bytes")


keys = list_keys(src)
print(f"moving {len(keys)} objects: {src} -> {dst}", flush=True)
for i, key in enumerate(keys, 1):
    if not key.startswith(src):
        raise SystemExit(f"listing returned {key!r} outside prefix {src!r} — refusing")
    new_key = dst + key[len(src):]
    tmp = tempfile.NamedTemporaryFile(prefix="r2-move-", delete=False)
    tmp.close()
    try:
        headers, _body = call(f"GET {key}", "GET", key=key, stream_to=tmp.name)
        size = os.path.getsize(tmp.name)
        sha256 = file_digest(tmp.name, "sha256")
        md5 = file_digest(tmp.name, "md5")
        content_type = headers.get("Content-Type") or "application/octet-stream"
        call(f"PUT {new_key}", "PUT", key=new_key, payload_hash=sha256,
             file_path=tmp.name, content_type=content_type)
        h, _body = call(f"HEAD {new_key}", "HEAD", key=new_key)
        verify_copy(new_key, h, size, md5)
        call(f"DELETE {key}", "DELETE", ok=(200, 204), key=key)
    finally:
        os.unlink(tmp.name)
    if i % 50 == 0 or i == len(keys):
        print(f"  {i}/{len(keys)}", flush=True)
left = list_keys(src)
if left:
    raise SystemExit(f"ARCHIVE INCOMPLETE: {len(left)} objects remain under {src}")
print(f"ARCHIVE COMPLETE: {len(keys)} objects moved")
MOVEPY
