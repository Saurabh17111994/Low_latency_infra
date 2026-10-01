#!/usr/bin/env bash
# r2-delete-prefix.sh [--apply] [--allow-live] <prefix> [<prefix>...] (2026-10-01)
#
# Delete every object under one or more R2 prefixes. DRY-RUN BY DEFAULT: it
# lists every key with its size and exits without a single mutating request.
# --apply performs the deletes, then re-lists each prefix and fails if any
# object survived.
#
# Why this tool exists (2026-10-01, operator decision): the lake prefix had
# accumulated stale/test objects (old raw_table_1 v3 parquet, probe tables,
# remote-log test artifacts). The repo's never-delete convention is relaxed
# for explicit operator cleanup, so a deletion must be: explicit (--apply),
# previewed (dry-run lists every key), scoped (prefixes normalized, duplicates
# and overlaps refused) and verified (the prefixes must be empty afterwards).
#
# Guards:
#   * the live Iceberg metadata of raw_table_1 is protected — a prefix that is
#     or covers lake/default/raw_table_1/metadata/ is refused unless
#     --allow-live is passed;
#   * the v3 rollback archive lake/_stale-20260831-v1/raw_table_1/ is out of
#     scope of the 2026-10-01 cleanup — point this at it only with an explicit
#     operator decision (docs/06_operations/07-lake-archive-ops.md).
#
# Test seams (used by test_r2_delete_prefix.py, unset in production):
#   R2_ENV_FILE / R2_SECRETS_FILE — point the config lookup at a fixture.
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_APPLY=0
_ALLOW_LIVE=0
_HELP=0
PREFIXES=()
for arg in "$@"; do
  case "$arg" in
    --apply)      _APPLY=1 ;;
    --allow-live) _ALLOW_LIVE=1 ;;
    -h|--help)    _HELP=1 ;;
    -*) echo "r2-delete-prefix: unknown option '$arg'" >&2; exit 2 ;;
    *)  PREFIXES+=("$arg") ;;
  esac
done
if [ "$_HELP" = "1" ]; then
  cat >&2 <<'USAGE'
usage: r2-delete-prefix.sh [--apply] [--allow-live] <prefix> [<prefix>...]
  dry-run (default): list every object under each prefix, delete nothing
  --apply:           delete the listed objects, then verify the prefixes are empty
  --allow-live:      permit a prefix that covers the live raw_table_1 Iceberg metadata
USAGE
  exit 0
fi
[ ${#PREFIXES[@]} -ge 1 ] || {
  echo "r2-delete-prefix: no prefix given (dry-run lists, --apply deletes)" >&2; exit 2; }

_ENV="${R2_ENV_FILE:-$_D/../01_docker/.env}"
_SEC="${R2_SECRETS_FILE:-$_D/../01_docker/secrets.env}"

# Read KEY=value (also `export KEY=value`), last match wins, strip CR, spaces
# and surrounding quotes. Never fails the script on a missing key (P6-158/490).
get_kv() {
  grep -E "^(export +)?$2=" "$1" 2>/dev/null | tail -n1 | cut -d= -f2- \
    | tr -d '\r' | sed -e 's/^[[:space:]"'"'"']*//' -e 's/[[:space:]"'"'"']*$//' || true
}

[[ -r "$_ENV" && -r "$_SEC" ]] || {
  echo "r2-delete-prefix: cannot read $_ENV or $_SEC" >&2; exit 2; }

R2_ENDPOINT="$(get_kv "$_ENV" R2_ENDPOINT)"
R2_BUCKET="$(get_kv "$_ENV" R2_BUCKET)"
AK="$(get_kv "$_SEC" AWS_ACCESS_KEY_ID)"
SK="$(get_kv "$_SEC" AWS_SECRET_ACCESS_KEY)"
[[ -n "$R2_ENDPOINT" && -n "$R2_BUCKET" && -n "$AK" && -n "$SK" ]] || {
  echo "r2-delete-prefix: incomplete R2 config in $_ENV / $_SEC" >&2; exit 2; }

# Prefixes are not secret and go on argv (like r2-list.sh); credentials travel
# through the environment (P6-158: argv is world-readable via /proc).
R2_ENDPOINT="$R2_ENDPOINT" R2_BUCKET="$R2_BUCKET" R2_AK="$AK" R2_SK="$SK" \
python3 - "$_APPLY" "$_ALLOW_LIVE" "${PREFIXES[@]}" <<'DELPY'
import datetime, hashlib, hmac, os, random, sys, time
import urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

endpoint = os.environ.get("R2_ENDPOINT", "")
bucket = os.environ.get("R2_BUCKET", "")
ak = os.environ.get("R2_AK", "")
sk = os.environ.get("R2_SK", "")
apply_ = sys.argv[1] == "1"
allow_live = sys.argv[2] == "1"
raw_prefixes = sys.argv[3:]
LIVE_METADATA = "lake/default/raw_table_1/metadata/"


def fail(msg, code=1):
    """Print to stderr and exit. 2 = refused before touching the bucket
    (nothing was deleted), 1 = an operation failed mid-run."""
    print(msg, file=sys.stderr)
    raise SystemExit(code)


u = urllib.parse.urlparse(endpoint.strip())
if u.scheme not in ("http", "https") or not u.netloc:
    fail(f"bad R2_ENDPOINT {endpoint!r}: need scheme://host", 2)
BASE = f"{u.scheme}://{u.netloc}"
missing = [n for n, v in (("R2_BUCKET", bucket), ("R2_AK", ak), ("R2_SK", sk)) if not v]
if missing:
    fail("missing " + ", ".join(missing), 2)


def normalize(p, name):
    p = p.strip().lstrip("/")
    if not p:
        fail(f"{name} is empty after normalization", 2)
    return p if p.endswith("/") else p + "/"


# Prefixes are raw string prefixes, so `table` also lists `table_backup/...`.
# Normalize to a delimiter-terminated prefix; refuse duplicates and overlaps
# (an overlap double-counts and can hide a mistaken argument).
prefixes = []
for i, p in enumerate(raw_prefixes):
    p = normalize(p, f"prefix {i + 1}")
    if p in prefixes:
        fail(f"refusing duplicate prefix {p}", 2)
    for q in prefixes:
        if p.startswith(q) or q.startswith(p):
            fail(f"refusing overlapping prefixes {q} and {p}", 2)
    prefixes.append(p)

for p in prefixes:
    if not allow_live and (LIVE_METADATA.startswith(p) or p.startswith(LIVE_METADATA)):
        fail(f"refusing {p}: it covers the live Iceberg metadata {LIVE_METADATA} "
             f"(pass --allow-live to override)", 2)

TIMEOUT = 600
RETRIES = 3
RETRY_BASE = 0.5           # first backoff ~1s: long enough to clear a throttle
RETRYABLE = {408, 429, 500, 502, 503, 504}


class Transient(RuntimeError):
    """Retryable HTTP status or transport error."""


def enc(s):
    return urllib.parse.quote(s, safe='')


def sign(method, key, payload_hash, qs='', canonical_uri=None):
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
    return {"Authorization": f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, "
                             f"SignedHeaders={signed}, Signature={sig}",
            "x-amz-date": amzdate, "x-amz-content-sha256": payload_hash}


def send(method, key, qs='', canonical_uri=None):
    """Signed request. Returns (status, headers, body)."""
    payload_hash = hashlib.sha256(b"").hexdigest()
    hdrs = sign(method, key, payload_hash, qs=qs, canonical_uri=canonical_uri)
    url = BASE + (canonical_uri if canonical_uri is not None
                  else "/" + bucket + "/" + urllib.parse.quote(key, safe='/'))
    if qs:
        url += "?" + qs
    req = urllib.request.Request(url, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()


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


def local(tag):
    # Namespace-agnostic: R2 has returned these documents both with and without
    # the s3 namespace, and a namespaced findall reads that as "bucket empty".
    return tag.rsplit("}", 1)[-1]


def list_keys(prefix):
    """All (key, size) under prefix; paginated, retried, and fail-closed on an
    unparseable or truncated-without-token listing (P6-157/488/493)."""
    keys, token = [], None
    while True:
        params = {"list-type": "2", "max-keys": "1000", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        qs = "&".join(f"{k}={enc(v)}" for k, v in sorted(params.items()))
        # ListObjectsV2 targets the BUCKET ROOT (/bucket/), prefix is a query
        # param — signing /bucket/<prefix> instead yields SignatureDoesNotMatch.
        def page():
            status, _h, body = send("GET", prefix, qs=qs, canonical_uri=f"/{bucket}/")
            check(status, body, f"LIST {prefix}", ok=(200,))
            return body
        body = retrying(f"LIST {prefix}", page)
        try:
            root = ET.fromstring(body)
        except ET.ParseError as e:
            fail(f"LIST {prefix}: unparseable listing: {e}: {body[:200]!r}")
        for c in root.iter():
            if local(c.tag) != "Contents":
                continue
            key = size = None
            for child in c:
                if local(child.tag) == "Key":
                    key = child.text
                elif local(child.tag) == "Size":
                    size = child.text
            if key is not None:
                keys.append((key, int(size or 0)))
        truncated, tok = None, None
        for el in root.iter():
            if local(el.tag) == "IsTruncated":
                truncated = (el.text or "").strip().lower() == "true"
            elif local(el.tag) == "NextContinuationToken":
                tok = el.text
        if truncated is None:
            fail(f"LIST {prefix}: no IsTruncated element — refusing to guess completeness")
        if not truncated:
            break
        if not tok:
            fail(f"LIST {prefix}: truncated listing without a continuation token")
        token = tok
    return keys


plan = []
for p in prefixes:
    keys = list_keys(p)
    bad = [k for k, _s in keys if not k.startswith(p)]
    if bad:
        fail(f"listing returned {bad[0]!r} outside prefix {p!r} — refusing")
    plan.append((p, keys))

total_n = sum(len(keys) for _p, keys in plan)
total_b = sum(size for _p, keys in plan for _k, size in keys)

for p, keys in plan:
    print(f"{p}: {len(keys)} objects, {sum(s for _k, s in keys)} bytes", flush=True)
if not apply_:
    for _p, keys in plan:
        for key, size in keys:
            print(f"  {key}\t{size}")
    print(f"DRY RUN: would delete {total_n} objects ({total_b} bytes). "
          f"Re-run with --apply to delete.")
    raise SystemExit(0)

deleted = 0
for p, keys in plan:
    for i, (key, _size) in enumerate(keys, 1):
        def once():
            status, _h, body = send("DELETE", key=key)
            # 404 = already gone (a re-run of a partial delete): success.
            check(status, body, f"DELETE {key}", ok=(200, 204, 404))
        retrying(f"DELETE {key}", once)
        deleted += 1
        if i % 100 == 0 or i == len(keys):
            print(f"  {p} {i}/{len(keys)}", flush=True)

left = []
for p, _keys in plan:
    remaining = list_keys(p)
    if remaining:
        left.append((p, len(remaining)))
if left:
    fail("DELETE INCOMPLETE: " + ", ".join(
        f"{n} objects remain under {p}" for p, n in left))
print(f"DELETE COMPLETE: {deleted} objects ({total_b} bytes) removed")
DELPY
