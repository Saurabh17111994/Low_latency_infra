#!/usr/bin/env python3
"""o2_ingest.py — push JSON records into an OpenObserve stream (_bulk).

Single source of truth policy (2026-09-02 decline hunt): every observability
artifact produced by host-side capture scripts also lands in OpenObserve so
metrics + logs + traces live in one queryable place. This tool is the
ingestion half for artifacts that do not originate from an OTLP pipeline:
  host iostat latency samples   -> stream host_io_latency
  Flink checkpoint events       -> stream flink_checkpoints

Usage:
  o2_ingest.py STREAM < records.jsonl
Each input line must be one JSON object. Records get "@timestamp" (now, ms)
unless they already carry one. Ingestion is best-effort BY DESIGN for capture
scripts (the local evidence file remains raw truth); exit codes still say what
happened so callers can log:
  0 = all records accepted (HTTP 200 + status ok)
  3 = config error (no O2_AUTH_BASIC; see reason)
  4 = O2 refused (non-200, or 200 with a non-JSON body — something other
      than O2 answered; reason + body prefix in stderr)
  5 = zero records on stdin (nothing to do, not an error for callers)
Auth resolution: env O2_AUTH_BASIC, else code/01_platform/01_docker/secrets.env
(never printed). Endpoint: ${O2_URL:-http://localhost:5080}/api/default.
"""
import json
import os
import sys
import urllib.request

# o2_base/cwd-independent default: script lives in 04_scripts; secrets.env in
# 01_docker. Walk up from $0.
_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_SECRETS = os.path.join(_HERE, "..", "01_docker", "secrets.env")


def _secrets_path() -> str:
    """The secrets file consulted when O2_AUTH_BASIC is not in the env.

    Single resolver: the error message below must name the same file the read
    actually used (O2_SECRETS_FILE override), not always the built-in default.
    """
    return os.environ.get("O2_SECRETS_FILE", _DEFAULT_SECRETS)


def _auth_basic() -> str:
    val = os.environ.get("O2_AUTH_BASIC", "")
    if val:
        return val
    path = _secrets_path()
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("O2_AUTH_BASIC="):
                    return line.rstrip("\n").split("=", 1)[1]
    except OSError:
        pass
    return ""


def main() -> int:
    stream = sys.argv[1] if len(sys.argv) > 1 else ""
    if not stream or not stream.replace("-", "").replace("_", "").isalnum():
        print("o2_ingest: REFUSED — pass a stream name (alnum/-/_), got: "
              f"{stream!r}", file=sys.stderr)
        return 3

    records = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"o2_ingest: skip non-JSON line: {e}", file=sys.stderr)
            continue
        if not isinstance(rec, dict):
            print("o2_ingest: skip non-object record", file=sys.stderr)
            continue
        records.append(rec)

    if not records:
        print("o2_ingest: nothing to ingest", file=sys.stderr)
        return 5

    auth = _auth_basic()
    if not auth:
        print("o2_ingest: REFUSED — no O2_AUTH_BASIC (env or "
              f"{_secrets_path()}); cannot authenticate to OpenObserve",
              file=sys.stderr)
        return 3

    # Per-stream _json endpoint accepts a JSON array of records (verified
    # live 2026-09-02: _bulk org-level refused NDJSON via curl body handling;
    # _json returned 200 + successful:N — simplest correct shape).
    import time as _time
    for rec in records:
        rec.setdefault("@timestamp", int(_time.time() * 1000))
    body = json.dumps(records)

    url = (os.environ.get("O2_URL", "http://localhost:5080")
           + f"/api/default/{stream}/_json")
    req = urllib.request.Request(
        url, data=body.encode("utf-8"), method="POST",
        headers={"Authorization": f"Basic {auth}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", "replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                # A 200 that is not JSON means something other than O2 answered
                # (proxy, portal, login page). Refuse (documented exit 4) instead
                # of raising JSONDecodeError through the caller.
                print(f"o2_ingest: O2 refused (http={resp.status}, non-JSON body): "
                      f"{raw[:200]}", file=sys.stderr)
                return 4
            # _json responds {"code":200,"status":[{"name":...,"successful":N,
            # "failed":M}]} — fail if any record failed or top-level code>=400.
            code = payload.get("code", resp.status)
            stats = payload.get("status", [])
            failed = sum(s.get("failed", 0) for s in stats
                         if isinstance(s, dict))
            if resp.status != 200 or code >= 400 or failed:
                print(f"o2_ingest: O2 refused (http={resp.status} "
                      f"code={code} failed_records={failed}): "
                      f"{str(payload)[:200]}", file=sys.stderr)
                return 4
            print(f"o2_ingest: OK — {len(records)} record(s) -> "
                  f"{stream}", file=sys.stderr)
            return 0
    except urllib.error.HTTPError as e:
        print(f"o2_ingest: O2 refused (http={e.code}): "
              f"{e.read().decode('utf-8', 'replace')[:200]}", file=sys.stderr)
        return 4
    except (urllib.error.URLError, OSError) as e:
        print(f"o2_ingest: O2 unreachable: {e}", file=sys.stderr)
        return 4


if __name__ == "__main__":
    sys.exit(main())
