#!/usr/bin/env python3
"""alert-routing-selftest — mechanical proof that G6 alert routing works.

Standing-rule guard (levers map gotcha-15: every fix gets a guard): proves
the FULL chain O2 alert rule -> dev-webhook destination -> alert-consumer
JSONL persistence, end to end, on demand. CHG-093 proved delivery once with
a stdout receiver; this selftest proves the durable consumer and can be
re-run after any routing change.

Steps:
  1. healthz via the consumer (docker exec in O2's netns — port 9999 is not
     published to the host).
  2. NEGATIVE: POST a malformed body -> expect HTTP 400 and the consumer
     still alive after (a crash here would lose every future delivery).
  3. Create a temporary always-firing alert on the `up` stream
     (value >= 0 — always true), destination dev-webhook.
  4. Poll the consumer's GET /alerts until the temp alert is recorded
     (O2 scheduled granularity ~1 min; budget 4 min).
  5. Assert the record is classified correctly (severity/class).
  6. Delete the temp alert (always, even on failure).

Usage (env: O2_PASSWORD required; O2_API_URL/O2_ORG/O2_USER optional):
  O2_PASSWORD=... python3 code/01_platform/04_scripts/alert-routing-selftest.py

Exit: 0 pass, 1 fail (message on stderr).
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("O2_API_URL", "http://localhost:5080")
ORG = os.environ.get("O2_ORG", "default")
USER = os.environ.get("O2_USER", "admin@example.com")
TEST_ALERT = "selftest-g6-routing-probe"
CONTAINER = os.environ.get(
    "ALERT_CONSUMER_CONTAINER", "01_docker-alert-consumer-1"
)
POLL_BUDGET_S = 240
POLL_INTERVAL_S = 10


def fail(msg: str) -> "NoReturn":  # type: ignore[valid-type]
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def _safe_json(raw: str) -> dict:
    """Body -> dict. A proxy's HTML error page must not become a traceback."""
    try:
        return json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {"raw": raw[:200]}


def o2_api(path: str, method: str = "GET", body: dict | None = None):
    """Call the O2 v2 API (v2 BEFORE the org id — v0.91.5 quirk)."""
    url = f"{BASE.rstrip('/')}/api/v2/{ORG}/{path.lstrip('/')}"
    pwd = os.environ.get("O2_PASSWORD", "")
    if not pwd:
        fail("O2_PASSWORD required (secrets.env)")
    headers = {
        "Authorization": f"Basic {base64.b64encode(f'{USER}:{pwd}'.encode()).decode()}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, _safe_json(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, _safe_json(e.read().decode())
    except (urllib.error.URLError, TimeoutError) as e:
        # Unreachable, refused or slow: this script exists to localise routing
        # breakage, so say which endpoint failed instead of raising.
        fail(f"O2 API unreachable at {url}: {e}")


def consumer(path: str, method: str = "GET", body: str | None = None) -> tuple[int, str]:
    """Reach the consumer inside O2's netns via docker exec (python image)."""
    py = (
        "import sys,urllib.request;"
        f"req=urllib.request.Request('http://127.0.0.1:9999{path}',"
        f"method='{method}'"
        + (
            f",data=sys.argv[1].encode(),headers={{'Content-Type':'application/json'}}"
            if body
            else ""
        )
        + ");\n"
        "try:\n"
        " r=urllib.request.urlopen(req,timeout=15);print(r.status);print(r.read().decode())\n"
        "except urllib.error.HTTPError as e:print(e.code);print(e.read().decode())"
    )
    cmd = ["docker", "exec", CONTAINER, "python3", "-c", py] + ([body] if body else [])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        fail(f"docker exec to consumer failed: {r.stderr.strip()[:300]}")
    lines = r.stdout.strip().split("\n")
    return int(lines[0]), "\n".join(lines[1:])


def main() -> int:
    print(f"alert-routing selftest: O2={BASE} org={ORG} consumer={CONTAINER}")

    # --- 1. consumer alive --------------------------------------------------
    code, out = consumer("/healthz")
    if code != 200 or '"ok": true' not in out:
        fail(f"consumer healthz -> {code} {out[:200]}")
    print("1. consumer healthz OK")

    # --- 2. negative: malformed body rejected, consumer survives ------------
    code, _ = consumer("/noop", method="POST", body="not-json{")
    if code != 400:
        fail(f"malformed delivery accepted (HTTP {code}) — silent corruption risk")
    code, out = consumer("/healthz")
    if code != 200:
        fail("consumer crashed on malformed body")
    print("2. malformed delivery rejected with 400, consumer alive")

    # --- 3. temp always-firing alert ---------------------------------------
    # Delete a leftover from an earlier aborted run first (idempotent start).
    status, existing = o2_api("alerts")
    for a in (existing.get("list") or existing.get("data") or []):
        if a.get("name") == TEST_ALERT:
            st, resp = o2_api(f"alerts/{a.get('id')}", "DELETE")
            if st not in (200, 202, 204):
                fail(f"cannot delete leftover {TEST_ALERT} (HTTP {st}: "
                     f"{json.dumps(resp)[:200]}) — leaving it would keep it firing")
            print(f"   deleted leftover {TEST_ALERT}")
    body = {
        "name": TEST_ALERT,
        "stream_type": "metrics",
        "stream_name": "up",
        "is_real_time": False,
        "query_condition": {
            "type": "custom",
            "sql": None,
            "promql": None,
            "conditions": [
                {"column": "value", "operator": ">=", "value": 0, "ignore_case": False}
            ],
            "aggregation": None,
            "vrl_function": None,
        },
        "trigger_condition": {
            "period": 1,
            "operator": ">=",
            "threshold": 1,
            "frequency": 1,
            "silence": 0,
            "frequency_type": "minutes",
            "timezone": "UTC",
            "tolerance_in_secs": None,
        },
        "enabled": True,
        "destinations": ["dev-webhook"],
        "description": "SELFTEST temp alert — always fires; deleted by the run",
        "tz_offset": 0,
        "row_template": f"Alert {TEST_ALERT}",
        "row_template_type": "String",
    }
    status, resp = o2_api("alerts", "POST", body)
    if status not in (200, 201):
        fail(f"create temp alert -> {status}: {json.dumps(resp)[:300]}")
    print(f"3. temp alert created (always-firing, silence=0)")

    # --- 4-5. poll the durable record ---------------------------------------
    deadline = time.monotonic() + POLL_BUDGET_S
    record = None
    while time.monotonic() < deadline:
        code, out = consumer("/alerts?limit=200")
        if code == 200:
            for rec in json.loads(out).get("alerts", []):
                if rec.get("name") == TEST_ALERT:
                    record = rec
                    break
        if record:
            break
        print(f"   waiting for delivery ({int(deadline - time.monotonic())}s budget left)")
        time.sleep(POLL_INTERVAL_S)

    # --- 6. cleanup FIRST, then assert (never leak the always-firing alert) --
    cleaned = False
    status, existing = o2_api("alerts")
    for a in (existing.get("list") or existing.get("data") or []):
        if a.get("name") == TEST_ALERT:
            st, resp = o2_api(f"alerts/{a.get('id')}", "DELETE")
            if st not in (200, 202, 204):
                fail(f"cleanup delete failed for {TEST_ALERT} (HTTP {st}: "
                     f"{json.dumps(resp)[:200]}) — the always-firing probe is STILL enabled")
            cleaned = True
    if not record:
        fail(f"alert never reached the consumer within {POLL_BUDGET_S}s "
             f"(temp alert deleted={cleaned}) — routing chain broken")
    if record.get("severity") != "info" or record.get("class") != "other":
        fail(f"classification wrong: {record}")
    if not record.get("received_at"):
        fail(f"record missing received_at: {record}")
    print(f"4. delivery recorded: severity={record['severity']} class={record['class']} "
          f"at {record['received_at']}")
    print(f"5. temp alert deleted: {cleaned}")
    print("PASS: O2 -> dev-webhook -> alert-consumer JSONL routing verified end-to-end")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
