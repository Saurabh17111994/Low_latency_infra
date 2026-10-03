#!/usr/bin/env python3
"""seed_alerts — idempotent OpenObserve alert provisioning from the JSON corpus.

Ensures every alert in ``code/01_platform/01_docker/openobserve/alerts/*.json``
exists in the configured OpenObserve org (the storage/disk corpus today, and any
later corpus added to that directory). Mirrors ``seed_dashboards.py`` credential gate.

A fresh OpenObserve (the daily VM, or after a fresh start wiped the O2 volume)
has NO alerts and NO destination until the provisioning runs — see
``docs/05_deployment/CLOUDPE_DAILY_VM.md`` §3 and ``07-lake-archive-ops.md``.

  O2_API_URL  (default http://localhost:5080)
  O2_ORG      (default default)
  O2_USER     (default admin@example.com)
  O2_PASSWORD (REQUIRED)

Exit: 0 ok, 1 api error, 2 cred gate, 3 invalid file.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ALERT_DIR = ROOT / "code/01_platform/01_docker/openobserve/alerts"


def default_alert_files() -> list[Path]:
    """Every alert corpus in the alerts directory (sorted by file name).

    A single file per domain keeps the diff reviewable; the seeder carries
    whichever corpora exist, so adding a domain is adding a file.
    """
    return sorted(ALERT_DIR.glob("*.json"))


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _api(base: str, org: str, user: str, pwd: str, path: str, method="GET", body=None):
    # v0.91.5: v2 alerts live at /api/v2/{org}/... (v2 BEFORE the org id;
    # /api/{org}/v2/alerts 404s). Callers signal v2 with a "v2/" path prefix.
    if path.startswith("v2/"):
        url = f"{base.rstrip('/')}/api/v2/{org}/{path[len('v2/'):]}"
    else:
        url = f"{base.rstrip('/')}/api/{org}{path}"
    headers = {
        "Authorization": f"Basic {_b64(f'{user}:{pwd}')}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except urllib.error.URLError as e:
        raise SystemExit(f"exit 1: cannot reach {url}: {e.reason}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="print plan, change nothing")
    p.add_argument("--force", action="store_true", help="PUT existing alerts")
    p.add_argument("--file", action="append", default=[], metavar="PATH",
                   help="seed only these files (repeatable; default: every "
                        "*.json in openobserve/alerts/)")
    args = p.parse_args()

    pwd = os.environ.get("O2_PASSWORD", "")
    if not pwd:
        print("alert gate: O2_PASSWORD required (refuses to guess credentials).", file=sys.stderr)
        return 2
    base = os.environ.get("O2_API_URL", "http://localhost:5080")
    org = os.environ.get("O2_ORG", "default")
    user = os.environ.get("O2_USER", "admin@example.com")

    files = [Path(f) for f in args.file] if args.file else default_alert_files()
    if not files:
        print(f"exit 3: no alert files under {ALERT_DIR}", file=sys.stderr)
        return 3
    alerts: list[dict] = []
    owners: dict[str, Path] = {}
    for path in files:
        try:
            part = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as e:
            print(f"exit 3: cannot load {path}: {e}", file=sys.stderr)
            return 3
        if not isinstance(part, list) or not part:
            print(f"exit 3: {path} must be a non-empty JSON array", file=sys.stderr)
            return 3
        for a in part:
            if not isinstance(a, dict):
                print(f"exit 3: {path} elements must be objects, got {type(a).__name__}",
                      file=sys.stderr)
                return 3
            # stream_type is printed below and sent to O2: validate it here so a typo
            # is exit 3, not a KeyError traceback (P6-779).
            for field in ("name", "stream_name", "stream_type"):
                if not a.get(field):
                    print(f"exit 3: alert missing {field}: {a}", file=sys.stderr)
                    return 3
            # Two corpora claiming one name would make "which rule is live?"
            # unanswerable; fail closed instead of last-file-wins.
            if a["name"] in owners:
                print(f"exit 3: duplicate alert name {a['name']!r} in {path} "
                      f"(also in {owners[a['name']]})", file=sys.stderr)
                return 3
            owners[a["name"]] = path
            alerts.append(a)

    # The list is a read-only GET, so dry-run fetches it too: a plan that says
    # "create" for an alert that already exists is a wrong plan (P6-778).
    existing: dict[str, dict] = {}
    status, body = _api(base, org, user, pwd, "v2/alerts")
    if status == 200:
        try:
            data = json.loads(body)
            # O2 returns {"list": [...]} or {"data": [...]}
            lst = data.get("list") or data.get("data") or data.get("alerts") or []
            if not isinstance(lst, list):
                raise ValueError(f"unexpected catalog shape: {type(lst).__name__}")
            existing = {x.get("name"): x for x in lst if x.get("name")}
        except (ValueError, TypeError, AttributeError) as exc:
            # XC-14: a 200 we cannot parse is an UNKNOWN catalog, not an empty
            # one. Treating it as empty made dry-run print "create" for every
            # alert and a real run re-POST existing alerts as duplicates
            # (contradicts P6-778: "the plan can be trusted").
            print(f"exit 3: list alerts returned 200 but the body is not a "
                  f"readable catalog ({exc}) — refusing to plan against an "
                  "unknown catalog", file=sys.stderr)
            return 3
    else:
        print(f"warn: list alerts failed ({status}): {body[:200]} — treating as empty", file=sys.stderr)

    created = updated = untouched = failed = 0
    for alert in alerts:
        name = alert["name"]
        exists = name in existing
        if args.dry_run:
            action = "update" if (exists and args.force) else ("create" if not exists else "keep")
            print(f"  [{action:6s}] {name} -> {alert['stream_type']}/{alert['stream_name']}")
            if action == "create":
                created += 1
            elif action == "update":
                updated += 1
            else:
                untouched += 1
            continue
        if not exists:
            status, body = _api(base, org, user, pwd, "v2/alerts", "POST", alert)
            if status not in (200, 201):
                # One domain's rules must not block another's: a fresh O2 does
                # not have every metric stream yet (off-session compute streams
                # appear once signals flow), and the storage/disk rules must
                # still seed. The exit code stays non-zero when anything failed.
                print(f"warn: create {name} failed ({status}): {body[:200]} — continuing",
                      file=sys.stderr)
                failed += 1
                continue
            created += 1
        elif args.force:
            # PUT needs the id O2 assigned; without one the update cannot happen.
            aid = existing[name].get("id") or existing[name].get("alert_id")
            if not aid:
                print(f"warn: no id for existing alert '{name}' — force update skipped, "
                      f"counted as untouched", file=sys.stderr)
            if aid:
                status, body = _api(base, org, user, pwd, f"v2/alerts/{aid}", "PUT", alert)
                if status == 200:
                    updated += 1
                else:
                    print(f"warn: update {name} failed ({status}): {body[:200]} — continuing",
                          file=sys.stderr)
                    failed += 1
            else:
                untouched += 1
        else:
            untouched += 1

    print(f"RESULT: created={created} updated={updated} untouched={untouched} "
          f"failed={failed} ({len(alerts)} alerts)")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
