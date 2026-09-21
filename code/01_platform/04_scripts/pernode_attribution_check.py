#!/usr/bin/env python3
"""pernode_attribution_check.py — every node agent must report under its own address.

Why this exists
---------------
The collector scraped each node agent through its Swarm service name. A service
name resolves to EVERY task of that service, but a Prometheus `static_configs`
target keeps ONE address per name: measured 2026-09-21 against a real name with
eight A records, `static_configs` produced one target labelled with the name
while `dns_sd_configs` produced eight, one per address. Per-node metrics
therefore arrived as a single series (`tasks.node-exporter:9100`), the fleet
looked like one machine, and no deploy ever failed. CHG-283 moved both infra
jobs to `dns_sd_configs`; the acceptance is the query below — one address per
node. This check fails if any instance is still a `tasks.` name, so the old
shape cannot come back unnoticed.

Auth and transport follow soak-o2-evidence.py: env `O2_AUTH_BASIC`, else the
`O2_AUTH_BASIC=` line of `O2_SECRETS_FILE` (default
code/01_platform/01_docker/secrets.env). The credential is never printed. Plain
http on a non-local host is refused (exit 2) unless `O2_ALLOW_INSECURE_HTTP=1`.

Usage:
  pernode_attribution_check.py --expect 2
      [--url http://localhost:5080] [--stream node_boot_time_seconds]
      [--column instance] [--minutes 60] [--size 50]

Exit 0 = at least `--expect` distinct instances and every one an address; exit 1
= the check failed (a task name, a malformed instance, too few instances, or no
samples in the window); exit 2 = usage, missing credential, refused transport,
or O2 could not answer — an outage, which is not a failed check.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_SECRETS = os.path.join(_HERE, "..", "01_docker", "secrets.env")

# The pre-CHG-283 label. A Swarm service name resolves to every task, but a
# static_configs target keeps only one of them, so a fleet behind this name
# collapses into a single series.
_TASK_NAME = re.compile(r"^tasks\.")

# host:port — an IPv4 address or a hostname, then a port.
_ADDRESS = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*:\d{1,5}$")


def _auth_basic() -> str:
    """The O2 Basic credential (P6-029): env first, then the secrets file."""
    val = os.environ.get("O2_AUTH_BASIC", "")
    if val:
        return val
    path = os.environ.get("O2_SECRETS_FILE", _DEFAULT_SECRETS)
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("O2_AUTH_BASIC="):
                    return line.rstrip("\r\n").split("=", 1)[1]
    except OSError:
        pass
    return ""


def _check_transport(base: str) -> str:
    """Basic auth over plain http is only acceptable on this machine (P6-789)."""
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme != "http":
        return ""
    host = (parsed.hostname or "").lower()
    if host in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
        return ""
    return (f"O2_URL={base} is plain http on a non-local host ({host}) — the "
            "Basic Authorization header would be sent unencrypted; use https:// "
            "or set O2_ALLOW_INSECURE_HTTP=1 to accept the risk")


def _search(base: str, auth: str, sql: str, start_us: int, end_us: int,
            size: int) -> tuple[dict | None, str]:
    """One metrics SQL search -> (payload|None, reason).

    `?type=metrics` is not optional: without it the search goes to the LOGS
    streams and answers 400 "Search stream not found" (measured 2026-09-21).
    """
    body = json.dumps({"query": {"sql": sql, "start_time": start_us,
                                 "end_time": end_us, "size": size}}).encode()
    req = urllib.request.Request(
        f"{base}/api/default/_search?type=metrics",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Basic {auth}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp), ""
    except urllib.error.HTTPError as e:
        # A 4xx means O2 is up and answered: the stream or the query is the
        # problem, which is a different operator action from an outage.
        return None, (f"O2 answered HTTP {e.code} for this query — the stream or "
                      "the query is the problem, not the network")
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as e:
        return None, f"O2 unreachable ({e.__class__.__name__})"


def _instances(payload: dict, column: str) -> list[str]:
    """The distinct values, sorted — O2 answers flat rows, `_source` optional."""
    out: list[str] = []
    for hit in payload.get("hits") or []:
        row = hit.get("_source") or hit
        value = row.get(column)
        if isinstance(value, str) and value:
            out.append(value)
    return sorted(set(out))


def _problems(instances: list[str], expect: int, minutes: int, stream: str,
              column: str) -> list[str]:
    """Every reason the check failed, so one run reports all of them."""
    if not instances:
        return [f'no {column} values in the last {minutes}m of "{stream}" — the '
                "stream has no recent samples. Look at the collector and the "
                "scrape targets, not at the label."]
    problems: list[str] = []
    tasks = [i for i in instances if _TASK_NAME.match(i)]
    if tasks:
        problems.append(
            "these are Swarm task names, not addresses: " + ", ".join(tasks) +
            " — a service name resolves to every task, but a static_configs "
            "target keeps only one of them, so the fleet collapses into a "
            "single series (CHG-283)")
    malformed = [i for i in instances if not _ADDRESS.match(i)]
    if malformed:
        problems.append("these do not look like host:port: " + ", ".join(malformed))
    if len(instances) < expect:
        problems.append(
            f"expected at least {expect} distinct instance(s) — one per node — "
            f"but found {len(instances)}")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Fail unless every node agent reports under its own address.")
    ap.add_argument("--expect", type=int, required=True, metavar="N",
                    help="how many nodes the fleet has (one instance each)")
    ap.add_argument("--url", default=os.environ.get("O2_URL", "http://localhost:5080"))
    ap.add_argument("--stream", default="node_boot_time_seconds")
    ap.add_argument("--column", default="instance")
    ap.add_argument("--minutes", type=int, default=60,
                    help="query window; a node that stopped reporting earlier is "
                         "deliberately not counted")
    ap.add_argument("--size", type=int, default=50,
                    help="row cap; must exceed the node count or the fleet is "
                         "undercounted")
    args = ap.parse_args(argv)

    if args.expect < 1:
        print("pernode-attribution: --expect must be at least 1", file=sys.stderr)
        return 2

    base = args.url.rstrip("/")
    refusal = _check_transport(base)
    if refusal and os.environ.get("O2_ALLOW_INSECURE_HTTP") != "1":
        print(f"pernode-attribution: REFUSED — {refusal}", file=sys.stderr)
        return 2

    auth = _auth_basic()
    if not auth:
        print("pernode-attribution: REFUSED — no O2_AUTH_BASIC (env or "
              f"{os.environ.get('O2_SECRETS_FILE', _DEFAULT_SECRETS)})",
              file=sys.stderr)
        return 2

    now_us = int(time.time() * 1_000_000)
    sql = f'select distinct({args.column}) from "{args.stream}"'
    payload, reason = _search(base, auth, sql, now_us - args.minutes * 60_000_000,
                              now_us, args.size)
    if payload is None:
        print(f"pernode-attribution: {reason}", file=sys.stderr)
        return 2

    instances = _instances(payload, args.column)
    problems = _problems(instances, args.expect, args.minutes, args.stream,
                         args.column)
    if problems:
        for p in problems:
            print(f"pernode-attribution: FAIL — {p}", file=sys.stderr)
        for i in instances:
            print(f"  {i}", file=sys.stderr)
        return 1

    print(f'pernode-attribution: OK — {len(instances)} distinct instance(s) '
          f'in the last {args.minutes}m of "{args.stream}"')
    for i in instances:
        print(f"  {i}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
