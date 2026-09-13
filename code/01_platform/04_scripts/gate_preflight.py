#!/usr/bin/env python3
"""Gate preflight: fail closed on environment drift BEFORE step 1 of the monday gate.

Why this exists: three gate attempts on 2026-09-13 were lost to environment drift
that the gate only discovered 10+ minutes in — a wiped Fluss catalog surfaced at
step 9 (attempt 22), and a compose-form divergence that recreated
fluss-coordinator/fluss-tablet mid-run surfaced at step 11 (attempts 24 and 26,
after ~9.5 min of steps). Both are detectable in seconds, before step 1, and both
have an exact remedy (`make up`, which uses the canonical env-file compose form).

Read-only: this never starts, stops, or recreates anything. It reports, and the
operator (or the wrapper, which already runs `make up`) converges the stack.

Checks
  1. frozen tree  — working tree clean, HEAD recorded;
  2. convergence — the canonical compose form takes no `Recreate` action, using
     compose's own decision function (`up -d --dry-run`). A bare `-f` invocation
     resolves different values for every service that interpolates a secret from
     secrets.env (compose auto-loads .env but not secrets.env) and reports 22
     recreate actions, so a divergent caller silently restarts the cluster
     underneath the gate;
  3. catalog — live table count under /fluss/metadata/databases/default/tables is
     at least the manifest's table count (the wiped-catalog class);
  4. stack — the containers the live steps need are running, and the
     stack-generation stamp (container ids + images + catalog count + convergence)
     is printed for attribution in SUMMARY.txt.

Exit codes: 0 = verified; 1 = drift (do not start the gate); 2 = prerequisite
missing (the caller must record a SKIP, never a pass — see run-monday-gates.sh).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
COMPOSE_DIR = os.path.join(PROJECT_ROOT, "code", "01_platform", "01_docker")
COMPOSE_FILE = os.path.join(COMPOSE_DIR, "docker-compose.yml")
MANIFEST = os.path.join(PROJECT_ROOT, "code", "01_platform", "02_sql", "ddl",
                        "schema_manifest.json")

# The live steps (9, 11, 12, 16) need these; one-shot services are excluded.
REQUIRED_SERVICES = ("fluss-coordinator", "fluss-tablet", "zookeeper")
ZK_CONTAINER = "01_docker-zookeeper-1"
ZK_PATH = "/fluss/metadata/databases/default/tables"
ZK_CLI = "/apache-zookeeper-3.9.2-bin/bin/zkCli.sh"


def canonical_compose() -> list[str]:
    """The one compose form for this stack; must equal the Makefile's $(COMPOSE)."""
    return [
        "docker", "compose",
        "--env-file", os.path.join(COMPOSE_DIR, ".env"),
        "--env-file", os.path.join(COMPOSE_DIR, "secrets.env"),
        "-f", COMPOSE_FILE,
    ]


def recreate_services(dry_run_output: str) -> list[str]:
    """Services compose would RECREATE, from `up -d --dry-run` output.

    compose prints an action line and, for actions that complete, a past-tense
    line ("... Recreate" then "... Recreated"); only the action lines count, and
    each service is reported once. `Created` (a one-shot service that is not
    running yet) is not drift.
    """
    services: list[str] = []
    for line in dry_run_output.splitlines():
        if "Recreate" not in line or "Recreated" in line:
            continue
        match = re.search(r"Container\s+(\S+)\s+Recreate\b", line)
        if match and match.group(1) not in services:
            services.append(match.group(1))
    return services


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def clean_tree() -> tuple[str, list[str]]:
    head = run(["git", "-C", PROJECT_ROOT, "rev-parse", "--short", "HEAD"]).stdout.strip()
    porcelain = run(["git", "-C", PROJECT_ROOT, "status", "--porcelain"]).stdout.strip()
    return head, [line for line in porcelain.splitlines() if line.strip()]


def stack_containers() -> list[dict]:
    """`compose ps` for this project under the canonical form (tolerant of both
    the JSON-array and the JSON-lines output compose versions use)."""
    result = run(canonical_compose() + ["ps", "--format", "json", "--all"])
    if result.returncode != 0:
        return []
    text = result.stdout.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        rows = []
        for line in text.splitlines():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows


def catalog_tables() -> int | None:
    result = run(["docker", "exec", ZK_CONTAINER, ZK_CLI, "-server", "127.0.0.1:2181",
                  "ls", ZK_PATH])
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("["):
            return len([entry for entry in line.strip("[]").split(",") if entry.strip()])
    return None


def manifest_tables() -> int | None:
    try:
        with open(MANIFEST, encoding="utf-8") as handle:
            return len(json.load(handle)["tables"])
    except (OSError, KeyError, json.JSONDecodeError):
        return None


def main() -> int:
    drift: list[str] = []
    prereq: list[str] = []

    if shutil.which("docker") is None:
        prereq.append("docker CLI not found on PATH")
    if not os.path.isfile(os.path.join(COMPOSE_DIR, "secrets.env")):
        prereq.append(f"{os.path.join(COMPOSE_DIR, 'secrets.env')} missing — "
                      "the canonical compose form cannot resolve")

    head, dirty = clean_tree()
    if dirty:
        drift.append(f"working tree is dirty at {head}: "
                     f"{'; '.join(dirty[:3])}{' ...' if len(dirty) > 3 else ''}")
    else:
        print(f"  OK    tree clean at {head}")

    recreate: list[str] = []
    if not prereq:
        dry = run(canonical_compose() + ["up", "-d", "--dry-run"])
        if dry.returncode != 0:
            tail = (dry.stderr or dry.stdout).strip().splitlines()
            prereq.append(f"canonical compose invocation failed: {tail[-1] if tail else 'no output'}")
        else:
            recreate = recreate_services(dry.stdout + dry.stderr)
            if recreate:
                drift.append("compose would RECREATE " + ", ".join(recreate) +
                             " — run `make up` (canonical form) before the gate")
            else:
                print("  OK    no recreation under the canonical compose form")

    live = catalog_tables() if not prereq else None
    expected = manifest_tables()
    if expected is None:
        prereq.append("cannot read the table count from the DDL schema_manifest.json")
    elif live is None:
        prereq.append("cannot read the live catalog from zookeeper")
    elif live < expected:
        drift.append(f"catalog has {live}/{expected} tables (wiped or partial) — run `make up`")
    else:
        print(f"  OK    catalog {live}/{expected} tables")

    rows = stack_containers() if not prereq else []
    by_service = {row.get("Service"): row for row in rows}
    for service in REQUIRED_SERVICES:
        row = by_service.get(service)
        state = (row or {}).get("State", "absent")
        if state == "running":
            print(f"  OK    {service} running")
        else:
            drift.append(f"{service} is {state} (live steps need it up)")

    if not prereq:
        material = "|".join(
            f"{s}:{(by_service.get(s) or {}).get('ID', '?')}:{(by_service.get(s) or {}).get('Image', '?')}"
            for s in REQUIRED_SERVICES)
        material += f"|catalog={live}|recreate={len(recreate)}|head={head}"
        print(f"  OK    stack_generation={hashlib.sha256(material.encode()).hexdigest()[:16]} "
              f"(head {head}, catalog {live}, recreations {len(recreate)})")

    if prereq:
        for reason in prereq:
            print(f"  SKIP  {reason}")
        print("PREFLIGHT: SKIP — prerequisite missing (the caller must record a skip, not a pass)")
        return 2
    if drift:
        for reason in drift:
            print(f"  DRIFT {reason}")
        print(f"PREFLIGHT: FAIL ({len(drift)} drift item(s)) — do NOT start the gate")
        return 1
    print(f"PREFLIGHT: PASS (HEAD {head})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
