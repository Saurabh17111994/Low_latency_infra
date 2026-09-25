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
     is printed for attribution in SUMMARY.txt;
  5. drill-cost config — the tablet's effective remote.data.dir is read from its
     rendered server.yaml. While remote-log tiering is off (0s) a remote URI is
     pure cost: every KV bucket delete probes S3 (~1 s/bucket) on the tablet's
     single replica-state-change thread, and placements queue behind it (781 s of
     backlog measured on the prewarm drill, 2026-09-25). Local is required then;
     the tiering lanes set the R2 URI only inside their own recreate windows;
  6. orphan dirs — the tablet must carry no empty, non-live database/table
     directories. Fluss never deletes a dropped database's directory, so every
     drill iteration leaks one; they fed a SchemaNotExist retry storm on restart.
     Read-only via tablet-orphan-sweep.py --check (the drill step sweeps its own
     leftovers with the same script);
  7. tablet registry — advisory when /fluss/tabletservers/tables clearly leaks
     (stale drop entries accumulate until a tablet restart reconciles them).

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
ZK_DATABASES = "/fluss/metadata/databases"
ZK_REGISTRY = "/fluss/tabletservers/tables"
ZK_CLI = "/apache-zookeeper-3.9.2-bin/bin/zkCli.sh"
TABLET_CONTAINER = "01_docker-fluss-tablet-1"
ORPHAN_SWEEP = os.path.join(SCRIPT_DIR, "tablet-orphan-sweep.py")

# A URI scheme on remote.data.dir while remote-log tiering is off is pure drill cost:
# every KV bucket deletion then probes S3 (~1 s/bucket) on the tablet's single
# replica-state-change thread, and placements queue behind it. Measured 2026-09-25:
# 781 s of placement backlog in the prewarm drill (thread dump: KvManager
# .deleteRemoteKvSnapshot -> HadoopFileSystem.exists -> S3A HEAD/LIST).
REMOTE_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


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


def worktree_digest() -> str:
    """Fingerprint of the *working* state. A dirty repair loop must not share a
    stack_generation with the clean commit it started from, so the diff, the
    status list and the contents of untracked files all go in."""
    porcelain = run(["git", "-C", PROJECT_ROOT, "status", "--porcelain", "-uall"]).stdout
    parts = [run(["git", "-C", PROJECT_ROOT, "diff", "--binary", "HEAD"]).stdout, porcelain]
    for line in porcelain.splitlines():
        if not line.startswith("??"):
            continue
        try:
            with open(os.path.join(PROJECT_ROOT, line[3:].strip()), "rb") as fh:
                parts.append(hashlib.sha256(fh.read()).hexdigest())
        except OSError:
            parts.append("unreadable")
    return hashlib.sha256("".join(parts).encode()).hexdigest()[:16]


def tree_verdict(head: str, dirty: list[str], certifying: bool) -> tuple[list[str], str]:
    """Certifying runs need a frozen commit; a repair loop may run a dirty tree as
    long as the fingerprint says so. Returns (drift items, line to print)."""
    if not dirty:
        return [], f"  OK    tree clean at {head}"
    listed = "; ".join(dirty[:3]) + (" ..." if len(dirty) > 3 else "")
    if certifying:
        return ([f"working tree is dirty at {head}: {listed} (only a non-certifying "
                 "--steps/--sweep run may proceed)"], "")
    return [], (f"  WARN  tree dirty at {head} ({len(dirty)} path(s)) — non-certifying run, "
                f"the fingerprint covers the working state: {listed}")


def image_freshness(git_root: str) -> str:
    """Empty when the ddl-apply image matches the sources it was built from.

    Step 8 checks this too, but only after ~10 minutes of gate, and the recreate
    check above only sees an image the *containers* no longer match. Editing a
    ddl-apply source and forgetting to rebuild leaves both silent: the container
    still matches its image, and only the stamp knows the sources moved on.
    """
    script = os.path.join(git_root, "code", "01_platform", "04_scripts",
                          "image_staleness_check.py")
    if not os.path.isfile(script):
        return ""
    result = run([sys.executable, script, "--git-root", PROJECT_ROOT,
                  "--compose", COMPOSE_FILE, "--service", "ddl-apply"])
    if result.returncode == 0:
        return ""
    lines = (result.stdout or result.stderr).strip().splitlines()
    # Prefer the per-image reason ("STALE: stamp a != sources b") to the summary
    # line, so the drift item says *why* rather than just that something is wrong.
    detail = next((line for line in lines if "[FAIL]" in line),
                  lines[-1] if lines else "no output")
    return (f"ddl-apply image is not current: {detail} "
            "(rebuild: make ddl-image, or make images for every service)")


def recreate_verdict(recreated: list[str]) -> tuple[list[str], list[str]]:
    """Split a recreate plan into what the gate's verdicts depend on and what they
    do not.

    Step 11 applies DDL against the Fluss cluster, so a coordinator/tablet/
    zookeeper mid-recreation is real drift and must refuse. flink-jobmanager,
    flink-taskmanager and compute are not touched by any step (compute is a
    one-shot job that exited hours before the gate), and re-running them is
    ordinary stack churn that would otherwise refuse a certificate for nothing.
    """
    drift = [c for c in recreated if any(s in c for s in REQUIRED_SERVICES)]
    return drift, [c for c in recreated if c not in drift]


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


def tablet_remote_dir() -> tuple[str | None, str | None]:
    """(remote.data.dir, remote.log.task-interval-duration) from the running tablet.

    The rendered server.yaml carries remote.data.dir twice (image base + injected
    FLUSS_PROPERTIES); the last occurrence is the effective one, so the loop
    overwrites. (None, None) when the tablet cannot be read.
    """
    result = run(["docker", "exec", TABLET_CONTAINER, "sh", "-c",
                  "grep -E '^[[:space:]]*(remote[.]data[.]dir|"
                  "remote[.]log[.]task-interval-duration):' /opt/fluss/conf/server.yaml"])
    if result.returncode != 0:
        return None, None
    remote_dir = interval = None
    for line in result.stdout.splitlines():
        key, _, value = line.strip().partition(":")
        if key == "remote.data.dir":
            remote_dir = value.strip()
        elif key == "remote.log.task-interval-duration":
            interval = value.strip()
    return remote_dir, interval


def drill_cost_verdict(remote_dir: str, interval: str | None) -> str:
    """Empty when the drill-cost contract holds, else the drift text.

    Local paths (or file://) are always fine. A remote URI is fine while
    remote-log tiering is enabled (a tiering window); with tiering off it is pure
    cost and the prewarm drill pays it in placement backlog.
    """
    if not REMOTE_SCHEME.match(remote_dir) or remote_dir.startswith("file:"):
        return ""
    if interval is not None and interval in ("0", "0s"):
        return (f"tablet remote.data.dir is {remote_dir} while remote-log tiering is off "
                f"({interval}) — every KV bucket deletion then probes S3 (~1 s/bucket) and "
                f"placements queue behind it (+13 min on the prewarm drill, 2026-09-25). "
                f"Fix: set FLUSS_REMOTE_DATA_DIR=/tmp/fluss-remote-data in "
                f"code/01_platform/01_docker/.env and recreate the tablet (`make up`), or run "
                f"the tiering lane, which sets the R2 URI for its own recreate window")
    return ""


def orphan_dir_verdict() -> tuple[str | None, list[str], list[str]]:
    """(drift, prereq, lines) from tablet-orphan-sweep.py --check (read-only).

    Fluss never deletes a dropped database's directory, so drill fixtures leak
    empty dirs; on restart they fed a SchemaNotExist retry storm. The check is
    delegated so `make drill-live` and this preflight share one definition of
    "orphan".
    """
    if not os.path.isfile(ORPHAN_SWEEP):
        return None, [], []
    result = run([sys.executable, ORPHAN_SWEEP, "--check"])
    lines = (result.stdout or "").strip().splitlines()
    if result.returncode == 1:
        found = [line.strip() for line in lines if "orphan:" in line]
        shown = "; ".join(found[:3]) + (f" (+{len(found) - 3} more)" if len(found) > 3 else "")
        return (f"tablet has orphan fixture directories ({shown}) — remove them with "
                f"`python3 code/01_platform/04_scripts/tablet-orphan-sweep.py --sweep`; "
                f"`make drill-live` sweeps its own leftovers automatically"), [], lines
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        return None, [f"cannot verify tablet orphan directories: "
                      f"{detail[-1] if detail else 'no output'}"], []
    return None, [], lines


def main(certifying: bool = True) -> int:
    drift: list[str] = []
    prereq: list[str] = []

    if shutil.which("docker") is None:
        prereq.append("docker CLI not found on PATH")
    if not os.path.isfile(os.path.join(COMPOSE_DIR, "secrets.env")):
        prereq.append(f"{os.path.join(COMPOSE_DIR, 'secrets.env')} missing — "
                      "the canonical compose form cannot resolve")

    head, dirty = clean_tree()
    dirty_digest = worktree_digest() if dirty else ""
    tree_drift, tree_line = tree_verdict(head, dirty, certifying)
    drift.extend(tree_drift)
    if tree_line:
        print(tree_line)

    recreate: list[str] = []
    if not prereq:
        dry = run(canonical_compose() + ["up", "-d", "--dry-run"])
        if dry.returncode != 0:
            tail = (dry.stderr or dry.stdout).strip().splitlines()
            prereq.append(f"canonical compose invocation failed: {tail[-1] if tail else 'no output'}")
        else:
            recreate = recreate_services(dry.stdout + dry.stderr)
            if recreate:
                rec_drift, rec_churn = recreate_verdict(recreate)
                if rec_drift:
                    drift.append("compose would RECREATE " + ", ".join(rec_drift) +
                                 " — run `make up` (canonical form) before the gate")
                if rec_churn:
                    print("  WARN  compose would recreate " + ", ".join(rec_churn) +
                          " — no gate step uses them (advisory)")
            image_state = image_freshness(PROJECT_ROOT)
            if image_state:
                if dirty:
                    # A dirty tree is *supposed* to move the stamp; that is
                    # information for the repair loop, not drift.
                    print(f"  WARN  {image_state} (expected while the tree is dirty)")
                else:
                    drift.append(image_state)
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

    if not prereq:
        remote_dir, interval = tablet_remote_dir()
        if remote_dir is None:
            prereq.append("cannot read remote.data.dir from the running tablet "
                          "(needed to verify the drill-cost configuration)")
        else:
            cost = drill_cost_verdict(remote_dir, interval)
            if cost:
                drift.append(cost)
            elif REMOTE_SCHEME.match(remote_dir) and not remote_dir.startswith("file:"):
                print(f"  OK    tablet remote.data.dir {remote_dir} with remote-log tiering "
                      f"{interval}")
            else:
                print(f"  OK    tablet remote.data.dir local ({remote_dir}), remote-log tiering "
                      f"{interval or 'unset'}")
        orphan_drift, orphan_prereq, orphan_lines = orphan_dir_verdict()
        prereq.extend(orphan_prereq)
        if orphan_drift:
            drift.append(orphan_drift)
        for index, line in enumerate(orphan_lines):
            print(f"  {'INFO' if index == 0 else '    '}  {line.strip()}")

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
        tree_material = f"head={head}"
        if dirty_digest:
            tree_material += f"|worktree={dirty_digest}"
        tree_hash = hashlib.sha256(tree_material.encode()).hexdigest()[:16]
        material = "|".join(
            f"{s}:{(by_service.get(s) or {}).get('ID', '?')}:{(by_service.get(s) or {}).get('Image', '?')}"
            for s in REQUIRED_SERVICES)
        material += f"|catalog={live}|recreate={len(recreate)}|head={head}"
        if dirty_digest:
            material += f"|worktree={dirty_digest}"
        stack_generation = hashlib.sha256(material.encode()).hexdigest()[:16]
        print(f"  OK    stack_generation={stack_generation} "
              f"(head {head}, catalog {live}, recreations {len(recreate)})")
        # The fingerprint is the pair the replay memo keys on: this tree, on this
        # stack. Same pair again -> the second green is a replay, not new evidence.
        print(f"  OK    fingerprint={tree_hash}:{stack_generation}")

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
    # Fail closed: only an explicit --allow-dirty lets a dirty tree through.
    sys.exit(main(certifying="--allow-dirty" not in sys.argv[1:]))
