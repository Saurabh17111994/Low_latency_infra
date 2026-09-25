#!/usr/bin/env python3
"""Remove the empty directories a Fluss DROP DATABASE leaves on the tablet.

Why this exists (2026-09-25): Fluss deletes every table directory of a dropped
table (ReplicaManager.stopReplicas -> dropEmptyTableOrPartitionDir) but never the
*database* directory, and a tablet kill mid-teardown can leave an empty table
directory. Every drill iteration that creates and drops a database therefore
leaks one empty dir; 177 of them (105 db + 72 table, ~1.92 GiB) had accumulated by
2026-09-25 and on restart they fed a SchemaNotExist retry storm that delayed
placements (thread + probe evidence:
logs/soak/tablet-orphan-prune-20260925T152110Z/).

Safety (fail-closed):
  * only directories EMPTY at two scans ~3 s apart are considered (a mid-drop
    teardown can transiently show an empty dir);
  * a directory is removed only when its database -- or, for a depth-2
    {db}/{table}_{id} dir, its table -- is NOT in the live ZooKeeper catalog;
  * dot-dirs, remote-log-index-cache and top-level files are never touched;
  * removal is `rmdir` only: a non-empty directory is refused by the kernel.

Modes:
  --check   report orphan dirs plus a tablet-registry advisory; exit 1 when any
            orphan dir exists (read-only; called by gate_preflight.py).
  --sweep   remove the orphan dirs (called by `make drill-live` after the drill
            classes).

Exit codes: 0 = clean/swept; 1 = orphans found in --check; 3 = cannot verify
(tablet or ZooKeeper unreachable, or a removal was refused).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time

TABLET_CONTAINER = "01_docker-fluss-tablet-1"
ZK_CONTAINER = "01_docker-zookeeper-1"
ZK_CLI = "/apache-zookeeper-3.9.2-bin/bin/zkCli.sh"
ZK_SERVER = "127.0.0.1:2181"
ZK_DATABASES = "/fluss/metadata/databases"
ZK_INDEX = "/fluss/tabletservers/tables"
DATA_DIR = "/tmp/fluss/data"

# Never considered, even when empty: Fluss's own caches and any dot-dir.
SKIP_TOP_LEVEL = {".historical-lookup-cache", "remote-log-index-cache"}


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def zk_ls(path: str) -> list[str]:
    result = run(["docker", "exec", ZK_CONTAINER, ZK_CLI, "-server", ZK_SERVER, "ls", path])
    if result.returncode != 0:
        raise RuntimeError(f"zkCli ls {path} failed: {result.stderr.strip() or result.stdout.strip()}")
    text = (result.stdout or "") + "\n" + (result.stderr or "")
    if "Node does not exist" in text:
        return []
    if result.returncode != 0:
        raise RuntimeError(f"zkCli ls {path} failed: {result.stderr.strip() or result.stdout.strip()}")
    # zkCli interleaves INFO logs on both streams; the node list is the one line
    # that is exactly [...] (log lines carry a timestamp before their brackets).
    matches = re.findall(r"^\[(.*)\]\s*$", text, re.MULTILINE)
    if not matches:
        return []
    return [entry.strip() for entry in matches[-1].split(",") if entry.strip()]


def live_catalog() -> tuple[set[str], dict[str, set[str]]]:
    """Live databases and their tables, from the coordinator's ZooKeeper catalog."""
    databases = zk_ls(ZK_DATABASES)
    tables: dict[str, set[str]] = {}
    for database in databases:
        try:
            tables[database] = set(zk_ls(f"{ZK_DATABASES}/{database}/tables"))
        except RuntimeError:
            # A database without a tables znode is simply empty.
            tables[database] = set()
    return set(databases), tables


def tablet_empty_dirs() -> set[str]:
    """Empty dirs under {DATA_DIR}, at depth 1 (databases) and 2 (tables)."""
    result = run(["docker", "exec", TABLET_CONTAINER, "find", DATA_DIR,
                  "-mindepth", "1", "-maxdepth", "2", "-type", "d", "-empty", "-print"])
    if result.returncode != 0:
        raise RuntimeError(
            f"cannot scan {TABLET_CONTAINER}:{DATA_DIR}: "
            f"{result.stderr.strip() or result.stdout.strip()}")
    prefix = DATA_DIR.rstrip("/") + "/"
    return {line[len(prefix):].strip() for line in result.stdout.splitlines()
            if line.startswith(prefix) and line[len(prefix):].strip()}


def classify(candidates: set[str], databases: set[str],
             tables: dict[str, set[str]]) -> list[tuple[str, str]]:
    orphans: list[tuple[str, str]] = []
    for relative in sorted(candidates):
        parts = relative.split("/")
        if parts[0].startswith(".") or parts[0] in SKIP_TOP_LEVEL:
            continue
        # Defence in depth: never hand a path with traversal or an absolute root to rmdir.
        if any(part in ("", "..") for part in parts):
            continue
        if len(parts) == 1:
            if parts[0] not in databases:
                orphans.append((relative, f"database {parts[0]!r} is not in the live catalog"))
        elif len(parts) == 2:
            database, name = parts
            if database not in databases:
                orphans.append((relative, f"database {database!r} is not in the live catalog"))
                continue
            table = re.sub(r"_\d+$", "", name)
            known = tables.get(database, set())
            if table not in known and name not in known:
                orphans.append((relative, f"table {database}.{name!r} is not in the live catalog"))
    return orphans


def registry_advisory() -> str:
    """A stale tablet registry self-reconciles on restart; only alarm on a leak."""
    try:
        entries = len(zk_ls(ZK_INDEX))
        databases, tables = live_catalog()
        live_total = sum(len(names) for names in tables.values())
    except RuntimeError as error:
        return f"  warn: cannot read the tablet registry: {error}"
    if live_total and entries > max(live_total * 2, live_total + 16):
        return (f"  warn: tablet registry has {entries} entries for {live_total} live tables "
                f"(databases {len(databases)}) — stale drop entries accumulate until a "
                f"tablet restart reconciles them")
    return f"  ok: tablet registry {entries} entries for {live_total} live tables"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true",
                      help="report orphan dirs (default); read-only")
    mode.add_argument("--sweep", action="store_true",
                      help="remove the orphan dirs instead of only reporting them")
    parser.add_argument("--stabilize-seconds", type=float, default=3.0,
                        help="gap between the two emptiness scans (default 3)")
    args = parser.parse_args()

    try:
        databases, tables = live_catalog()
        first = tablet_empty_dirs()
        time.sleep(args.stabilize_seconds)
        second = tablet_empty_dirs()
    except RuntimeError as error:
        print(f"tablet-orphan-sweep: cannot verify: {error}", file=sys.stderr)
        return 3

    orphans = classify(first & second, databases, tables)
    print(f"tablet-orphan-sweep: {len(second)} empty dir(s) under {DATA_DIR} on "
          f"{TABLET_CONTAINER}; {len(orphans)} orphan(s) after the "
          f"{args.stabilize_seconds:.0f}s stability check")
    for relative, reason in orphans:
        print(f"  orphan: {relative} ({reason})")
    if not args.sweep:
        print(registry_advisory())
        return 1 if orphans else 0

    removed = refused = 0
    for relative, _ in orphans:
        result = run(["docker", "exec", TABLET_CONTAINER, "rmdir", f"{DATA_DIR}/{relative}"])
        if result.returncode == 0:
            removed += 1
        else:
            refused += 1
            print(f"  !! rmdir refused for {relative}: "
                  f"{(result.stderr or result.stdout).strip()}", file=sys.stderr)
    print(f"tablet-orphan-sweep: removed {removed}, refused {refused}")
    return 0 if refused == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
