#!/usr/bin/env python3
"""R2 archive sync (DEC-060, S2) — apply the archive list to the live stack.

Dry-run by default. Live flags are read through `eod_controller.py tiering`
(which exits non-zero on unreadable flags — fail-closed); the plan comes from
`r2_archive_selection.plan()`, the single rule source shared with the gate
preflight guard. `--apply` executes the ALTERs one table at a time through
`eod_controller.py tiering --set on|off`.

Exit codes:
  0  clean, or dry-run with pending changes
  1  a live ALTER failed (reported per table)
  2  config/listing error: empty list, missing table, unreadable flag, or a
     failed listing — nothing is changed

Why per table, never merged: each selected table archives as its own lake
table; a merged live table never implies a merged lake archive (DEC-060).
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from subprocess import CompletedProcess
from typing import Callable, Sequence

from r2_archive_selection import parse_selection, plan

SCRIPT_DIR = Path(__file__).resolve().parent
LAUNCHER = SCRIPT_DIR / "eod_controller.py"

LINE = re.compile(r"^tiering ([A-Za-z0-9_]+)=(enabled|disabled|unreadable)$")


class SyncError(RuntimeError):
    """Config or listing errors — exit 2, nothing changed."""


def parse_listing(text: str) -> dict[str, bool]:
    """Parse `tiering <name>=enabled|disabled` lines; refuse unreadable/empty."""
    live: dict[str, bool] = {}
    for raw in text.splitlines():
        match = LINE.match(raw.strip())
        if not match:
            continue
        name, state = match.group(1), match.group(2)
        if state == "unreadable":
            raise SyncError(f"live archive flag unreadable for {name} — refusing (fail-closed)")
        live[name] = state == "enabled"
    if not live:
        raise SyncError("listing produced no table flags — is the stack up?")
    return live


def run_sync(selection: str, runner: Callable[[Sequence[str]], CompletedProcess],
             apply: bool = False) -> int:
    """Plan from the live flags; print; apply when asked. Returns the exit code."""
    tables = parse_selection(selection)

    listing = runner(["tiering", "--list"])
    if listing.returncode != 0:
        raise SyncError("tiering --list failed (exit "
                        f"{listing.returncode}): {listing.stderr.strip()[-300:]}")
    live = parse_listing(listing.stdout)

    pending = plan(live, tables)
    for line in pending.render():
        print(line)
    if pending.missing:
        print("REFUSING: the list names tables that do not exist in the live catalog")
        return 2
    if not apply:
        print(f"dry-run: {len(pending.enable)} to enable, {len(pending.disable)}"
              " to disable — pass --apply to execute")
        return 0

    failures: list[str] = []
    changed = 0
    for table in pending.enable:
        changed += _set(runner, table, "on", failures)
    for table in pending.disable:
        changed += _set(runner, table, "off", failures)
    if failures:
        for failure in failures:
            print(f"FAILED {failure}")
        return 1
    print(f"applied: {len(pending.enable)} enabled, {len(pending.disable)} disabled"
          f" ({changed} ALTERs)")
    return 0


def _set(runner: Callable[[Sequence[str]], CompletedProcess], table: str, value: str,
         failures: list[str]) -> int:
    result = runner(["tiering", "--set", value, "--tables", table])
    if result.returncode != 0:
        failures.append(f"{table} -> {value} (exit {result.returncode}): "
                        f"{result.stderr.strip()[-200:]}")
        return 0
    print(f"set {table} {value}: ok")
    return 1


def _launcher_runner(args: Sequence[str], bootstrap: str | None) -> CompletedProcess:
    cmd = [sys.executable, str(LAUNCHER), *args]
    if bootstrap:
        cmd += ["--bootstrap", bootstrap]
    return subprocess.run(cmd, capture_output=True, text=True)


def main(argv: Sequence[str] | None = None,
         runner_factory: Callable[[str | None], Callable[[Sequence[str]], CompletedProcess]]
         | None = None) -> int:
    parser = argparse.ArgumentParser(description="R2 archive sync (DEC-060)")
    parser.add_argument("--tables", default=os.environ.get("EOD_TABLES", ""),
                        help="comma list of tables to archive (env EOD_TABLES)")
    parser.add_argument("--apply", action="store_true",
                        help="execute the ALTERs (default: dry-run)")
    parser.add_argument("--bootstrap", default=None,
                        help="Fluss coordinator (default: env FLUSS_BOOTSTRAP)")
    args = parser.parse_args(argv)

    factory = runner_factory or (lambda bootstrap: (
        lambda argv_: _launcher_runner(argv_, bootstrap)))
    try:
        return run_sync(args.tables, factory(args.bootstrap), args.apply)
    except (ValueError, SyncError) as e:
        print(f"r2-archive-sync: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
