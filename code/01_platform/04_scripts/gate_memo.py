#!/usr/bin/env python3
"""gate_memo.py — remember which (tree, stack) pairs have been certified.

A Monday-gate certificate is evidence about one frozen commit on one stack. Running
the same gate again on the same pair yields a green that looks like new evidence and
is not — the machine was already in that state when the first certificate was
written. This module is the record that lets the gate say so out loud.

Storage: <root>/logs/.gate-memo.jsonl — one JSON object per line. `logs/` is not
tracked, so remembering can never dirty the tree or show up in a commit.

First certificate wins. A replay is not a new certificate, so `record` leaves an
existing entry alone; the entry keeps pointing at the run that first certified the
pair. Malformed lines (a half-written append, a hand edit) are skipped, never fatal:
the memo is an aid, so it must not be able to break a gate run.

CLI:
    gate_memo.py --root <repo> lookup <fingerprint>
        prints `REPLAYED@<first ISO time> — first certified in <run dir>` and exits
        0 when the pair is known; exits 1 when it is not.
    gate_memo.py --root <repo> record <fingerprint> [run dir]
        appends the pair when it is new; prints `RECORDED`/`ALREADY-RECORDED`.
"""

from __future__ import annotations

import datetime
import json
import os
import sys

MEMO_NAME = ".gate-memo.jsonl"


def memo_path(root: str) -> str:
    return os.path.join(root, "logs", MEMO_NAME)


def entries(path: str) -> list[dict]:
    """Every readable entry, oldest first. Unreadable lines are skipped."""
    try:
        with open(path, encoding="utf-8") as handle:
            raw = handle.read().splitlines()
    except FileNotFoundError:
        return []
    out = []
    for line in raw:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and item.get("fingerprint"):
            out.append(item)
    return out


def lookup(path: str, fingerprint: str) -> dict | None:
    """The first certificate for this pair, or None."""
    for item in entries(path):
        if item.get("fingerprint") == fingerprint:
            return item
    return None


def describe(item: dict) -> str:
    return (f"REPLAYED@{item.get('first_certified', 'unknown time')} — first "
            f"certified in {item.get('run_dir', 'an unrecorded run dir')}")


def record(path: str, fingerprint: str, run_dir: str = "",
           now: str | None = None) -> bool:
    """Append the pair when it is new. True when this call was the first one."""
    if not fingerprint:
        raise ValueError("fingerprint must not be empty")
    if lookup(path, fingerprint):
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    stamp = now or datetime.datetime.now().astimezone().isoformat(timespec="seconds")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"fingerprint": fingerprint,
                                 "first_certified": stamp,
                                 "run_dir": run_dir}, sort_keys=True) + "\n")
    return True


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    root = "."
    if "--root" in args:
        index = args.index("--root")
        if index + 1 >= len(args):
            print("FATAL: --root needs a path", file=sys.stderr)
            return 2
        root = args[index + 1]
        del args[index:index + 2]
    if not args:
        print("usage: gate_memo.py [--root DIR] lookup|record <fingerprint> [run dir]",
              file=sys.stderr)
        return 2
    action, rest = args[0], args[1:]
    if not rest:
        print(f"FATAL: {action} needs a fingerprint", file=sys.stderr)
        return 2
    fingerprint, path = rest[0], memo_path(root)
    if action == "lookup":
        item = lookup(path, fingerprint)
        if item is None:
            return 1
        print(describe(item))
        return 0
    if action == "record":
        run_dir = rest[1] if len(rest) > 1 else ""
        print("RECORDED" if record(path, fingerprint, run_dir) else "ALREADY-RECORDED")
        return 0
    print(f"FATAL: unknown action '{action}'", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
