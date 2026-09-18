#!/usr/bin/env python3
"""catalog_drift.py — name the tables that make a live Fluss catalog differ from the manifest.

`catalog-guard.sh` reports "catalog has MORE tables than the manifest describes (33/27) — extra
tables are drift, not health" and stops at the count. The count says how much drift there is, not
which tables, and on 2026-09-18 the six extras behind that message took a hand-run ZK listing to
identify: four `chg100_sweep_*` leftovers of an interrupted drill run, a probe's `probe_tbl_1`, and
a lowercase `signal_candidates` sitting next to the real `Signal_Candidates`.

Comparison is exact-set, so a lowercase twin is drift rather than a match — it is a different table.

Usage:  catalog_drift.py --manifest <schema_manifest.json> --live "Name1,Name2,..."
Output: `extra:   <name>` then `missing: <name>` lines (sorted), or a single "no drift" line.
Exit:   0 either way — naming the drift is this tool's job; deciding what to do about it is the
        caller's (the guard refuses to apply; an operator drops an extra that nothing references).
"""
from __future__ import annotations

import argparse
import json
import sys


def diff(live: list[str], manifest: list[str]) -> tuple[list[str], list[str]]:
    """(tables live but not in the manifest, tables in the manifest but not live), both sorted."""
    live_set, manifest_set = set(live), set(manifest)
    return sorted(live_set - manifest_set), sorted(manifest_set - live_set)


def manifest_names(path: str) -> list[str]:
    with open(path, encoding="utf-8") as fh:
        return [entry["table_name"] for entry in json.load(fh)["tables"]]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="schema_manifest.json")
    parser.add_argument("--live", required=True, help="comma-separated live table names")
    args = parser.parse_args(argv)

    live = [name.strip() for name in args.live.split(",") if name.strip()]
    extra, missing = diff(live, manifest_names(args.manifest))
    for name in extra:
        print(f"extra:   {name}")
    for name in missing:
        print(f"missing: {name}")
    if not extra and not missing:
        print("no drift: the live names and the manifest agree")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
