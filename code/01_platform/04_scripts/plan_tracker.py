#!/usr/bin/env python3
"""Recount a plan's `- [marker]` checkboxes and rewrite its roll-up table.

A plan marked up in place but whose summary table is maintained by hand drifts
within a day. This keeps the table derived: it parses the markers under
`## 0. Live tracker`, counts them per `#### ` group, and rewrites the `|`-row
block that follows the "**Roll-up**" line. The sentence above the table is left
alone, so a refresh only ever changes the rows.

Usage:
    python3 plan_tracker.py --plan docs/plans/<plan>.md --write   # refresh the table
    python3 plan_tracker.py --plan docs/plans/<plan>.md --check   # exit 1 if stale

Markers: [x] done  [~] in progress  [ ] todo  [L] needs live cluster/prod
         [?] needs a decision  [-] skipped/downgraded
"""
from __future__ import annotations

import argparse
import pathlib
import sys

# (marker, column label) — order fixes the column order in the emitted table.
MARKERS = [("x", "done"), ("~", "wip"), (" ", "todo"), ("L", "live"), ("?", "decide"), ("-", "skip")]
VALID = {m for m, _ in MARKERS}
GROUP = "#### "


def parse(text: str) -> tuple[list[str], dict[str, list[str]]]:
    """Return (group order, {group title: markers}) for the tracker section."""
    try:
        start = text.index("## 0. Live tracker")
        end = text.index("## Overview", start)
    except ValueError as exc:
        raise SystemExit(f"plan_tracker: tracker section not found ({exc})") from exc
    order: list[str] = []
    groups: dict[str, list[str]] = {}
    for chunk in text[start:end].split("\n" + GROUP)[1:]:
        title = chunk.splitlines()[0].strip()
        marks = [l[3] for l in chunk.splitlines() if l.startswith("- [") and len(l) > 4 and l[4] == "]"]
        if marks:
            groups[title] = marks
            order.append(title)
    return order, groups


def table_bounds(lines: list[str]) -> tuple[int, int]:
    """First and last+1 line index of the `|`-row block after the `**Roll-up**` line."""
    head = next((n for n, l in enumerate(lines) if l.startswith("**Roll-up**")), None)
    if head is None:
        raise SystemExit("plan_tracker: no '**Roll-up**' line found")
    start = next((n for n in range(head, len(lines)) if lines[n].startswith("|")), None)
    if start is None:
        raise SystemExit("plan_tracker: no table rows after '**Roll-up**'")
    end = start
    while end < len(lines) and lines[end].startswith("|"):
        end += 1
    return start, end


def render(order: list[str], groups: dict[str, list[str]]) -> list[str]:
    """The table rows only — header, separator, one row per group, then totals."""
    rows = [
        "| Stage | Tasks | " + " | ".join(lbl for _, lbl in MARKERS) + " |",
        "|---|" + "---|" * (len(MARKERS) + 1),
    ]
    for g in order:
        cells = [str(sum(1 for m in groups[g] if m == k)) for k, _ in MARKERS]
        rows.append(f"| {g} | {len(groups[g])} | " + " | ".join(cells) + " |")
    n = sum(len(groups[g]) for g in order)
    totals = [str(sum(1 for g in order for m in groups[g] if m == k)) for k, _ in MARKERS]
    rows.append(f"| **Total** | **{n}** | **" + "** | **".join(totals) + "** |")
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--plan", required=True, type=pathlib.Path)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--write", action="store_true", help="rewrite the roll-up table in place")
    g.add_argument("--check", action="store_true", help="exit 1 if the table is stale")
    args = ap.parse_args()

    text = args.plan.read_text(encoding="utf-8")
    lines = text.split("\n")
    bad = sorted({m for m in (l[3] for l in lines if l.startswith("- [") and len(l) > 4 and l[4] == "]")
                  if m not in VALID})
    if bad:
        print(f"plan_tracker: unknown marker(s) {bad}; valid are {sorted(VALID)}", file=sys.stderr)
        return 2

    order, groups = parse(text)
    if not order:
        raise SystemExit("plan_tracker: no groups found under the tracker heading")
    start, end = table_bounds(lines)
    new_rows = render(order, groups)
    current = lines[start:end]

    if args.check:
        if current == new_rows:
            print(f"plan_tracker: OK — roll-up matches {sum(len(groups[g]) for g in order)} markers")
            return 0
        print("plan_tracker: STALE — run with --write", file=sys.stderr)
        return 1

    # Only the table rows are replaced; everything else in the file is preserved verbatim.
    args.plan.write_text("\n".join(lines[:start] + new_rows + lines[end:]), encoding="utf-8")
    print(f"plan_tracker: rewrote roll-up for {sum(len(groups[g]) for g in order)} markers "
          f"across {len(order)} groups")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
