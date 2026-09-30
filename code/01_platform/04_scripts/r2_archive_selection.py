#!/usr/bin/env python3
"""R2 archive selection core (DEC-060, 2026-09-30).

One config list rules which tables are archived to Cloudflare R2:

  * exactly the listed tables are enabled (turned on when off),
  * every live-enabled table outside the list is disabled ("not any other"),
  * a listed table missing from the live catalog is reported, never skipped,
  * an empty list is refused — a config error must not mass-disable.

Pure functions only: the catalog reader and the ALTERs live in the sync
tooling; this module decides what they should do, and the gate preflight guard
reuses `plan()` so the live stack is checked against the same rule.

Why per table, never merged: a merged live table (DEC-059, if built) must not
become a merged lake archive — each selected table lands as its own lake table
(DEC-060 supersedes DEC-059's "one lake table" clause).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence


@dataclass(frozen=True)
class SelectionPlan:
    """What the sync must do so the live flags equal the configured list."""

    enable: list[str] = field(default_factory=list)
    disable: list[str] = field(default_factory=list)
    ok: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)

    def is_clean(self) -> bool:
        """True when applying this plan would change nothing."""
        return not (self.enable or self.disable or self.missing)

    def render(self) -> list[str]:
        """The dry-run lines the sync tool prints (also used in evidence)."""
        lines: list[str] = []
        for t in self.enable:
            lines.append(f"enable  {t}")
        for t in self.disable:
            lines.append(f"disable {t}")
        for t in self.ok:
            lines.append(f"ok      {t}")
        for t in self.missing:
            lines.append(f"MISSING {t} (named in the list, not in the live catalog)")
        return lines


def enabled_outside(live: Mapping[str, bool], selected: Sequence[str]) -> list[str]:
    """Live-enabled tables the list does not allow (the guard-side rule).

    Unlike `plan`, an empty `selected` is legal: it means "nothing may be
    archived", so every enabled table is outside the list. Sorted for
    deterministic reports.
    """
    allowed = set(selected)
    return sorted(t for t, on in live.items() if on and t not in allowed)


def parse_selection(csv: str) -> list[str]:
    """Parse the configured list; refuse an empty result (config error)."""
    if csv is None:
        raise ValueError("selection list is missing")
    tables: list[str] = []
    for raw in csv.split(","):
        name = raw.strip()
        if name and name not in tables:
            tables.append(name)
    if not tables:
        raise ValueError(
            "selection list is empty — refusing to plan (a config error must "
            "not mass-disable)")
    return tables


def plan(live: Mapping[str, bool], selected: Sequence[str]) -> SelectionPlan:
    """Diff the configured list against the live per-table archive flags.

    `live` maps every table in the live catalog to its `table.datalake.enabled`
    value; `selected` is the configured archive list. An empty `selected` is a
    config error, not an instruction to disable everything.
    """
    if not selected:
        raise ValueError(
            "selection list is empty — refusing to plan (a config error must "
            "not mass-disable)")
    enable = [t for t in selected if live.get(t) is False]
    ok = [t for t in selected if live.get(t) is True]
    missing = [t for t in selected if t not in live]
    disable = enabled_outside(live, selected)
    return SelectionPlan(enable=enable, disable=disable, ok=ok, missing=missing)
