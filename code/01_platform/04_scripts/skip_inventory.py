#!/usr/bin/env python3
"""Itemize a verbose unittest log's skips into one auditable line — CHG-220.

Why this exists: gate step 3 quotes unittest's summary ("OK (skipped=11)") into
the certificate, and nothing anywhere recorded *which* eleven tests skipped. An
accepted live-cluster refusal and a test somebody quietly disabled look
identical in the record. `run-monday-gates.sh` therefore runs the suite with
`-v` and pipes the log here; the gate fails if the summary claims skips the log
cannot name, so a count can never again pass as audited.

Usage: skip_inventory.py <unittest-verbose.log>
Output: SKIPS: 11 — 5× <reason>, 3× <reason>, … (one line, reasons truncated)
Exit:   0 the summary and the named skips agree; 1 anything else.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

# unittest -v writes "<test-id-or-docstring> ... skipped 'reason'"; a reasonless
# SkipTest() renders as "skipped ''". Anchored on " ... skipped" so test chatter
# mentioning the word cannot be counted (the CHG-201 lesson, applied here).
SKIP_RE = re.compile(r"\s\.\.\. skipped(?: '(.*)')?$")
# Matches OK (skipped=4) and FAILED (failures=1, skipped=2) alike — itemizing
# and judging are separate jobs; a red suite's skips are still real skips.
COUNT_RE = re.compile(r"\bskipped=(\d+)\b")
SUMMARY_RE = re.compile(r"^(\s*Ran \d+ tests?\b|\s*OK\b|\s*FAILED\b)")
MAX_REASON = 60
NO_REASON = "(no reason given)"


@dataclass(frozen=True)
class Report:
    """reported: what the summary line claims. found: what the log names."""

    reported: int
    found: int
    reasons: list[tuple[int, str]]


def summarize(text: str) -> Report:
    """Parse a verbose unittest log. Raises ValueError without a summary line."""
    if not any(SUMMARY_RE.match(line) for line in text.splitlines()):
        raise ValueError("no unittest summary line (^Ran/^OK/^FAILED) to audit")

    counts: dict[str, int] = {}
    for line in text.splitlines():
        match = SKIP_RE.search(line.rstrip())
        if match:
            reason = (match.group(1) or "").strip() or NO_REASON
            counts[reason] = counts.get(reason, 0) + 1

    claim = COUNT_RE.search(text)
    reasons = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return Report(
        reported=int(claim.group(1)) if claim else 0,
        found=sum(counts.values()),
        reasons=[(count, reason) for reason, count in reasons],
    )


def format_line(report: Report) -> str:
    """One bounded line: count first, then each reason the suite can name."""
    if not report.reasons:
        return "SKIPS: 0 — none"
    named = ", ".join(f"{count}× {_shorten(reason)}" for count, reason in report.reasons)
    return f"SKIPS: {report.found} — {named}"


def _shorten(reason: str) -> str:
    return reason if len(reason) <= MAX_REASON else reason[:MAX_REASON] + "…"


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: skip_inventory.py <unittest-verbose.log>", file=sys.stderr)
        return 2
    path = argv[0]
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        print(f"skip inventory: cannot read {path}: {exc}", file=sys.stderr)
        return 1
    try:
        report = summarize(text)
    except ValueError as exc:
        print(f"skip inventory: {exc} — cannot audit skips of {path}", file=sys.stderr)
        return 1
    if report.reported != report.found:
        print(
            f"skip inventory: the suite's summary claims skipped={report.reported} "
            f"but the verbose log names only {report.found} skip(s) — the count is "
            f"not auditable, so it must not pass as one",
            file=sys.stderr,
        )
        return 1
    print(format_line(report))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
