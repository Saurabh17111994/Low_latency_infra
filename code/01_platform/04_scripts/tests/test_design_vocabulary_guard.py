"""Guard: retired dedup designs must never be named as if they were current.

Design A and Design B are option labels from DEC-040, not mechanism names. Live
text uses the canonical names instead: "Fluss KV-table dedup (retired)",
"Flink keyed MapState + state-TTL dedup (retired)", "heap count-window dedup",
and "dual-sink signal build" for the artifact the 2026-08 material calls
"Design-B artifact (DB2)".

The anchor that names them lives in docs/08_implementation/04-signal-job.md as
"### Retired designs and their names"; this test fails if that section is
deleted, and flags any unmarked mention outside the exempted history.

Case-sensitive on purpose: the verb phrase "design a ..." is not a hit.
"""
from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[4]   # tests -> 04_scripts -> 01_platform -> code -> repo root

PATTERNS = [re.compile(p) for p in (r"Design A\b", r"Design-A\b", r"Design B\b", r"Design-B\b", r"DesignB\b")]
MARKERS = re.compile(r"retired|superseded|historical|deleted|removed|dual-sink|option label", re.I)
HEADING = re.compile(r"^\s{0,3}#{1,6}\s")

SCAN_GLOBS = ("docs/**/*.md", "code/**/*.java", "code/**/*.py", "code/**/*.json", "code/**/*.sql")
SKIP_PARTS = ("/target/", "/.git/", "/logs/", "/node_modules/")

# Append-only history and dated rationale: these keep the labels by design.
EXEMPT_PREFIXES = ("docs/05_deployment/change-records/", "docs/plans/")

# Lines that are genuinely unchangeable, as (relative path, exact stripped line) -> why.
# Dated historical text that must keep its original label (measurements, root-cause
# narratives, rehearsal evidence). Keys are the first characters of the stripped line.
ALLOWED_LINES: dict[tuple[str, str], str] = {
    ("docs/08_implementation/04-signal-job.md", '**Root cause 2 — DEC-038 synchronous lookup st'): "dated root-cause / absorbed-investigation block (2026-08-17 era)",
    ("docs/08_implementation/04-signal-job.md", 'authoritative Flink keyed state (Design B) — s'): "dated root-cause / absorbed-investigation block (2026-08-17 era)",
    ("docs/08_implementation/04-signal-job.md", 'DEC-040.** Design B has no RPC on the hot path'): "dated root-cause / absorbed-investigation block (2026-08-17 era)",
    ("docs/08_implementation/04-signal-job.md", 'C/D — INTEGRATED 2026-08-23 into § Integrated '): "dated root-cause / absorbed-investigation block (2026-08-17 era)",
    ("docs/08_implementation/04-signal-job.md", '`09-production-swarm.md` §13; CHG-025.)** re-r'): "dated root-cause / absorbed-investigation block (2026-08-17 era)",
    ("docs/08_implementation/04-signal-job.md", '- **D (P10):** the 10-box matrix is PASS on De'): "dated root-cause / absorbed-investigation block (2026-08-17 era)",
    ("docs/08_implementation/09-production-swarm.md", '- **2026-08-17 Design-B re-run (P10.1 boxes 1–'): "dated P10 rehearsal evidence line (2026-08-17)",
    ("docs/08_implementation/09-production-swarm.md", '**Status:** READY-RUNBOOK — delivered 2026-08-'): "dated P10 rehearsal evidence line (2026-08-17)",
    ("docs/08_implementation/09-production-swarm.md", 'Every machinery step above was rehearsed on th'): "dated P10 rehearsal evidence line (2026-08-17)",
    ("docs/08_implementation/11-testing-and-release.md", '**Pending (no implementing test yet):** ~~`SIG'): "dated measurement appendix row (2026-08-17 values)",
    ("docs/08_implementation/11-testing-and-release.md", '> **Design-B re-run (2026-08-17):** the P7.2/P'): "dated measurement appendix row (2026-08-17 values)",
    ("docs/08_implementation/11-testing-and-release.md", '> 1024-instrument envelope). The rows below ca'): "dated measurement appendix row (2026-08-17 values)",
    ("docs/08_implementation/11-testing-and-release.md", '| PERF-THROUGHPUT-001 (50k sustained / 90k pea'): "dated measurement appendix row (2026-08-17 values)",
    ("docs/08_implementation/11-testing-and-release.md", '| PERF-LATENCY-001 (p99 < 100 ms) | NOT MEASUR'): "dated measurement appendix row (2026-08-17 values)",
    ("docs/08_implementation/11-testing-and-release.md", '| DEDUP-MEMORY-001 (bounded memory + expiry sw'): "dated measurement appendix row (2026-08-17 values)",
}

ANCHOR_FILE = "docs/08_implementation/04-signal-job.md"
ANCHOR_HEADING = "### Retired designs and their names"


def _hits(line: str) -> bool:
    return any(p.search(line) for p in PATTERNS)


def violations(lines: list[str], path: str) -> list[int]:
    """1-based line numbers that name a retired design without marking it."""
    out: list[int] = []
    for idx, line in enumerate(lines):
        if not _hits(line) or MARKERS.search(line):
            continue
        if _marked_block(lines, idx) or _marked_section(lines, idx):
            continue
        if any(p == path and line.strip().startswith(k) for (p, k) in ALLOWED_LINES):
            continue
        out.append(idx + 1)
    return out


def _marked_block(lines: list[str], idx: int) -> bool:
    """Marker anywhere in the enclosing blockquote or run of non-blank lines."""
    up, down = idx, idx
    if lines[idx].lstrip().startswith(">"):
        while up and lines[up - 1].lstrip().startswith(">") and lines[up - 1].strip():
            up -= 1
        while down + 1 < len(lines) and lines[down + 1].lstrip().startswith(">"):
            down += 1
    else:
        while up and lines[up - 1].strip():
            up -= 1
        while down + 1 < len(lines) and lines[down + 1].strip():
            down += 1
    return any(MARKERS.search(lines[i]) for i in range(up, down + 1))


def _marked_section(lines: list[str], idx: int) -> bool:
    for i in range(idx, -1, -1):
        if HEADING.match(lines[i]):
            return bool(MARKERS.search(lines[i]))
    return False


def _scan() -> list[str]:
    bad: list[str] = []
    for pattern in SCAN_GLOBS:
        for path in sorted(REPO.glob(pattern)):
            rel = path.relative_to(REPO).as_posix()
            if any(part in f"/{rel}" for part in SKIP_PARTS) or rel.startswith(EXEMPT_PREFIXES):
                continue
            lines = path.read_text(errors="ignore").splitlines()
            bad += [f"{rel}:{n}: {lines[n - 1].strip()[:110]}" for n in violations(lines, rel)]
    return bad


def test_no_unmarked_design_labels_in_live_text() -> None:
    bad = _scan()
    assert not bad, "retired design labels used as mechanism names:\n  " + "\n  ".join(bad)


def test_the_anchor_still_exists() -> None:
    text = (REPO / ANCHOR_FILE).read_text(errors="ignore")
    assert ANCHOR_HEADING in text, "the vocabulary anchor was deleted"
    for needle in ("heap count-window", "fingerprint-dedup-v2", "must not be used as mechanism names"):
        assert needle in text, f"the anchor lost its content: {needle!r}"


def test_guard_detects_a_bare_label_and_accepts_a_marked_one() -> None:
    assert violations(["the Design B dedup set is authoritative"], "x.md") == [1]
    assert violations(["> Design B is retired; see the anchor"], "x.md") == []
    assert violations(["### Retired designs and their names", "", "| Design A | was a Fluss KV table |"], "x.md") == []
    assert violations(["a design a plan here"], "x.md") == []          # case-sensitive: not a hit
