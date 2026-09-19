#!/usr/bin/env python3
"""Structural validation for version_matrix.yaml.

Enforces the foundation rule that no boundary may use `latest`, an
unpinned/empty value, or a mutable image tag, and that every boundary
carries an owner, an evidence method, and a valid compatibility class.

This validates the *shape and pin discipline* of the matrix only.
It does NOT run capability tests; classification stays UNKNOWN until
those tests pass (see docs/08_implementation/11-testing-and-release.md).
External/verification boundaries may remain TO_BE_VERIFIED (explicit
'not yet captured' sentinel) and are permitted by this check.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is a standard CI dependency
    sys.stderr.write("PyYAML is required: pip install pyyaml\n")
    sys.exit(2)

ALLOWED_CLASSES = {
    "COMPATIBLE",
    "COMPATIBLE_WITH_LIMITATION",
    "INCOMPATIBLE",
    "UNKNOWN",
    "NOT_APPLICABLE",
}
# Platform versions must be pinned (no placeholder / mutable tag).
# External/verification boundaries may legitimately remain TO_BE_VERIFIED
# (not yet captured) -- that is an explicit sentinel, not a lazy tag, so
# it is permitted here; classification stays UNKNOWN until tests pass.
PIN_BLOCKERS = {"TO_BE_PINNED", "LATEST", "LATEST_FORWARD"}


def fail(msg: str) -> None:
    sys.stderr.write(f"FAIL: {msg}\n")


def _as_str(row, key):
    """Return a trimmed string for a YAML value, coercing non-string scalars
    (YAML parses `2.2` as float, `2024-01-01` as date, `true` as bool — R-093)."""
    v = row.get(key)
    return str(v).strip() if v is not None else ""


def verify(path: Path) -> int:
    errors = 0
    try:
        with path.open(encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
    except (yaml.YAMLError, OSError, UnicodeDecodeError) as exc:
        # R-092: an unreadable or malformed matrix must fail with a clear message,
        # not a traceback. P6-644: OSError covers a missing path, a directory or a
        # permission problem, and UnicodeDecodeError a non-UTF-8 file — all of which
        # used to escape as raw exceptions from the path.open() above.
        fail(f"cannot parse {path}: {exc}")
        return 1

    if doc is None:
        # R-092: empty or comment-only file parses to None.
        fail(f"{path} is empty (no YAML document)")
        return 1
    if not isinstance(doc, dict):
        fail(f"{path}: top-level YAML must be a mapping, got {type(doc).__name__}")
        return 1

    boundaries = doc.get("boundaries")
    if not isinstance(boundaries, list) or not boundaries:
        fail("matrix has no 'boundaries' list")
        return 1

    for row in boundaries:
        if not isinstance(row, dict):
            # R-093: a non-mapping row must be flagged, not crash the validator.
            fail(f"matrix row is not a mapping: {row!r}")
            errors += 1
            continue
        cid = _as_str(row, "compatibility_id") or "<no-id>"
        # P6-846: _as_str() stringifies anything, so a YAML list or mapping became
        # text like "[1.4, 2.0]" and sailed through the empty/latest/blocker checks.
        # A pin is a single scalar; anything else is a shape error, not a pin.
        if isinstance(row.get("proposed_version"), (list, dict)):
            fail(f"{cid}: proposed_version must be a single pinned scalar, got "
                 f"{type(row['proposed_version']).__name__}: {row['proposed_version']!r}")
            errors += 1
            continue
        version = _as_str(row, "proposed_version")
        owner = _as_str(row, "evidence_owner")
        method = _as_str(row, "evidence_method")
        cls = _as_str(row, "compatibility_class").upper()

        if not version:
            fail(f"{cid}: proposed_version is empty (must be pinned)")
            errors += 1
        elif "latest" in version.lower():
            fail(f"{cid}: proposed_version '{version}' is mutable (latest)")
            errors += 1
        elif version.upper() in PIN_BLOCKERS:
            fail(
                f"{cid}: proposed_version '{version}' is a pin blocker; "
                "resolve via capability test before COMPATIBLE"
            )
            errors += 1

        if not owner:
            fail(f"{cid}: evidence_owner missing")
            errors += 1
        if not method:
            fail(f"{cid}: evidence_method missing")
            errors += 1
        if cls not in ALLOWED_CLASSES:
            fail(f"{cid}: invalid compatibility_class '{cls}'")
            errors += 1

    if errors:
        sys.stderr.write(f"\n{errors} matrix discipline violation(s).\n")
        return 1

    print(
        f"OK: {len(boundaries)} boundaries; pin discipline satisfied. "
        "Classification remains UNKNOWN until capability tests pass."
    )
    return 0


if __name__ == "__main__":
    target = Path(__file__).with_name("version_matrix.yaml")
    if len(sys.argv) > 1:
        target = Path(sys.argv[1])
    sys.exit(verify(target))
