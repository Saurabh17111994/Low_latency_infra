#!/usr/bin/env python3
"""Pre-flight verification + truncation for repair-tablet.sh (P6-098, P6-099).

Reads a TSV of "<path>\t<end>" pairs (written by repair-tablet.sh), verifies EVERY
pair, and only then truncates. Two properties this buys us:

  * P6-098 — a destructive truncate is only ever done on bytes that are really
    zeroed. "0 < end < size" is not enough: a LogScan.py bug, a stale offset or a
    parser mismatch would otherwise discard complete records irreversibly.
  * P6-099 — all-or-nothing. Doing the check inside the truncate loop cut the
    earlier segments before refusing on a later one, i.e. a partial repair that
    the caller reported as a refusal.

Exit codes: 0 = verified and truncated, 1 = a pair failed verification (nothing
was cut), 2 = bad input (usage, unreadable or empty pairs file).
"""

import os
import sys

CHUNK = 1 << 20


def read_pairs(path):
    """[('<container path>', <offset>), ...] — raises ValueError on a malformed line."""
    pairs = []
    with open(path, encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, 1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            path_field, sep, end_field = line.rpartition("\t")
            if not sep or not end_field.isdigit():
                raise ValueError(f"line {lineno}: expected '<path>\\t<offset>', got {line!r}")
            pairs.append((path_field, int(end_field)))
    return pairs


def problems(pairs):
    """Every reason to refuse, for ALL pairs. Nothing is truncated while this runs."""
    bad = []
    for path, end in pairs:
        try:
            size = os.path.getsize(path)
        except OSError as exc:
            bad.append(f"{path}: cannot stat: {exc}")
            continue
        if end <= 0 or end >= size:
            bad.append(f"{path}: unsafe truncation for {path}: end={end} size={size}")
            continue
        with open(path, "rb") as handle:
            handle.seek(end)
            while True:
                chunk = handle.read(CHUNK)
                if not chunk:
                    break
                if chunk.strip(b"\0"):
                    bad.append(f"{path}: bytes [{end},{size}) are not all zero — "
                               "cutting them would drop complete records")
                    break
    return bad


def main(argv):
    if len(argv) != 2:
        print("usage: verify-and-truncate.py <pairs.tsv>", file=sys.stderr)
        return 2
    try:
        pairs = read_pairs(argv[1])
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if not pairs:
        print("ERROR: no pairs to verify — refusing to touch anything", file=sys.stderr)
        return 2
    bad = problems(pairs)
    if bad:
        for problem in bad:
            print(f"ERROR: {problem}", file=sys.stderr)
        print(f"ERROR: {len(bad)} of {len(pairs)} segment(s) failed verification — "
              "NO segment was truncated", file=sys.stderr)
        return 1
    for path, end in pairs:
        os.truncate(path, end)
        print(f"truncated {path} -> {end}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
