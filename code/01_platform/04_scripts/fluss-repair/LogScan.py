#!/usr/bin/env python3
"""LogScan.py — scan Fluss log segments for truncated (unclean-shutdown) tails.

An unclean tablet shutdown can leave a segment file preallocated/zeroed past
the last complete batch. Fluss's own reader misparses the zeroed batch header
as valid and jumps through the garbage region, so the recovery error position
is NOT the true boundary (observed: reported 670,347,188 while the real
boundary was 670,345,400). This scanner walks the batches with the same
arithmetic the server uses and reports the exact end of the last complete
batch.

Batch layout (unchanged in Fluss 1.0.0; verified against DefaultLogRecordBatch,
re-verified 2026-09-23 against 30 real 1.0.0 segments up to 198 MB, each walked
to its exact end):
    48-byte header; batch total size = 12 + int32_le(header[8:12])
    (little-endian, confirmed against a known batch: bytes 24 09 00 00 = 2340).
A header of all zero bytes is the preallocated tail marker.

Usage:
    python3 LogScan.py <segment.log> [segment.log ...]

Prints per file: size, last-complete-batch end, delta, and (when truncated)
    TRUNCATE_TO=<end>
Exit 0 always (the caller decides); callers parse TRUNCATE_TO lines.

TRUNCATE_TO is printed ONLY when at least one complete batch was found. A file
with no complete batch at all (a fully preallocated segment, or a partial write
smaller than one header) prints NO_COMPLETE_BATCH instead of TRUNCATE_TO=0, so
a caller cannot mistake "nothing to keep" for "keep everything up to zero"
(P6-383). repair-tablet.sh's parser already accepts only TRUNCATE_TO=[1-9]..., so
this is not a behavior change for that caller.
"""

import os
import sys

HEADER_SIZE = 48


def scan(path):
    """Return (last_complete_batch_end, file_size) or (None, size) on error."""
    try:
        size = os.path.getsize(path)
    except OSError as e:
        print(f"ERROR {path}: {e}", file=sys.stderr)
        return None, -1
    last_end = 0
    pos = 0
    try:
        with open(path, "rb") as f:
            while pos + HEADER_SIZE <= size:
                # P6-735: read sequentially. The old absolute f.seek(pos) on every
                # iteration forced the buffered reader to discard and refill its
                # buffer once per batch (hundreds of thousands of batches in a
                # 670 MB segment); the relative skip below stays in the buffer.
                header = f.read(HEADER_SIZE)
                if not any(header):
                    # Preallocated/never-written tail — the boundary.
                    break
                total = 12 + int.from_bytes(header[8:12], "little")
                if total < HEADER_SIZE or pos + total > size:
                    # Corrupt/incomplete batch — stop at the last complete one.
                    break
                f.seek(total - HEADER_SIZE, os.SEEK_CUR)  # skip the batch body
                pos += total
                last_end = pos
    except OSError as e:
        print(f"ERROR {path}: {e}", file=sys.stderr)
        return None, size
    return last_end, size


def main(argv):
    files = argv[1:]
    if not files:
        print("usage: python3 LogScan.py <segment.log> [...]", file=sys.stderr)
        return 2
    for path in files:
        end, size = scan(path)
        if end is None or size < 0:
            continue
        delta = size - end
        if end == 0 and delta > 0:
            # P6-383: no complete batch at all. Printing TRUNCATE_TO=0 invited a
            # caller to shrink the segment to zero bytes; make it explicit.
            print(f"{path}: size={size} last_complete_batch_end=0 "
                  f"zero_tail={delta} bytes")
            print(f"NO_COMPLETE_BATCH={path}")
        elif delta > 0:
            print(f"{path}: size={size} last_complete_batch_end={end} "
                  f"zero_tail={delta} bytes")
            print(f"TRUNCATE_TO={end}")
        else:
            print(f"{path}: OK (size={size}, no truncated tail)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
