#!/usr/bin/env python3
"""Summarize reset-aware live Flink counters for the C2 recovery drill."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PhaseProgress:
    first_read: int
    last_read: int
    read_delta: int
    read_increases: int
    first_write: int
    last_write: int
    write_delta: int
    write_increases: int
    read_resets: int
    write_resets: int
    samples: int

    def as_tsv(self) -> str:
        return "\t".join(
            str(value)
            for value in (
                self.first_read,
                self.last_read,
                self.read_delta,
                self.read_increases,
                self.first_write,
                self.last_write,
                self.write_delta,
                self.write_increases,
                self.read_resets,
                self.write_resets,
                self.samples,
            )
        )


def _counter_delta(previous: int, current: int) -> tuple[int, int, int]:
    """Return (delta, increases, resets) for one adjacent counter pair."""
    if current >= previous:
        return current - previous, int(current > previous), 0
    # Flink task/aggregate metrics can restart from zero after recovery. The
    # current value is useful progress, but only a later upward move proves
    # the recovered task is still processing records.
    return current, 0, 1


def summarize_phase(path: Path, phase: str) -> PhaseProgress | None:
    previous_read: int | None = None
    previous_write: int | None = None
    first_read = last_read = first_write = last_write = 0
    read_delta = write_delta = 0
    read_increases = write_increases = 0
    read_resets = write_resets = 0
    samples = 0

    for line in path.read_text().splitlines():
        fields = line.split("\t")
        if len(fields) < 6 or fields[1] != phase:
            continue
        try:
            current_read = int(fields[4])
            current_write = int(fields[5])
        except ValueError:
            continue

        if previous_read is None or previous_write is None:
            first_read = current_read
            first_write = current_write
        else:
            delta, increases, resets = _counter_delta(previous_read, current_read)
            read_delta += delta
            read_increases += increases
            read_resets += resets
            delta, increases, resets = _counter_delta(previous_write, current_write)
            write_delta += delta
            write_increases += increases
            write_resets += resets

        previous_read = current_read
        previous_write = current_write
        last_read = current_read
        last_write = current_write
        samples += 1

    if samples == 0:
        return None
    return PhaseProgress(
        first_read,
        last_read,
        read_delta,
        read_increases,
        first_write,
        last_write,
        write_delta,
        write_increases,
        read_resets,
        write_resets,
        samples,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("progress_file", type=Path)
    parser.add_argument("phase")
    args = parser.parse_args()
    summary = summarize_phase(args.progress_file, args.phase)
    if summary is None:
        parser.error(f"no numeric samples found for phase {args.phase!r}")
    print(summary.as_tsv())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
