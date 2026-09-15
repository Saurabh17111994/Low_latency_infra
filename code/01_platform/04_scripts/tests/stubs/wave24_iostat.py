#!/usr/bin/env python3
"""`iostat -x` stub for the wave-24 io-latency-probe tests.

One token per column, exactly like sysstat 12.6.1: the probe parses by column
NAME, so the layout has to be realistic. Scenarios are selected with
W24_IOSTAT_SCENARIO (normal | no-cpu-block | malformed-row | verbose-stderr |
half-exit).
"""
import os
import sys
import time

SCENARIO = os.environ.get("W24_IOSTAT_SCENARIO", "normal")
FIELDS = ["Device", "r/s", "w/s", "rkB/s", "wkB/s", "rrqm/s", "wrqm/s",
          "%rrqm", "%wrqm", "r_await", "w_await", "aqu-sz", "rareq-sz",
          "wareq-sz", "svctm", "%util"]


def row(dev, r_await):
    vals = {"Device": dev, "r/s": "10.00", "w/s": "20.00", "rkB/s": "100.00",
            "wkB/s": "200.00", "rrqm/s": "0.00", "wrqm/s": "0.00",
            "%rrqm": "0.00", "%wrqm": "0.00", "r_await": r_await,
            "w_await": "2.00", "aqu-sz": "0.50", "rareq-sz": "10.00",
            "wareq-sz": "10.00", "svctm": "0.10", "%util": "5.00"}
    return " ".join(str(vals[f]) for f in FIELDS)


def block(nice, with_cpu=True, malformed=False):
    print("Linux 6.8.0 (stub) 2026-09-15 _x86_64_ (1 CPU)")
    if with_cpu:
        print("avg-cpu:  %user   %nice %system %iowait    %steal   %idle")
        print("11.0 %s 3.0 1.5 0.0 83.5" % nice)
    print(" ".join(FIELDS))
    devices = ["nvme0n1", "nvme1"]
    if malformed:
        # truncated row: the probe must drop it LOUDLY (P6-432), not silently
        print(devices[0] + " 1.00 2.00")
        devices = devices[1:]
    for i, dev in enumerate(devices):
        print(row(dev, "%.2f" % (1.0 + i)))
        sys.stdout.flush()
        if SCENARIO == "paced-rows" and i + 1 < len(devices):
            # >1s between two rows of the SAME interval: a per-row timestamp
            # (P6-433) then straddles a second boundary and the two devices of
            # one sample cannot share an epoch_s.
            time.sleep(1.1)


if SCENARIO == "verbose-stderr":
    # ~1.4 MB of warnings: with stderr=PIPE and nothing reading it, the child
    # blocks here forever once the 64KiB pipe buffer fills (P6-430).
    sys.stderr.write("iostat: warning: something ignored\n" * 40000)
    sys.stderr.flush()

block("1.0")
# interval 2: the avg-cpu block is absent when the scenario says so (P6-431)
block("2.0", with_cpu=(SCENARIO != "no-cpu-block"),
      malformed=(SCENARIO == "malformed-row"))

if SCENARIO == "half-exit":
    # stdout at EOF but the process stays alive: the probe's wait() times out
    os.close(1)
    time.sleep(300)
