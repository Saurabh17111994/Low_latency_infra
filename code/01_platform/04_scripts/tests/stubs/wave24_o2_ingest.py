#!/usr/bin/env python3
"""`o2_ingest.py` stub: drains stdin, then optionally hangs.

W24_O2_MODE=hang reproduces an unresponsive OpenObserve (P6-751); anything else
returns a successful ingest, and a non-zero exit is reached by pointing the
probe's `python3` at a failing stub instead.
"""
import os
import sys
import time

sys.stdin.read()
if os.environ.get("W24_O2_MODE") == "hang":
    time.sleep(60)
if os.environ.get("W24_O2_MODE") == "fail":
    sys.exit(1)
print("stub o2 ingest: ingested")
