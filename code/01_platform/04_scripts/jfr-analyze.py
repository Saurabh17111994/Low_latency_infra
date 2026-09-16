#!/usr/bin/env python3
"""Aggregate a TM JFR recording into per-period stack-class time budgets.

Read-only diagnosis tooling over a .jfr produced by the compose
StartFlightRecording flag. Events are read via host `jfr print --json`
(schema: {"type": ..., "values": {startTime, sampledThread, stackTrace,
frames[{method:{type:{name},name}}]}}) and bucketed into caller-supplied
UTC epoch periods from the fused timeline.
"""
import argparse
import collections
import datetime
import json
import subprocess

CLASSES = [
    ("RocksDB", ["org.rocksdb", "rocksdb::"]),
    ("FlussClient", ["com.alibaba.fluss"]),
    ("Netty/SocketIO", ["io.netty", "sun.nio.ch", "java.net.",
                        "SocketInputStream", "SocketOutputStream"]),
    ("FlinkMailbox", ["mailbox", "MailboxThread", "StreamTask"]),
    ("FlinkTimer", ["InternalTimerService", "TimerHeapInternalTimerService",
                    "ProcessingTimeService", "TimerService"]),
    ("FlinkCheckpoint", ["CheckpointBarrier", "SubtaskCheckpointCoordinator",
                         "checkpoint", "Snapshot", "SnapshotContext"]),
    ("FlinkSerde", ["flink.shaded", "TypeSerializer", "Serializer"]),
    ("FlinkRuntime", ["org.apache.flink"]),
    ("Lock/Monitor", ["Object.wait", "Monitor", "ReentrantLock",
                      "AbstractQueuedSynchronizer", "LockSupport"]),
    ("JVM/Park", ["Thread.sleep", "Unsafe.park", "jdk.internal.misc",
                  "Safepoint"]),
    ("Alloc/GC", ["Arrays.copyOf", "G1", "TLAB"]),
]

INTERESTING = ("jdk.ExecutionSample", "jdk.NativeMethodSample",
               "jdk.ThreadPark", "jdk.JavaMonitorEnter", "jdk.JavaMonitorWait",
               "jdk.JavaMonitorBlocked", "jdk.SocketRead", "jdk.SocketWrite",
               "jdk.ThreadSleep", "jdk.ObjectAllocationSample")


def classify(frames):
    """frames innermost-first; first marker found innermost-out wins.

    A frame is matched on three forms: the class name, the method name, and the
    joined "Class.method" form. The joined form is required, not cosmetic: JFR
    delivers class and method as separate fields, so a marker written as a
    Class.method pair ("Object.wait", "Thread.sleep", "Unsafe.park",
    "Arrays.copyOf") matches neither field on its own and would mark its bucket
    unreachable. Verified 2026-09-16 — before this, a thread parked in
    Object.wait (the single most common idle signature) was reported as
    FlinkRuntime, and the Lock/Monitor and JVM/Park buckets could only be
    reached through their class-substring markers.
    """
    for cls, meth in frames:
        joined = f"{cls}.{meth}"
        for name, markers in CLASSES:
            for mk in markers:
                if mk in cls or mk in meth or mk in joined:
                    return name
    return "Other/Java"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("recording")
    ap.add_argument("--periods", required=True,
                    help="NAME=START-END,... epoch seconds UTC")
    ap.add_argument("--stack-depth", type=int, default=16)
    ap.add_argument("--top", type=int, default=8)
    args = ap.parse_args()

    periods = []
    for spec in args.periods.split(","):
        name, rng = spec.split("=")
        s, e = rng.split("-")
        periods.append((name, float(s), float(e)))

    out = subprocess.run(
        ["jfr", "print", "--json", "--stack-depth", str(args.stack_depth),
         "--events", ",".join(INTERESTING), args.recording],
        capture_output=True, text=True, check=True)
    doc = json.loads(out.stdout)
    events = doc["recording"]["events"]

    counts = collections.defaultdict(collections.Counter)   # (per,etype)->cls
    tops = collections.defaultdict(collections.Counter)     # (per,etype)->frame
    threads = collections.defaultdict(collections.Counter)  # (per,etype,cls)->th
    totals = collections.Counter()
    overall = collections.Counter()                         # etype total

    for ev in events:
        v = ev.get("values", ev)
        etype = ev.get("type", "?")
        overall[etype] += 1
        st = str(v.get("startTime", ""))
        try:
            dt = datetime.datetime.fromisoformat(st.replace("Z", "+00:00"))
        except ValueError:
            continue
        t = dt.timestamp()
        matched = None
        for name, ps, pe in periods:
            if ps <= t <= pe:
                matched = name
                break
        if matched is None:
            continue
        stack = (v.get("stackTrace") or {})
        frames = []
        for f in (stack.get("frames") or []):
            # `or {}` rather than a .get default: jfr print emits an explicit
            # JSON null for a method/type it could not record, and .get returns
            # that null instead of the default, so .get("name") would raise.
            m = f.get("method") or {}
            frames.append(((m.get("type") or {}).get("name", "?"),
                           m.get("name", "?")))
        cls = classify(frames) if frames else "NoStack"
        key = (matched, etype)
        counts[key][cls] += 1
        totals[key] += 1
        if frames:
            tops[key][frames[0][0].split(".")[-1] + "#" + frames[0][1]] += 1
        th = (v.get("sampledThread") or {})
        th = th.get("javaName") or th.get("osName") or "?"
        threads[(matched, etype, cls)][th] += 1

    print("== overall event counts ==")
    for et, n in overall.most_common():
        print(f"  {et:32s} {n}")
    for (per, etype) in sorted(counts):
        tot = totals[(per, etype)]
        print(f"\n=== {per} | {etype} | {tot} events ===")
        for cls, n in counts[(per, etype)].most_common():
            pct = 100.0 * n / tot
            if pct >= 1.0:
                print(f"  {cls:16s} {pct:6.1f}%  ({n})")
        if tops[(per, etype)]:
            print("  -- top innermost frames --")
            for fr, n in tops[(per, etype)].most_common(args.top):
                print(f"  {100.0*n/tot:6.1f}%  {fr}")
        # thread breakdown for the top 2 classes
        for cls, _ in counts[(per, etype)].most_common(2):
            tc = threads[(per, etype, cls)]
            if tc:
                print(f"  -- [{cls}] threads --")
                for th, n in tc.most_common(5):
                    print(f"     {th}: {n}")


if __name__ == "__main__":
    main()
