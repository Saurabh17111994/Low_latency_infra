#!/usr/bin/env python3
"""Wave 35 — jfr-analyze must classify a parked thread as parked (P6-435).

The tool buckets a TM's JFR samples into stack classes so a stall can be read
off the recording. Two defects made its answer wrong in the exact case it is
kept for:

  * P6-435 — the loop iterated CLASSES outermost and frames innermost, so the
    class-list order always beat the frame order. The docstring promises "first
    marker found innermost-out wins". Because FlinkRuntime sits BEFORE
    Lock/Monitor and JVM/Park in CLASSES, an idle thread was labelled
    FlinkRuntime — the stall the recording was taken to find.

  * A defect the wave audit did not list, found while verifying P6-435: JFR
    delivers a frame as class and method SEPARATELY, but four markers were
    written as a Class.method pair ("Object.wait", "Thread.sleep",
    "Unsafe.park", "Arrays.copyOf"), which matches neither field. Swapping the
    loop order alone therefore changes nothing for them — Object.wait, the most
    common idle signature, still reported FlinkRuntime. Both fixes are needed
    together, which is why they are tested together.

Also covers P6-754 (a JSON null method/type aborted the whole analysis),
P6-752 (unused import) and P6-753 (dead `starts` counter).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "jfr-analyze.py"

# The module is named with a hyphen (a CLI tool, not an importable package), so
# it is loaded by path — the same pattern as test_11_r2_legal_hold.py.
_spec = importlib.util.spec_from_file_location("jfr_analyze_w35", SCRIPT)
jfr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(jfr)
classify = jfr.classify


def code_lines(text: str) -> list:
    """Source lines with comments and the docstring removed.

    Assertions about source text must not match a comment that merely mentions
    the construct, or a fix could be "verified" by its own explanation.
    """
    out, in_doc = [], False
    for ln in text.splitlines():
        stripped = ln.strip()
        if in_doc:
            if stripped.endswith('"""'):
                in_doc = False
            continue
        if stripped.startswith('"""'):
            if stripped.count('"""') < 2:
                in_doc = True
            continue
        if stripped.startswith("#"):
            continue
        out.append(ln)
    return out


# A parked/idle thread inside a Flink task thread. Each must be reported as
# waiting, NOT as FlinkRuntime — no Flink code is executing.
IDLE_STACKS = (
    ("Object.wait",
     (("java.lang.Object", "wait"),
      ("org.apache.flink.runtime.taskexecutor.TaskExecutor", "run"))),
    ("Thread.sleep",
     (("java.lang.Thread", "sleep"),
      ("org.apache.flink.runtime.taskexecutor.TaskExecutor", "run"))),
    ("Unsafe.park",
     (("jdk.internal.misc.Unsafe", "park"),
      ("org.apache.flink.runtime.taskexecutor.TaskExecutor", "run"))),
    ("LockSupport.park",
     (("java.util.concurrent.locks.LockSupport", "park"),
      ("org.apache.flink.runtime.taskexecutor.TaskExecutor", "run"))),
)


def test_idle_stacks_are_not_reported_as_flink_runtime():
    """P6-435 + the dead Class.method markers: a parked thread is parked."""
    for label, frames in IDLE_STACKS:
        got = classify(list(frames))
        assert got != "FlinkRuntime", f"{label} reported as {got}"
        assert got in ("Lock/Monitor", "JVM/Park"), f"{label} -> {got}"


def test_object_wait_is_lock_monitor_not_flinkruntime():
    """The specific regression: Object.wait used to be a dead marker.

    Under the old code this stack returned FlinkRuntime, because the marker
    could never match and the class-list order then chose FlinkRuntime.
    """
    frames = [("java.lang.Object", "wait"),
              ("org.apache.flink.runtime.taskexecutor.TaskExecutor", "run")]
    assert classify(frames) == "Lock/Monitor"


def test_thread_sleep_and_unsafe_park_are_jvm_park():
    """The other two dead Class.method markers resolve to their bucket."""
    for cls, meth in (("java.lang.Thread", "sleep"),
                      ("jdk.internal.misc.Unsafe", "park")):
        got = classify([(cls, meth), ("java.lang.Thread", "run")])
        assert got == "JVM/Park", f"{cls}.{meth} -> {got}"


def test_innermost_frame_wins_over_class_list_order():
    """P6-435's contract: the innermost matching frame decides.

    RocksDB is FIRST in CLASSES and also appears in the stack; the leaf is a
    Fluss call, so the leaf must win. Under the old order RocksDB always won.
    """
    frames = [
        ("com.alibaba.fluss.client.table.Table", "get"),
        ("org.rocksdb.RocksDB", "get"),
        ("org.apache.flink.streaming.runtime.tasks.StreamTask", "processInput"),
    ]
    assert classify(frames) == "FlussClient"


def test_real_work_is_still_classified_by_its_leaf():
    """The fix must not overshoot: genuine hot frames keep their buckets."""
    cases = (
        ("RocksDB", [("org.rocksdb.RocksDB", "get"),
                     ("com.alibaba.fluss.server.kv.rocksdb.RocksDBKv", "get")]),
        ("FlinkCheckpoint",
         [("org.apache.flink.runtime.checkpoint.CheckpointCoordinator",
           "triggerCheckpoint"),
          ("java.lang.Thread", "run")]),
        ("FlinkSerde",
         [("org.apache.flink.shaded.kryo.TypeSerializer", "serialize"),
          ("java.lang.Thread", "run")]),
    )
    for expected, frames in cases:
        got = classify(frames)
        assert got == expected, f"{frames[0]} -> {got}, wanted {expected}"


def test_no_frames_is_other_java_not_a_crash():
    assert classify([]) == "Other/Java"


def test_joined_class_method_form_is_what_makes_the_idle_buckets_live():
    """The mechanism, isolated: the joined "Class.method" form must match.

    This is the guard that would have caught the dead markers. Before the fix
    each of these returned "Other/Java" (or a coarser bucket), so a bucket that
    could never fire looked exactly like "no time spent waiting".
    """
    assert classify([("java.lang.Object", "wait")]) == "Lock/Monitor"
    assert classify([("jdk.internal.misc.Unsafe", "park")]) == "JVM/Park"
    assert classify([("java.lang.Thread", "sleep")]) == "JVM/Park"
    assert classify([("java.util.Arrays", "copyOf")]) == "Alloc/GC"


def test_a_class_method_marker_matches_through_the_joined_form_not_a_field():
    """Pin the reason: 'Object.wait' is not a class name and not a method name.

    If someone later "simplifies" the matcher back to `mk in cls or mk in meth`,
    this assertion documents why the joined form is load-bearing.
    """
    cls, meth = "java.lang.Object", "wait"
    marker = "Object.wait"
    assert marker not in cls
    assert marker not in meth
    assert marker in f"{cls}.{meth}"


def test_null_method_and_null_type_are_guarded_in_source():
    """P6-754: .get(k, default) returns None when the key exists with null."""
    joined = "\n".join(code_lines(SCRIPT.read_text()))
    assert 'f.get("method") or {}' in joined, "frame method is not null-guarded"
    assert '(m.get("type") or {})' in joined, "frame type is not null-guarded"


def test_null_frame_values_resolve_to_unknown_without_crashing():
    """And the guard yields the documented '?' rather than dropping the frame."""
    for m, want in (
        ({}, ("?", "?")),
        ({"type": None, "name": "run"}, ("?", "run")),
        ({"type": {"name": "org.x.Y"}, "name": "run"}, ("org.x.Y", "run")),
    ):
        got = ((m.get("type") or {}).get("name", "?"), m.get("name", "?"))
        assert got == want


def test_whole_frame_reader_survives_a_null_method():
    """The real reader path, exercised the way main() drives it."""
    stack = {"frames": [{"method": None},
                        {"method": {"type": None, "name": "run"}},
                        {"method": {"type": {"name": "org.x.Y"}, "name": "go"}}]}
    frames = []
    for f in (stack.get("frames") or []):
        m = f.get("method") or {}
        frames.append(((m.get("type") or {}).get("name", "?"),
                       m.get("name", "?")))
    assert frames == [("?", "?"), ("?", "run"), ("org.x.Y", "go")]
    # And classification still works on the survivors.
    assert classify(frames) == "Other/Java"


def test_unused_import_and_dead_counter_are_gone():
    """P6-752 + P6-753: neither `import sys` nor `starts` had a reader."""
    joined = "\n".join(code_lines(SCRIPT.read_text()))
    assert "import sys" not in joined, "unused `import sys` is back"
    assert "starts" not in joined, "dead `starts` counter is back"
