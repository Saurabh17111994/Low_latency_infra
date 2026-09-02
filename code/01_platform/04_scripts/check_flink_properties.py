#!/usr/bin/env python3
"""Fail-fast validator for the FLINK_PROPERTIES block in docker-compose.yml.

WHY THIS EXISTS (2026-09-02, two production incidents in one day):

1. The flink image's docker-entrypoint feeds EVERY line of the
   FLINK_PROPERTIES env var to Flink as a config property
   (process_flink_properties never skips '#'). A YAML comment line inside
   the block becomes a garbage dotted key (e.g. "#P8(tracker14)" ->
   value "distributed-safemetrics..."), which is merged into config.yaml.

2. The merge (BashJavaUtils -> YamlParserUtils
   convertAndDumpYamlFromFlatMap) re-nests dotted keys. When one key is a
   strict PATH PREFIX of another (e.g. `state.backend` vs
   `state.backend.rocksdb.localdir`), the re-nest either:
   - throws ClassCastException: String cannot be cast to Map -> the
     entrypoint writes the error text INTO config.yaml -> JM/TM
     crash-loop on "expected '<document start>'" (observed 2026-09-02), or
   - silently DROPS one side (HashMap iteration-order dependent;
     observed 2026-09-02: localdir + incremental vanished from the
     generated config.yaml, putting RocksDB back on the container
     overlay - the CHG-120 ~1.7k/s degradation class).

This validator fails LOUD at edit time (exit 1 + reason) for:
  A. comment lines inside the FLINK_PROPERTIES block
  B. any key that is a strict path prefix of another key
  C. required keys missing from the block

Pinned by G22 in test-pipeline-lib.sh; unit-tested by
tests/test_check_flink_properties.py.
"""
import re
import sys
from pathlib import Path

COMPOSE = Path(__file__).resolve().parent.parent / "01_docker" / "docker-compose.yml"

# Keys the generated TM config.yaml MUST carry. Every one of these was
# silently droppable by the collision before the fix; a regression (edit
# removing one) must fail the guard suite, not a 3am run.
REQUIRED_KEYS = [
    "jobmanager.rpc.address",
    "metrics.reporter.prom.port",
    "metrics.latency.interval",
    "taskmanager.numberOfTaskSlots",
    "taskmanager.memory.process.size",
    "taskmanager.memory.managed.fraction",
    "state.backend.incremental",
    "state.backend.rocksdb.localdir",
]

# Keys that must NOT appear as bare leaves because they prefix-collide with
# nested keys (or are known to abort the merge).
FORBIDDEN_LEAVES = {
    "state.backend": (
        "prefix-collides with state.backend.rocksdb.localdir / "
        "state.backend.incremental in YamlParserUtils "
        "convertAndDumpYamlFromFlatMap -> ClassCastException crash-loop or a "
        "SILENT drop of the nested keys (RocksDB falls back to the container "
        "overlay, the CHG-120 ~1.7k/s degradation). Pin the backend at JOB "
        "level (SignalJob.applyRuntimeOptions) instead."
    ),
    "env.java.opts": (
        "collides with the image's nested env.java.opts.all key and aborts "
        "startup (ClassCastException, documented in docker-compose.yml)."
    ),
}


def extract_block(compose_text: str) -> list[str]:
    """Return the FLINK_PROPERTIES block lines (6-space indented, no blanks)."""
    m = re.search(r"    FLINK_PROPERTIES: \|\n((?:      .*\n|\n)*?)(?=    [A-Za-z_]+:)",
                  compose_text)
    if not m:
        raise ValueError("FLINK_PROPERTIES block not found in docker-compose.yml")
    return [l.strip() for l in m.group(1).split("\n") if l.strip()]


def parse_props(lines: list[str]) -> list[tuple[str, str]]:
    """Mirror the entrypoint's process_flink_properties parse (first ':')."""
    props = []
    for line in lines:
        if ":" not in line:
            props.append((line, ""))
            continue
        key, value = line.split(":", 1)
        props.append((key.strip(), value.strip()))
    return props


def check(compose_path: Path = COMPOSE) -> list[str]:
    """Return a list of failure reasons (empty = valid)."""
    failures = []
    lines = extract_block(compose_path.read_text())
    props = parse_props(lines)
    keys = [k for k, _ in props]

    # A. comment lines inside the block
    for line in lines:
        if line.startswith("#"):
            failures.append(
                f"COMMENT LINE inside FLINK_PROPERTIES: {line!r}\n"
                f"    WHY: docker-entrypoint process_flink_properties does NOT skip '#'\n"
                f"    lines - every line becomes a Flink config property, so this comment\n"
                f"    becomes a garbage dotted key that can crash or corrupt config.yaml\n"
                f"    (ClassCastException crash-loop observed 2026-09-02).\n"
                f"    FIX: move the comment ABOVE the 'FLINK_PROPERTIES: |' line."
            )

    # B. prefix collisions (a key that is a strict path prefix of another)
    for a in keys:
        for b in keys:
            if a != b and b.startswith(a + "."):
                failures.append(
                    f"PREFIX COLLISION: {a!r} vs {b!r}\n"
                    f"    WHY: YamlParserUtils.convertAndDumpYamlFromFlatMap re-nests dotted\n"
                    f"    keys; a String leaf ({a}) cannot also be a Map for ({b}) ->\n"
                    f"    ClassCastException crash-loop, or one side SILENTLY dropped\n"
                    f"    (HashMap-order dependent). Observed 2026-09-02: localdir+\n"
                    f"    incremental vanished -> RocksDB on the container overlay.\n"
                    f"    FIX: keep only the nested key; pin the leaf at JOB level."
                )

    # B2. known forbidden leaves
    for k, why in FORBIDDEN_LEAVES.items():
        if k in keys:
            failures.append(f"FORBIDDEN LEAF {k!r} present in FLINK_PROPERTIES.\n    WHY: {why}")

    # C. required keys
    for req in REQUIRED_KEYS:
        if req not in keys:
            failures.append(
                f"REQUIRED KEY MISSING: {req!r}\n"
                f"    WHY: this key was silently droppable by the prefix collision before\n"
                f"    the 2026-09-02 fix; a missing key here means the TM runs without it\n"
                f"    (e.g. no localdir -> RocksDB on the container overlay, the CHG-120\n"
                f"    ~1.7k/s degradation class).\n"
                f"    FIX: restore the key in the FLINK_PROPERTIES block."
            )

    return failures


def main() -> int:
    if not COMPOSE.exists():
        print(f"FAIL: {COMPOSE} not found", file=sys.stderr)
        return 1
    failures = check()
    if failures:
        print("FLINK_PROPERTIES validation FAILED "
              f"({len(failures)} problem(s)):", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print("FLINK_PROPERTIES OK: no comments, no prefix collisions, "
          "all required keys present")
    return 0


if __name__ == "__main__":
    sys.exit(main())
