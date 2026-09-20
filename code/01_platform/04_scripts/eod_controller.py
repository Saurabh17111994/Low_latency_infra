#!/usr/bin/env python3
"""EOD controller CLI launcher (SCH-23): runs the plain-JVM
com.trading.common.schema.eod.EodControllerTool against the live Fluss
cluster, mirroring the ddl-apply host-side pattern (same pinned jar set,
FLUSS_BOOTSTRAP via env, machine-readable RESULT sentinel).

Usage:
  python3 eod_controller.py <subcommand> [tool args...]   # status|run|extend|reconcile|reset

Env:
  FLUSS_BOOTSTRAP  (default localhost:9123)   passed to the tool
  EOD_*            (EOD_DATABASE, EOD_STATE_TABLE, EOD_TABLES, EOD_TTL,
                    EOD_SAFETY_FLOOR, EOD_EXTENSION, EOD_OFFLOAD, EOD_ZONE ...)
                   read directly by the tool
  EOD_M2_REPO      overrides ~/.m2/repository

The tool's exit code is propagated; the sentinel line the tool already prints
(`eod-controller: RESULT=... EXIT=...`) is echoed unchanged.
"""
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# EOD_M2_REPO first (host runs), then the layout the ddl-apply image already sets for its own
# tool — the same jars, so a container needs no extra configuration to find them.
M2_REPO = (os.environ.get("EOD_M2_REPO") or os.environ.get("DDL_APPLY_M2_REPO")
           or os.path.expanduser("~/.m2/repository"))

# Pinned Fluss + Jackson versions — same set the ddl-apply engine uses
# (ddl_apply.py JACKSON_VERSION/SLF4J_VERSION).
FLUSS_VERSION = os.environ.get("FLUSS_VERSION", "0.9.1-incubating")
JACKSON_VERSION = os.environ.get("JACKSON_VERSION", "2.16.1")
SLF4J_VERSION = os.environ.get("SLF4J_VERSION", "2.0.9")

COMMON_CLASSES = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..", "common", "target", "classes")
)

MAIN_CLASS = "com.trading.common.schema.eod.EodControllerTool"


def build_classpath():
    jars = [
        (FLUSS_VERSION, "org/apache/fluss", "fluss-client"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-databind"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-core"),
        (JACKSON_VERSION, "com/fasterxml/jackson/core", "jackson-annotations"),
        (SLF4J_VERSION, "org/slf4j", "slf4j-api"),
    ]
    entries = [COMMON_CLASSES]
    missing = []
    for version, group, name in jars:
        path = os.path.join(M2_REPO, *group.split("/"), name, version, f"{name}-{version}.jar")
        if os.path.isfile(path):
            entries.append(path)
        else:
            missing.append(path)
    if not os.path.isfile(os.path.join(COMMON_CLASSES,
                                       "com/trading/common/schema/eod/EodControllerTool.class")):
        missing.append(COMMON_CLASSES + " (run `cd code && mvn -o compile -pl common` first)")
    if missing:
        print("EOD CONTROLLER CLASSPATH INCOMPLETE:", file=sys.stderr)
        for m in missing:
            print("  - " + m, file=sys.stderr)
        return None
    return os.pathsep.join(entries)


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return 2 if not args else 0

    classpath = build_classpath()
    if classpath is None:
        return 2

    cmd = ["java", "-cp", classpath, MAIN_CLASS] + args
    # Inherit the environment untouched — the tool reads FLUSS_BOOTSTRAP / EOD_*.
    try:
        proc = subprocess.run(cmd, env=os.environ.copy())
    except FileNotFoundError:
        print("ERROR: java not found on PATH", file=sys.stderr)
        return 2
    print(f"eod-controller-launcher: EXIT={proc.returncode} CMD={args[0]}")
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
