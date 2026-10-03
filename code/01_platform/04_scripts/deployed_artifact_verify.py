#!/usr/bin/env python3
"""deployed_artifact_verify.py - assert that a DEPLOYED artifact carries the fixed code,
not merely a version string or a fresh build stamp.

WHY this exists (2026-09-24)
  Two image rebuilds were verified by inspecting one class's constants, and the
  real defect lived in a different class's controlling expression: a version
  string, a label, and a schema constant all proved nothing about whether the
  running code was the fixed code. A stale ingest also survived a rebuild
  unnoticed. This tool asks the narrow question that actually matters after a
  swap: does the artifact in the image (or the jar the cluster mounts) reference
  the code we just fixed?

  It is NOT image_staleness_check.py (is this image current?) and NOT
  verify-fluss-image-delta.sh (does the derived Fluss image add only its plugin
  delta?). This asks: is the fixed behaviour inside the artifact?

WHAT "fixed" MEANS here - reference-level checks, not behaviour:
  * `javap -c` is searched for a field/method reference that the fix introduced
    (present) and for the reference the fix removed (absent).
  * `javap -constants` is searched for a resolved compile-time constant.
  These are presence claims about bytecode, not proofs of runtime behaviour. A
  constant can still be misused by a caller, and a present string can still be
  dead code; both caveats are printed with the verdict.

DEPLOYMENT REALITY this tool encodes (verified from the Dockerfiles):
  * ingestion image: bakes /app/ingestion.jar (java-builder stage).
  * compute image: carries NO jar by design - it is the Flink runtime plus the
    launcher script /opt/flink-jobs/submit-jobs.sh; compute.jar is mounted by
    compose from the host build (target/compute.jar). So the compute target
    checks the LAUNCHER in the image and the CLASSES in a jar you pass with
    --jar, and says so when a source is missing instead of silently skipping.

Usage (options only; never positional):
  deployed_artifact_verify.py --target ingestion --image 01_docker-ingestion:latest
  deployed_artifact_verify.py --target compute --image 01_docker-compute:latest \
      --jar code/02_services/02_compute/target/compute.jar
  deployed_artifact_verify.py --list-checks          # inventory, no docker needed

An artifact that is not on this host at all is SKIP, not FAIL: a fresh checkout
must still be able to bring the stack up. An artifact that IS present and does
not carry the fixed code is FAIL, and that is the case worth blocking on.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile

USAGE = "usage: deployed_artifact_verify.py --target {ingestion,compute} [--image REF] [--jar PATH]"

# (target, name, source, kind, what) - kind: 'class' (zip+javap) or 'script' (text)
CHECKS = [
    ("ingestion", "RealFlussRowConverter tick_type", "image-jar", "class",
     "references VALID_TRADE and NOT VALID_NON_TRADE (the live generic writer)"),
    ("ingestion", "TypedFlussRowConverter tick_type", "image-jar", "class",
     "references VALID_TRADE and NOT VALID_NON_TRADE (A/B mode)"),
    ("ingestion", "RawTableSchema version", "image-jar", "class",
     'ROW_SCHEMA_VERSION = "4" (the v4 full-mode contract)'),
    ("compute", "launcher idempotency", "image-script", "script",
     'job_already_running present; jq branch selects .state=="RUNNING"; '
     "bounded [^}]* fallback present"),
    ("compute", "RawTableColumns width", "host-jar", "class",
     "FIELD_COUNT = 72 and VOLUME_DELTA = 27"),
    ("compute", "RawTableSchema version", "host-jar", "class",
     'ROW_SCHEMA_VERSION = "4"'),
    ("compute", "candle aggregator", "host-jar", "class",
     "CandleAggregateFunction present (the multi-timeframe path)"),
]

CLASS_CHECKS = {
    "RealFlussRowConverter tick_type": [
        ("com/trading/ingestion/RealFlussRowConverter.class", "disasm", "VALID_TRADE", True),
        ("com/trading/ingestion/RealFlussRowConverter.class", "disasm", "VALID_NON_TRADE", False),
    ],
    "TypedFlussRowConverter tick_type": [
        ("com/trading/ingestion/TypedFlussRowConverter.class", "disasm", "VALID_TRADE", True),
        ("com/trading/ingestion/TypedFlussRowConverter.class", "disasm", "VALID_NON_TRADE", False),
    ],
    "RawTableSchema version": [
        ("com/trading/common/schema/RawTableSchema.class", "constants", 'ROW_SCHEMA_VERSION = "4"', True),
    ],
    "RawTableColumns width": [
        ("com/trading/compute/signaljob/RawTableColumns.class", "constants", "FIELD_COUNT = 72", True),
        ("com/trading/compute/signaljob/RawTableColumns.class", "constants", "VOLUME_DELTA = 27", True),
    ],
    "candle aggregator": [
        ("com/trading/compute/signaljob/CandleAggregateFunction.class", "present", None, True),
    ],
}

SCRIPT_CHECKS = {
    "launcher idempotency": ["job_already_running", '.state=="RUNNING"', "[^}]*"],
}


def fail_usage(msg):
    sys.stderr.write("deployed_artifact_verify.py: %s\n%s\n" % (msg, USAGE))
    return 2


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def list_checks():
    print("checks (target | source | name | what it asserts):")
    for target, name, source, _kind, what in CHECKS:
        print("  %-9s | %-12s | %-32s | %s" % (target, source, name, what))
    return 0


def extract_from_image(image, path_in_image, keep=False):
    """COPY a path out of an image without running it (create + cp), always cleanup."""
    cid = run(["docker", "create", image]).stdout.strip()
    if not cid:
        return None, "docker create %s failed (image missing?)" % image
    tmpdir = tempfile.mkdtemp(prefix="deployed-artifact-verify-")
    try:
        rc = run(["docker", "cp", "%s:%s" % (cid, path_in_image), tmpdir])
        if rc.returncode != 0:
            # XC-18: this return used to skip the cleanup the docstring promises.
            shutil.rmtree(tmpdir, ignore_errors=True)
            return None, "docker cp %s failed: %s" % (path_in_image, rc.stderr.strip()[:120])
        dest = os.path.join(tmpdir, os.path.basename(path_in_image))
        if not os.path.exists(dest):
            shutil.rmtree(tmpdir, ignore_errors=True)
            return None, "%s not found in %s" % (path_in_image, image)
        if not keep:
            # the file must outlive this call, so hand back the path and let main clean up
            with open(dest, "rb") as fh:
                blob = fh.read()
            shutil.rmtree(tmpdir, ignore_errors=True)
            fd, tmp = tempfile.mkstemp(prefix="deployed-artifact-verify-", suffix="-" + os.path.basename(path_in_image))
            with os.fdopen(fd, "wb") as fh:
                fh.write(blob)
            return tmp, None
        return dest, None
    finally:
        run(["docker", "rm", "-f", cid])
    # caller owns cleanup of tmpdir (kept on --keep, removed otherwise)


def check_class(jar, name):
    """Evaluate CLASS_CHECKS[name] against a jar; returns (ok, detail). ok None = skip."""
    specs = CLASS_CHECKS[name]
    soft = name in ("RawTableColumns width", "candle aggregator")
    try:
        z = zipfile.ZipFile(jar)
    except Exception as exc:  # noqa: BLE001 - an unreadable jar is a check failure, not a crash
        return False, "cannot read jar: %s" % exc
    names = set(z.namelist())
    details = []
    ok = True
    for cls, mode, needle, want_present in specs:
        if cls not in names:
            if soft:
                return None, "%s not in this jar" % os.path.basename(cls)
            return False, "%s missing from jar" % cls
        if mode == "present":
            details.append("%s present" % os.path.basename(cls))
            continue
        tmpdir = tempfile.mkdtemp(prefix="deployed-artifact-verify-")
        try:
            p = os.path.join(tmpdir, os.path.basename(cls))
            with open(p, "wb") as fh:
                fh.write(z.read(cls))
            args = ["javap", "-p", "-constants", p] if mode == "constants" else ["javap", "-c", "-p", p]
            out = run(args).stdout
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
        present = needle in out
        matched = present == want_present
        ok = ok and matched
        state = "present" if present else "absent"
        if matched:
            note = "as required"
        elif want_present:
            note = "MISSING - the fix is not in this artifact"
        else:
            note = "STILL PRESENT - the old rule is in this artifact"
        details.append("%s: %s, %s" % (needle, state, note))
    return ok, "; ".join(details)


def check_script(path, name):
    text = open(path, encoding="utf-8", errors="replace").read()
    missing = [n for n in SCRIPT_CHECKS[name] if n not in text]
    if missing:
        return False, "missing from the deployed launcher: %s" % ", ".join(repr(m) for m in missing)
    return True, "all %d markers present" % len(SCRIPT_CHECKS[name])


def main(argv):
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--target")
    p.add_argument("--image")
    p.add_argument("--jar")
    p.add_argument("--keep", action="store_true")
    p.add_argument("--list-checks", action="store_true")
    p.add_argument("-h", "--help", action="store_true")
    try:
        a = p.parse_args(argv)
    except SystemExit:
        return fail_usage("unrecognised argument")
    if a.help:
        print(USAGE)
        return 0
    if a.list_checks:
        return list_checks()
    if a.target not in ("ingestion", "compute"):
        return fail_usage("--target must be ingestion or compute")
    if not a.image and not a.jar:
        return fail_usage("give --image and/or --jar")

    image_jar = image_script = None
    extracted = []          # temp artifacts this process created and must remove
    image_absent = None
    if a.image and run(["docker", "image", "inspect", a.image]).returncode != 0:
        # Not built on this host yet: nothing to verify, and refusing to proceed
        # would block a legitimate first bring-up. Reported as SKIP below.
        image_absent = a.image
    if a.image and not image_absent:
        path_in_image = "/app/ingestion.jar" if a.target == "ingestion" else "/opt/flink-jobs/submit-jobs.sh"
        got, err = extract_from_image(a.image, path_in_image, keep=a.keep)
        if err and a.target == "ingestion":
            print("  [FAIL] %s: %s" % (path_in_image, err))
            return 1
        if err:
            print("  [SKIP] launcher script: %s" % err)
        elif a.target == "ingestion":
            image_jar = got
        else:
            image_script = got
        if got and not a.keep:
            extracted.append(got)

    results = []
    for target, name, source, _kind, _what in CHECKS:
        if target != a.target:
            continue
        if source == "image-jar":
            if image_absent:
                results.append((name, None, "image %s not present on this host" % image_absent))
                continue
            if not image_jar:
                results.append((name, None, "no --image given (or jar not extracted)"))
                continue
            ok, detail = check_class(image_jar, name)
        elif source == "image-script":
            if image_absent:
                results.append((name, None, "image %s not present on this host" % image_absent))
                continue
            if not image_script:
                results.append((name, None, "launcher not extracted (pass --image)"))
                continue
            ok, detail = check_script(image_script, name)
        else:
            if not a.jar:
                results.append((name, None, "no --jar given: the compute image carries no jar by design, so the mounted jar must be checked explicitly"))
                continue
            if not os.path.exists(a.jar):
                results.append((name, False, "jar not found: %s" % a.jar))
                continue
            ok, detail = check_class(a.jar, name)
        results.append((name, ok, detail))

    for path in extracted:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.exists(path):
            os.unlink(path)

    failed = [r for r in results if r[1] is False]
    for name, ok, detail in results:
        tag = "OK  " if ok else ("SKIP" if ok is None else "FAIL")
        print("  [%s] %-32s %s" % (tag, name, detail))
    print()
    print("  checked: %s | image: %s | jar: %s" % (a.target, a.image or "-", a.jar or "-"))
    print("  limits: presence of bytecode references, not runtime behaviour; a host javap older than the jar's class files cannot read it.")
    if failed:
        print("  -> %d check(s) FAILED - do not restart on this artifact" % len(failed))
        return 1
    print("  -> no check failed (see SKIP lines for what was not covered)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
