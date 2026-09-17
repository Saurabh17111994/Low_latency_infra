#!/usr/bin/env python3
"""ddl_apply_smoke.py — regression-guards the DDL apply exit-code contract.

The 9-step application contract's terminal surface (dedicated exit codes 0 full
PASS / 6 acknowledged PASS_WITH_LIMITATION / 1 refused limitation, plus the
machine-readable `ddl-apply: RESULT=... EXIT=...` and `DDL-APPLY-RESULT: ...`
sentinels — see docs/08_implementation/02-schema-storage.md steps 7-8and DdlApplyTool.decideStatus) is what downstream automation branches on. The pure
decision function is unit-tested (DdlApplyToolStatusTest), but the LIVE contract
— orchestrator propagation, sentinel emission, evidence recording, the in-band
COMPAT-FLUSS-005 matrix gate, and the fail-closed refusal — needs end-to-end
regression coverage. This smoke provides it by running the REAL orchestrator CLI
(`ddl_apply.py --apply-verified`) twice against scratch-prefixed catalogs:

  S1  full PASS        DDL_APPLY_SKIP_SMOKE=1                  -> exit 0  RESULT=PASS
  S2  full PASS        write/read smoke enabled (default)      -> exit 0  RESULT=PASS
      NOTE: no live ack-mode scenario. DDL_APPLY_ACK_LIMITATIONS=auto is a
      proven no-op while the manifest predicts zero limitations
      (COMPAT-FLUSS-005 RESOLVED by the owner-approved DDL fix:
      kv.format-version=2 + single-field subset bucket key — the former S3
      ran the IDENTICAL full apply as S2 with the no-op flag and asserted
      the same sentinels plus ack_mode, so it proved nothing S2 does not;
      removed to save one full 27-table live cycle). The
      refusal/acknowledgment machinery (exit 6 / exit 1) stays unit-tested
      for any FUTURE table that re-introduces bucket key == PK.
  S4  containerized    mounts a PRE-SEEDED bad-ownership evidence record
      bad-ownership    (engine-uid-owned, mode 644 — the exact umask/setgid
      drill            regression class) into a scratch evidence dir and runs
                       the FULL containerized apply (`docker compose run
                       ddl-apply apply`): the in-band ownership gate must flip
                       the final exit to 1 with 'EVIDENCE OWNERSHIP CHECK
                       FAILED' naming the seeded record, while the engine's own
                       RESULT= sentinel still documents the apply
                       (PASS EXIT=0) — the apply ran; the gate flipped
                       the exit. Runs only when docker + the built
                       ddl-apply image are available on the HOST.

Every scenario additionally asserts the evidence record's `matrix` object
(status PASS, 4 cells) — the COMPAT-FLUSS-005 matrix is verified IN-BAND by
every apply (never merely referenced as capability evidence), so a matrix
deviation fails the smoke before any status/sentinel assertion matters.

Each scenario uses a fresh unique DDL_APPLY_TABLE_PREFIX, so the empty-catalog
precondition applies to the prefixed names and no platform table is touched;
the tool drops the scratch tables itself and the smoke additionally runs a
best-effort --cleanup-prefix pass.

Env-gated like the live integration tests: SKIPPED (exit 0) when FLUSS_BOOTSTRAP
is unset; otherwise ANY deviation from the pinned contract FAILS the smoke. S4
additionally requires docker + the built `ddl-apply` image on the HOST and SKIPs
with a reason otherwise (the in-container `smoke` subcommand always skips it —
the image ships no docker CLI). When the default evidence dir (repo-root
`logs/ddl-apply`, container-owned 10001:10001 2775 after any container run) is
not writable by the host user, the smoke redirects evidence to a per-run temp
dir via DDL_APPLY_EVIDENCE_DIR — the scenarios assert the evidence CONTENT
(parsed from the orchestrator's printed path), not the default location, so the
host smoke stays regression-worthy without the documented shared group.
Run:  FLUSS_BOOTSTRAP=localhost:9123 python3 ddl_apply_smoke.py
      make ddl-apply-smoke
Wired into the Monday verification gate (run-monday-gates.sh) after the Java
full gate, which already guarantees Fluss up + common compiled.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import ddl_apply  # noqa: E402  (reuse pins, DDL dir, classpath builder)

REPO_ROOT = ddl_apply.REPO_ROOT
# The composite-PK tables the raw 0.9.1 client cannot upsert (bucket key == PK,
# iceberg key encoder) — manifest-predicted by --ack-limitations auto. RESOLVED
# by the owner-approved DDL fix (COMPAT-FLUSS-005): Order_Lifecycle /
# Order_Correlation now use kv.format-version=2 + a single-field subset bucket
# key (account_scope_id / instruction_id), so the raw client can write them and
# the current manifest predicts NO limitation (verified live full PASS). Keep
# the prediction list empty; the acknowledgment machinery stays unit-tested for
# any future table shape that re-introduces bucket key == PK.
EXPECTED_LIMITED = []
# Real capability evidence when present (enrich_evidence just records path+sha).
REAL_EVIDENCE = os.path.join(
    REPO_ROOT, "logs", "schema-compat", "composite-pk-raw-client-20260815.md"
)
SCENARIO_TIMEOUT_S = 900
CLEANUP_TIMEOUT_S = 120
# The ddl-apply image the containerized S4 drill runs (compose's default
# <project>-<service> tag; override for a non-default compose project name).
DDL_APPLY_IMAGE = os.environ.get("DDL_APPLY_IMAGE", "01_docker-ddl-apply:latest")


def cleanup_prefix(prefix, classpath, bootstrap):
    """Best-effort drop of scratch tables left by an interrupted scenario.

    The tool already drops its own prefixed tables on every terminal path; this
    is a safety net for JVMs killed mid-run (subprocess timeout sends SIGKILL,
    so the tool's finally never runs).
    """
    cmd = [
        "java",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "-cp", classpath,
        "com.trading.common.schema.ddl.DdlApplyTool",
        "--ddl-dir", ddl_apply.DDL_DIR,
        "--bootstrap", bootstrap,
        "--cleanup-prefix", prefix,
    ]
    try:
        subprocess.run(cmd, capture_output=True, text=True,
                       timeout=CLEANUP_TIMEOUT_S)
    except (OSError, subprocess.TimeoutExpired):
        pass  # best-effort only


def _compose_cmd(compose_file):
    """`docker compose` argv prefix for this stack — env files included.

    The Makefile always invokes compose as
    `docker compose --env-file <dir>/.env --env-file <dir>/secrets.env -f ...`.
    compose auto-loads `.env` beside the compose file but NOT `secrets.env`, so
    a bare `-f` invocation resolves a DIFFERENT config for the services that
    interpolate secrets: `docker compose config --hash=*` returns different
    hashes for fluss-coordinator and fluss-tablet with and without these files
    (verified 2026-09-13; ddl-apply's hash is identical). Compose then treats
    the running containers as out of date and RECREATES them on the next
    `run`/`up`, so this drill used to restart the Fluss cluster immediately
    before applying against it — the apply died on "CoordinatorEventProcessor
    is not initialized yet", the `ddl-apply: RESULT=PASS EXIT=0` sentinel never
    printed, and the scenario failed against a cluster that the gate's step 9
    had already warmed (2026-09-13, gate attempts 24 and 26). Passing the same
    env files keeps both forms in agreement, so nothing is recreated.
    Missing files are skipped here; compose then fails its own required-variable
    validation, which is the accurate error for that state.
    """
    env_dir = os.path.dirname(os.path.abspath(compose_file))
    cmd = ["docker", "compose"]
    for name in (".env", "secrets.env"):
        path = os.path.join(env_dir, name)
        if os.path.isfile(path):
            cmd += ["--env-file", path]
    return cmd + ["-f", compose_file]


def _docker_smoke_available(compose_file):
    """Return (image_ref, skip_reason) for the containerized S4 drill.

    S4 shells out to docker compose on the HOST; the in-container `smoke`
    subcommand has no docker CLI and always skips. Prereqs: docker CLI, a
    valid compose config (the compose file interpolates required .env vars
    for the whole stack), and the ddl-apply image built (`make ddl-image`).
    """
    if not shutil.which("docker"):
        return None, "docker CLI not found on this host"
    if not os.path.isfile(compose_file):
        return None, f"compose file not found: {compose_file}"
    # P6-352: these two probes had no timeout and no exception handling, unlike
    # every other docker call in this file — a present-but-unresponsive daemon
    # hung the whole drill instead of degrading to a SKIP.
    try:
        cfg = subprocess.run(_compose_cmd(compose_file) + ["config"],
                             capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"the compose config probe did not run: {exc}"
    if cfg.returncode != 0:
        return None, ("docker compose config invalid "
                      "(missing required .env vars?)")
    try:
        image = subprocess.run(["docker", "image", "inspect", DDL_APPLY_IMAGE],
                               capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"docker image inspect did not run: {exc}"
    if image.returncode != 0:
        return None, (f"ddl-apply image not built ({DDL_APPLY_IMAGE}) — "
                      "run `make ddl-image`")
    return DDL_APPLY_IMAGE, None


def _seed_bad_record(seed_dir, image_ref):
    """Pre-seed an engine-uid-owned non-group-writable apply.json (root helper).

    The host user cannot chown to the engine uid (10001), so the seed runs in a
    throwaway root container using the ddl-apply image itself: a record owned
    10001:10001 mode 644 — exactly the umask/setgid regression class the
    ownership gate guards. The wrapper's TOP-LEVEL repair never touches it
    (non-recursive contract), so the in-band gate must catch it.
    """
    cmd = [
        "docker", "run", "--rm", "--user", "root",
        "-v", f"{seed_dir}:/seed",
        "--entrypoint", "bash", image_ref,
        "-c", "mkdir -p /seed/bad && : > /seed/bad/apply.json && "
              "chown -R 10001:10001 /seed/bad && chmod 644 /seed/bad/apply.json",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return r.returncode == 0


def _rm_seed_dir(seed_dir, image_ref):
    """Best-effort cleanup of the seeded dir (engine-uid files need root).

    The wrapper claims the mounted evidence root for the engine user during the
    apply, so the seed dir's top level lands 10001:10001 2775 on the host — the
    host user cannot remove it from /tmp (sticky) without root. The root helper
    mounts the seed dir's PARENT and removes the leaf by name: `rm -rf /seed`
    from inside a mount cannot remove the mountpoint itself (EBUSY).
    """
    parent, leaf = os.path.split(seed_dir)
    if image_ref and os.path.isdir(seed_dir) and os.path.isdir(parent):
        try:
            subprocess.run(
                ["docker", "run", "--rm", "--user", "root",
                 "-v", f"{parent}:/seed-parent",
                 "--entrypoint", "bash", image_ref,
                 "-c", f"rm -rf '/seed-parent/{leaf}'"],
                capture_output=True, text=True, timeout=CLEANUP_TIMEOUT_S)
        except (OSError, subprocess.TimeoutExpired):
            pass
    shutil.rmtree(seed_dir, ignore_errors=True)


def scenario_container_bad_ownership(compose_file, bootstrap, classpath):
    """S4 — containerized negative drill for the in-band evidence-ownership gate.

    The image's `apply` path validates its OWN evidence corpus before exiting
    (ddl-apply-run.sh): a violation of the non-root ownership contract flips
    the final container exit to 1 with 'EVIDENCE OWNERSHIP CHECK FAILED'. This
    drill mounts a PRE-SEEDED bad record (engine-uid-owned 644) into a scratch
    evidence dir and asserts the containerized apply:
      * exits 1 (the gate overrides the apply's own exit 0 for a full PASS),
      * prints EVIDENCE OWNERSHIP CHECK FAILED NAMING the seeded record
        (positive control: the failure is OUR seed, not a wrapper regression),
      * while the engine's RESULT= sentinel still documents the apply itself
        (PASS EXIT=0 — no composite-PK limitation remains after COMPAT-FLUSS-005)
        — the apply ran fully; the gate flipped the exit.
    SKIPs (True) when docker or the ddl-apply image are unavailable; any actual
    deviation from the pinned outcome FAILs.
    """
    prefix = f"ddlsmoke{int(time.time())}{os.getpid()}4_"
    seed_dir = None
    image_ref = None
    try:
        image_ref, reason = _docker_smoke_available(compose_file)
        if image_ref is None:
            print(f"--- scenario 4 [container bad-ownership] SKIPPED — {reason}")
            return True
        seed_dir = tempfile.mkdtemp(prefix="ddl-apply-s4-")
        print(f"--- scenario 4 [container bad-ownership] prefix={prefix} "
              f"seed={seed_dir}")
        if not _seed_bad_record(seed_dir, image_ref):
            print("  FAIL: could not seed the bad-ownership record "
                  "(root helper container failed)")
            return False
        # Invoked through _compose_cmd so the resolved config matches the
        # running containers (see the docstring there): a mismatched form makes
        # `run` recreate fluss-coordinator/fluss-tablet and the apply then races
        # their startup.
        cmd = _compose_cmd(compose_file) + [
            "run", "--rm",
            "-v", f"{seed_dir}:/bad",
            "-e", "DDL_APPLY_EVIDENCE_DIR=/bad",
            "-e", f"DDL_APPLY_TABLE_PREFIX={prefix}",
            "-e", "DDL_APPLY_ACK_LIMITATIONS=auto",
            # The dispatcher's own default (/app/logs/schema-compat/...) is a
            # host mount that the operator flow never creates; catalog-guard.sh
            # defaults to the in-image manifest instead. Without this the apply
            # exits 2 ("matrix evidence not found") before the in-band ownership
            # gate can flip the exit, so the drill measured an unmet env
            # precondition instead of the contract (gate step 11, attempt 8).
            "-e", "DDL_APPLY_MATRIX_EVIDENCE="
                  "/app/code/01_platform/02_sql/ddl/schema_manifest.json",
            "ddl-apply", "apply",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=SCENARIO_TIMEOUT_S)
        except subprocess.TimeoutExpired as exc:
            print(f"  FAIL: timed out after {SCENARIO_TIMEOUT_S}s")
            tail = (exc.stdout or "")[-2000:] if isinstance(exc.stdout, str) else ""
            if tail:
                print("  --- output tail ---")
                print(tail)
            return False

        combined = (result.stdout or "") + (result.stderr or "")
        problems = []
        if result.returncode != 1:
            problems.append(f"exit code {result.returncode} != expected 1 "
                            "(ownership gate must flip the apply exit)")
        for part in ["EVIDENCE OWNERSHIP CHECK FAILED",
                     "/bad/bad/apply.json",
                     "ddl-apply: RESULT=PASS EXIT=0"]:
            if part not in combined:
                problems.append(f"output missing {part!r}")
        if problems:
            print("  FAIL:")
            for p in problems:
                print("    - " + p)
            print("  --- output tail ---")
            print(combined[-4000:])
            return False
        print("  PASS (exit 1 — in-band ownership gate caught the seeded record)")
        return True
    finally:
        if seed_dir is not None:
            _rm_seed_dir(seed_dir, image_ref)
        cleanup_prefix(prefix, classpath, bootstrap)


def scenario(index, extra_env, expect_rc, expect_parts, expect_absent=(),
             check_evidence=None, classpath=None, bootstrap=None):
    """Run one orchestrator apply against a fresh scratch prefix and assert.

    Returns True on full agreement with the pinned contract; on failure prints
    the deviations + output tail and returns False.
    """
    prefix = f"ddlsmoke{int(time.time())}{os.getpid()}{index}_"
    try:
        env = dict(os.environ)
        env["DDL_APPLY_TABLE_PREFIX"] = prefix
        env.update(extra_env)
        cmd = [
            sys.executable,
            os.path.join(SCRIPT_DIR, "ddl_apply.py"),
            "--apply-verified",
            "--matrix-evidence", MATRIX_EVIDENCE,
        ]
        label = ("skip-smoke" if extra_env.get("DDL_APPLY_SKIP_SMOKE") == "1"
                 else extra_env.get("DDL_APPLY_ACK_LIMITATIONS") or "no-ack")
        print(f"--- scenario {index} [{label}] prefix={prefix}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=SCENARIO_TIMEOUT_S, env=env)
        except subprocess.TimeoutExpired as exc:
            print(f"  FAIL: timed out after {SCENARIO_TIMEOUT_S}s")
            tail = (exc.stdout or "")[-2000:] if isinstance(exc.stdout, str) else ""
            if tail:
                print("  --- output tail ---")
                print(tail)
            return False

        combined = (result.stdout or "") + (result.stderr or "")
        problems = []
        if result.returncode != expect_rc:
            problems.append(f"exit code {result.returncode} != expected {expect_rc}")
        for part in expect_parts:
            if part not in combined:
                problems.append(f"output missing {part!r}")
        for part in expect_absent:
            if part in combined:
                problems.append(f"output unexpectedly contains {part!r}")

        if check_evidence is not None:
            m = re.search(r"(\S+/apply\.json)", combined)
            if not m:
                problems.append("no evidence path found in output")
            else:
                try:
                    with open(m.group(1), encoding="utf-8") as fh:
                        evidence = json.load(fh)
                    for key, want in check_evidence.items():
                        got = evidence
                        for part in key.split("."):
                            if not isinstance(got, dict) or part not in got:
                                got = None
                                break
                            got = got[part]
                        if isinstance(want, int) and isinstance(got, list):
                            ok = len(got) == want
                        else:
                            ok = got == want
                        if not ok:
                            problems.append(f"evidence {key}={got!r} != {want!r}")
                except (OSError, json.JSONDecodeError) as exc:
                    problems.append(f"cannot read evidence {m.group(1)}: {exc}")

        if problems:
            print("  FAIL:")
            for p in problems:
                print("    - " + p)
            print("  --- output tail ---")
            print(combined[-3000:])
            return False
        print(f"  PASS (exit {expect_rc})")
        return True
    finally:
        cleanup_prefix(prefix, classpath, bootstrap)


def main():
    parser = argparse.ArgumentParser(
        description="Live smoke for the DDL apply exit-code contract (0/6/1 + "
                    "sentinels). Env-gated on FLUSS_BOOTSTRAP.")
    parser.add_argument(
        "--matrix-evidence",
        help="capability-evidence file (default: the COMPAT-FLUSS-005 evidence "
             "record if present, else a generated placeholder)")
    args = parser.parse_args()

    bootstrap = os.environ.get("FLUSS_BOOTSTRAP")
    if not bootstrap:
        print("ddl-apply-smoke: SKIPPED — FLUSS_BOOTSTRAP unset "
              "(env-gated like the live integration tests)")
        return 0

    versions = ddl_apply.load_versions(ddl_apply.VERSIONS_PIN)
    classpath = ddl_apply.build_tool_classpath(
        versions.get("FLUSS_VERSION", "unknown"))
    if classpath is None:
        print("ddl-apply-smoke: FAIL — tool classpath incomplete "
              "(run `cd code && mvn -o compile -pl common` first)")
        return 2

    global MATRIX_EVIDENCE
    MATRIX_EVIDENCE = args.matrix_evidence
    if MATRIX_EVIDENCE is None and os.path.isfile(REAL_EVIDENCE):
        MATRIX_EVIDENCE = REAL_EVIDENCE
    if MATRIX_EVIDENCE is None:
        fd, MATRIX_EVIDENCE = tempfile.mkstemp(
            prefix="ddl-apply-smoke-evidence-", suffix=".md")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("ddl-apply-smoke placeholder matrix evidence "
                     "(exit-code contract only)\n")
    if not os.path.isfile(MATRIX_EVIDENCE):
        print(f"ddl-apply-smoke: FAIL — matrix-evidence file not found: "
              f"{MATRIX_EVIDENCE}")
        return 2

    print(f"ddl-apply-smoke: cluster={bootstrap} "
          f"matrix_evidence={MATRIX_EVIDENCE}")
    print("ddl-apply-smoke: expected limited tables = "
          "[] (COMPAT-FLUSS-005 resolved by the owner-approved DDL fix: "
          "kv.format-version=2 + single-field subset bucket key on "
          "Order_Lifecycle/Order_Correlation -> every table raw-client writable)")

    # The default evidence dir (repo-root logs/ddl-apply) becomes
    # container-owned (10001:10001 2775) after any container run — the host
    # user cannot write into it without the documented one-time shared group
    # (sudo groupadd -g 10001 ddlapply && sudo usermod -aG ddlapply $USER).
    # The scenarios assert the evidence CONTENT (parsed from the orchestrator's
    # printed path), not the default location, so fall back to a per-run temp
    # dir and keep the host smoke regression-worthy regardless of the evidence
    # dir's current owner.
    tmp_evidence = None
    try:
        os.makedirs(ddl_apply.EVIDENCE_ROOT, exist_ok=True)
        probe = os.path.join(ddl_apply.EVIDENCE_ROOT, ".ddl-apply-smoke-probe")
        with open(probe, "w", encoding="utf-8") as fh:
            fh.write("probe")
        os.unlink(probe)
    except OSError:
        tmp_evidence = tempfile.mkdtemp(prefix="ddl-apply-smoke-evidence-")
        os.environ["DDL_APPLY_EVIDENCE_DIR"] = tmp_evidence
        print(f"ddl-apply-smoke: default evidence dir "
              f"{ddl_apply.EVIDENCE_ROOT} not writable by the host user — "
              f"using temp {tmp_evidence}")

    ok = True
    # S1 — full PASS: smoke skipped, every table PASS -> exit 0. The
    # COMPAT-FLUSS-005 matrix still runs IN-BAND (it gates every apply) and must
    # be PASS in the evidence.
    ok &= scenario(1, {"DDL_APPLY_SKIP_SMOKE": "1"},
                   expect_rc=0,
                   expect_parts=[
                       "DDL-APPLY-RESULT: PASS exit=0",
                       "ddl-apply: RESULT=PASS EXIT=0 TABLES=27 MANIFEST="],
                   expect_absent=["PASS_WITH_LIMITATION", "LIMITATION"],
                   check_evidence={"status": "PASS",
                                   "acknowledged_limitations": [],
                                   "matrix.status": "PASS",
                                   "matrix.cells": 4},
                   classpath=classpath, bootstrap=bootstrap)

    # S2 — full PASS with the real write/read smoke enabled. The owner-approved
    # composite-key DDL fix (COMPAT-FLUSS-005) makes Order_Lifecycle /
    # Order_Correlation raw-client writable (kv.format-version=2 + single-field
    # subset bucket key), so the per-table KV upsert+lookup smoke now passes for
    # ALL tables (including both composite-PK tables) -> full PASS, no ack.
    ok &= scenario(2, {},
                   expect_rc=0,
                   expect_parts=[
                       "DDL-APPLY-RESULT: PASS exit=0",
                       "ddl-apply: RESULT=PASS EXIT=0 TABLES=27 MANIFEST="],
                   expect_absent=["PASS_WITH_LIMITATION", "LIMITATION",
                                  "REFUSED"],
                   check_evidence={"status": "PASS",
                                   "acknowledged_limitations": [],
                                   "matrix.status": "PASS",
                                   "matrix.cells": 4},
                   classpath=classpath, bootstrap=bootstrap)

    # S4 — containerized negative drill: a pre-seeded bad-ownership record must
    # flip the containerized apply to exit 1 with EVIDENCE OWNERSHIP CHECK
    # FAILED even though the apply itself succeeded (sentinel PASS EXIT=0).
    # Host-only:
    # docker CLI + the ddl-apply image are required, otherwise SKIP (incl.
    # always in the in-container `smoke` subcommand — the image has no docker).
    compose_file = os.path.join(SCRIPT_DIR, "..", "01_docker",
                                "docker-compose.yml")
    ok &= scenario_container_bad_ownership(compose_file, bootstrap, classpath)

    print()
    if tmp_evidence is not None:
        shutil.rmtree(tmp_evidence, ignore_errors=True)
    if not ok:
        print("ddl-apply-smoke: FAIL — the live contract deviates from the "
              "pinned outcomes (see deviations above)")
        return 1
    print("ddl-apply-smoke: PASS — exit-code contract (full PASS exit 0 after "
          "the COMPAT-FLUSS-005 composite-key resolution) + sentinels + "
          "evidence + the containerized bad-ownership drill verified on "
          "scratch catalogs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
