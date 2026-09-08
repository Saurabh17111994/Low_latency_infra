#!/usr/bin/env bash
# ddl-apply-run — DDL application contract runner (engine dispatcher, run as
# the non-root ddlapply user via the entrypoint wrapper).
#
# Subcommands (extra args pass through to the underlying script):
#   validate    the plain `make ddl` check: version gate, manifest staleness,
#               checksum verification (exit 0 when the corpus is current)
#   apply       the FULL 9-step contract: empty-catalog precondition, in-band
#               COMPAT-FLUSS-005 matrix gate, deterministic apply, parity,
#               write/read smoke, evidence record. Exit 0 full PASS; 6
#               acknowledged PASS_WITH_LIMITATION; 1 failure/refused limitation.
#               Honours DDL_APPLY_TABLE_PREFIX, DDL_APPLY_ACK_LIMITATIONS
#               (=auto), DDL_APPLY_SKIP_SMOKE via the environment.
#   smoke       the exit-code contract battery (0/6/1 + sentinels + evidence
#               matrix) against scratch-prefixed catalogs; when docker + the
#               ddl-apply image are available on the HOST it also runs the
#               containerized bad-ownership drill (a pre-seeded engine-uid
#               644 record must flip the apply to exit 1 with EVIDENCE
#               OWNERSHIP CHECK FAILED)
#   evidence-check  the non-root ownership gate: every container-written
#               evidence record must be group-writable and none root-owned
#               (evidence_ownership_check.py); also auto-runs at the end of
#               every `apply` so the container validates its own corpus before
#               exiting
#   self-test   the engine's python unit tests (test_ddl_apply.py +
#               test_evidence_ownership_check.py), no cluster needed — sibling
#               host tooling tests are deliberately NOT in this image
#   version     print the running Python version and assert it matches the
#               pinned PYTHON_VERSION from versions.pin (VM-PYTHON-002 —
#               catches distro drift like the 3.14.4 image before exit 0)
#
# FLUSS_BOOTSTRAP defaults to fluss-coordinator:9123 — compose DNS inside
# trading-net, so no host /etc/hosts aliases are involved. The Dockerfile ENV
# sets the same default; this export keeps direct ./ddl-apply-run.sh runs
# (outside the image) on the same contract.
set -euo pipefail

SCRIPTS=/app/code/01_platform/04_scripts
PIN_FILE="$SCRIPTS/versions.pin"
MATRIX_EVIDENCE="${DDL_APPLY_MATRIX_EVIDENCE:-/app/logs/schema-compat/composite-pk-raw-client-20260815.md}"
export FLUSS_BOOTSTRAP="${FLUSS_BOOTSTRAP:-fluss-coordinator:9123}"
# In-container marker: the entrypoint wrapper already emitted the APPLIED
# ownership contract, so the engine's host-style echo (ddl_apply.py
# echo_ownership_contract) is suppressed — one contract line per run, never two.
export DDL_APPLY_IN_CONTAINER=1
# P4-165: this dispatcher must never run as root — root-owned evidence would
# poison the corpus the ownership gate enforces. The entrypoint wrapper drops
# before exec, so reaching here as root means direct invocation; refuse unless
# the operator explicitly opts into a root drill (DDL_APPLY_ALLOW_ROOT=1).
if [ "$(id -u)" -eq 0 ] && [ "${DDL_APPLY_ALLOW_ROOT:-0}" != "1" ]; then
  echo "ddl-apply: FAIL — must run as the non-root ddlapply user (run via the entrypoint wrapper, not as root)" >&2
  exit 1
fi

cmd="${1:-validate}"
if [ "$#" -gt 0 ]; then shift; fi

case "$cmd" in
  validate)
    exec python3 "$SCRIPTS/ddl_apply.py" "$@"
    ;;
  version)
    # P4-166: normalize before comparing — strip CR (CRLF pin file), quotes,
    # whitespace, and trailing comments; then regex-validate both sides so a
    # malformed pin/version fails with a clear message instead of a confusing
    # series mismatch. Reuses $actual_version (no second python3 fork).
    actual_version="$(python3 --version 2>&1 | sed 's/^Python //')"
    echo "python3 Python $actual_version"
    pinned_raw="$(sed -n 's/^PYTHON_VERSION=//p' "$PIN_FILE" | head -n1)"
    pinned="$(printf '%s' "$pinned_raw" | tr -d '\r' | cut -d'#' -f1 | tr -d "\"'" | tr -d '[:space:]')"
    echo "pinned  PYTHON_VERSION=${pinned:-<missing from versions.pin>}"
    if [ -z "${pinned:-}" ]; then
      echo "ddl-apply: FAIL — PYTHON_VERSION missing from versions.pin" >&2
      exit 1
    fi
    if [[ ! "$actual_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      echo "ddl-apply: FAIL — malformed python3 version '$actual_version' (expected X.Y.Z)" >&2
      exit 1
    fi
    if [[ ! "$pinned" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
      echo "ddl-apply: FAIL — malformed PYTHON_VERSION pin '$pinned' in $PIN_FILE (expected X.Y or X.Y.Z, e.g. 3.11.9)" >&2
      exit 1
    fi
    # Match on the major.minor series (pin "3.11.9" accepts any 3.11.x patch —
    # the distro/PPA ships the newest 3.11 patch, e.g. 3.11.16). A 2-part pin
    # ("3.11") IS the series; only strip the patch from a 3-part pin/version.
    if [[ "$pinned" == *.*.* ]]; then pin_series="${pinned%.*}"; else pin_series="$pinned"; fi
    if [[ "$actual_version" == *.*.* ]]; then actual_series="${actual_version%.*}"; else actual_series="$actual_version"; fi
    if [ "$actual_series" = "$pin_series" ]; then
      echo "ddl-apply: python version matches pin series ${pin_series} (VM-PYTHON-002)"
    else
      echo "ddl-apply: FAIL — python3 $actual_version (series ${actual_series}) != pinned series ${pin_series} (VM-PYTHON-002 drift)" >&2
      exit 1
    fi
    # P4-157: JRE side is pinned by the Dockerfile comment chain (openjdk-21
    # headless, trixie has no 17) but has no runtime series gate like Python.
    # A java-21-series check here would need a pinned JAVA_VERSION in
    # versions.pin first — deferred until that pin exists.
    exit 0
    ;;
  apply)
    # Run the full 9-step contract, then validate the corpus we just wrote
    # BEFORE exiting: a broken non-root ownership contract is a hard failure
    # even when the apply itself passed (the RESULT= sentinel from the engine
    # still documents the apply's own status).
    # P4-164: fail fast on a missing matrix file (rotated/unmounted default)
    # instead of dying deep inside Python with a confusing error.
    if [ ! -f "$MATRIX_EVIDENCE" ]; then
      echo "ddl-apply: FAIL — matrix evidence not found: $MATRIX_EVIDENCE (set DDL_APPLY_MATRIX_EVIDENCE)" >&2
      exit 2
    fi
    # P4-167: the enforced flags are dispatcher-managed (argparse store =
    # last-wins would let a user-supplied duplicate silently override the
    # gate). catalog-guard.sh passes these same two flags through `ddl-apply
    # apply` (compose run), so operator habit already points here — refuse
    # with a pointer to the env var.
    for a in "$@"; do
      case "$a" in
        --matrix-evidence|--matrix-evidence=*)
          echo "ddl-apply: FAIL — --matrix-evidence is managed by the dispatcher (use DDL_APPLY_MATRIX_EVIDENCE)" >&2
          exit 2
          ;;
        --apply-verified)
          echo "ddl-apply: FAIL — --apply-verified is implied by the apply subcommand (do not pass it)" >&2
          exit 2
          ;;
      esac
    done
    set +e
    python3 "$SCRIPTS/ddl_apply.py" \
      --apply-verified --matrix-evidence "$MATRIX_EVIDENCE" "$@"
    rc=$?
    set -e
    # P4-168: the auto-gate intentionally takes NO args — evidence_ownership_check.py
    # is env-only (DDL_APPLY_EVIDENCE_DIR), so there is no CLI selection to
    # forward; the manual `evidence-check` subcommand forwards "$@" for future
    # flags. Both read the same inherited env, so they can never diverge.
    if ! python3 "$SCRIPTS/evidence_ownership_check.py"; then
      echo "ddl-apply: apply exit=$rc but EVIDENCE OWNERSHIP CHECK FAILED — " \
        "the non-root ownership contract is broken" >&2
      exit 1
    fi
    exit "$rc"
    ;;
  smoke)
    # P4-164: same fail-fast as apply.
    if [ ! -f "$MATRIX_EVIDENCE" ]; then
      echo "ddl-apply: FAIL — matrix evidence not found: $MATRIX_EVIDENCE (set DDL_APPLY_MATRIX_EVIDENCE)" >&2
      exit 2
    fi
    # P4-167: same dispatcher-managed guard (smoke also injects the flag).
    for a in "$@"; do
      case "$a" in
        --matrix-evidence|--matrix-evidence=*)
          echo "ddl-apply: FAIL — --matrix-evidence is managed by the dispatcher (use DDL_APPLY_MATRIX_EVIDENCE)" >&2
          exit 2
          ;;
      esac
    done
    exec python3 "$SCRIPTS/ddl_apply_smoke.py" \
      --matrix-evidence "$MATRIX_EVIDENCE" "$@"
    ;;
  evidence-check)
    exec python3 "$SCRIPTS/evidence_ownership_check.py" "$@"
    ;;
  self-test)
    # The image ships only the engine's tests (test_ddl_apply.py +
    # test_evidence_ownership_check.py) — the whole tests dir is ours, so a
    # plain discover runs exactly the engine's suite.
    exec python3 -m unittest discover -s "$SCRIPTS/tests"
    ;;
  *)
    echo "ddl-apply: unknown subcommand '$cmd'" >&2
    echo "usage: ddl-apply {validate|apply|smoke|evidence-check|self-test|version} [args...]" >&2
    echo "  env: FLUSS_BOOTSTRAP, DDL_APPLY_TABLE_PREFIX, DDL_APPLY_ACK_LIMITATIONS," >&2
    echo "       DDL_APPLY_SKIP_SMOKE, DDL_APPLY_MATRIX_EVIDENCE, DDL_APPLY_EVIDENCE_DIR," >&2
    echo "       DDL_APPLY_UID/GID" >&2
    exit 2
    ;;
esac
