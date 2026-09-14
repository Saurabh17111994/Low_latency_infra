#!/usr/bin/env bash
#
# catalog-guard — idempotent Fluss catalog recovery guard.
#
# WHY (2026-09-05): ZooKeeper held the Fluss catalog in an anonymous volume,
# so `docker compose down` deleted the table list while tablet data volumes
# survived. On the next `up` every table dir was orphaned (soak preflight
# "fluss not ready"; TableNotExistException). The root fix is the named
# volumes in docker-compose.yml (zookeeper-data/zookeeper-datalog); this
# guard is belt-and-suspenders for the remaining failure mode — a genuinely
# lost/emptied catalog — and for operators who still run bare `compose up`
# on an old checkout without the volume fix.
#
# WHAT IT DOES (read-only probe first, DDL only when needed):
#   1. Preflight the compose inputs and the docker daemon (P6-318).
#   2. Take an exclusive lock so two guards cannot repair at once (P6-321).
#   3. Wait for the compose project's own zookeeper container to answer (P6-319).
#   4. Count live tables under /fluss/metadata/databases/default/tables.
#      A probe that could not be trusted is NOT an empty catalog (P6-034):
#      "the node does not exist" is the repairable state, "zkCli could not
#      tell us" is not, because the next step is a DDL apply.
#   5. Count == EXPECTED_TABLES -> catalog healthy, exit 0 (no DDL).
#   6. Count == 0              -> catalog lost; run the DDL apply contract
#      (empty-catalog precondition enforced by the tool itself).
#   7. Count > EXPECTED_TABLES -> MORE tables than the manifest describes:
#      report, exit 3, never auto-apply (P6-320: extra tables are drift, and
#      the old `-ge` test called any over-count healthy).
#   8. 0 < Count < EXPECTED   -> PARTIAL: report, exit 3, never auto-apply
#      (the DDL contract refuses non-empty catalogs; partial = different
#      problem, needs investigation).
#   9. A non-zero exit from the apply tool is reported even when the post-apply
#      count looks complete — exit 5 (P6-035): the smoke's verdict is evidence,
#      and swallowing it is how a broken apply looks healthy.
#
# The probe is a direct zkCli read — no Fluss client, no coordinator hop —
# so it cannot fail because Fluss is mid-startup. DDL runs inside the
# ddl-apply container against the compose network, exactly like the manual
# recovery (2026-09-05).
#
# Usage:   catalog-guard.sh            # probe, then repair only when lost
# Env:     EXPECTED_TABLES=<n>         # default = schema_manifest.json table count
#          DDL_APPLY_MATRIX_EVIDENCE   # path inside ddl-apply container
#          COMPOSE_PROJECT_DIR         # where docker-compose.yml lives
#          DRY_RUN=1                   # probe only, never apply
#          CATALOG_GUARD_LOCK          # lock file (default /tmp/catalog-guard-<uid>.lock)
#          CATALOG_GUARD_ZK_WAIT       # 2s ticks to wait for the container (default 30)
#          CATALOG_GUARD_ANSWER_WAIT   # 2s ticks to wait for the ZK server (default 30)
#          ZK_CLI                      # zkCli path inside the container
#
# Exit codes: 0 healthy or repaired; 3 catalog needs attention and the guard
# will not repair it (empty with DRY_RUN, partial, over-count, or a repair that
# did not finish the job); 4 preconditions missing (compose inputs, daemon,
# containers down); 5 the probe or the apply could not be trusted; 6 another
# guard holds the lock.
set -u

log()  { printf '[catalog-guard] %s\n' "$*"; }
# P6-714: $1 only — `fail "msg" 3` used to print "ERROR: msg 3".
fail() { printf '[catalog-guard] ERROR: %s\n' "$1" >&2; exit "${2:-1}"; }

# ── Config ────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_DIR="${COMPOSE_PROJECT_DIR:-$ROOT/code/01_platform/01_docker}"
COMPOSE=(docker compose --env-file "$COMPOSE_DIR/.env" --env-file "$COMPOSE_DIR/secrets.env" -f "$COMPOSE_DIR/docker-compose.yml")
ZK_PATH="${ZK_PATH:-/fluss/metadata/databases/default/tables}"
ZK_CLI="${ZK_CLI:-}"
MANIFEST="$ROOT/code/01_platform/02_sql/ddl/schema_manifest.json"
# Matrix evidence for the apply. The dispatcher (ddl-apply-run.sh, P4-167) owns
# this flag: it REFUSES --matrix-evidence/--apply-verified on the command line
# and reads DDL_APPLY_MATRIX_EVIDENCE only. Default = the in-image manifest,
# matching ddl_apply_smoke.py's own drill invocation; the schema-compat markdown
# the host-side `make ddl APPLY=1 EVIDENCE=<md>` uses lives under logs/ on the
# host and is not part of the image, so it cannot be the container default.
MATRIX_EVIDENCE="${DDL_APPLY_MATRIX_EVIDENCE:-/app/code/01_platform/02_sql/ddl/schema_manifest.json}"
LOCK_FILE="${CATALOG_GUARD_LOCK:-/tmp/catalog-guard-$(id -u).lock}"
PROBE_ERR=""

# ── 1. Preflight (P6-318) ─────────────────────────────────────────────────
# The COMPOSE array names two env files that compose requires: without them the
# first `docker compose` call fails with compose's own wording, which reads like
# a guard bug rather than a missing checkout input.
command -v docker >/dev/null 2>&1 || fail "docker is not on PATH" 4
[ -f "$COMPOSE_DIR/docker-compose.yml" ] \
    || fail "compose file missing: $COMPOSE_DIR/docker-compose.yml (set COMPOSE_PROJECT_DIR)" 4
for _f in .env secrets.env; do
    [ -f "$COMPOSE_DIR/$_f" ] \
        || fail "compose input missing: $COMPOSE_DIR/$_f — copy it from the tracked example before running the guard" 4
done
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable" 4

# ── 2. Expected table count (P6-317) ──────────────────────────────────────
# Default = the manifest's table count (was a hardcoded 31 while the candle-era
# DDLs 03/04/30/31 still existed; the 2026-09-05 retirement left 27 entries, so a
# hardcoded count drifts on every DDL change and makes a repaired catalog look
# incomplete). Env override wins; an unreadable manifest fails closed. Either way
# the value is validated: it is compared arithmetically below, so a non-integer
# used to surface as an undocumented bash error.
if [ -z "${EXPECTED_TABLES:-}" ]; then
    EXPECTED_TABLES="$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["tables"]))' "$MANIFEST" 2>/dev/null)" \
        || EXPECTED_TABLES=""
    [ -n "$EXPECTED_TABLES" ] || fail "cannot read the table count from code/01_platform/02_sql/ddl/schema_manifest.json (set EXPECTED_TABLES to override)" 5
fi
case "$EXPECTED_TABLES" in
    ''|*[!0-9]*) fail "EXPECTED_TABLES must be a whole number, got '$EXPECTED_TABLES'" 5 ;;
    0) fail "EXPECTED_TABLES=0 would make any catalog look complete — refusing" 5 ;;
esac

# ── 3. Exclusive lock (P6-321) ────────────────────────────────────────────
# Two guards observing 0 concurrently (parallel CI jobs, operator + hook) used to
# start two DDL applies against the same empty catalog. The lock is held for the
# whole run and released by the kernel when the process exits.
exec 9>"$LOCK_FILE" || fail "cannot open the lock file $LOCK_FILE" 6
flock -n 9 || fail "another catalog-guard is already running (lock $LOCK_FILE) — refusing to run a second repair" 6

# ── 4. Zookeeper container (P6-319) ───────────────────────────────────────
resolve_zk() {
    # Resolve the compose project's OWN zookeeper service and require it to run.
    # A loose `docker ps --filter name=zookeeper | head -1` can match a stale
    # container, another project's zookeeper, or a sibling service whose name
    # merely contains the word.
    local cid
    cid="$("${COMPOSE[@]}" ps -q zookeeper 2>/dev/null | head -1)"
    [ -n "$cid" ] || return 1
    [ "$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)" = "true" ] || return 1
    printf '%s' "$cid"
}
ZK_CONTAINER=""
for _i in $(seq 1 "${CATALOG_GUARD_ZK_WAIT:-30}"); do
    if ZK_CONTAINER="$(resolve_zk)"; then
        break
    fi
    ZK_CONTAINER=""
    sleep 2
done
[ -n "$ZK_CONTAINER" ] || fail "zookeeper container not found or not running — start the stack first (make up)" 4

zk_cli() {
    # The zkCli path inside the image is discovered once, not assumed: the
    # zookeeper version in the image is not part of this script's contract.
    if [ -z "$ZK_CLI" ]; then
        ZK_CLI="$(docker exec "$ZK_CONTAINER" sh -c 'ls -d /apache-zookeeper-*-bin/bin/zkCli.sh 2>/dev/null | head -1')" || ZK_CLI=""
        [ -n "$ZK_CLI" ] || fail "cannot find zkCli.sh inside $ZK_CONTAINER (set ZK_CLI)" 5
    fi
}
zk_ls() { docker exec "$ZK_CONTAINER" "$ZK_CLI" -server 127.0.0.1:2181 ls "$1" 2>&1; }

# ── 5. Wait for zookeeper to answer ───────────────────────────────────────
zk_cli
_answered=0
for _i in $(seq 1 "${CATALOG_GUARD_ANSWER_WAIT:-30}"); do
    if zk_ls / >/dev/null 2>&1; then
        _answered=1
        break
    fi
    sleep 2
done
[ "$_answered" = "1" ] || fail "zookeeper not answering after 60s (container $ZK_CONTAINER)" 4

# ── 6. Probe (P6-034, P6-715) ─────────────────────────────────────────────
probe_tables() {
    # Sets LIVE to the number of table names listed by ZK.
    #   return 0 : zkCli answered. An absent node IS 0 — that is exactly the
    #              lost-catalog state this guard repairs.
    #   return 1 : the probe could not be trusted (P6-034). A failed exec, a
    #              connection error or an unparseable listing must never read as
    #              "catalog empty", because the next step is a DDL apply. The
    #              caller reports PROBE_ERR and stops.
    # State is set in THIS shell, not echoed: a command substitution would put
    # PROBE_ERR in a subshell and the caller would report an empty reason.
    # zkCli output: "...\n[table1, table2, ...]\n...". Names may contain commas?
    # No — Fluss table names are [A-Za-z0-9_], so a plain comma split of the
    # bracketed line is exact.
    local out rc line cleaned
    out="$(zk_ls "$ZK_PATH")"
    rc=$?
    line="$(printf '%s\n' "$out" | grep -E '^\[' | head -1)"
    if [ -z "$line" ]; then
        case "$out" in
            *"Node does not exist"*|*"NoNode"*|*"no node"*)
                LIVE=0
                return 0 ;;
        esac
        PROBE_ERR="zkCli could not list $ZK_PATH (rc=$rc): $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-200)"
        return 1
    fi
    cleaned="${line#\[}"
    cleaned="${cleaned%\]}"
    # P6-715: a whitespace-only list ("[ ]") is empty, not one table.
    LIVE="$(printf '%s' "$cleaned" | awk -F, '{n=0; for (i=1; i<=NF; i++) if ($i ~ /[^[:space:]]/) n++; print n}')"
    return 0
}

# ── 7. State machine (P6-320) ─────────────────────────────────────────────
LIVE=""
probe_tables || fail "$PROBE_ERR" 5
log "catalog probe: $LIVE/$EXPECTED_TABLES tables under $ZK_PATH"

if [ "$LIVE" -eq "$EXPECTED_TABLES" ]; then
    log "catalog healthy — nothing to do"
    exit 0
fi

if [ "$LIVE" -gt "$EXPECTED_TABLES" ]; then
    log "catalog has MORE tables than the manifest describes ($LIVE/$EXPECTED_TABLES) —"
    log "not applying: extra tables are drift, not health. Compare the manifest with"
    log "'make ddl' and reconcile the extras."
    exit 3
fi

if [ "$LIVE" -gt 0 ]; then
    log "catalog PARTIAL ($LIVE/$EXPECTED_TABLES) — not applying (the DDL contract needs an"
    log "empty catalog). Investigate: tables exist but fewer than the manifest expects."
    exit 3
fi

# ── 8. Repair ─────────────────────────────────────────────────────────────
if [ "${DRY_RUN:-0}" = "1" ]; then
    fail "catalog EMPTY and DRY_RUN=1 — refusing to apply" 3
fi

log "catalog EMPTY — applying DDL contract"
# The evidence ROOT (logs/ddl-apply) can hold legacy host-owned dirs from
# runs before the uid-10001 ownership contract (P4-161/P4-163), and the
# entrypoint refuses to write when ANY top-level entry under the evidence
# dir is not engine-owned. Its documented remedy is a DEDICATED subdir:
# redirect this run's records there and leave the old evidence untouched.
GUARD_EVIDENCE_DIR="${DDL_APPLY_EVIDENCE_DIR:-/app/logs/ddl-apply/guard-repair-$(date -u +%Y%m%dT%H%M%SZ)}"
# Both knobs go through the environment: the dispatcher rejects the equivalent
# CLI flags as duplicated state (P4-167), and `apply` already implies
# --apply-verified. Passing them on the command line made every recovery run
# exit 2 on the flag check even once the evidence file was reachable.
rc=0
"${COMPOSE[@]}" run --rm \
    -e "DDL_APPLY_EVIDENCE_DIR=$GUARD_EVIDENCE_DIR" \
    -e "DDL_APPLY_MATRIX_EVIDENCE=$MATRIX_EVIDENCE" \
    ddl-apply apply || rc=$?

# ── 9. Verify ─────────────────────────────────────────────────────────────
LIVE=""
probe_tables || fail "$PROBE_ERR" 5
log "post-apply probe: $LIVE/$EXPECTED_TABLES tables"
if [ "$LIVE" -lt "$EXPECTED_TABLES" ]; then
    fail "catalog still incomplete after apply ($LIVE/$EXPECTED_TABLES); apply exit=$rc — see ddl-apply evidence" 3
fi
if [ "$LIVE" -gt "$EXPECTED_TABLES" ]; then
    fail "catalog has MORE tables than the manifest describes after apply ($LIVE/$EXPECTED_TABLES); apply exit=$rc — reconcile the manifest with 'make ddl'" 3
fi
if [ "$rc" -ne 0 ]; then
    # P6-035: the count says the tables exist; the apply tool's non-zero exit says
    # something else in the contract failed (most often the smoke racing leader
    # election on a freshly created table). Reporting plain success here is how a
    # broken apply looks healthy, so the run is not complete: exit 5 and say what
    # to re-run and why.
    fail "catalog complete ($LIVE tables) but the DDL apply exited $rc — read the ddl-apply evidence; re-run 'make ddl APPLY=1' to clear the smoke before trusting the catalog" 5
fi
log "catalog repaired — $LIVE tables present"
exit 0
