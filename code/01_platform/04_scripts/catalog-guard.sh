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
#   1. Wait for the zookeeper container to answer.
#   2. Count live tables under /fluss/metadata/databases/default/tables.
#   3. Count == EXPECTED  -> catalog healthy, exit 0 (no DDL).
#   4. Count == 0         -> catalog lost; run the DDL apply contract
#      (empty-catalog precondition enforced by the tool itself).
#   5. 0 < Count < EXPECTED -> PARTIAL: report, exit 3, never auto-apply
#      (the DDL contract refuses non-empty catalogs; partial = different
#      problem, needs investigation).
#   6. Exit non-zero with a clear message if repair cannot run.
#
# The probe is a direct zkCli read — no Fluss client, no coordinator hop —
# so it cannot fail because Fluss is mid-startup. DDL runs inside the
# ddl-apply container against the compose network, exactly like the manual
# recovery (2026-09-05).
#
# Usage:   catalog-guard.sh            # probe only? no — probe + auto-apply
# Env:     EXPECTED_TABLES=31          # default 31 (schema_manifest.json)
#          DDL_APPLY_MATRIX_EVIDENCE   # path inside ddl-apply container
#          COMPOSE_PROJECT_DIR         # where docker-compose.yml lives
#          DRY_RUN=1                   # probe only, never apply
#
# Exit codes: 0 healthy or repaired; 3 catalog empty and repair refused
# (DRY_RUN) or failed; 4 preconditions missing (containers down).
set -u

# ── Config ────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_DIR="${COMPOSE_PROJECT_DIR:-$ROOT/code/01_platform/01_docker}"
EXPECTED_TABLES="${EXPECTED_TABLES:-31}"
MATRIX_EVIDENCE="${DDL_APPLY_MATRIX_EVIDENCE:-/app/code/01_platform/02_sql/ddl/schema_manifest.json}"
COMPOSE=(docker compose --env-file "$COMPOSE_DIR/.env" --env-file "$COMPOSE_DIR/secrets.env" -f "$COMPOSE_DIR/docker-compose.yml")
ZK_CONTAINER="$(docker ps --filter "name=zookeeper" --format '{{.Names}}' | head -1)"
ZK_PATH="${ZK_PATH:-/fluss/metadata/databases/default/tables}"

log()  { printf '[catalog-guard] %s\n' "$*"; }
fail() { printf '[catalog-guard] ERROR: %s\n' "$*" >&2; exit "${2:-1}"; }

count_tables() {
    # Returns the number of table names listed by ZK, or 0 when the path is
    # absent. zkCli output: "...\n[table1, table2, ...]\n...". Names may
    # contain commas? No — Fluss table names are [A-Za-z0-9_], so a plain
    # comma split of the bracketed line is exact.
    local out
    out="$(docker exec "$ZK_CONTAINER" /apache-zookeeper-3.9.2-bin/bin/zkCli.sh \
        -server 127.0.0.1:2181 ls "$ZK_PATH" 2>/dev/null)"
    local line
    line="$(printf '%s\n' "$out" | grep -E '^\[' | head -1)"
    [ -n "$line" ] || { echo 0; return; }
    local cleaned="${line#\[}"; cleaned="${cleaned%\]}"
    # Count names as comma-separated fields + 1 (the trailing field has no
    # comma; a bare wc -l undercounts by one after $(...) strips the final
    # newline). Empty list "[]" -> cleaned empty -> 0.
    if [ -n "$cleaned" ]; then
        echo "$(printf '%s' "$cleaned" | awk -F, '{print NF}')"
    else
        echo 0
    fi
}

# ── 1. Preconditions ──────────────────────────────────────────────────────
[ -n "$ZK_CONTAINER" ] || fail "zookeeper container not found — start the stack first (make up)" 4

# ── 2. Wait for ZK to answer ──────────────────────────────────────────────
for _ in $(seq 1 30); do
    if docker exec "$ZK_CONTAINER" /apache-zookeeper-3.9.2-bin/bin/zkCli.sh \
            -server 127.0.0.1:2181 ls / >/dev/null 2>&1; then
        break
    fi
    sleep 2
done
docker exec "$ZK_CONTAINER" /apache-zookeeper-3.9.2-bin/bin/zkCli.sh \
    -server 127.0.0.1:2181 ls / >/dev/null 2>&1 \
    || fail "zookeeper not answering after 60s" 4

# ── 3. Probe ──────────────────────────────────────────────────────────────
LIVE="$(count_tables)"
log "catalog probe: $LIVE/$EXPECTED_TABLES tables under $ZK_PATH"

if [ "$LIVE" -ge "$EXPECTED_TABLES" ]; then
    log "catalog healthy — nothing to do"
    exit 0
fi

if [ "$LIVE" -gt 0 ]; then
    log "catalog PARTIAL ($LIVE/$EXPECTED_TABLES) — not applying (DDL contract needs an"
    log "empty catalog). Investigate: tables exist but fewer than the manifest expects."
    exit 3
fi

# ── 4. Repair ─────────────────────────────────────────────────────────────
if [ "${DRY_RUN:-0}" = "1" ]; then
    fail "catalog EMPTY and DRY_RUN=1 — refusing to apply" 3
fi

log "catalog EMPTY — applying DDL contract"
"${COMPOSE[@]}" run --rm ddl-apply apply \
    --apply-verified \
    --matrix-evidence "$MATRIX_EVIDENCE"
rc=$?
if [ "$rc" -ne 0 ]; then
    # A failed smoke on a freshly-created table (leader election race) makes
    # the apply exit non-zero even when every table WAS created. The catalog
    # count is the source of truth for THIS guard — re-probe before failing.
    log "DDL apply exited $rc — re-probing catalog (smoke may have raced leader election)"
fi

# ── 5. Verify ─────────────────────────────────────────────────────────────
LIVE="$(count_tables)"
log "post-apply probe: $LIVE/$EXPECTED_TABLES tables"
if [ "$LIVE" -lt "$EXPECTED_TABLES" ]; then
    fail "catalog still incomplete after apply ($LIVE/$EXPECTED_TABLES); apply exit=$rc — see ddl-apply evidence" 3
fi
if [ "$rc" -ne 0 ]; then
    log "catalog COMPLETE ($LIVE tables) despite apply exit $rc (smoke flake on fresh table)"
    log "re-run 'make ddl APPLY=1' later to clear the smoke"
else
    log "catalog repaired — $LIVE tables present"
fi
exit 0
