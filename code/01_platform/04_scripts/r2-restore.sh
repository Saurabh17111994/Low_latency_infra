#!/usr/bin/env bash
# r2-restore.sh <yyyyMMdd> [out.parquet] (2026-08-31, Item 5) — pull one
# trading day's raw ticks from the R2 iceberg lake into a local parquet via
# DuckDB read_parquet on the day-folder glob (partition pruning by folder).
# Usage: r2-restore.sh 20260831 [raw_table_1-20260831.parquet]
#
# 2026-09-14 guardrails (P6-014/P6-164/P6-165/P6-166/P6-497/P6-498):
#   * the SQL — secrets included — goes to a 0600 temp file fed to DuckDB on
#     stdin: never on argv, where /proc/<pid>/cmdline exposes it, and never
#     through a bash double-quoted -c string, which re-expands $ ` \ " inside
#     a secret (R2 keys routinely contain + / = and sometimes $);
#   * DAY must be a real yyyyMMdd calendar day; OUT is refused if it carries a
#     quote, newline, ';' or a '..' segment (both end up inside the SQL and on
#     the local filesystem);
#   * COPY writes a temp file that is renamed into place only once the row
#     count came back non-zero, so an interrupted or empty run cannot clobber
#     an existing good parquet;
#   * the glob path is NOT Iceberg-aware, so a *delete*.parquet anywhere under
#     the partition aborts the restore (read_parquet would silently include
#     rows that were deleted) and schema drift across the day's files fails
#     loudly instead of being masked by union_by_name.
#
# Known limitation (CHG-144): a folder glob sees every parquet under the day,
# not the committed Iceberg snapshot, so files left behind by an uncommitted or
# failed write are included. Switching this script to iceberg_scan (with
# allow_moved_paths) is the fix once that extension is available offline.
#
# Test seams (used by test_r2_restore_guardrails.py, unset in production):
#   DUCKDB_BIN, R2_ENV_FILE, R2_SECRETS_FILE
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-$HOME/bin/duckdb}"
DAY="${1:?usage: r2-restore.sh <yyyyMMdd> [out.parquet]}"
OUT="${2:-raw_table_1-${DAY}.parquet}"
_ENV="${R2_ENV_FILE:-$_D/../01_docker/.env}"
_SEC="${R2_SECRETS_FILE:-$_D/../01_docker/secrets.env}"

# P6-164: the day is part of an S3 key, not an arbitrary string.
[[ "$DAY" =~ ^[0-9]{8}$ ]] || {
  echo "r2-restore: DAY must be yyyyMMdd, got '$DAY'" >&2; exit 2; }
date -d "${DAY:0:4}-${DAY:4:2}-${DAY:6:2}" >/dev/null 2>&1 || {
  echo "r2-restore: '$DAY' is not a real calendar day" >&2; exit 2; }

# P6-166: OUT is interpolated into SQL and used as a local path.
case "$OUT" in
  "") echo "r2-restore: OUT must not be empty" >&2; exit 2 ;;
  *"'"*|*'"'*|*';'*|*$'\n'*|*$'\r'*)
    echo "r2-restore: refusing OUT containing a quote, newline or ';': '$OUT'" >&2
    exit 2 ;;
esac
case "/$OUT/" in
  */../*) echo "r2-restore: refusing OUT with a '..' segment: '$OUT'" >&2; exit 2 ;;
esac

# Read KEY=value (also `export KEY=value`), last match wins, strip CR, spaces
# and surrounding quotes; a missing key yields empty rather than aborting the
# script through pipefail (P6-497).
get_kv() {
  grep -E "^(export +)?$2=" "$1" 2>/dev/null | tail -n1 | cut -d= -f2- \
    | tr -d '\r' | sed -e 's/^[[:space:]"'"'"']*//' -e 's/[[:space:]"'"'"']*$//' || true
}
[[ -r "$_ENV" && -r "$_SEC" ]] || {
  echo "r2-restore: cannot read $_ENV or $_SEC" >&2; exit 2; }
_ENDPOINT="$(get_kv "$_ENV" R2_ENDPOINT)"
_BUCKET="$(get_kv "$_ENV" R2_BUCKET)"
_AK="$(get_kv "$_SEC" AWS_ACCESS_KEY_ID)"
_SK="$(get_kv "$_SEC" AWS_SECRET_ACCESS_KEY)"
[[ -n "$_ENDPOINT" && -n "$_BUCKET" && -n "$_AK" && -n "$_SK" ]] || {
  echo "r2-restore: incomplete R2 config in $_ENV / $_SEC" >&2; exit 2; }
# P6-497: strip either scheme, then any trailing slash or path.
_HOST="${_ENDPOINT#https://}"; _HOST="${_HOST#http://}"; _HOST="${_HOST%%/*}"
[[ -n "$_HOST" ]] || { echo "r2-restore: R2_ENDPOINT has no host: '$_ENDPOINT'" >&2; exit 2; }
[[ -x "$DUCKDB_BIN" ]] || { echo "r2-restore: duckdb not executable: $DUCKDB_BIN" >&2; exit 2; }

_PREFIX="s3://${_BUCKET}/lake/default/raw_table_1/data/event_day=${DAY}"
_SQL="$(mktemp)"
TMP_OUT="${OUT}.tmp.$$"
chmod 600 "$_SQL"
trap 'rm -f "$_SQL" "$TMP_OUT"' EXIT
sql_q() { printf "%s" "$1" | sed "s/'/''/g"; }

{
  printf "INSTALL httpfs; LOAD httpfs;\n"
  printf "SET s3_endpoint='%s';\n" "$(sql_q "$_HOST")"
  printf "SET s3_region='auto';\n"
  printf "SET s3_url_style='path';\n"
  printf "SET s3_access_key_id='%s';\n" "$(sql_q "$_AK")"
  printf "SET s3_secret_access_key='%s';\n" "$(sql_q "$_SK")"
  # P6-165: an Iceberg delete file under the partition means a plain parquet
  # scan would resurrect deleted rows — count them first and abort on any.
  printf "SELECT 'delete_files=' || count(*) FROM glob('%s/**/*delete*.parquet');\n" \
    "$(sql_q "$_PREFIX")"
  # union_by_name is gone on purpose: a schema change inside one day must fail
  # loudly rather than be papered over.
  printf "COPY (SELECT * FROM read_parquet('%s/**/*.parquet')) TO '%s' (FORMAT PARQUET);\n" \
    "$(sql_q "$_PREFIX")" "$(sql_q "$TMP_OUT")"
  printf "SELECT 'restored_rows=' || count(*) FROM read_parquet('%s');\n" \
    "$(sql_q "$TMP_OUT")"
} >"$_SQL"

# -csv -noheader: one labelled line per SELECT, parsed below.
_OUT="$("$DUCKDB_BIN" -csv -noheader <"$_SQL")"
_DEL="$(printf '%s\n' "$_OUT" | grep -m1 '^delete_files=' | cut -d= -f2- || true)"
_ROWS="$(printf '%s\n' "$_OUT" | grep -m1 '^restored_rows=' | cut -d= -f2- || true)"
[[ -n "$_DEL" && -n "$_ROWS" ]] || {
  echo "r2-restore: duckdb did not report both counts; output was:" >&2
  printf '%s\n' "$_OUT" >&2
  exit 1; }
if [[ "$_DEL" != 0 ]]; then
  echo "r2-restore: refusing to restore $DAY — $_DEL Iceberg delete file(s) under the partition." >&2
  echo "  read_parquet ignores delete files, so this restore would silently include deleted rows." >&2
  exit 1
fi
[[ "$_ROWS" -gt 0 ]] || {
  echo "r2-restore: zero rows for $DAY — refusing to write an empty restore to $OUT" >&2
  exit 1; }
mv -- "$TMP_OUT" "$OUT"
trap 'rm -f "$_SQL"' EXIT
echo "r2-restore: $OUT ($_ROWS rows from $DAY)"
