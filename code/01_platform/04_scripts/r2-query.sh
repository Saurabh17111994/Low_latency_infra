#!/usr/bin/env bash
# r2-query.sh — run SQL against the R2 iceberg lake via DuckDB (Item 3,
# 2026-08-31; hardened 2026-09-14, wave 10).
#
# Usage: r2-query.sh "<sql>"
#
# This is an OPERATOR tool, not a sandbox: the SQL runs with your R2
# credentials and DuckDB's filesystem access, so never feed it text from an
# untrusted source (P6-496).
#
# HARDENING (2026-09-14): config values are read by r2_var, so a missing key or
# a quoted/CR value is a named error instead of a silent empty SET (P6-162); the
# endpoint is validated and normalised, so `https://acct.r2.../` no longer sets
# s3_endpoint to a value with a slash (P6-494); the binary is looked up instead of
# assuming $HOME/bin/duckdb exists (P6-495); LOAD is tried before INSTALL, so
# re-runs work offline once the extensions are cached (P6-495); and the SQL plus
# the credentials go to DuckDB on stdin from a 0600 temp file, because argv is
# world-readable via /proc/<pid>/cmdline (P6-163, P6-496).
set -euo pipefail

_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./r2-env.sh
. "$_D/r2-env.sh"

SQL="${1:-}"
if [ -z "$SQL" ] || [ "$#" -gt 1 ]; then
  echo "usage: r2-query.sh '<sql>'" >&2
  exit 2
fi

# Seams for the tests (unset in production).
_ENV_FILE="${R2_ENV_FILE:-$_D/../01_docker/.env}"
_SEC_FILE="${R2_SECRETS_FILE:-$_D/../01_docker/secrets.env}"

_ENDPOINT="$(r2_var "$_ENV_FILE" R2_ENDPOINT)" || exit 2
_AK="$(r2_var "$_SEC_FILE" AWS_ACCESS_KEY_ID)" || exit 2
_SK="$(r2_var "$_SEC_FILE" AWS_SECRET_ACCESS_KEY)" || exit 2

case "$_ENDPOINT" in
  https://?*) ;;
  *) echo "r2-query: R2_ENDPOINT must be an https:// URL, got '${_ENDPOINT}'" >&2; exit 2 ;;
esac
_HOST="${_ENDPOINT#https://}"
_HOST="${_HOST%%/*}"                       # DuckDB wants host[:port], never a path
[ -n "$_HOST" ] || { echo "r2-query: R2_ENDPOINT has no usable host: '${_ENDPOINT}'" >&2; exit 2; }

_DUCKDB="${DUCKDB_BIN:-}"
if [ -z "$_DUCKDB" ]; then
  if [ -n "${HOME:-}" ] && [ -x "$HOME/bin/duckdb" ]; then
    _DUCKDB="$HOME/bin/duckdb"
  else
    _DUCKDB="$(command -v duckdb || true)"
  fi
fi
if [ -z "$_DUCKDB" ] || [ ! -x "$_DUCKDB" ]; then
  echo "r2-query: duckdb not found — put it in \$HOME/bin, on PATH, or set DUCKDB_BIN" >&2
  exit 2
fi

_SQL_FILE="$(mktemp "${TMPDIR:-/tmp}/r2-query.sql.XXXXXX")"
_ERR_FILE="$(mktemp "${TMPDIR:-/tmp}/r2-query.err.XXXXXX")"
trap 'rm -f "$_SQL_FILE" "$_ERR_FILE"' EXIT
chmod 600 "$_SQL_FILE" "$_ERR_FILE"

sql_lit() { printf "'%s'" "${1//\'/\'\'}"; }

_write_sql() {   # _write_sql <load|install>
  {
    # INSTALL only when the cached-extension load failed: it needs the network.
    [ "$1" = "install" ] && printf 'INSTALL httpfs; INSTALL iceberg;\n'
    printf 'LOAD httpfs; LOAD iceberg;\n'
    printf 'SET s3_endpoint=%s;\n' "$(sql_lit "$_HOST")"
    printf "SET s3_region='auto';\n"
    printf "SET s3_url_style='path';\n"
    printf 'SET s3_access_key_id=%s;\n' "$(sql_lit "$_AK")"
    printf 'SET s3_secret_access_key=%s;\n' "$(sql_lit "$_SK")"
    printf '%s\n' "$SQL"
  } > "$_SQL_FILE"
}

_run() {         # _run <load|install> — SQL on stdin, never argv
  _write_sql "$1"
  "$_DUCKDB" ":memory:" < "$_SQL_FILE" 2>"$_ERR_FILE"
}

if _run load; then
  exit 0
fi
_load_err="$(cat "$_ERR_FILE")"
if _run install; then
  exit 0
fi
{
  echo "r2-query: DuckDB failed, and the INSTALL retry failed too"
  echo "--- LOAD-only attempt ---"
  printf '%s\n' "$_load_err" | head -20
  echo "--- INSTALL + LOAD retry ---"
  head -20 "$_ERR_FILE"
} >&2
exit 1
