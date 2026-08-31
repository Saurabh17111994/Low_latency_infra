#!/usr/bin/env bash
# r2-query.sh — run SQL against the R2 iceberg lake via DuckDB (Item 3,
# 2026-08-31). Usage: r2-query.sh "<sql>"
# First run installs httpfs + iceberg extensions (needs internet).
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL="${1:?usage: r2-query.sh '<sql>'}"
_ENV="$_D/../01_docker/.env" _SEC="$_D/../01_docker/secrets.env"
_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_ENV" | cut -d= -f2-)"
_AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_SEC" | cut -d= -f2-)"
_SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_SEC" | cut -d= -f2-)"
_HOST="${_ENDPOINT#https://}"
exec "$HOME/bin/duckdb" -c "
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
SET s3_endpoint='${_HOST}';
SET s3_region='auto';
SET s3_url_style='path';
SET s3_access_key_id='${_AK}';
SET s3_secret_access_key='${_SK}';
${SQL}
"
