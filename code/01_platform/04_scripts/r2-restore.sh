#!/usr/bin/env bash
# r2-restore.sh <yyyyMMdd> [out.parquet] (2026-08-31, Item 5) — pull one
# trading day's raw ticks from the R2 iceberg lake into a local parquet via
# DuckDB read_parquet on the day-folder glob (partition pruning by folder).
# Usage: r2-restore.sh 20260831 [raw_table_1-20260831.parquet]
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAY="${1:?usage: r2-restore.sh <yyyyMMdd> [out.parquet]}"
OUT="${2:-raw_table_1-${DAY}.parquet}"
_ENV="$_D/../01_docker/.env" _SEC="$_D/../01_docker/secrets.env"
_ENDPOINT="$(grep -E '^R2_ENDPOINT=' "$_ENV" | cut -d= -f2-)"
_BUCKET="$(grep -E '^R2_BUCKET=' "$_ENV" | cut -d= -f2-)"
_AK="$(grep -E '^AWS_ACCESS_KEY_ID=' "$_SEC" | cut -d= -f2-)"
_SK="$(grep -E '^AWS_SECRET_ACCESS_KEY=' "$_SEC" | cut -d= -f2-)"
_HOST="${_ENDPOINT#https://}"
"$HOME/bin/duckdb" -c "
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='${_HOST}';
SET s3_region='auto';
SET s3_url_style='path';
SET s3_access_key_id='${_AK}';
SET s3_secret_access_key='${_SK}';
COPY (SELECT * FROM read_parquet(
  's3://${_BUCKET}/lake/default/raw_table_1/data/event_day=${DAY}/**/*.parquet',
  union_by_name=true))
  TO '${OUT}' (FORMAT PARQUET);
SELECT count(*) AS restored_rows FROM read_parquet('${OUT}');
"
