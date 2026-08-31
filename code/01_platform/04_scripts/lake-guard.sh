#!/usr/bin/env bash
# lake-guard.sh (2026-08-31, Item 6) — daily R2 lake health check. Cron-able;
# fail-fast exit 1 with the reason. Checks (Asia/Kolkata):
#   1. yesterday's day-folder has >= 1 data object
#   2. iceberg manifests exist (>= 2)
#   3. after 18:30 IST today's folder has >= 1 object
# Test hook: LAKE_GUARD_CHECK_DAY=20991231 forces the "yesterday" check
# against a nonexistent day (negative test).
set -euo pipefail
_D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_D/r2-list.sh"
TODAY="$(TZ=Asia/Kolkata date +%Y%m%d)"
YEST="${LAKE_GUARD_CHECK_DAY:-$(TZ=Asia/Kolkata date -d 'yesterday' +%Y%m%d)}"
KEYS="$(r2_list_lake)"
echo "lake-guard: today=${TODAY} yesterday=${YEST}"
Y="$(echo "$KEYS" | grep -c "^lake/default/raw_table_1/data/event_day=${YEST}/" || true)"
echo "yesterday folder objects: ${Y}"
[ "${Y:-0}" -gt 0 ] || { echo "!! lake-guard FAIL: no R2 objects for ${YEST}"; exit 1; }
M="$(echo "$KEYS" | grep -c "^lake/default/raw_table_1/metadata/.*\.avro" || true)"
echo "iceberg manifests: ${M}"
[ "${M:-0}" -ge 2 ] || { echo "!! lake-guard FAIL: iceberg manifests missing"; exit 1; }
HOUR="$(TZ=Asia/Kolkata date +%H%M)"
if [ "$HOUR" -ge 1830 ]; then
  T="$(echo "$KEYS" | grep -c "^lake/default/raw_table_1/data/event_day=${TODAY}/" || true)"
  echo "today folder objects: ${T}"
  [ "${T:-0}" -gt 0 ] || { echo "!! lake-guard FAIL: no R2 objects for ${TODAY} after 18:30 IST"; exit 1; }
fi
echo "lake-guard PASS"
