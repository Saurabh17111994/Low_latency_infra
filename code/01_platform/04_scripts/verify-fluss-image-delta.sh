#!/usr/bin/env bash
# =============================================================================
# verify-fluss-image-delta.sh -- prove the derived Fluss image adds nothing but
# the two upstream filesystem-plugin jars.
#
# WHY this exists
#   Fluss discovers plugins by directory (DirectoryBasedPluginFinder /
#   ComponentClassLoader), and the lake plugin registers no filesystem of its
#   own: fluss-lake-iceberg-1.0.0.jar declares only
#   META-INF/services/org.apache.fluss.lake.lakestorage.LakeStoragePlugin.
#   The filesystem plugins declare
#   META-INF/services/org.apache.fluss.fs.FileSystemPlugin and bundle the S3A
#   stack. For the lake plugin to write s3:// or hdfs://, those two jars must sit
#   inside the lake plugin's own directory, plugins/iceberg/.
#
#   That is the whole difference between the stock image and the derived one.
#   T2.16 measured it: stock alone fails tiering, stock plus those jars works.
#
# CONTRACT (measured 2026-09-24, Fluss 1.0.0)
#   stock   /opt/fluss = 30 files
#   derived /opt/fluss = 32 files
#   added   = plugins/iceberg/fluss-fs-s3-1.0.0.jar
#             plugins/iceberg/fluss-fs-hdfs-1.0.0.jar
#   removed = 0, changed (same path, different md5) = 0
#   (paths below are shown relative to /opt/fluss)
#
#   Any other difference means the image drifted from "stock plus the native
#   plugin jars" and must be explained before that image is used.
#
# USAGE
#   verify-fluss-image-delta.sh [stock-image] [derived-image]
#   Defaults: stock   = apache/fluss:1.0.0
#             derived = FLUSS_IMAGE from code/01_platform/01_docker/.env
#   Both images must already be present locally; this script never pulls.
#
# EXIT  0 = exact delta   1 = delta differs   2 = probe broken (no verdict)
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
STOCK_IMAGE="${1:-apache/fluss:1.0.0}"
DERIVED_IMAGE="${2:-}"

# The compose stack and its env files live under code/01_platform/01_docker/.
ENV_CANDIDATES="$ROOT/code/01_platform/01_docker/.env $ROOT/.env"
if [ -z "$DERIVED_IMAGE" ]; then
  for f in $ENV_CANDIDATES; do
    [ -f "$f" ] || continue
    DERIVED_IMAGE="$(sed -nE 's/^[[:space:]]*(export[[:space:]]+)?FLUSS_IMAGE=[[:space:]]*//p' "$f" | head -1 | tr -d '"' | tr -d "'")"
    [ -n "$DERIVED_IMAGE" ] && break
  done
fi
if [ -z "$DERIVED_IMAGE" ]; then
  echo "verify-fluss-image-delta: no derived image argument and no FLUSS_IMAGE in: $ENV_CANDIDATES" >&2
  exit 2
fi

for img in "$STOCK_IMAGE" "$DERIVED_IMAGE"; do
  if ! docker image inspect "$img" >/dev/null 2>&1; then
    echo "verify-fluss-image-delta: image not present locally: $img" >&2
    exit 2
  fi
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# One container per image. Output: sorted "md5sum" lines, exactly 32-char hash,
# two spaces, then the absolute path.
snapshot() {
  docker run --rm --entrypoint sh "$1" -c 'find /opt/fluss -type f -exec md5sum {} + 2>/dev/null | sort'
}

snapshot "$STOCK_IMAGE"   > "$TMP/stock.raw"
snapshot "$DERIVED_IMAGE" > "$TMP/derived.raw"

# Guard: refuse to judge on anything that is not a well-formed inventory.
for f in "$TMP/stock.raw" "$TMP/derived.raw"; do
  total=$(wc -l < "$f" | tr -d ' ')
  wellformed=$(grep -cE '^[0-9a-f]{32}  /' "$f" || true)
  if [ "$total" -lt 20 ] || [ "$wellformed" -ne "$total" ]; then
    echo "verify-fluss-image-delta: PROBE BROKEN - $f has $total lines, $wellformed well-formed - no verdict" >&2
    exit 2
  fi
done

# Paths (col 35 onwards) for add/remove, re-sorted because the raw file is ordered by hash.
# Paths are reported relative to /opt/fluss so the expectations read as shipped.
cut -c35- "$TMP/stock.raw"   | sed 's|^/opt/fluss/||' | sort > "$TMP/stock.paths"
cut -c35- "$TMP/derived.raw" | sed 's|^/opt/fluss/||' | sort > "$TMP/derived.paths"
comm -13 "$TMP/stock.paths" "$TMP/derived.paths" > "$TMP/added"
comm -23 "$TMP/stock.paths" "$TMP/derived.paths" > "$TMP/removed"

# Same path, different content. Column offsets, no field-separator escapes.
awk '
  NR==FNR { h[substr($0,35)] = substr($0,1,32); next }
  { p = substr($0,35); if ((p in h) && h[p] != substr($0,1,32)) print p }
' "$TMP/stock.raw" "$TMP/derived.raw" > "$TMP/changed"

n_stock=$(wc -l < "$TMP/stock.paths" | tr -d ' ')
n_derived=$(wc -l < "$TMP/derived.paths" | tr -d ' ')
n_added=$(wc -l < "$TMP/added" | tr -d ' ')
n_removed=$(wc -l < "$TMP/removed" | tr -d ' ')
n_changed=$(wc -l < "$TMP/changed" | tr -d ' ')

echo "verify-fluss-image-delta"
echo "  stock   ($n_stock files):   $STOCK_IMAGE"
echo "  derived ($n_derived files): $DERIVED_IMAGE"
echo "  added=$n_added removed=$n_removed changed=$n_changed"
[ "$n_added" -gt 0 ]   && sed 's/^/    + /' "$TMP/added"
[ "$n_removed" -gt 0 ] && sed 's/^/    - /' "$TMP/removed"
[ "$n_changed" -gt 0 ] && sed 's/^/    ~ /' "$TMP/changed"

rc=1
if [ "$n_added" -eq 2 ] && [ "$n_removed" -eq 0 ] && [ "$n_changed" -eq 0 ] \
   && grep -qx 'plugins/iceberg/fluss-fs-s3-1.0.0.jar'   "$TMP/added" \
   && grep -qx 'plugins/iceberg/fluss-fs-hdfs-1.0.0.jar' "$TMP/added"; then
  echo "  VERDICT: exact delta - stock plus the two native filesystem-plugin jars, nothing else"
  rc=0
else
  echo "  VERDICT: DELTA DIFFERS - the image is not 'stock plus the native plugin jars'" >&2
fi

# Gates are disabled under set -e when they are the last command of an && chain
# whose failure is expected; keep the exit explicit.
exit $rc
