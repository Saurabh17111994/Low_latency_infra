#!/usr/bin/env bash
# =============================================================================
# provision-observability.sh — ONE entry point for a fresh OpenObserve (M2-1).
#
# A fresh OpenObserve (the daily VM, or after a fresh start wiped the O2 volume)
# has NO dashboards, NO alerts and NO destination. This script is the single path
# that provisions them:
#
#   1. refuses without code/01_platform/01_docker/secrets.env (guide §3 step 2
#      injects it — the password never comes from a guess),
#   2. sources .env (optional, defaults exist) and secrets.env,
#   3. derives O2_AUTH_BASIC from O2_PASSWORD when the file does not carry it,
#   4. runs o2-provision.py, then seed_alerts.py, under `set -e` so a failed
#      provisioner stops the chain instead of a later success masking it.
#
# Usage: bash code/01_platform/04_scripts/provision-observability.sh
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ENV_DIR="$ROOT/code/01_platform/01_docker"
ENV_FILE="$ENV_DIR/.env"
SECRETS_FILE="$ENV_DIR/secrets.env"

if [ ! -f "$SECRETS_FILE" ]; then
  echo "provision-observability: $SECRETS_FILE missing — inject it first" \
       "(docs/05_deployment/CLOUDPE_DAILY_VM.md §3 step 2)." >&2
  exit 1
fi

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090  # a deploy-provided file, not a tracked source
  . "$ENV_FILE"
  set +a
fi
set -a
# shellcheck disable=SC1090
. "$SECRETS_FILE"
set +a

if [ -z "${O2_PASSWORD:-}" ]; then
  echo "provision-observability: O2_PASSWORD is empty in $SECRETS_FILE" >&2
  exit 1
fi
if [ -z "${O2_AUTH_BASIC:-}" ]; then
  O2_AUTH_BASIC="$(printf '%s:%s' "${O2_USER:-admin@example.com}" "$O2_PASSWORD" \
    | base64 | tr -d '\n')"
  export O2_AUTH_BASIC
fi
export O2_API_URL="${O2_API_URL:-http://localhost:5080}"
export O2_ORG="${O2_ORG:-default}"
export O2_USER="${O2_USER:-admin@example.com}"

python3 "$ROOT/code/01_platform/04_scripts/o2-provision.py" "$O2_API_URL"
python3 "$ROOT/code/01_platform/04_scripts/seed_alerts.py"
echo "provision-observability: OpenObserve provisioned (dashboards + alerts + retention)"
