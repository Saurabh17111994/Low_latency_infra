#!/usr/bin/env bash
# Start ingestion pipeline: Arrow broker → Go bridge → Java → Fluss
#
# SECURITY: credentials are NOT committed. This script sources Arrow secrets
# from ~/.env.arrow (git-ignored, outside the repo). Create it by copying the
# template block below:
#
#   umask 077; touch ~/.env.arrow && chmod 600 ~/.env.arrow
#
# Template (~/.env.arrow) — TOTP AutoLogin is the only supported auth
# (ARROW_TOKEN was removed 2026-08-24):
#   ARROW_APP_ID=your_app_id
#   ARROW_APP_SECRET=your_app_secret
#   ARROW_USER_ID=your_user_id
#   ARROW_PASSWORD=your_password
#   ARROW_TOTP_KEY=your_totp_key
#
set -euo pipefail

SECRETS_FILE="${SECRETS_FILE:-$HOME/.env.arrow}"

if [ ! -f "$SECRETS_FILE" ]; then
	echo "ingestion: FATAL — no secrets file at $SECRETS_FILE." >&2
	echo "Create it from the template in the header of run-ingestion.sh (chmod 600)." >&2
	exit 1
fi

# R-135/P6-682: the secrets file is sourced (i.e. executed) below, so it must
# be a regular file we own, with no group/other access. Refuse symlinks: `-f`
# and `stat` both follow them, so an attacker-swapped link target would be
# measured and then executed. Mode 400 is as safe as 600.
if [ -L "$SECRETS_FILE" ]; then
	echo "ingestion: FATAL — $SECRETS_FILE is a symlink. Refusing to source a symlinked secrets file (it could point anywhere)." >&2
	exit 1
fi
# P6-692: `stat -c` is GNU-only and BSD/macOS uses `-f %Lp`; under 2>/dev/null
# the wrong one yields an empty mode and a false FATAL on a correct file.
SECRETS_MODE=""
if stat -c '%a' "$SECRETS_FILE" >/dev/null 2>&1; then
	SECRETS_MODE="$(stat -c '%a' "$SECRETS_FILE")"
elif stat -f '%Lp' "$SECRETS_FILE" >/dev/null 2>&1; then
	SECRETS_MODE="$(stat -f '%Lp' "$SECRETS_FILE")"
fi
if [ "$SECRETS_MODE" != "600" ] && [ "$SECRETS_MODE" != "400" ]; then
	echo "ingestion: FATAL — $SECRETS_FILE is not owner-only (mode ${SECRETS_MODE:-unknown}). Run: chmod 600 \"$SECRETS_FILE\"" >&2
	exit 1
fi
SECRETS_UID="$(stat -c '%u' "$SECRETS_FILE" 2>/dev/null || true)"
if [ -z "$SECRETS_UID" ]; then
	SECRETS_UID="$(stat -f '%u' "$SECRETS_FILE" 2>/dev/null || true)"
fi
if [ -z "$SECRETS_UID" ]; then
	echo "ingestion: FATAL — cannot determine the owner of $SECRETS_FILE; refusing to source it." >&2
	exit 1
fi
if [ "$SECRETS_UID" != "$(id -u)" ]; then
	echo "ingestion: FATAL — $SECRETS_FILE is not owned by the current user (uid $SECRETS_UID); refusing to source it." >&2
	exit 1
fi

# P6-295: sourced assignments are shell-only unless exported, so the execed
# child inherited nothing. `set -a` exports everything the file defines; the
# explicit export below re-asserts the credential names the child requires.
set -a
# shellcheck disable=SC1090
source "$SECRETS_FILE"
set +a

# Required creds (fail fast, never print values)
: "${ARROW_APP_ID:?ARROW_APP_ID must be set in $SECRETS_FILE}"
: "${ARROW_APP_SECRET:?ARROW_APP_SECRET must be set in $SECRETS_FILE}"
# P6-027: TOTP-only since 2026-08-24. Reject a leftover ARROW_TOKEN here
# rather than letting it through to the JVM, which fails with the same reason.
if [ -n "${ARROW_TOKEN:-}" ]; then
	echo "ingestion: FATAL — ARROW_TOKEN removed 2026-08-24; use ARROW_USER_ID+ARROW_PASSWORD+ARROW_TOTP_KEY (TOTP AutoLogin) only." >&2
	exit 1
fi
: "${ARROW_USER_ID:?ARROW_USER_ID must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_PASSWORD:?ARROW_PASSWORD must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"
: "${ARROW_TOTP_KEY:?ARROW_TOTP_KEY must be set (ARROW_TOKEN removed 2026-08-24, TOTP only)}"

export ARROW_APP_ID ARROW_APP_SECRET ARROW_USER_ID ARROW_PASSWORD ARROW_TOTP_KEY

# P6-693: default-assign, then export — a blanket `export VAR=default` made the
# operator's environment (remote/tuned Fluss, latency override) unsettable.
: "${ARROW_HFT_LATENCY_MS:=50}"
: "${FLUSS_BOOTSTRAP:=localhost:9123}"
: "${FLUSS_BOOTSTRAP_SERVERS:=$FLUSS_BOOTSTRAP}"
export ARROW_HFT_LATENCY_MS FLUSS_BOOTSTRAP FLUSS_BOOTSTRAP_SERVERS

# R-053: derive the chained launcher from this script's own location so the
# script is portable (any checkout, any user).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# P6-694: check before exec, so a broken checkout layout gives a FATAL instead
# of a bare "No such file or directory" from exec (exit 127).
TARGET="$SCRIPT_DIR/code/run-ingestion-full.sh"
if [ ! -x "$TARGET" ]; then
	echo "ingestion: FATAL — chained launcher not found or not executable at $TARGET" >&2
	exit 1
fi
exec "$TARGET"
