#!/usr/bin/env bash
# r2-env.sh (2026-09-14) — shared .env / secrets.env reader for the R2 tools.
#
# Sourced, never executed. It deliberately sets NO shell options and never calls
# `exit`, because `source` runs in the caller's shell: a missing file used to
# abort lake-guard.sh at source time with grep's output instead of a reason
# (P6-765, P6-766). Every failure is a `return 1` plus one line on stderr.
#
# Values are de-quoted, CR-stripped and comment-trimmed; a missing key, an
# unreadable file and an empty value are explicit errors, not empty strings
# (P6-485, P6-162).
#
# Usage:  value="$(r2_var FILE KEY)" || return 1

r2_var() {
  local file="$1" key="$2" line value

  [ -r "$file" ] || { printf 'missing %s: cannot read %s\n' "$key" "$file" >&2; return 1; }
  # last match wins, matching compose's own "later key overrides" behaviour
  # "KEY=", "export KEY=" and "KEY = value" (compose accepts all three)
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}[[:space:]]*=" "$file" | tail -1 || true)"
  [ -n "$line" ] || { printf 'missing %s in %s\n' "$key" "$file" >&2; return 1; }

  value="${line#*=}"
  value="${value%$'\r'}"                                            # CRLF files
  value="$(printf '%s' "$value" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  case "$value" in
    \"*\") value="${value#\"}"; value="${value%\"}" ;;              # "value"
    \'*\') value="${value#\'}"; value="${value%\'}" ;;              # 'value'
    *)     value="${value%%[[:space:]]#*}" ;;                       # value  # comment
  esac
  [ -n "$value" ] || { printf 'empty %s in %s\n' "$key" "$file" >&2; return 1; }

  printf '%s' "$value"
}
