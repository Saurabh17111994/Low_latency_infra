#!/usr/bin/env bash
# secrets-bootstrap.sh — create the nine Swarm secrets the production stack declares.
#
# Run on a Swarm MANAGER (VM1), after `docker swarm init`. Values arrive two ways:
#   supplied  — your git-ignored values file (base64/Excel/OpenObserve credentials)
#   generated — on this host, for internal or derivable secrets
# A supplied value always wins over generation, so rotating by hand is not fought.
#
# Every secret is created with `docker secret create <name> -`: the value travels
# on stdin and never appears in an argument, where `ps` and shell history see it.
#
# Usage:
#   secrets-bootstrap.sh --values-file FILE [--o2-user USER]   # create all nine
#   ssh <vm1> 'secrets-bootstrap.sh --values-file /dev/stdin' < FILE  # no file on the VM
#   secrets-bootstrap.sh --check                               # presence only, no values
#   secrets-bootstrap.sh --print-names                         # the nine names
#   secrets-bootstrap.sh --self-check                          # offline logic proof
#
# Exit: 0 ok · 2 usage or no usable manager · 3 something required is missing
#       4 a secret already exists (values are immutable — see the printed rotation)

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
STACK=${STACK:-$SCRIPT_DIR/../01_docker/docker-stack.yml}

# The nine names the stack declares. `--print-names` and the parity test in
# tests/test_15_secrets_bootstrap.py both read this list, so a tenth secret in
# the stack fails at the workstation instead of as "secret not found" at deploy.
NAMES=(
  arrow_app_secret
  arrow_password
  arrow_totp_key
  aws_access_key_id
  aws_secret_access_key
  execution_bridge_auth_token
  gateway_shared_secret
  o2_auth_basic
  o2_password
)

# Generated on this host unless the values file pins them. o2_auth_basic is
# derived from the O2 user and the o2_password chosen here — see note below.
GENERATED=(
  execution_bridge_auth_token
  gateway_shared_secret
  o2_auth_basic
  o2_password
)

VALUES_FILE=""
O2_USER=${O2_USER:-}
MODE=create

usage() {
  sed -n '3,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

log()  { printf '%s\n' "$*"; }
fail() { local code=$1; shift; printf 'FAIL: %s\n' "$*" >&2; exit "$code"; }

# 32 random bytes as 64 hex characters. od consumes the whole stream, so no
# writer takes SIGPIPE (which pipefail would turn into a silent 141).
gen_hex() {
  head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'
}

is_generated() {
  local name=$1 candidate
  for candidate in "${GENERATED[@]}"; do
    [ "$candidate" = "$name" ] && return 0
  done
  return 1
}

stack_secret_names() {
  # Read the authoritative list out of the stack's top-level `secrets:` block.
  local in_block=0 line
  while IFS= read -r line; do
    if [ "$line" = "secrets:" ]; then in_block=1; continue; fi
    [ "$in_block" = 1 ] || continue
    case "$line" in
      "  "*) ;;
      *) break ;;
    esac
    if [[ $line =~ ^\ \ ([a-z_][a-z0-9_]*): ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
    fi
  done < "$STACK"
}

print_names() {
  printf '%s\n' "${NAMES[@]}"
}

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --values-file) [ $# -ge 2 ] || { usage; exit 2; }; VALUES_FILE=$2; shift 2 ;;
      --o2-user)     [ $# -ge 2 ] || { usage; exit 2; }; O2_USER=$2; shift 2 ;;
      --check)       MODE=check; shift ;;
      --print-names) MODE=names; shift ;;
      --self-check)  MODE=selfcheck; shift ;;
      -h|--help)     usage; exit 0 ;;
      *) printf 'unknown argument: %s\n\n' "$1" >&2; usage; exit 2 ;;
    esac
  done
}

# Offline proof of the parts that need no daemon: the name list still matches
# the stack, and the base64 derivation carries no "Basic " prefix. otel supplies
# that scheme itself (otel-collector-config.swarm.yaml: `Authorization: "Basic
# ${file:/run/secrets/o2_auth_basic}"`), so a prefixed value authenticates as
# "Basic Basic …" and OpenObserve answers 401 with nothing useful in the log.
self_check() {
  local rc=0 names_from_stack derived
  names_from_stack=$(stack_secret_names | sort)
  if [ "$names_from_stack" != "$(print_names | sort)" ]; then
    printf 'FAIL: name list differs from %s\n' "$STACK" >&2
    diff <(printf '%s\n' "$names_from_stack") <(print_names | sort) >&2 || true
    rc=4
  fi
  derived=$(printf '%s' "user@example.com:s3cret" | base64 -w0)
  if [ "$derived" != "dXNlckBleGFtcGxlLmNvbTpzM2NyZXQ=" ]; then
    printf 'FAIL: base64 derivation changed: %s\n' "$derived" >&2
    rc=4
  fi
  printf '%s\n' "SUPPLIED (from --values-file): 5 — arrow_app_secret arrow_password arrow_totp_key aws_access_key_id aws_secret_access_key"
  printf '%s\n' "GENERATED (on this host): 4 — ${GENERATED[*]}"
  if [ "$rc" = 0 ]; then printf '[PASS] secrets-bootstrap self-check\n'; fi
  return "$rc"
}

# Values file: KEY=VALUE per line, `#` comments, keys matched case-insensitively
# so both `ARROW_APP_SECRET` (copied from .env) and `arrow_app_secret` work.
load_values() {
  local line key value
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) printf 'ignoring unparseable line in %s\n' "$VALUES_FILE" >&2; continue ;; esac
    key=${line%%=*}
    value=${line#*=}
    key=$(printf '%s' "$key" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]')
    VALUES[$key]=$value
    FROM_FILE[$key]=1
  done < "$VALUES_FILE"
}

require_manager() {
  command -v docker >/dev/null 2>&1 || fail 2 "docker is not on PATH"
  docker info >/dev/null 2>&1 || fail 2 "docker daemon is not reachable — is it running?"
  if [ "$(docker info --format '{{.Swarm.ControlAvailable}}')" != "true" ]; then
    fail 2 "this node is not a Swarm manager — secrets can only be created on one
      run this on VM1 after 'docker swarm init' (a worker cannot create secrets)"
  fi
}

existing_secrets() {
  docker secret ls --format '{{.Name}}' 2>/dev/null | tr -d '\r' | sort -u
}

check_mode() {
  local existing missing=() name
  existing=$(existing_secrets)
  for name in "${NAMES[@]}"; do
    if printf '%s\n' "$existing" | grep -qx "$name"; then
      printf '  present  %s\n' "$name"
    else
      printf '  MISSING  %s\n' "$name"
      missing+=("$name")
    fi
  done
  printf '\nAlso required as plain deploy values (not secrets): ARROW_APP_ID ARROW_USER_ID CHECKPOINT_DIR\n'
  if [ ${#missing[@]} -eq 0 ]; then
    printf '[PASS] all %d secrets exist\n' "${#NAMES[@]}"
    return 0
  fi
  printf 'FAIL: %d of %d secrets missing — run: %s --values-file <your-file>\n' \
    "${#missing[@]}" "${#NAMES[@]}" "$(basename "${BASH_SOURCE[0]}")" >&2
  return 3
}

create_mode() {
  local name value missing=() existing clash=()
  [ -n "$VALUES_FILE" ] || fail 2 "--values-file is required (see $0 --help)"
  # -r, not -f: the values file may be a pipe. `--values-file /dev/stdin` over
  # SSH keeps the five secret values off the VM's disk entirely, which is the
  # point of piping them in rather than copying a file there.
  [ -r "$VALUES_FILE" ] || fail 2 "values file not readable: $VALUES_FILE"
  declare -gA VALUES=()
  declare -gA FROM_FILE=()
  load_values

  # The values file is the one-stop shop: accept the O2 user from it too, so a
  # single git-ignored file describes the whole secret set.
  if [ -z "$O2_USER" ] && [ -n "${VALUES[o2_user]:-}" ]; then
    O2_USER=${VALUES[o2_user]}
  fi

  # Validate the whole set before creating anything: half a secret set looks
  # deployed and fails at runtime as an auth error, which is far harder to read.
  for name in "${NAMES[@]}"; do
    if [ -n "${VALUES[$name]:-}" ] || is_generated "$name"; then continue; fi
    missing+=("$name")
  done
  if [ ${#missing[@]} -gt 0 ]; then
    printf 'FAIL: %d value(s) missing from %s: %s\n' \
      "${#missing[@]}" "$VALUES_FILE" "${missing[*]}" >&2
    printf 'Add them (any case) or omit the key to accept a generated value.\n' >&2
    return 3
  fi

  existing=$(existing_secrets)
  for name in "${NAMES[@]}"; do
    printf '%s\n' "$existing" | grep -qx "$name" && clash+=("$name")
  done
  if [ ${#clash[@]} -gt 0 ]; then
    printf 'FAIL: %d secret(s) already exist and Swarm values are immutable: %s\n' \
      "${#clash[@]}" "${clash[*]}" >&2
    printf 'To rotate: docker secret rm <name>; re-create; then docker service update --force <service>.\n' >&2
    return 4
  fi

  # o2_auth_basic is base64("<user>:<password>") with NO "Basic " prefix — the
  # otel config writes the scheme itself. Derived from whichever o2_password is
  # in play (supplied or generated) so the pair can never drift apart.
  if [ -z "${VALUES[o2_password]:-}" ]; then
    # A pinned header with no password to match it would authenticate against
    # nothing; refuse rather than invent a mismatch.
    [ -z "${VALUES[o2_auth_basic]:-}" ] \
      || fail 3 "o2_auth_basic is pinned but o2_password is not — supply both or neither"
    VALUES[o2_password]=$(gen_hex)
  fi
  if [ -z "${VALUES[o2_auth_basic]:-}" ]; then
    [ -n "$O2_USER" ] || fail 3 "o2_auth_basic must be derived but no O2 user is known — pass --o2-user USER (or set o2_user in the values file)"
    VALUES[o2_auth_basic]=$(printf '%s' "$O2_USER:${VALUES[o2_password]}" | base64 -w0)
  fi

  for name in "${NAMES[@]}"; do
    value=${VALUES[$name]:-}
    if [ -z "$value" ]; then
      value=$(gen_hex)
      printf '  generate %s\n' "$name"
    elif [ -n "${FROM_FILE[$name]:-}" ]; then
      printf '  supplied %s\n' "$name"
    else
      printf '  derived  %s\n' "$name"
    fi
    printf '%s' "$value" | docker secret create "$name" -
  done

  printf '\n[PASS] created %d secrets — names only, values never printed\n' "${#NAMES[@]}"
  printf 'Confirm with: %s --check\n' "$(basename "${BASH_SOURCE[0]}")"
}

main() {
  parse_args "$@"
  case "$MODE" in
    names)     print_names; return 0 ;;
    selfcheck) self_check; return $? ;;
  esac
  require_manager
  case "$MODE" in
    check)  check_mode ;;
    create) create_mode ;;
  esac
}

main "$@"
