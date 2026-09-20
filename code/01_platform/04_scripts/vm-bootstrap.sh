#!/usr/bin/env bash
# vm-bootstrap.sh — S4 "Bootstrap hosts" for ONE node: idempotent apply, fail-fast check.
#
# Why this exists: S4 is a list of commands in a document, and a document cannot refuse. A host that
# skipped the clock step, or that cannot run docker without sudo, otherwise shows up days later.
#
# What it deliberately does NOT do:
#   * no sysctls — this repository does not define the production list (runbook §6.1). A host with a
#     guessed kernel tuning is worse than a host that is visibly not ready, so --check FAILs on the
#     missing decision instead of hiding it.
#   * no `docker swarm join` — that needs a token from VM1 and is step S5. --apply prints it.
#   * no secrets — that is S6 (`secrets-bootstrap.sh`).
#   * nothing on the cluster — no SSH, no docker service calls.
#
# Testability: every filesystem path is rooted at $VM_BOOTSTRAP_ROOT and every external command is
# resolved through PATH, so the tests run this exact script against fakes, without root.
#
# Usage:
#   vm-bootstrap.sh --apply [--registry <host:port>] [--extra-free-ports "5000"]
#   vm-bootstrap.sh --check [--registry <host:port>] [--extra-free-ports "5080"]
# --check exits with the number of FAILs (0 = the node is ready for S5).
set -uo pipefail

ROOT="${VM_BOOTSTRAP_ROOT:-/}"
REPO_DIR="${VM_BOOTSTRAP_REPO:-$HOME/arrow-infra}"
SYSLOG_GROUP="${VM_BOOTSTRAP_SYSLOG_GROUP:-adm}"
MAX_OFFSET="${VM_BOOTSTRAP_MAX_OFFSET:-1.0}"
SUDO="${VM_BOOTSTRAP_SUDO-sudo}"
CLONE_URL="https://github.com/Saurabh17111994/Low_latency_infra.git"
SWARM_PORTS="2377 7946 4789"
REGISTRY=""
EXTRA_PORTS=""
MODE="apply"
FAILS=0

usage() {
    cat <<'EOF'
vm-bootstrap.sh — S4 for ONE node: idempotent apply, fail-fast check.

  vm-bootstrap.sh --apply [--registry <host:port>] [--extra-free-ports "5000"]
  vm-bootstrap.sh --check [--registry <host:port>] [--extra-free-ports "5080"]
  vm-bootstrap.sh --check --max-offset 0.5 --repo ~/arrow-infra

--check exits with the number of FAILs (0 = the node is ready for S5).
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --apply) MODE="apply" ;;
        --check) MODE="check" ;;
        --registry) REGISTRY="${2:-}"; shift ;;
        --extra-free-ports) EXTRA_PORTS="${2:-}"; shift ;;
        --max-offset) MAX_OFFSET="${2:-}"; shift ;;
        --repo) REPO_DIR="${2:-}"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
    esac
    shift
done

ok()   { printf '[PASS] %s\n' "$1"; }
bad()  { printf '[FAIL] %s\n' "$1"; FAILS=$((FAILS + 1)); }
note() { printf '[SKIP] %s\n' "$1"; }
info() { printf '[INFO] %s\n' "$1"; }

# Apply steps are echoed before they run, so a skipped step and an executed step are never confusable.
run() {
    printf '  $ %s\n' "$*"
    if ! "$@"; then
        bad "command failed: $*"
        return 1
    fi
}

# ---------------------------------------------------------------------------- apply

apply_repo() {
    if [ -d "$REPO_DIR/.git" ]; then
        note "repository already cloned at $REPO_DIR"
        return 0
    fi
    note "installing git"
    run $SUDO apt-get install -y git || return 1
    note "cloning the public repository (never copy the repo to a node by hand)"
    run git clone "$CLONE_URL" "$REPO_DIR" || return 1
}

apply_docker() {
    if command -v docker >/dev/null 2>&1; then
        note "docker already installed: $(docker --version 2>/dev/null)"
    else
        note "installing Docker Engine from Docker's repository (runbook S4 step 1)"
        run $SUDO apt-get update || return 1
        run $SUDO apt-get install -y ca-certificates curl gnupg || return 1
        run $SUDO install -m 0755 -d /etc/apt/keyrings || return 1
        if ! curl -fsSL https://download.docker.com/linux/ubuntu/gpg |
            $SUDO gpg --dearmor -o /etc/apt/keyrings/docker.gpg; then
            bad "fetching Docker's signing key failed"
            return 1
        fi
        if ! echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" |
            $SUDO tee /etc/apt/sources.list.d/docker.list >/dev/null; then
            bad "writing /etc/apt/sources.list.d/docker.list failed"
            return 1
        fi
        run $SUDO apt-get update || return 1
        run $SUDO apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin || return 1
    fi
    if id -nG 2>/dev/null | tr ' ' '\n' | grep -qx docker; then
        note "this user is already in the docker group"
    else
        note "adding $USER to the docker group — re-login (or newgrp docker) before S5"
        run $SUDO usermod -aG docker "$USER" || return 1
    fi
}

apply_clock() {
    if command -v chronyc >/dev/null 2>&1; then
        note "chrony already installed"
    else
        note "installing chrony (TOTP is clock-based; the platform halts beyond the offset limit)"
        run $SUDO apt-get install -y chrony || return 1
    fi
    if systemctl is-active --quiet chrony 2>/dev/null; then
        note "chrony is already running"
    else
        note "enabling chrony"
        run $SUDO systemctl enable --now chrony || return 1
    fi
}

do_apply() {
    apply_repo || return 1
    apply_docker || return 1
    apply_clock || return 1
    echo
    info "sysctls are NOT applied here: the production list is still undefined (runbook §6.1)"
    info "next: re-run this script with --check, then do S5 (docker swarm join) with a token from VM1"
}

# ---------------------------------------------------------------------------- check

check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        bad "docker is not installed (runbook S4 step 1)"
        return
    fi
    local v
    if v=$(docker version --format '{{.Server.Version}}' 2>/dev/null) && [ -n "$v" ]; then
        ok "docker $v is usable by $USER without sudo"
    else
        bad "docker is installed but this user cannot talk to the daemon — re-login after 'usermod -aG docker $USER'"
    fi
}

check_clock() {
    command -v timedatectl >/dev/null 2>&1 || { bad "timedatectl not found (systemd expected)"; return; }
    command -v chronyc >/dev/null 2>&1 || { bad "chronyc not installed — the clock is not disciplined (runbook S4 step 2)"; return; }
    if timedatectl 2>/dev/null | grep -q 'System clock synchronized: yes'; then
        ok "system clock is synchronized"
    else
        bad "system clock is not synchronized ('timedatectl' does not say yes)"
    fi
    local raw offset
    raw=$(chronyc tracking 2>/dev/null | awk '/^System time/{print $4}')
    if [ -z "$raw" ]; then
        bad "cannot read the offset from 'chronyc tracking'"
        return
    fi
    offset="$raw"
    if awk -v o="$offset" -v m="$MAX_OFFSET" 'BEGIN{d = o < 0 ? -o : o; exit !(d > m)}'; then
        bad "clock offset ${offset}s is beyond the ${MAX_OFFSET}s limit (TOTP halts the platform on drift)"
    else
        ok "clock offset ${offset}s is within ${MAX_OFFSET}s"
    fi
}

check_repo() {
    if [ -d "$REPO_DIR/.git" ]; then
        ok "repository present at $REPO_DIR ($(git -C "$REPO_DIR" log -1 --format='%h %ci' 2>/dev/null))"
    else
        bad "no repository clone at $REPO_DIR — a node runs what is pushed, never a hand-copied tree"
    fi
}

check_host_logs() {
    local path="$ROOT/var/log/syslog"
    if [ ! -f "$path" ]; then
        bad "$path is missing — the collector reads /var/log/*.log and /var/log/syslog (CHG-263)"
        return
    fi
    local group
    group=$(stat -c '%G' "$path" 2>/dev/null)
    if [ "$group" = "$SYSLOG_GROUP" ]; then
        ok "$path belongs to group $SYSLOG_GROUP, which the collector can read"
    else
        bad "$path belongs to group '$group', not '$SYSLOG_GROUP' — the collector's log job reads nothing (CHG-263)"
    fi
}

check_ports() {
    command -v ss >/dev/null 2>&1 || { bad "ss not found (iproute2) — cannot check the Swarm ports"; return; }
    local listeners port free=""
    listeners=$(ss -H -ltnu 2>/dev/null)
    for port in $SWARM_PORTS $EXTRA_PORTS; do
        if printf '%s\n' "$listeners" | grep -qE "[:.]${port}[[:space:]]"; then
            bad "port ${port} is already taken — Swarm cannot use it"
        else
            free="$free $port"
        fi
    done
    [ -n "$free" ] && ok "ports free:$free"
}

check_daemon_json() {
    local f="$ROOT/etc/docker/daemon.json"
    if [ ! -f "$f" ]; then
        bad "$f is missing — S4 step 5 declares the plain-HTTP registry there"
        return
    fi
    if ! python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$f" 2>/dev/null; then
        bad "$f is not valid JSON"
        return
    fi
    if [ -z "$REGISTRY" ]; then
        if python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); e=d.get("insecure-registries") or []; sys.exit(0 if isinstance(e, list) and e else 1)' "$f"; then
            info "$f declares insecure-registries, but --registry was not given: membership not verified"
        else
            bad "$f has no non-empty 'insecure-registries' list — the nodes cannot pull from the registry"
        fi
        return
    fi
    if python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if sys.argv[2] in (d.get("insecure-registries") or []) else 1)' "$f" "$REGISTRY"; then
        ok "$f declares the registry $REGISTRY as insecure (plain HTTP on purpose)"
    else
        bad "$f does not list $REGISTRY in insecure-registries"
    fi
}

check_sysctls() {
    # Deliberate, permanent FAIL until the decision is recorded — see the header.
    bad "sysctls — the production list is not defined in this repository (runbook §6.1); this host cannot be called ready while that decision is missing"
}

do_check() {
    check_docker
    check_clock
    check_repo
    check_host_logs
    check_ports
    check_daemon_json
    check_sysctls
    echo
    printf '%d failure(s)\n' "$FAILS"
    return "$FAILS"
}

case "$MODE" in
    apply) do_apply ;;
    check) do_check ;;
esac
