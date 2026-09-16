#!/usr/bin/env bash
# stack_selfcheck.sh — one-host Swarm self-check for docker-stack.yml (M2 mimic).
#
# Purpose: exercise the PRODUCTION stack deploy path on a SINGLE host (no 4/7
# VMs). This is the offline-prep bridge that proves `docker stack deploy`
# succeeds against a real (local) Swarm, without needing the real rig. It is
# NOT multi-VM HA evidence (SWARM-MGR-001..006 need a 3-manager quorum) — that
# is M3.
#
# What it does:
#   1. Requires the docker CLI + a running daemon (SKIP otherwise).
#   2. If not already a manager, `docker swarm init` on this single node.
#   3. Labels this node `role=worker` + `observability=true` so the same
#      constraint set the real cluster uses actually schedules here
#      (`node.labels.role == worker`, plus the separate boolean
#      `node.labels.observability == true`; a label key holds one value).
#   4. `docker stack config -c <stack>` — compile the manifest (no services
#      started). Exit non-zero on any stack error.
#   5. Optional real deploy: DEPLOY=1 -> docker stack deploy -c <stack> prod.
#   6. Teardown: DEPLOY=1 + DOWN=1 (the default) -> docker stack rm prod.
#      A validate-only run never removes anything (P6-019).
#
# Usage:
#   ./stack_selfcheck.sh                  # validate (swarm init + stack config)
#   ./stack_selfcheck.sh DEPLOY=1         # also deploy prod, then remove it
#   ./stack_selfcheck.sh DEPLOY=1 DOWN=0  # deploy and leave the stack up
#   DEPLOY=1 ./stack_selfcheck.sh         # env form (equivalent)
# DEPLOY=, DOWN=, STACK= and STACK_NAME= are accepted as arguments or as the
# environment; anything else is refused with exit 2.
set -euo pipefail

# 0. Key=value overrides. The Makefile passes these positionally (P6-209), so
#    read them as arguments as well as from the environment — and parse them
#    rather than `eval`ing them, so an argument cannot smuggle a command.
for arg in "$@"; do
  case "$arg" in
    DEPLOY=*)     DEPLOY="${arg#DEPLOY=}" ;;
    DOWN=*)       DOWN="${arg#DOWN=}" ;;
    STACK=*)      STACK="${arg#STACK=}" ;;
    STACK_NAME=*) STACK_NAME="${arg#STACK_NAME=}" ;;
    *) echo "unknown argument: $arg (expected DEPLOY=1, DOWN=0, STACK=…, STACK_NAME=…)" >&2; exit 2 ;;
  esac
done

# Repo root from this file rather than `$0`/cwd, so the script still resolves
# when sourced, symlinked or run from its own directory (P6-550).
SELF="${BASH_SOURCE[0]:-$0}"
ROOT="$(cd "$(dirname "$SELF")/../../.." && pwd)" || { echo "FAIL: cannot resolve repo root from $SELF" >&2; exit 1; }
cd "$ROOT" || exit 1
STACK="${STACK:-code/01_platform/01_docker/docker-stack.yml}"
STACK_NAME="${STACK_NAME:-prod}"
if [ ! -f "$STACK" ]; then
  echo "FAIL: stack file not found: $STACK" >&2
  exit 1
fi

echo "== stack_selfcheck: one-host Swarm deploy path =="

if ! command -v docker >/dev/null 2>&1; then
  echo "SKIP: docker CLI not installed — run offline static check via make stack-selfcheck."
  exit 0
fi
if ! docker info >/dev/null 2>&1; then
  echo "SKIP: docker daemon not running/usable."
  exit 0
fi

# 2. Ensure we are a swarm manager on this single node. Ask docker for its own
#    opinion: a failing `docker node ls` can also mean "no permission", which
#    must not become a `swarm init` on a host that is already in a swarm.
swarm_state="$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || echo unknown)"
if [ "$swarm_state" != "active" ]; then
  echo ">> swarm not active (state=${swarm_state}) — docker swarm init (single-node mimic)"
  docker swarm init --advertise-addr "${SWARM_ADVERTISE_ADDR:-127.0.0.1}"
else
  echo ">> swarm already active"
fi

# This node's own id. `docker node ls -q | head -1` can return any node, and on
# a multi-node swarm labelling that one mutates a peer (P6-207).
node_id="$(docker info --format '{{.Swarm.NodeID}}')"
if [ -z "$node_id" ]; then
  echo "FAIL: cannot determine this node's swarm NodeID" >&2
  exit 1
fi
node_count="$(docker node ls -q | wc -l | tr -d ' ')"
if [ "${node_count:-0}" -gt 1 ]; then
  echo "FAIL: swarm has $node_count nodes — refusing to label (single-host mimic, not a cluster)" >&2
  exit 1
fi
echo ">> current node: $node_id (swarm nodes: $node_count)"

# 3. Label the single node as both worker + observability, so every role-based
#    constraint in the stack schedules on this one host (mirror of v1 M1-3).
docker node update --label-add role=worker "$node_id" >/dev/null
docker node update --label-add observability=true "$node_id" >/dev/null
echo ">> labelled $node_id role=worker + observability=true"

# 4b. Interpolation values for `docker stack config`. The stack marks all of
#     these `:?`, so a validate-only run may fill them with obviously fake
#     placeholders (real digests/endpoints come from .env + runtime.lock in
#     production); DEPLOY=1 must get real ones — defaulting them to
#     `*:unset`/`placeholder` is how an unpullable deploy "succeeds" (P6-208).
#     Credentials are external Swarm secrets and deliberately have NO env
#     fallback here (fail closed).
: "${ZOOKEEPER_IMAGE:=zookeeper:3.9.2}"     # real default in the stack
# NOTE: the stack's own `VAR:?msg` guards do NOT fire for keys inside the
# FLUSS_PROPERTIES block scalar (compose renders s3:///remote-data with the
# var unset instead of erroring - verified 2026-09-16 against the dev twin,
# which carries the same single-$ form). So R2_BUCKET's fail-closed lives
# HERE: DEPLOY=1 refuses an unset/empty R2_BUCKET before any deploy.
required_vars=(
  FLUSS_IMAGE FLINK_IMAGE INGESTION_IMAGE EXECUTION_BRIDGE_IMAGE
  EXECUTION_GATEWAY_IMAGE NAUTILUS_IMAGE OPENOBSERVE_IMAGE
  S3_WAREHOUSE_PATH R2_ENDPOINT R2_BUCKET ARROW_APP_ID ARROW_USER_ID CHECKPOINT_DIR
)
if [ "${DEPLOY:-0}" = "1" ]; then
  missing=()
  for v in "${required_vars[@]}"; do
    if [ -z "${!v:-}" ]; then missing+=("$v"); fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "FAIL: DEPLOY=1 needs real values for: ${missing[*]}" >&2
    echo "      image digests, endpoints and paths — no placeholders on a deploy" >&2
    exit 2
  fi
else
  : "${FLUSS_IMAGE:=fluss:unset}"
  : "${FLINK_IMAGE:=flink:unset}"
  : "${INGESTION_IMAGE:=ingestion:unset}"
  : "${EXECUTION_BRIDGE_IMAGE:=exec-bridge:unset}"
  : "${EXECUTION_GATEWAY_IMAGE:=exec-gateway:unset}"
  : "${NAUTILUS_IMAGE:=nautilus:unset}"
  : "${OPENOBSERVE_IMAGE:=openobserve:unset}"
  : "${S3_WAREHOUSE_PATH:=s3://placeholder/warehouse}"
  : "${R2_ENDPOINT:=https://placeholder.example}"
  : "${R2_BUCKET:=placeholder-bucket}"
  : "${ARROW_APP_ID:=000000}"
  : "${ARROW_USER_ID:=00000000}"
  : "${CHECKPOINT_DIR:=s3://placeholder/checkpoints}"
  echo ">> compile-only: placeholder images/paths in use — a green stack config here is not evidence that a deploy is ready"
fi
export FLUSS_IMAGE FLINK_IMAGE INGESTION_IMAGE EXECUTION_BRIDGE_IMAGE \
       EXECUTION_GATEWAY_IMAGE NAUTILUS_IMAGE OPENOBSERVE_IMAGE \
       ZOOKEEPER_IMAGE S3_WAREHOUSE_PATH R2_ENDPOINT R2_BUCKET ARROW_APP_ID ARROW_USER_ID \
       CHECKPOINT_DIR

# 4. Compile the stack (catches YAML/deploy-schema errors without starting).
echo ">> docker stack config"
docker stack config -c "$STACK" >/dev/null
echo "   stack config: OK"

# 5. Optional real deploy.
if [ "${DEPLOY:-0}" = "1" ]; then
  echo ">> docker stack deploy -c $STACK $STACK_NAME"
  docker stack deploy -c "$STACK" "$STACK_NAME"
  echo "   deploy initiated — inspect with: docker stack services $STACK_NAME"
  echo "   node labels: $(docker node inspect "$node_id" --format '{{.Spec.Labels}}')"
fi

# 6. Optional teardown — only for the stack this run deployed. Teardown used to
#    run unconditionally, so a validate-only run removed an unrelated `prod`
#    stack; and its `|| true` plus unconditional echo reported success for a
#    removal that failed (P6-019).
if [ "${DEPLOY:-0}" = "1" ] && [ "${DOWN:-1}" = "1" ]; then
  if [ -n "$(docker stack ls --format '{{.Name}}' | grep -x "$STACK_NAME" || true)" ]; then
    docker stack rm "$STACK_NAME"
    echo ">> requested removal of stack $STACK_NAME (single-node swarm left active)"
  else
    echo ">> stack $STACK_NAME not present, nothing to remove"
  fi
elif [ "${DEPLOY:-0}" = "1" ]; then
  echo ">> stack $STACK_NAME left in place (DOWN=0)"
fi

echo "== stack_selfcheck: DONE =="
