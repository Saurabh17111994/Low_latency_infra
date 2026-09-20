#!/usr/bin/env bash
# stack_selfcheck.sh — Swarm self-check for docker-stack.yml: one-host mimic,
# or validation-only against an existing multi-node cluster (CLUSTER=1, CHG-270).
#
# Purpose: exercise the PRODUCTION stack deploy path on a SINGLE host (no 4/7
# VMs). This is the offline-prep bridge that proves `docker stack deploy`
# succeeds against a real (local) Swarm, without needing the real rig. It is
# NOT multi-VM HA evidence (SWARM-MGR-001..006 need a 3-manager quorum) — that
# is M3. CLUSTER=1 closes the other half: a real cluster's stack is validated
# before `docker stack deploy`, instead of being refused as "not a mimic".
#
# What it does:
#   1. Requires the docker CLI + a running daemon (SKIP otherwise).
#   2. Default mode: `docker swarm init` when the swarm is not active.
#      CLUSTER=1: refuse instead — a node that was meant to join a cluster must
#      not become a one-node swarm of its own, which schedules nothing and looks
#      healthy while it does it.
#   3. Default mode: label this node `role=worker` + `observability=true` so the
#      same constraint set the real cluster uses actually schedules here
#      (`node.labels.role == worker`, plus the separate boolean
#      `node.labels.observability == true`; a label key holds one value).
#      CLUSTER=1: labels are read and verified, never written — a validator that
#      mutates what it measures cannot report a real misconfiguration.
#   4. `docker stack config -c <stack>` — compile the manifest (no services
#      started). Exit non-zero on any stack error.
#   5. CLUSTER=1 only: verify what a green `docker stack config` hides — every
#      node Ready+Active, a live manager, and every `node.labels.… == …`
#      constraint of the *rendered* stack satisfied by some node's labels
#      (rendered, not source: anchors, per-service overrides and compose's own
#      normalisation all land in the render).
#   6. Optional real deploy: DEPLOY=1 -> docker stack deploy -c <stack> prod.
#   7. Teardown: DEPLOY=1 + DOWN=1 (the default) -> docker stack rm prod.
#      A validate-only run never removes anything (P6-019).
#
# Usage:
#   ./stack_selfcheck.sh                  # validate (swarm init + stack config)
#   ./stack_selfcheck.sh CLUSTER=1        # validate an existing real cluster
#   ./stack_selfcheck.sh DEPLOY=1         # also deploy prod, then remove it
#   ./stack_selfcheck.sh DEPLOY=1 DOWN=0  # deploy and leave the stack up
#   DEPLOY=1 ./stack_selfcheck.sh         # env form (equivalent)
# DEPLOY=, DOWN=, CLUSTER=, STACK= and STACK_NAME= are accepted as arguments or
# as the environment; anything else is refused with exit 2.
set -euo pipefail

# 0. Key=value overrides. The Makefile passes these positionally (P6-209), so
#    read them as arguments as well as from the environment — and parse them
#    rather than `eval`ing them, so an argument cannot smuggle a command.
for arg in "$@"; do
  case "$arg" in
    DEPLOY=*)     DEPLOY="${arg#DEPLOY=}" ;;
    DOWN=*)       DOWN="${arg#DOWN=}" ;;
    CLUSTER=*)    CLUSTER="${arg#CLUSTER=}" ;;
    STACK=*)      STACK="${arg#STACK=}" ;;
    STACK_NAME=*) STACK_NAME="${arg#STACK_NAME=}" ;;
    *) echo "unknown argument: $arg (expected DEPLOY=1, DOWN=0, CLUSTER=1, STACK=…, STACK_NAME=…)" >&2; exit 2 ;;
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

if [ "${CLUSTER:-0}" = "1" ]; then
  echo "== stack_selfcheck: cluster validation path =="
else
  echo "== stack_selfcheck: one-host Swarm deploy path =="
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "SKIP: docker CLI not installed — run offline static check via make stack-selfcheck."
  exit 0
fi
if ! docker info >/dev/null 2>&1; then
  echo "SKIP: docker daemon not running/usable."
  exit 0
fi

# CLUSTER=1 is a validator. Deploying from it would also mean honouring the
# default DOWN=1 afterwards, i.e. removing the production stack as a side effect
# of a check (P6-019's lesson, one level up).
if [ "${CLUSTER:-0}" = "1" ] && [ "${DEPLOY:-0}" = "1" ]; then
  echo "FAIL: CLUSTER=1 is validation only — deploying and tearing down the stack is the runbook's own step, not this validator's" >&2
  exit 2
fi

# 2. Ensure we are a swarm manager on this single node. Ask docker for its own
#    opinion: a failing `docker node ls` can also mean "no permission", which
#    must not become a `swarm init` on a host that is already in a swarm.
swarm_state="$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || echo unknown)"
if [ "${CLUSTER:-0}" = "1" ]; then
  if [ "$swarm_state" != "active" ]; then
    echo "FAIL: CLUSTER=1 but the swarm is not active (state=${swarm_state}) — join this node to the cluster first; this script never runs 'docker swarm init' against a cluster" >&2
    exit 1
  fi
  echo ">> cluster mode: swarm active"
elif [ "$swarm_state" != "active" ]; then
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
if [ "${CLUSTER:-0}" = "1" ]; then
  # Never write a label here: on a real cluster the labels are the bootstrap's
  # business (`--label-add` at join time), and a validator that fixes what it
  # measures cannot report a real misconfiguration.
  echo ">> cluster mode: $node_count node(s) — labels are verified, never written"
else
  if [ "${node_count:-0}" -gt 1 ]; then
    echo "FAIL: swarm has $node_count nodes — refusing to label (single-host mimic, not a cluster). For a real cluster use CLUSTER=1." >&2
    exit 1
  fi
  echo ">> current node: $node_id (swarm nodes: $node_count)"

  # 3. Label the single node as both worker + observability, so every role-based
  #    constraint in the stack schedules on this one host (mirror of v1 M1-3).
  docker node update --label-add role=worker "$node_id" >/dev/null
  docker node update --label-add observability=true "$node_id" >/dev/null
  echo ">> labelled $node_id role=worker + observability=true"
fi

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
  # CHG-269: the EOD scheduler is the first consumer of DDL_APPLY_IMAGE, and it
  # refuses to guess which tables are EOD-eligible.
  DDL_APPLY_IMAGE EOD_TABLES
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
  : "${DDL_APPLY_IMAGE:=ddl-apply:unset}"
  : "${EOD_TABLES:=placeholder-tables}"
  echo ">> compile-only: placeholder images/paths in use — a green stack config here is not evidence that a deploy is ready"
fi
export FLUSS_IMAGE FLINK_IMAGE INGESTION_IMAGE EXECUTION_BRIDGE_IMAGE \
       EXECUTION_GATEWAY_IMAGE NAUTILUS_IMAGE OPENOBSERVE_IMAGE \
       ZOOKEEPER_IMAGE S3_WAREHOUSE_PATH R2_ENDPOINT R2_BUCKET ARROW_APP_ID ARROW_USER_ID \
       CHECKPOINT_DIR DDL_APPLY_IMAGE EOD_TABLES

# 4. Compile the stack (catches YAML/deploy-schema errors without starting).
#    The render is kept: CLUSTER=1 reads the placement constraints out of it, so
#    the check sees exactly what `docker stack deploy` would see.
echo ">> docker stack config"
rendered="$(docker stack config -c "$STACK")"
echo "   stack config: OK"

# 4c. CLUSTER=1: verify the cluster can actually run this stack. Two failures a
#     green `docker stack config` hides: a node that cannot schedule (Down,
#     Unreachable, Drain) and a placement constraint no node satisfies.
if [ "${CLUSTER:-0}" = "1" ]; then
  echo ">> cluster mode: verifying ${node_count} node(s)"
  node_rows="$(docker node ls --format '{{.ID}}|{{.Status}}|{{.Availability}}|{{.ManagerStatus}}')"
  bad_nodes="$(printf '%s\n' "$node_rows" | awk -F'|' 'NF>=4 && ($2!="Ready" || $3!="Active") {print "      " $1 " (" $2 ", " $3 ")"}')"
  if [ -n "$bad_nodes" ]; then
    echo "FAIL: node(s) not Ready+Active — tasks cannot schedule there:" >&2
    printf '%s\n' "$bad_nodes" >&2
    exit 1
  fi
  live_managers="$(printf '%s\n' "$node_rows" | awk -F'|' '$4=="Leader" || $4=="Reachable"' | wc -l | tr -d ' ')"
  if [ "${live_managers:-0}" -eq 0 ]; then
    echo "FAIL: no live manager in this swarm — nothing can accept the deploy" >&2
    exit 1
  fi
  echo "   nodes Ready+Active; live managers: $live_managers"

  constraints="$(printf '%s\n' "$rendered" \
    | grep -oE 'node\.labels\.[A-Za-z0-9_.]+[[:space:]]*==[[:space:]]*[^][[:space:]]+' \
    | sort -u || true)"
  if [ -z "$constraints" ]; then
    echo "   placement constraints in the rendered stack: none"
  else
    node_labels=""
    while read -r id; do
      [ -n "$id" ] || continue
      node_labels="${node_labels}$(docker node inspect "$id" --format '{{json .Spec.Labels}}')"$'\n'
    done <<< "$(docker node ls -q)"
    unsatisfied=""
    while read -r line; do
      [ -n "$line" ] || continue
      key="$(printf '%s' "$line" | awk -F'[[:space:]]*==[[:space:]]*' '{print $1}')"
      value="$(printf '%s' "$line" | awk -F'[[:space:]]*==[[:space:]]*' '{print $2}' | tr -d "\"'")"
      key="${key#node.labels.}"
      if ! printf '%s\n' "$node_labels" | grep -qF "\"${key}\":\"${value}\""; then
        unsatisfied="${unsatisfied}${line}"$'\n'
      fi
    done <<< "$constraints"
    if [ -n "$unsatisfied" ]; then
      echo "FAIL: no node satisfies these placement constraints — the stack would schedule nowhere:" >&2
      printf '      %s\n' $unsatisfied >&2
      echo "      fix at join time: docker node update --label-add <key>=<value> <node>" >&2
      exit 1
    fi
    echo "   placement constraints satisfied: $(printf '%s\n' "$constraints" | wc -l | tr -d ' ')"
  fi
fi

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
