#!/usr/bin/env bash
# image-publish.sh — push the seven project-built images to a registry and emit
# digest-pinned deploy-environment values.
#
# Why this exists: a locally built image that was never pushed has no registry
# manifest digest, so `repo@sha256:<image-id>` does not resolve and the deploy
# cannot be digest-pinned at all (CHG-218). This is the "push, then pin THAT"
# half of the VM guide's S4 step 6 — the half that made S7 undeployable.
#
# Usage:
#   ./image-publish.sh --registry HOST:PORT [--tag prod] [--env-registry HOST:PORT]
#                      [--write-env FILE]
#   ./image-publish.sh --registry https://ghcr.io/<owner> --env-registry ghcr.io/<owner> \
#                      --write-env code/01_platform/01_docker/.env    # public GHCR (Decision 2026-09-21)
#   ./image-publish.sh --print-map          # VAR=local-image, one per line
#   ./image-publish.sh --merge-env FILE     # rewrite VAR=ref lines read on stdin
#   ./image-publish.sh --self-check         # offline: map vs stack, no push
#
# The registry value may carry an owner path. The reachability probe uses its HOST
# (`<host>/v2/`), because an owner path is not part of the v2 API root: probing
# `ghcr.io/<owner>/v2/` answers 404, which used to stop the run with "bring it up on
# VM1 first" for a registry that was healthy (CHG-274, measured 2026-09-21). A 401
# means "auth-enabled registry" (GHCR) and passes — only the pushes need credentials.
#
# Exit: 0 ok | 2 usage or registry preflight refusal | 3 env file unusable |
#       4 an image is missing locally or a push/digest failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
STACK="$REPO_ROOT/code/01_platform/01_docker/docker-stack.yml"
LOCK="$REPO_ROOT/code/01_platform/01_docker/runtime.lock"
DIGEST_PIN="$SCRIPT_DIR/digest-pin.sh"

# The six project-built images the production stack takes from the environment,
# plus the DDL-apply TOOL image (CHG-256). A fresh production cluster boots with
# an empty Fluss catalog and ingestion refuses to start on one
# (allowRuntimeDdl=false). The first-boot step that creates the catalog runs
# outside the stack, so its image is never a `${X_IMAGE:?}` the stack demands --
# without this entry nothing on the VMs can run it.
#
# FLINK_IMAGE and FLUSS_IMAGE are the BUILT runtime images, not the stock
# upstream ones: the stock Flink image cannot run this job at all (CHG-179), and
# the stack mounts no connector jar, so the image must already contain it.
#
# OPENOBSERVE_IMAGE is deliberately absent — it is a public image whose digest
# runtime.lock already pins, so there is nothing to push.
IMAGE_MAP=(
	"FLINK_IMAGE=trading-flink-runtime:0.1.0"
	"FLUSS_IMAGE=trading-fluss-runtime:0.1.0"
	"INGESTION_IMAGE=01_docker-ingestion:latest"
	"NAUTILUS_IMAGE=01_docker-nautilus:latest"
	"EXECUTION_BRIDGE_IMAGE=01_docker-execution-bridge:latest"
	"EXECUTION_GATEWAY_IMAGE=01_docker-execution-gateway:latest"
	"DDL_APPLY_IMAGE=01_docker-ddl-apply:latest"
)

usage() {
	sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
}

print_map() {
	printf '%s\n' "${IMAGE_MAP[@]}"
}

# Rewrite `VAR=ref` lines in an env file from the VAR=ref pairs on stdin.
# Every other line — comments, unrelated variables, order — is preserved, and a
# variable that already exists is replaced in place rather than duplicated, so
# re-running is idempotent.
merge_env() {
	local file="$1"
	if [ ! -f "$file" ]; then
		echo "FAIL: env file not found: $file" >&2
		echo "  create it first: cp code/01_platform/01_docker/.env.example $file" >&2
		return 3
	fi
	local updates out
	updates="$(mktemp)"
	out="$(mktemp)"
	cat >"$updates"
	if ! python3 -c '
import re, sys

path, updates_path = sys.argv[1], sys.argv[2]
updates = {}
with open(updates_path) as fh:
    for raw in fh:
        raw = raw.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        updates[key.strip()] = value.strip()

with open(path) as fh:
    lines = fh.read().splitlines()

pattern = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=")
seen, result = set(), []
for line in lines:
    match = pattern.match(line)
    if match and match.group(1) in updates:
        key = match.group(1)
        if key not in seen:
            result.append(key + "=" + updates[key])
            seen.add(key)
        continue
    result.append(line)
for key, value in updates.items():
    if key not in seen:
        result.append(key + "=" + value)
sys.stdout.write("\n".join(result) + "\n")
' "$file" "$updates" >"$out"; then
		rm -f "$updates" "$out"
		echo "FAIL: could not rewrite $file" >&2
		return 3
	fi
	rm -f "$updates"
	cat "$out" >"$file"   # keeps the file's inode and permissions
	rm -f "$out"
	echo ">> rewrote $file" >&2
}

self_check() {
	local rc=0
	echo "== image-publish self-check (offline) =="

	local demanded pushed pinned
	demanded="$(grep -oE '\$\{[A-Z_]+_IMAGE:\?' "$STACK" 2>/dev/null \
		| sed -E 's/^\$\{//; s/:\?$//' | sort -u)"
	if [ -z "$demanded" ]; then
		echo "FAIL  could not read \${X_IMAGE:?} references from $STACK"
		return 4
	fi
	pushed="$(print_map | cut -d= -f1 | sort -u)"
	pinned="$(sed -E -e 's/^[[:space:]]+//' -e 's/^export([[:space:]]+|$)//' "$LOCK" 2>/dev/null \
		| grep -E '^[A-Za-z0-9_]+_IMAGE=.*@sha256:' | cut -d= -f1 | sort -u)"

	local uncovered
	uncovered="$(comm -23 <(printf '%s\n' "$demanded") <(printf '%s\n%s\n' "$pushed" "$pinned" | sort -u))"
	if [ -n "$uncovered" ]; then
		echo "FAIL  the stack demands $(printf '%s' "$uncovered" | tr '\n' ' ')— not pushed here and not pinned in runtime.lock"
		rc=4
	else
		echo "PASS  every \${X_IMAGE:?} the stack demands is pushed here or pinned in runtime.lock"
	fi

	# The stock upstream images are the trap this script exists to close.
	local stock=""
	local entry var img
	for entry in "${IMAGE_MAP[@]}"; do
		var="${entry%%=*}"
		img="${entry#*=}"
		case "$var:$img" in
		FLINK_IMAGE:flink:* | FLUSS_IMAGE:apache/fluss*)
			stock="$stock $var"
			;;
		esac
	done
	if [ -n "$stock" ]; then
		echo "FAIL $stock points at the STOCK upstream image — the stock Flink image cannot run this job (CHG-179)"
		rc=4
	else
		echo "PASS  the map names the built runtime images, not the stock upstream ones"
	fi

	# Image presence is only checkable where docker is. Report, never guess.
	if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
		local missing=""
		for entry in "${IMAGE_MAP[@]}"; do
			img="${entry#*=}"
			docker image inspect "$img" >/dev/null 2>&1 || missing="$missing $img"
		done
		if [ -n "$missing" ]; then
			echo "FAIL  not built locally:$missing"
			echo "      build them: make images && make flink-image && make fluss-image"
			rc=4
		else
			echo "PASS  all seven images exist locally and are ready to push"
		fi
	else
		echo "SKIP  docker unavailable — image presence not checked"
	fi

	[ "$rc" -eq 0 ] && echo "PASS  image-publish self-check"
	return "$rc"
}

push_all() {
	local registry="$1" tag="$2" write_env="$3" env_registry="$4"
	local scheme="http"
	case "$registry" in
	http://*) registry="${registry#http://}" ;;
	https://*)
		scheme="https"
		registry="${registry#https://}"
		;;
	esac
	registry="${registry%/}"

	# The address that goes into the deploy environment is NOT always the address
	# the push used. Pushing through an SSH tunnel uses `localhost:5000`, but a VM
	# resolving `localhost` reaches itself, so the env must carry the registry's
	# real address. The digest is content-addressed and therefore identical.
	env_registry="${env_registry:-$registry}"
	case "$env_registry" in
	http://*) env_registry="${env_registry#http://}" ;;
	https://*) env_registry="${env_registry#https://}" ;;
	esac
	env_registry="${env_registry%/}"

	if ! command -v curl >/dev/null 2>&1; then
		echo "FAIL: curl is required for the registry probe" >&2
		return 2
	fi
	# The v2 API root lives at the registry HOST, never under the repository path.
	# A GHCR target carries an owner path (`ghcr.io/<owner>`), so probing
	# `https://ghcr.io/<owner>/v2/` answers 404 and the run stops with "bring it up
	# on VM1 first" for a registry that is healthy — measured 2026-09-21, before
	# this split, against a local registry under an owner path.
	local registry_host="${registry%%/*}"
	local code
	code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${scheme}://${registry_host}/v2/" || true)"
	case "$code" in
	200 | 401) ;; # 401 = auth-enabled registry; the probe only proves it answers
	*)
		echo "FAIL: no registry answered at ${scheme}://${registry_host}/v2/ (http ${code:-none})" >&2
		echo "  for a private registry bring it up first (VM guide S4 step 5); for GHCR check" >&2
		echo "  network access to ghcr.io, then re-run." >&2
		return 2
		;;
	esac

	local refs rc=0
	refs="$(mktemp)"
	local entry var img target ref
	for entry in "${IMAGE_MAP[@]}"; do
		var="${entry%%=*}"
		img="${entry#*=}"
		if ! docker image inspect "$img" >/dev/null 2>&1; then
			echo "FAIL: $img is not built locally — run: make images && make flink-image && make fluss-image" >&2
			rc=4
			continue
		fi
		target="${registry}/${img%%:*}:${tag}"
		docker tag "$img" "$target"
		echo ">> pushing $target" >&2
		if ! docker push "$target" >&2; then
			echo "FAIL: push failed for $target" >&2
			rc=4
			continue
		fi

		# The registry's own answer, through the repository's resolver.
		# Measured 2026-09-19 against a local `registry:2` over plain HTTP:
		# buildx imagetools resolves it with no extra flags, and the value
		# matched the digest `docker push` printed. A *remote* plain-HTTP
		# registry needs `insecure-registries` in daemon.json (VM guide S4 step
		# 5) — a daemon setting this script cannot work around.
		ref=""
		if [ -x "$DIGEST_PIN" ]; then
			ref="$(bash "$DIGEST_PIN" "$target" 2>/dev/null || true)"
		fi
		if ! printf '%s' "$ref" | grep -qE '@sha256:[0-9a-f]{64}$'; then
			echo "FAIL: could not resolve a manifest digest for $target" >&2
			echo "  use the 'digest: sha256:…' line docker push printed just above." >&2
			rc=4
			continue
		fi
		# rewrite the reference onto the address the deployment will use
		printf '%s=%s\n' "$var" "${env_registry}${ref#"$registry"}" >>"$refs"
	done

	if [ "$rc" -eq 0 ]; then
		echo
		echo "== digest-pinned deploy-environment values =="
		cat "$refs"
		if [ -n "$write_env" ]; then
			merge_env "$write_env" <"$refs" || rc=$?
		fi
	else
		echo "FAIL: not every image could be published — no env values written" >&2
	fi
	rm -f "$refs"
	return "$rc"
}

mode=""
registry=""
tag="prod"
write_env=""
env_registry=""

while [ $# -gt 0 ]; do
	case "$1" in
	--print-map)
		mode="print-map"
		shift
		;;
	--self-check)
		mode="self-check"
		shift
		;;
	--merge-env)
		mode="merge-env"
		write_env="${2:-}"
		[ -n "$write_env" ] || {
			echo "FAIL: --merge-env needs a file" >&2
			exit 2
		}
		shift 2
		;;
	--registry)
		registry="${2:-}"
		[ -n "$registry" ] || {
			echo "FAIL: --registry needs host:port" >&2
			exit 2
		}
		shift 2
		;;
	--tag)
		tag="${2:-}"
		[ -n "$tag" ] || {
			echo "FAIL: --tag needs a value" >&2
			exit 2
		}
		shift 2
		;;
	--env-registry)
		env_registry="${2:-}"
		[ -n "$env_registry" ] || {
			echo "FAIL: --env-registry needs host:port" >&2
			exit 2
		}
		shift 2
		;;
	--write-env)
		write_env="${2:-}"
		[ -n "$write_env" ] || {
			echo "FAIL: --write-env needs a file" >&2
			exit 2
		}
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "FAIL: unknown argument: $1" >&2
		usage
		exit 2
		;;
	esac
done

case "$mode" in
print-map) print_map ;;
self-check) self_check ;;
merge-env) merge_env "$write_env" ;;
"")
	if [ -z "$registry" ]; then
		usage
		exit 2
	fi
	push_all "$registry" "$tag" "$write_env" "$env_registry"
	;;
esac
