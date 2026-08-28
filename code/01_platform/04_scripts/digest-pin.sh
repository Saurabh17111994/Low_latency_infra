#!/usr/bin/env bash
# digest-pin.sh — resolve mutable image tags to immutable digests.
#
# Usage:
#   ./digest-pin.sh apache/fluss:0.9.1-incubating
#   → apache/fluss:0.9.1-incubating@sha256:<manifest-digest>
#
# Use this to fill the digest fields in runtime.lock before any production
# deployment. CI SHALL reject any runtime.lock that still contains a bare
# semantic tag without a digest.
#
# Notes (review R-015/R-221): `docker manifest inspect` without `--verbose`
# prints the manifest JSON whose FIRST sha256: is the *config blob* digest,
# not the manifest's own digest — pinning that produces an invalid image
# reference. We therefore prefer `docker buildx imagetools inspect` (which
# prints the real manifest digest) and `skopeo inspect --format` / `crane
# digest`. `docker manifest inspect` queries the registry directly and does
# NOT need the daemon, so the docker branch is not gated on `docker info`.
#
# Requires: docker (buildx) or skopeo or crane (tried in that order).

set -euo pipefail

if [ $# -lt 1 ]; then
	echo "Usage: $0 <image:tag> [<image:tag> ...]" >&2
	echo "  Resolves each image:tag to image:tag@sha256:<manifest digest>" >&2
	exit 2
fi

# R-222: refuse inputs that are already pinned or lack a tag — appending a
# second digest to `image:tag@sha256:abc` would corrupt runtime.lock.
validate_ref() {
	local img="$1"
	case "$img" in
	*@sha256:*)
		echo "ERROR: '$img' is already digest-pinned — refusing to double-pin" >&2
		return 1
		;;
	*:*)
		return 0
		;;
	*)
		echo "ERROR: '$img' has no tag — expected <image>:<tag>" >&2
		return 1
		;;
	esac
}

resolve_one() {
	local img="$1"
	local digest=""
	local err=""

	# R-056: capture resolver errors so a registry/auth failure is visible
	# instead of a generic "could not resolve digest".
	if command -v docker &>/dev/null; then
		err=$(docker buildx imagetools inspect "$img" \
			--format '{{.Manifest.Digest}}' 2>&1) && digest="$err" ||
			err="docker buildx imagetools inspect failed: $err"
		# R-223: for a multi-arch INDEX (e.g. fluss/flink official images),
		# buildx prints the whole index document (not a plain digest); the
		# index's own digest appears as `Digest: sha256:<hex>` on its own
		# line. Extract it so the defensive regex below passes. A single-arch
		# manifest prints the plain digest directly and is left untouched.
		if ! [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
			digest=$(printf '%s\n' "$digest" | sed -n 's/^[[:space:]]*Digest:[[:space:]]*\(sha256:[0-9a-f]\{64\}\)$/\1/p' | head -1)
		fi
	fi
	if [ -z "$digest" ] && command -v skopeo &>/dev/null; then
		err=$(skopeo inspect --format '{{.Digest}}' "docker://${img}" 2>&1) && digest="$err" ||
			err="skopeo inspect failed: $err"
	fi
	if [ -z "$digest" ] && command -v crane &>/dev/null; then
		err=$(crane digest "$img" 2>&1) && digest="$err" ||
			err="crane digest failed: $err"
	fi

	if [ -z "$digest" ]; then
		echo "ERROR: could not resolve digest for $img" >&2
		echo "  ${err:-no resolver available (docker buildx/skopeo/crane). Install one.}" >&2
		return 1
	fi

	# Defensive: the digest must be a plain sha256 manifest digest.
	if ! [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
		echo "ERROR: resolved value for $img is not a sha256 digest: $digest" >&2
		return 1
	fi

	echo "${img}@${digest}"
}

rc=0
for img in "$@"; do
	if ! validate_ref "$img" || ! resolve_one "$img"; then
		rc=1
	fi
done
exit $rc
