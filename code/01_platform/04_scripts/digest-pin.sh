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
	esac
	# P6-064/P6-065: only the name after the last '/' carries the tag — the
	# old `*:*` accepted `registry:5000/repo` (port colon, no tag), which
	# then pinned as `registry:5000/repo@sha256:...` without the required
	# `:tag` component. `*:?*` also rejects empty tags (`repo:`).
	name="${img##*/}"
	case "$name" in
	*:?*)
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
	# P6-062/P6-066: stdout goes to `digest`, stderr to a temp file — the old
	# `2>&1` merged them, so any warning line (auth notice, default-platform
	# note) contaminated a successful result into a false hard failure.
	# P6-354/P6-355/P6-357/P6-358: options before the ref + `--` — a ref
	# starting with `-` must never parse as a resolver flag, and the trailing
	# `--format` risked parsing as a second image on strict parsers.
	tmp_err="$(mktemp)"
	if command -v docker &>/dev/null; then
		# buildx v0.23+ ignores a bare struct field as a format: it falls back to
		# the human-readable dump (and the strict digest regex fails it closed
		# into the skopeo/crane fallbacks). `printf "%s"` forces template
		# evaluation of the field. Verified live on buildx v0.23.0-desktop.1:
		# bare `{{.Manifest.Digest}}` prints the full Name:/MediaType:/Digest:
		# dump; `{{printf "%s" .Manifest.Digest}}` prints the bare digest.
		if digest=$(docker buildx imagetools inspect --format '{{printf "%s" .Manifest.Digest}}' -- "$img" 2>"$tmp_err"); then
			: # keep stdout-only digest
		else
			err="docker buildx imagetools inspect failed: $(cat "$tmp_err")"
			digest=""
		fi
		# P6-063/P6-067: NEVER parse the human-readable `Digest:` line (with
		# multiple `Digest:` lines — index + per-platform manifests — head -1
		# can pin a single-platform digest instead of the index digest).
		# Malformed output falls through to skopeo/crane (P6-353/P6-356).
		if ! [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
			err="docker buildx imagetools inspect returned unexpected output: $digest"
			digest=""
		fi
	fi
	if [ -z "$digest" ] && command -v skopeo &>/dev/null; then
		if digest=$(skopeo inspect --format '{{.Digest}}' -- "docker://${img}" 2>"$tmp_err"); then
			: # keep stdout-only digest
		else
			err="skopeo inspect failed: $(cat "$tmp_err")"
			digest=""
		fi
		if ! [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
			err="skopeo inspect returned unexpected output: $digest"
			digest=""
		fi
	fi
	if [ -z "$digest" ] && command -v crane &>/dev/null; then
		if digest=$(crane digest -- "$img" 2>"$tmp_err"); then
			: # keep stdout-only digest
		else
			err="crane digest failed: $(cat "$tmp_err")"
			digest=""
		fi
		if ! [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
			err="crane digest returned unexpected output: $digest"
			digest=""
		fi
	fi
	rm -f "$tmp_err"

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
