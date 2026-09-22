#!/usr/bin/env bash
# fetch-jars.sh — stage the lake-tiering plugin jars for the production Fluss image.
#
# WHY A FETCH SCRIPT
# The production Swarm stack cannot `build:` (docker-stack.yml forbids it), so the
# Fluss image must be pre-built on a build host. The dev compose gets the lake
# plugin jars by bind-mounting host binaries out of fluss-plugins/ — files that
# are gitignored and therefore do NOT travel to another machine with the repo.
# A fresh clone on the production VM would start Fluss with no S3A filesystem in
# the iceberg plugin classloader, and lake tiering would fail there and only
# there. This script pulls every jar from Maven Central and verifies it against
# a pinned SHA256 BEFORE it can enter an image, so the image — not the host
# filesystem — is what carries them.
#
# WHAT IS DELIBERATELY NOT HERE
# fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar: the dev compose mounted it until
# 2026-09-22, but it is
# an unpublished build module — 404 on Maven Central AND on the Apache snapshot
# repository. A SNAPSHOT cannot be checksum-pinned, so it cannot be part of a
# reproducible image. All 12898 of its entries are already contained in the
# published fluss-fs-s3 + fluss-fs-hdfs pair staged below (verified entry by
# entry, 0 missing), and it declares no Fluss FileSystemPlugin service of its
# own. Dropping it is what makes this image reproducible. Same call, same
# reasoning as the Flink image (CHG-179).
#
# WHY NO hadoop-mapreduce-compat JAR HERE
# That jar (M-15/M-16) exists for iceberg's PARQUET WRITE path, which runs in
# the Flink tiering job — not in the Fluss servers. Fluss 0.9 tiers the lake
# from an external Flink job (see tiering-start.sh); the servers only
# coordinate. The dev tablet has no compat jar and lake tiering works there.
# The Flink image already ships it (flink-runtime/Dockerfile).
#
# Usage:
#   fetch-jars.sh --dest DIR      download + verify into DIR
#   fetch-jars.sh --verify DIR    verify FILES ALREADY IN DIR against the pins
#                                 (offline — used by the test suite)
#
# Requires: curl, sha256sum.

set -euo pipefail

MAVEN_BASE="${FLUSS_MAVEN_BASE:-https://repo1.maven.org/maven2}"
FLUSS_VERSION="1.0.0"

# --- The pins. name|sha256|maven-relative-path -----------------------------
# Identical artifacts (and identical SHA256s) to the ones the Flink image
# stages, so the two images cannot drift apart unnoticed.
PINS=(
	"fluss-fs-s3-${FLUSS_VERSION}.jar|5226480ec1905e7b90f5684fa7736580ed0be0de082697f82fb467bbe99a8a00|org/apache/fluss/fluss-fs-s3/${FLUSS_VERSION}/fluss-fs-s3-${FLUSS_VERSION}.jar"
	"fluss-fs-hdfs-${FLUSS_VERSION}.jar|bea4937198ffb52827402b057d0afcc0de0be9b761e7c727c824c5d8e9218597|org/apache/fluss/fluss-fs-hdfs/${FLUSS_VERSION}/fluss-fs-hdfs-${FLUSS_VERSION}.jar"
)

die() { echo "fetch-jars: $*" >&2; exit 1; }

usage() {
	echo "Usage: $0 --dest DIR | --verify DIR" >&2
	exit 2
}

verify_one() {
	local dir="$1" name="$2" want="$3"
	local file="$dir/$name"
	[ -f "$file" ] || { echo "MISSING  $name" >&2; return 1; }
	local got
	got="$(sha256sum "$file" | cut -d' ' -f1)"
	[ "$got" = "$want" ] || {
		echo "MISMATCH $name" >&2
		echo "  expected $want" >&2
		echo "  actual   $got" >&2
		return 1
	}
	return 0
}

# verify_all DIR — offline check of every pinned artifact present in DIR.
verify_all() {
	local dir="$1" rc=0 entry name want
	for entry in "${PINS[@]}"; do
		name="${entry%%|*}"
		want="$(echo "${entry#*|}" | cut -d'|' -f1)"
		verify_one "$dir" "$name" "$want" || rc=1
	done
	if [ "$rc" -eq 0 ]; then
		echo "fetch-jars: all ${#PINS[@]} artifacts match their pins."
	else
		echo "fetch-jars: pin verification FAILED (see above)." >&2
	fi
	return "$rc"
}

download_one() {
	local dir="$1" name="$2" want="$3" path="$4"
	local url="$MAVEN_BASE/$path"
	# --fail so an HTTP error becomes a non-zero exit (a 404 HTML body must
	# never be written to a .jar name), --location for repo redirects.
	curl -fsSL --retry 3 --retry-delay 2 -o "$dir/$name" "$url" \
		|| die "download failed: $url"
	verify_one "$dir" "$name" "$want" \
		|| die "artifact $name from $url does not match its pinned SHA256 — refusing to build an image from it"
	echo "  ok  $name"
}

case "${1:-}" in
--dest)
	[ "$#" -eq 2 ] || usage
	mkdir -p "$2"
	dest="$(cd "$2" && pwd)"
	echo "Staging Fluss runtime jars into $dest"
	for entry in "${PINS[@]}"; do
		name="${entry%%|*}"
		rest="${entry#*|}"
		want="${rest%%|*}"
		path="${rest#*|}"
		download_one "$dest" "$name" "$want" "$path"
	done
	echo "fetch-jars: staged ${#PINS[@]} artifacts."
	;;
--verify)
	[ "$#" -eq 2 ] || usage
	[ -d "$2" ] || die "not a directory: $2"
	verify_all "$(cd "$2" && pwd)"
	;;
*)
	usage
	;;
esac
