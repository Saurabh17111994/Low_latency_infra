#!/usr/bin/env bash
# fetch-jars.sh — stage the Flink runtime jars for the production image.
#
# WHY A FETCH SCRIPT
# The production Swarm stack cannot `build:` (docker-stack.yml forbids it), so
# the Flink image must be pre-built on a build host. That image needs the Fluss
# connector + filesystem plugins baked into /opt/flink/lib. Rather than depend
# on whatever jars happen to sit in a developer's working tree — the
# docker-compose volumes mount host binaries that are gitignored and therefore
# unversioned — this script pulls every jar from Maven Central and verifies it
# against a pinned SHA256 BEFORE it can enter an image. A silently swapped
# upstream artifact fails the build instead of shipping.
#
# WHAT IS DELIBERATELY NOT HERE
# fluss-fs-hadoop-shaded-0.9-SNAPSHOT.jar: an unpublished build module (HTTP 404
# on Central; the published name `fluss-fs-hadoop` is an unrelated 10 KB stub).
# A SNAPSHOT cannot be checksum-pinned, so it cannot be part of a reproducible
# image. Every one of its 12336 entries is already contained in the published
# fluss-fs-s3 + fluss-fs-hdfs pair — both are staged below, and hadoop-shaded
# declares no Fluss FileSystemPlugin service of its own, so nothing loads it by
# name. Dropping it is what makes this image reproducible.
#
# THE DERIVED COMPAT JAR
# Iceberg's shaded parquet write path lazily loads the UN-shaded
# org.apache.hadoop.mapreduce.lib.input.FileInputFormat, which exists only in
# hadoop-mapreduce-client-core (M-15/M-16, docs/06_operations/07-lake-archive-ops.md).
# The full 2.8.5 jar also carries org/apache/hadoop/mapred/**, whose
# Configuration would clash with the 3.4.3 one bundled in the fluss-fs jars
# (NoSuchMethodError getTimeDuration). So the compat jar is DERIVED here: it is
# exactly the org/apache/hadoop/mapreduce/** package, nothing else. File
# timestamps are pinned so an independent run reproduces the same bytes, and
# the result is checksum-verified like every downloaded jar.
#
# Usage:
#   fetch-jars.sh --dest DIR      download + verify + derive into DIR
#   fetch-jars.sh --verify DIR    verify FILES ALREADY IN DIR against the pins
#                                 (offline — used by the test suite)
#
# Requires: curl, sha256sum, unzip, zip.

set -euo pipefail

MAVEN_BASE="${FLUSS_MAVEN_BASE:-https://repo1.maven.org/maven2}"
FLUSS_VERSION="1.0.0"
MAPREDUCE_VERSION="2.8.5"

# --- The pins. name|sha256|maven-relative-path -----------------------------
# Every entry is verified before use; --verify checks the same table, so a
# corrupted or substituted jar in an image layer is a hard failure.
PINS=(
	"fluss-flink-2.2-${FLUSS_VERSION}.jar|8cad6d1342b7e2deaf3db7fe7fbc1911c05823a538517b58da32cc0e8601ed8e|org/apache/fluss/fluss-flink-2.2/${FLUSS_VERSION}/fluss-flink-2.2-${FLUSS_VERSION}.jar"
	"fluss-flink-tiering-${FLUSS_VERSION}.jar|f0b85db9b6cfdad8658406e31ce3a2aae6ffa0cdff81ce22fffc919e4032160e|org/apache/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar"
	"fluss-lake-iceberg-${FLUSS_VERSION}.jar|9b7e25d431510a4ea3d187a2161e40ec6e026d4c110d8ea863602001880c2670|org/apache/fluss/fluss-lake-iceberg/${FLUSS_VERSION}/fluss-lake-iceberg-${FLUSS_VERSION}.jar"
	"fluss-fs-s3-${FLUSS_VERSION}.jar|5226480ec1905e7b90f5684fa7736580ed0be0de082697f82fb467bbe99a8a00|org/apache/fluss/fluss-fs-s3/${FLUSS_VERSION}/fluss-fs-s3-${FLUSS_VERSION}.jar"
	"fluss-fs-hdfs-${FLUSS_VERSION}.jar|bea4937198ffb52827402b057d0afcc0de0be9b761e7c727c824c5d8e9218597|org/apache/fluss/fluss-fs-hdfs/${FLUSS_VERSION}/fluss-fs-hdfs-${FLUSS_VERSION}.jar"
	"hadoop-mapreduce-client-core-${MAPREDUCE_VERSION}.jar|d68af4f03e9d64b14476119f939a660a89a9116732511f025eea59e079a9102f|org/apache/hadoop/hadoop-mapreduce-client-core/${MAPREDUCE_VERSION}/hadoop-mapreduce-client-core-${MAPREDUCE_VERSION}.jar"
)

COMPAT_JAR="hadoop-mapreduce-compat-${MAPREDUCE_VERSION}.jar"
# Pinned from an observed build (see the README) — a real regression guard, not
# documentation. The bytes below are what a *C-collation* sort of the entries
# produces: measured 2026-09-21, the identical content sorted under en_IN.UTF-8
# hashes to 14c5a8e543a8232d26465d30579be82d929de373cb339b5e91cfdc60e1e42205
# instead, which is the trap the first CI run fell into.
COMPAT_SHA256="c19c414bc07b610b6d42c1806a511526113036dc5b6bb0bf0c3c730a7ae989df"
DERIVE_MTIME="198001010000"

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
	verify_one "$dir" "$COMPAT_JAR" "$COMPAT_SHA256" || rc=1
	if [ "$rc" -eq 0 ]; then
		echo "fetch-jars: all $(( ${#PINS[@]} + 1 )) artifacts match their pins."
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

# derive_compat DIR — build the compat jar from the already-verified source jar.
derive_compat() {
	local dir="$1" src="hadoop-mapreduce-client-core-${MAPREDUCE_VERSION}.jar"
	command -v unzip >/dev/null 2>&1 || die "unzip is required to derive $COMPAT_JAR"
	command -v zip >/dev/null 2>&1 || die "zip is required to derive $COMPAT_JAR"
	local work
	work="$(mktemp -d)"
	# shellcheck disable=SC2064  # expand $work now: the trap must not see it unset
	trap "rm -rf '$work'" EXIT
	(
		cd "$work"
		unzip -q "$dir/$src" 'org/apache/hadoop/mapreduce/*'
		# Fixed mtime makes the zip bytes reproducible across runs, and the
		# pinned collation makes the *entry order* reproducible too: `sort`
		# follows the caller's locale, so an unpinned sort ordered the same
		# classes differently on a C-locale runner and the pin below failed
		# (measured 2026-09-21 — the first CI run, against a pin that had held
		# for weeks on the workstation's en_IN.UTF-8).
		find org -exec touch -t "$DERIVE_MTIME" {} +
		find org -type f | LC_ALL=C sort | zip -q -X -@ "$dir/$COMPAT_JAR"
	)
	# Exactly mapreduce/** and nothing else: a stray mapred/** entry would
	# reintroduce the hadoop 2.8.3 Configuration clash this jar exists to avoid.
	local stray
	stray="$(unzip -Z1 "$dir/$COMPAT_JAR" | grep -v '^org/apache/hadoop/mapreduce/' || true)"
	[ -z "$stray" ] || die "$COMPAT_JAR contains entries outside org/apache/hadoop/mapreduce/**"
	verify_one "$dir" "$COMPAT_JAR" "$COMPAT_SHA256" \
		|| die "$COMPAT_JAR derivation is not reproducible (pinned bytes differ) — check the unzip/zip versions"
	echo "  ok  $COMPAT_JAR (derived)"
}

case "${1:-}" in
--dest)
	[ "$#" -eq 2 ] || usage
	# Absolutise: derive_compat() cd's into a scratch dir, so a relative
	# --dest would stop resolving halfway through the run.
	mkdir -p "$2"
	dest="$(cd "$2" && pwd)"
	echo "Staging Flink runtime jars into $dest"
	for entry in "${PINS[@]}"; do
		name="${entry%%|*}"
		rest="${entry#*|}"
		want="${rest%%|*}"
		path="${rest#*|}"
		download_one "$dest" "$name" "$want" "$path"
	done
	derive_compat "$dest"
	echo "fetch-jars: staged $(( ${#PINS[@]} + 1 )) artifacts."
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
