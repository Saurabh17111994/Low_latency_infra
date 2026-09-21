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
# Configuration would clash with the 3.3.x one bundled in the fluss-fs jars
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
FLUSS_VERSION="0.9.1-incubating"
MAPREDUCE_VERSION="2.8.5"

# --- The pins. name|sha256|maven-relative-path -----------------------------
# Every entry is verified before use; --verify checks the same table, so a
# corrupted or substituted jar in an image layer is a hard failure.
PINS=(
	"fluss-flink-2.2-${FLUSS_VERSION}.jar|5dddeb4cb9f21cd79fa1419b1cd76e2352a22726b17e7f181fecbbe9c7cb2f5e|org/apache/fluss/fluss-flink-2.2/${FLUSS_VERSION}/fluss-flink-2.2-${FLUSS_VERSION}.jar"
	"fluss-flink-tiering-${FLUSS_VERSION}.jar|54c2f4125a74bacb304d182760017502225b283b3d7364baa1c783b8f014cb4e|org/apache/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar"
	"fluss-lake-iceberg-${FLUSS_VERSION}.jar|b9d8aa37a1a1a1eb14c1a365ae40e8114dcff26367cdc08156ae4f6d0d911d93|org/apache/fluss/fluss-lake-iceberg/${FLUSS_VERSION}/fluss-lake-iceberg-${FLUSS_VERSION}.jar"
	"fluss-fs-s3-${FLUSS_VERSION}.jar|9d85c2d83daa0ad5a7c3980162e96022154467f378b08333faa9d37beab682e3|org/apache/fluss/fluss-fs-s3/${FLUSS_VERSION}/fluss-fs-s3-${FLUSS_VERSION}.jar"
	"fluss-fs-hdfs-${FLUSS_VERSION}.jar|04825f7dcba5768eb555c1acfc3d9f71c2cc2c42d6ba1a5e6e22dea6785f0ed4|org/apache/fluss/fluss-fs-hdfs/${FLUSS_VERSION}/fluss-fs-hdfs-${FLUSS_VERSION}.jar"
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
