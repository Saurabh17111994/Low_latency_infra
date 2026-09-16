#!/bin/sh
# fluss-r2-secrets-from-file.sh - bridge Swarm file-secrets into the environment.
#
# WHY THIS EXISTS
# docker-stack.yml delivers the R2 credentials as Swarm secrets, which appear
# inside the container as FILES under /run/secrets/ and are named by the
# AWS_ACCESS_KEY_ID_FILE / AWS_SECRET_ACCESS_KEY_FILE variables. The Fluss
# server.yaml, however, reads the values from the process environment via the
# $${env.AWS_ACCESS_KEY_ID} / $${env.AWS_SECRET_ACCESS_KEY} placeholders (the
# FLUSS_PROPERTIES s3.access-key / s3.secret-key lines). The stock Fluss image
# has no *_FILE handling, so something must read each file and export the
# variable before the JVM starts. That is all this script does.
#
# DASH CAVEAT
# A failed command substitution inside a prefix assignment does NOT abort the
# script even under `set -e` - `export VAR=$(failing)` still returns 0 because
# the assignment's own exit status wins. So the value is read into a plain
# variable first, checked explicitly (readable AND non-empty), and only then
# exported. Inline-exporting a substitution would silently bridge garbage.
#
# FAIL CLOSED
# A *_FILE variable that is set but unreadable or empty aborts startup instead
# of starting a server that cannot tier to R2. An unset variable is not an
# error here: dev compose passes the credentials directly as
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and this script leaves them alone.
#
# Runs under /bin/sh (dash) on purpose: it is the entrypoint of all four
# Fluss services, so it must survive the leanest shell in the image.

set -eu

bridge() {
	var="$1"
	file_var="${1}_FILE"
	# `:-` keeps this safe under `set -u` when the variable is absent.
	eval "file=\${$file_var:-}"
	[ -n "$file" ] || return 0
	if [ ! -r "$file" ]; then
		echo "fluss-r2-secrets: FATAL: $file_var is set to '$file' but the file is not readable." >&2
		echo "fluss-r2-secrets: FATAL: refusing to start a server that cannot tier to R2." >&2
		exit 1
	fi
	# Swarm secret files may carry a trailing newline; strip CR/LF so the
	# signature never includes whitespace.
	value=$(tr -d '\r\n' <"$file")
	if [ -z "$value" ]; then
		echo "fluss-r2-secrets: FATAL: $file_var points at '$file' but the file is empty." >&2
		echo "fluss-r2-secrets: FATAL: refusing to start a server that cannot tier to R2." >&2
		exit 1
	fi
	export "$var=$value"
}

bridge AWS_ACCESS_KEY_ID
bridge AWS_SECRET_ACCESS_KEY

# Hand off to the stock Fluss entrypoint with the container's own command
# (coordinatorServer / tabletServer), preserving the original dispatch.
exec /docker-entrypoint.sh "$@"
