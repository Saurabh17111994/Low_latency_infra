#!/usr/bin/env bash
# 20-r2-secrets-from-file.sh — bridge Swarm file-secrets into the environment.
#
# WHY THIS EXISTS
# docker-stack.yml delivers the R2 credentials as Swarm secrets, which appear
# inside the container as FILES under /run/secrets/ and are named by the
# AWS_ACCESS_KEY_ID_FILE / AWS_SECRET_ACCESS_KEY_FILE variables. /etc/hadoop/conf/
# core-site.xml in this image, however, expands ${env.AWS_ACCESS_KEY_ID} and
# ${env.AWS_SECRET_ACCESS_KEY} — Hadoop can only read the environment, not a
# file path. Flink's own entrypoint has no *_FILE handling (verified against
# flink:2.2.1-scala_2.12-java17), so something has to read the file and export
# the variable before the JVM starts. That is all this script does.
#
# WHY ENV RATHER THAN A JCEKS CREDENTIAL STORE
# A credential store would keep the secrets out of the process environment,
# but it needs a provider path wired into core-site.xml plus a store password
# delivered as yet another secret — more moving parts for a container that is
# already the only reader. The environment is the mechanism Hadoop's own
# EnvironmentVariableCredentialsProvider is designed for.
#
# FAIL CLOSED
# A *_FILE variable that is set but unreadable aborts startup instead of
# starting a Flink cluster that cannot write its checkpoints. An unset
# variable is not an error here: local docker-compose runs pass the
# credentials directly as AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and this
# script leaves them alone.

set -euo pipefail

bridge() {
	local var="$1" file_var="${1}_FILE" file
	# `:-` keeps this safe under `set -u` when the variable is absent.
	file="${!file_var:-}"
	[ -n "$file" ] || return 0
	if [ ! -r "$file" ]; then
		echo "r2-secrets: $file_var is set to '$file' but the file is not readable." >&2
		echo "r2-secrets: refusing to start Flink with credentials it cannot load." >&2
		exit 1
	fi
	# Swarm secret files may carry a trailing newline; strip CR/LF so the
	# signature never includes whitespace.
	export "$var=$(tr -d '\r\n' <"$file")"
}

bridge AWS_ACCESS_KEY_ID
bridge AWS_SECRET_ACCESS_KEY

# Hand off to the base image's entrypoint with the container's own command
# (jobmanager / taskmanager), preserving the original dispatch.
exec /docker-entrypoint.sh "$@"
