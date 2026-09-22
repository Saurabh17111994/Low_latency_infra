# Flink runtime image for the production Swarm stack.
#
# WHY THIS IMAGE EXISTS
# Three facts make the stock flink image unusable here, each verified in-container
# (2026-09-16) and each fatal on its own:
#   1. docker-stack.yml cannot `build:` — the image must be pre-built and pinned.
#   2. The stock image has no Fluss jars at all, so the job cannot load the
#      connector (`compute.jar` carries ZERO org/apache/fluss classes — they are
#      `provided` scope, supplied by the cluster).
#   3. Flink's own state backend must write checkpoints to an s3:// URI, but the
#      stock image neither enables flink-s3-fs-hadoop nor configures an endpoint.
#
# WHAT IT ADDS
#   * The Fluss connector + filesystem plugins in /opt/flink/lib (fetched and
#     SHA256-verified by fetch-jars.sh — never taken from a working tree).
#   * /etc/hadoop/conf/core-site.xml, which Flink's bin/config.sh auto-detects and
#     puts on the JVM classpath with zero settings, pointing both Flink's s3://
#     state backend and the Fluss client at R2.
#   * ENABLE_BUILT_IN_PLUGINS so Flink's bundled flink-s3-fs-hadoop is linked from
#     /opt/flink/opt into /opt/flink/plugins (verified: the stock image ships the
#     jar but does not enable it).
#   * An entrypoint wrapper bridging Swarm's *_FILE secrets into the environment
#     core-site.xml expands.
#
# BUILD
#   code/01_platform/01_docker/flink-runtime/fetch-jars.sh --dest <ctx>/jars
#   DOCKER_BUILDKIT=1 docker build -t <repo>:<tag> <ctx>
# The build context is this directory plus a `jars/` tree produced by the fetch
# script. `make flink-image` does both, or see the README for the manual steps.
#
# P2-105: digest-pinned base — a tag-only FROM silently accepts an upstream
# rebuild. Bump deliberately, in lockstep with runtime.lock's FLINK_IMAGE.
FROM flink:2.2.1-scala_2.12-java17@sha256:1934ab4b984d0873ef46825b1140377eed34475904aa077062497712d6e259f8

# The base image runs as USER flink; escalate only for the COPYs that need
# root-owned paths, then drop back (the base's own USER flink is restored at
# the end so jobmanager/taskmanager still run unprivileged).
USER root

# The Fluss connector (client + tiering builder + 2.2 adapter), the tiering
# entrypoint, the Iceberg lake plugin, and the two filesystem plugins. All
# verified against pinned SHA256s by fetch-jars.sh before the build sees them.
COPY jars/fluss-flink-2.2-1.0.0.jar         /opt/flink/lib/
COPY jars/fluss-flink-tiering-1.0.0.jar     /opt/flink/lib/
COPY jars/fluss-lake-iceberg-1.0.0.jar      /opt/flink/lib/
COPY jars/fluss-fs-s3-1.0.0.jar             /opt/flink/lib/
COPY jars/fluss-fs-hdfs-1.0.0.jar           /opt/flink/lib/
# M-15/M-16: iceberg's shaded parquet write path lazily loads the un-shaded
# org.apache.hadoop.mapreduce.lib.input.FileInputFormat. This jar is exactly
# that package — never the full hadoop-mapreduce-client-core, whose
# org/apache/hadoop/mapred/** would clash with the hadoop 3.3.x Configuration
# bundled in the fluss-fs jars (NoSuchMethodError getTimeDuration).
COPY jars/hadoop-mapreduce-compat-2.8.5.jar            /opt/flink/lib/

# Flink's config.sh auto-detects /etc/hadoop/conf and appends it to the JVM
# classpath, so this file needs no environment variable to take effect.
COPY core-site.xml /etc/hadoop/conf/core-site.xml

# Bridges the Swarm *_FILE secrets into the environment before Flink starts.
COPY --chmod=0555 20-r2-secrets-from-file.sh /opt/flink/bin/20-r2-secrets-from-file.sh

# Verified against the stock image: the jar ships in /opt/flink/opt but is NOT
# linked into plugins/, so s3:// has no FileSystemFactory until this is set.
# The entrypoint performs the link on first start.
ENV ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.2.1.jar

# Back to the least-privilege user the base image defines (uid 9999).
USER flink

# Replace the base entrypoint with the secret bridge, which execs the original
# with our arguments.
ENTRYPOINT ["/opt/flink/bin/20-r2-secrets-from-file.sh"]
# Docker clears CMD when ENTRYPOINT is set in a Dockerfile, so the base image's
# own default ("help") is lost and a container started with no command would
# exit 0 having printed nothing. Re-declare it so the usage message survives;
# docker-stack.yml passes jobmanager/taskmanager explicitly and overrides this.
CMD ["help"]
