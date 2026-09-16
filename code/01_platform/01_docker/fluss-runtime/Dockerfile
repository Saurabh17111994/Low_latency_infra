# Fluss runtime image for the production Swarm stack.
#
# WHY THIS IMAGE EXISTS
# The production stack cannot `build:` (a Swarm stack has no build step), so
# every service runs a pre-built, digest-pinned image. For the Fluss servers
# that image was the stock `apache/fluss` — which has no hadoop-aws/S3A inside
# its iceberg plugin directory, so lake tiering has no filesystem to write to.
#
# The dev compose appears to work because it bind-mounts two jars from
# fluss-plugins/ into /opt/fluss/plugins/iceberg/. Those files are gitignored
# host binaries: they are NOT part of the repository, so a fresh clone on the
# production VM does not have them and a Swarm stack cannot mount them
# portably anyway. Verified 2026-09-16: `git ls-files
# code/01_platform/01_docker/fluss-plugins/` returns exactly one file, the
# README. Everything else in that tree is untracked.
#
# WHAT IT ADDS
#   * fluss-fs-s3 + fluss-fs-hdfs copied into /opt/fluss/plugins/iceberg/, so
#     the iceberg plugin's own classloader can resolve `s3://` (S3AFileSystem,
#     via hadoop-aws) and `hdfs://` (HdfsConfiguration). The stock image ships
#     both jars, but each in its OWN plugin directory (plugins/s3, plugins/hdfs)
#     — separate plugin classloaders, so the iceberg one cannot see them.
#     This is what the dev compose's bind mounts were doing.
#
# WHAT IT DELIBERATELY DOES NOT ADD
#   * No secrets bridge. The production stack already delivers
#     fluss-r2-secrets-from-file.sh as a Swarm config mounted at
#     /opt/fluss/bin/, with the entrypoint set to run it. A Swarm config is
#     fine for a script; it is not a way to carry jars, which is why the jars
#     belong in the image instead.
#   * No hadoop-mapreduce-compat jar. That one is for iceberg's parquet WRITE
#     path, which runs in the Flink tiering job — not in the Fluss servers.
#     Fluss 0.9 tiers from an external Flink job (see tiering-start.sh); the
#     servers only coordinate. The dev tablet carries no compat jar and lake
#     tiering works there. The Flink image ships it (flink-runtime/Dockerfile).
#   * No fluss-fs-hadoop-shaded SNAPSHOT. Unpublished (404 on Maven Central and
#     on the Apache snapshot repo), therefore unpinnable. Its entries are all
#     present in the two jars above. See fetch-jars.sh.
#
# BUILD
#   code/01_platform/01_docker/fluss-runtime/fetch-jars.sh --dest <ctx>/jars
#   DOCKER_BUILDKIT=1 docker build -t <repo>:<tag> <ctx>
# The build context is this directory plus a `jars/` tree produced by the fetch
# script. `make fluss-image` does both, or see the README for manual steps.
#
# P2-105: digest-pinned base — a tag-only FROM silently accepts an upstream
# rebuild. Keep this in lockstep with runtime.lock's FLUSS_IMAGE.
FROM apache/fluss:0.9.1-incubating@sha256:65f5513b33dde10ace4f8adb3956f17226a2a1e2663f92b3096e4769b0ee1d1c

# The stock image defines no USER, so it runs as root and the plugin directory
# (/opt/fluss/plugins/iceberg, owned by fluss:fluss) is writable. No USER
# directive is added here: the servers already run as root in this deployment,
# and changing that is not this image's job.

# Both jars land in the ICEBERG plugin directory on purpose. Copying them into
# plugins/s3 and plugins/hdfs — where identically named files already sit —
# would not help: Fluss loads each plugin directory as a separate classloader,
# so the iceberg plugin only sees its own. Verified against the running stock
# image: `Plugin loader with ID not found, creating it: iceberg` shows the
# server creating that loader from this directory alone.
COPY jars/fluss-fs-s3-0.9.1-incubating.jar   /opt/fluss/plugins/iceberg/
COPY jars/fluss-fs-hdfs-0.9.1-incubating.jar /opt/fluss/plugins/iceberg/
