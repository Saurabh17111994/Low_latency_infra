# Native Flink split: the image is the PLATFORM (Flink + launcher) only.
# The job jar is a separate artifact — built on the host (`mvn package`) and
# mounted at runtime (compose volume: target/compute.jar ->
# /opt/flink-jobs/compute.jar) or submitted via `make rollout-savepoint`.
# NEVER bake the jar or compile code inside this image: that couples every
# code change to an image rebuild (the anti-pattern this replaces).
# P2-105: digest-pinned base — tag-only FROM silently accepts upstream
# rebuilds/mutations and breaks reproducibility. Update policy: bump digest
# deliberately when the platform image must change (verify jq still present,
# flink user uid/gid unchanged); keep tag+digest in lockstep.
FROM flink:2.2.1-scala_2.12-java17@sha256:1934ab4b984d0873ef46825b1140377eed34475904aa077062497712d6e259f8
WORKDIR /opt/flink-jobs
# The base image already runs as USER flink — escalate for the package
# install, then pin the least-privilege user explicitly below (P2-106).
USER root
# P2-080/081/083/085: submit-jobs.sh parses REST JSON with jq — the base
# image ships curl but NOT jq (verified: no jq/python3 in
# flink:2.2.1-scala_2.12-java17), so install it here or every parse
# degrades to empty and the launcher FATALs. (Digest pin: P2-105.)
RUN apt-get update && apt-get install -y --no-install-recommends jq \
    && rm -rf /var/lib/apt/lists/*
# P2-107: context-relative COPY — build with the service dir as context
# (compose sets build.context=02_services/02_compute). The old repo-root
# relative path broke every build that used the service dir as context.
COPY --chmod=0555 submit-jobs.sh /opt/flink-jobs/submit-jobs.sh
# The compute.jar is NOT copied here — it is mounted by compose (volume) or
# supplied by rollout-savepoint at submit time. The launcher reads it from
# /opt/flink-jobs/compute.jar (the mount point); the compose volume makes the
# host-built jar available without an image rebuild.
# P2-108: ENTRYPOINT so `docker run <img> <cmd>` cannot silently override
# submission; the script's own -r mount check gives the fail-fast message.
# P2-106: least-privilege — base default is root; run the launcher as flink
# (uid 9999 in the image; the 0555 script is world-executable).
USER flink
ENTRYPOINT ["/opt/flink-jobs/submit-jobs.sh"]
