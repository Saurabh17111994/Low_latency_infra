# syntax=docker/dockerfile:1
# =============================================================================
# ddl-apply — the DDL application contract engine as a runnable image.
#
# Packages everything the 9-step contract needs so it runs INSIDE the compose
# network (one-shot `docker compose run --rm ddl-apply ...`): the Java engine
# (com.trading.common.schema.ddl.DdlApplyTool + CompositeKeyMatrixVerifier +
# DdlText + SchemaManifest), the Python orchestrator (ddl_apply.py), the
# exit-code smoke (ddl_apply_smoke.py), the pinned DDL corpus + manifest, and
# the exact 5 runtime jars in the ~/.m2 layout ddl_apply.py's
# build_tool_classpath expects. FLUSS_BOOTSTRAP resolves via compose DNS
# (fluss-coordinator:9123) — no host /etc/hosts aliases.
#
# The image mirrors the repo-relative layout ddl_apply.py derives its paths
# from (REPO_ROOT = script dir/../../.. -> /app), so the scripts run
# UNCHANGED: no code edits, only packaging.
#
# Build:   make ddl-image
# Run:     docker compose -f code/01_platform/01_docker/docker-compose.yml \
#            run --rm ddl-apply {validate|apply|smoke|self-test} [args...]
# =============================================================================

# ── Stage 1: build the Java engine (online maven — pinned versions in
#    code/pom.xml; matches the repo's R-143 online default) ──────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /src
# Parent POM + module POMs first so dependency resolution is layer-cached.
# The ingestion pom must be present too: the parent declares it as a reactor
# module, and Maven builds the full reactor model even for `-pl common`.
COPY code/pom.xml /src/code/pom.xml
COPY code/common/pom.xml /src/code/common/pom.xml
COPY code/02_services/01_ingestion/pom.xml /src/code/02_services/01_ingestion/pom.xml
COPY code/02_services/06_execution_gateway/pom.xml /src/code/02_services/06_execution_gateway/pom.xml
# Best-effort dependency pre-fetch (layer cache). go-offline also resolves
# test-scoped transitives of fluss-client's pom, which flaky networks can
# truncate — tolerate that (P4-324: warn, never silent), since `package`
# below fetches exactly what common needs.
RUN cd code && (mvn -q -pl common dependency:go-offline -DskipTests || echo "WARN: go-offline pre-fetch incomplete — package below re-resolves (P4-324)")
# Engine sources.
COPY code/common /src/code/common
# P4-325: gate the shipped engine — DDL-contract unit tests only (all
# cluster-free: DdlApplyTool status/twin-sweep-unit/DdlText x2, matrix pin,
# manifest serialization). The two live halves (DdlSmokeTwinSweepTest,
# CompatFluss*IntegrationTest) stay out: they need FLUSS_BOOTSTRAP.
RUN cd code && mvn -q -pl common test -Dtest='DdlApplyToolStatusTest,DdlSmokeTwinSweepUnitTest,DdlTextFailFastTest,DdlTextPartitionTest,CompositeKeyMatrixVerifierTest,SchemaManifestSerializationTest' -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
RUN cd code && mvn -q -pl common package -DskipTests
# P4-158: jar-source guard — the five COPYs below hardcode
# fluss-client/jackson/slf4j versions that MUST match code/pom.xml
# (fluss.version) + ddl_apply.py (JACKSON_VERSION/SLF4J_VERSION) + versions.pin
# (FLUSS_VERSION). A bump anywhere without the others breaks the COPY with a
# confusing "file not found" — fail here with the real cause instead. The
# jars are already resolved in the builder m2, so this is a pure path assert.
ARG EXPECT_FLUSS=0.9.1-incubating
ARG EXPECT_JACKSON=2.16.1
ARG EXPECT_SLF4J=2.0.9
RUN POM_FLUSS="$(sed -n 's/.*<fluss.version>\([^<]*\)<\/fluss.version>.*/\1/p' /src/code/pom.xml)" \
 && POM_JACKSON="$(sed -n 's/.*<jackson.version>\([^<]*\)<\/jackson.version>.*/\1/p' /src/code/pom.xml)" \
 && POM_SLF4J="$(sed -n 's/.*<slf4j.version>\([^<]*\)<\/slf4j.version>.*/\1/p' /src/code/pom.xml)" \
 && [ -n "$POM_FLUSS" ] && [ "$POM_FLUSS" = "$EXPECT_FLUSS" ] \
 && [ -n "$POM_JACKSON" ] && [ "$POM_JACKSON" = "$EXPECT_JACKSON" ] \
 && [ -n "$POM_SLF4J" ] && [ "$POM_SLF4J" = "$EXPECT_SLF4J" ] \
 || { echo "P4-158 jar-source drift: Dockerfile EXPECT_* ($EXPECT_FLUSS/$EXPECT_JACKSON/$EXPECT_SLF4J) != code/pom.xml ($POM_FLUSS/$POM_JACKSON/$POM_SLF4J) — bump all four (Dockerfile ARGs, pom, ddl_apply.py, versions.pin) together"; exit 1; }

# ── Stage 2: runtime — Python 3.11 (official python:3.11-slim — currently
#    3.11.16 on Debian trixie) + JRE 21 headless (engine is precompiled; Java
#    17 bytecode runs on 21 — trixie has no openjdk-17, only 21/25). The
#    pinned series is **3.11** (versions.pin PYTHON_VERSION=3.11.9): the
#    previous temurin base's `apt-get python3` drifted to 3.14.4 (2026-08-24,
#    VM-PYTHON-002 DRIFT_RECORDED_PIN_UNSATISFIED). We deliberately use the
#    FLOATING 3.11-slim (newest 3.11 patch + patched OS) rather than the stale
#    3.11.9-slim: the 3.11.9-slim image's Debian bookworm OS layer carries
#    73 HIGH / 11 CRITICAL (2026-08-24 DB) vs 14 HIGH / 3 CRITICAL on 3.11-slim
#    (trixie, all latest-patch, fixed=None). Series is enforced at runtime by
#    `ddl-apply version`. The ENGINE runs as the non-root ddlapply user
#    (uid/gid 10001): the entrypoint wrapper repairs the evidence bind-mount
#    ownership as root, then setpriv-drops before any engine work. ─
FROM python:3.11-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && python3 --version \
    && java -version \
    && groupadd -g 10001 ddlapply \
    && useradd -u 10001 -g 10001 -d /nonexistent -s /usr/sbin/nologin ddlapply

# Compiled engine classes (target/classes — the classpath ddl_apply.py builds).
COPY --from=builder /src/code/common/target/classes /app/code/common/target/classes

# The exact runtime jars, in a fixed non-home Maven layout (DDL_APPLY_M2_REPO
# points build_tool_classpath at it — the non-root engine has no root home):
# fluss-client is the shaded 0.9.1 artifact; jackson-databind pulls core +
# annotations; slf4j-api is the 5th entry.
COPY --from=builder /root/.m2/repository/org/apache/fluss/fluss-client/0.9.1-incubating \
    /opt/ddl-apply/m2/repository/org/apache/fluss/fluss-client/0.9.1-incubating
COPY --from=builder /root/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.16.1 \
    /opt/ddl-apply/m2/repository/com/fasterxml/jackson/core/jackson-databind/2.16.1
COPY --from=builder /root/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.16.1 \
    /opt/ddl-apply/m2/repository/com/fasterxml/jackson/core/jackson-core/2.16.1
COPY --from=builder /root/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.16.1 \
    /opt/ddl-apply/m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.16.1
COPY --from=builder /root/.m2/repository/org/slf4j/slf4j-api/2.0.9 \
    /opt/ddl-apply/m2/repository/org/slf4j/slf4j-api/2.0.9

# Orchestrator + smoke + the non-root evidence-ownership gate + version pins.
COPY code/01_platform/04_scripts/ddl_apply.py \
     code/01_platform/04_scripts/ddl_apply_smoke.py \
     code/01_platform/04_scripts/evidence_ownership_check.py \
     code/01_platform/04_scripts/eod_controller.py \
     code/01_platform/04_scripts/versions.pin \
     /app/code/01_platform/04_scripts/
# The engine's own python unit tests only (self-test runs the whole shipped
# dir) — sibling host tooling tests (audit_r2, implementation_gate,
# change_control) are deliberately NOT packaged.
COPY code/01_platform/04_scripts/tests/test_ddl_apply.py \
     code/01_platform/04_scripts/tests/test_evidence_ownership_check.py \
     /app/code/01_platform/04_scripts/tests/

# The contract inputs: the pinned DDL corpus + the committed manifest.
COPY code/01_platform/02_sql/ddl /app/code/01_platform/02_sql/ddl

# COMPAT-FLUSS-005 capability evidence is RUNTIME-ONLY: the raw-client
# composite-PK evidence lives under logs/ (gitignored per R-051 — host paths,
# endpoints, account identifiers) and cannot be COPY'd into an image from a
# clean checkout. The smoke resolves it at runtime when present
# (ddl_apply_smoke.py REAL_EVIDENCE + isfile guard) and falls back to a
# placeholder otherwise — so no image COPY is needed here.

# Evidence output (host bind-mounts over this in compose): owned by the
# non-root engine user, setgid 2775 so records land group-writable (umask 002
# in the wrapper).
RUN mkdir -p /app/logs/ddl-apply \
    && chown 10001:10001 /app/logs/ddl-apply \
    && chmod 2775 /app/logs/ddl-apply

# Entrypoint: root wrapper (mount repair + setpriv drop) -> non-root runner
# (validate | apply | smoke | self-test).
COPY code/01_platform/01_docker/ddl-apply/ddl-apply-entrypoint.sh /usr/local/bin/ddl-apply
COPY code/01_platform/01_docker/ddl-apply/ddl-apply-run.sh /usr/local/bin/ddl-apply-run
RUN chmod +x /usr/local/bin/ddl-apply /usr/local/bin/ddl-apply-run

WORKDIR /app
ENV FLUSS_BOOTSTRAP=fluss-coordinator:9123 \
    DDL_APPLY_M2_REPO=/opt/ddl-apply/m2/repository \
    HOME=/app \
    PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["/usr/local/bin/ddl-apply"]
CMD ["validate"]
