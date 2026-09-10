# Action-capture image — build context MUST be the Maven reactor root `code/`
# (docker build -f 02_services/03_action_capture/Dockerfile . from code/).
# Why: the module declares <relativePath>../../pom.xml</relativePath> and depends
# on the sibling com.trading:common module, so a service-dir context resolves
# NEITHER (verified: the former service-dir build died with
# UnresolvableModelException). Same reactor-root pattern as 01_ingestion.

# P2-244: digest-pinned base (digest shared with 02_services/01_ingestion) — a
# tag-only FROM silently accepts upstream rebuilds/mutations. Update policy: bump
# the digest deliberately, keeping tag+digest in lockstep.
FROM maven:3.9-eclipse-temurin-17@sha256:a8746f15d5bb26b5b8bacb056cc76211553850f4c71d16aff845cfa004cbc197 AS build
WORKDIR /src
COPY pom.xml .
COPY common ./common
# The parent POM lists every reactor module in <modules>, so their POMs must
# exist or Maven fails the reactor read — copy sibling POMs (no sources needed).
COPY 02_services/01_ingestion/pom.xml ./02_services/01_ingestion/pom.xml
COPY 02_services/06_execution_gateway/pom.xml ./02_services/06_execution_gateway/pom.xml
COPY 02_services/03_action_capture/pom.xml ./02_services/03_action_capture/pom.xml
# P2-245: resolve the dependency graph in its own layer — a src-only change
# reuses this layer instead of re-resolving the whole reactor. Standard
# `-pl ... -am` (reactor member since 2026-09-10, P2 8.4) — the module is in
# code/pom.xml <modules>, so it is selectable and -am builds `common`.
RUN mvn -q dependency:go-offline -pl 02_services/03_action_capture -am 2>/dev/null || true
COPY 02_services/03_action_capture/src ./02_services/03_action_capture/src
# P2-202: the module's OWN unit tests RUN here (32) — no -DskipTests, so the
# artifact baked into this image is test-verified. common's tests are excluded:
# they are repo-layout-coupled (read code/01_platform/02_sql/ddl/*.sql and scan
# cross-module sources; 550 pass in the host root build) and cannot run inside a
# per-service image without shipping the whole repo.
RUN mvn -q package -pl 02_services/03_action_capture -am \
    -Dtest=com.trading.capture.** -Dsurefire.failIfNoSpecifiedTests=false

# P2-244: digest-pinned runtime base (same digest as 01_ingestion).
FROM eclipse-temurin:17-jre@sha256:13cc28a6cc72a38ce1f00c906be3580c1a3e604b8984d694f369a96742abc93b
WORKDIR /app
COPY --from=build /src/02_services/03_action_capture/target/action-capture.jar /app/action-capture.jar
COPY 02_services/03_action_capture/docker-entrypoint.sh /app/docker-entrypoint.sh
# P2-203: drop root. The service writes NO files at runtime (logs to stdout), so
# /app can stay root-owned and read-only for the app — no chown needed; the COPY'd
# jar is world-readable and the entrypoint is made world-executable here, as root.
RUN groupadd -r -g 10001 app && useradd -r -g app -u 10001 -d /app -s /usr/sbin/nologin app \
    && chmod +x /app/docker-entrypoint.sh
USER 10001:10001
ENTRYPOINT ["/app/docker-entrypoint.sh"]
