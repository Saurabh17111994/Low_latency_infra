# BuildKit cache mounts on the go/maven layers (2026-09-12): the module, build and
# local-repo caches are cross-build caches, not image inputs -- the runtime stage
# copies only /arrow-bridge and ingestion.jar out of the builder stages.
# Ingestion image — build context MUST be the Maven reactor root `code/`
# (docker build -f 02_services/01_ingestion/Dockerfile . from code/), so the
# parent POM, common module, and ingestion module are all present.

# Stage 1: Build Go arrow-bridge (self-contained; SDK vendored in-repo)
# P1-135: base pinned to manifest digest (resolved 2026-09-08 via
# digest-pin.sh / buildx imagetools inspect) — a bare tag floats.
FROM golang:1.24-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191 AS go-builder
WORKDIR /src
COPY 02_services/01_ingestion/go-bridge/go.mod 02_services/01_ingestion/go-bridge/go.sum ./
COPY 02_services/01_ingestion/go-bridge/third_party ./third_party
# In-repo SDK (relative replace in go.mod) — self-contained, reproducible.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build go mod download
COPY 02_services/01_ingestion/go-bridge/*.go ./
# Generated proto package (market_data.pb.go + convert.go) — imported by
# batch.go; the top-level *.go glob above does not descend into subdirs.
COPY 02_services/01_ingestion/go-bridge/marketdata ./marketdata
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -o /arrow-bridge .

# Stage 2: Build Java ingestion (full reactor: parent + common + ingestion)
# P1-135: pinned (2026-09-08, imagetools inspect).
FROM maven:3.9-eclipse-temurin-17@sha256:a8746f15d5bb26b5b8bacb056cc76211553850f4c71d16aff845cfa004cbc197 AS java-builder
WORKDIR /src
COPY pom.xml .
COPY common ./common
COPY 02_services/01_ingestion/pom.xml ./02_services/01_ingestion/pom.xml
COPY 02_services/06_execution_gateway/pom.xml ./02_services/06_execution_gateway/pom.xml
# Download deps first for layer caching
RUN --mount=type=cache,target=/root/.m2 mvn -q dependency:go-offline -pl 02_services/01_ingestion -am 2>/dev/null || true
COPY 02_services/01_ingestion/src ./02_services/01_ingestion/src
RUN --mount=type=cache,target=/root/.m2 mvn -q -DskipTests package -pl 02_services/01_ingestion -am

# Stage 3: Runtime
# P1-135: pinned (2026-09-08, imagetools inspect).
FROM eclipse-temurin:17-jre@sha256:13cc28a6cc72a38ce1f00c906be3580c1a3e604b8984d694f369a96742abc93b
WORKDIR /app
COPY --from=go-builder /arrow-bridge /app/arrow-bridge
COPY --from=java-builder /src/02_services/01_ingestion/target/ingestion.jar /app/ingestion.jar
COPY 02_services/01_ingestion/docker-entrypoint.sh /app/docker-entrypoint.sh
# 2026-08-22 single-pane: OTel Javaagent for JVM metrics (jvm.*) → collector.
# Agent is downloaded at build so image stays self-contained; same endpoint as gateway.
# P1-136: --checksum pins the agent bytes (sha256 computed 2026-09-08 by
# downloading v2.9.0 and hashing locally; upstream publishes no checksum
# file). A hijacked release fails the build instead of owning the fleet.
ADD --checksum=sha256:fa039f86082b559e53283a863e03de5df7db4bdb4840c5cbb8bf38f292987d5b --chmod=0444 https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.9.0/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar
ENV OTEL_SERVICE_NAME=ingestion
# 2026-09-07 live-proven: the javaagent defaults to OTLP/HTTP, so this
# MUST be :4318 (HTTP). :4317 is gRPC-only — the agent POSTs HTTP there
# and every export dies with 'unexpected end of stream' (no logs/metrics
# reach OpenObserve; verified live 2026-09-07, 12/12 records).
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
ENV OTEL_METRICS_EXPORTER=otlp
ENV OTEL_LOGS_EXPORTER=otlp
# Arrow instrument CSV mounted as volume at runtime (see docker-compose)
# COPY removed — CSV lives at Arrow_broker/instruments/cash_stocks/ on host
RUN chmod +x /app/arrow-bridge /app/docker-entrypoint.sh
# P1-137: drop root. Runtime writes to LOG_DIR (/data/ingestion/logs,
# named volume) AND the journal FILE /data/ingestion/uncertainty-journal.jsonl
# (UncertaintyJournal default) — so the PARENT /data/ingestion must be owned
# too, not just the logs child. Instruments mounts are read-only,
# /tmp/ingestion.ready is world-writable. chmod stays in the root build
# stage above.
# NOTE (existing volumes): a pre-P1-137 ingestion-logs volume created by a
# root-era run mounts root-owned content OVER this chown — first deploy
# after this change needs `docker compose down -v` (dev data only) or a
# one-time `chown -R 65532:65532` on the volume. The entrypoint probes
# below fail fast with the fix if the mount is still root-owned.
RUN groupadd -r -g 65532 appuser && useradd -r -g appuser -u 65532 -d /app -s /usr/sbin/nologin appuser \
    && mkdir -p /data/ingestion/logs && chown -R 65532:65532 /data/ingestion /app
USER 65532:65532
ENTRYPOINT ["/app/docker-entrypoint.sh"]
