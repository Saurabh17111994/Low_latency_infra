# Ingestion image — build context MUST be the Maven reactor root `code/`
# (docker build -f 02_services/01_ingestion/Dockerfile . from code/), so the
# parent POM, common module, and ingestion module are all present.

# Stage 1: Build Go arrow-bridge (self-contained; SDK vendored in-repo)
FROM golang:1.24-alpine AS go-builder
WORKDIR /src
COPY 02_services/01_ingestion/go-bridge/go.mod 02_services/01_ingestion/go-bridge/go.sum ./
COPY 02_services/01_ingestion/go-bridge/third_party ./third_party
# In-repo SDK (relative replace in go.mod) — self-contained, reproducible.
RUN go mod download
COPY 02_services/01_ingestion/go-bridge/*.go ./
# Generated proto package (market_data.pb.go + convert.go) — imported by
# batch.go; the top-level *.go glob above does not descend into subdirs.
COPY 02_services/01_ingestion/go-bridge/marketdata ./marketdata
RUN CGO_ENABLED=0 go build -o /arrow-bridge .

# Stage 2: Build Java ingestion (full reactor: parent + common + ingestion)
FROM maven:3.9-eclipse-temurin-17 AS java-builder
WORKDIR /src
COPY pom.xml .
COPY common ./common
COPY 02_services/01_ingestion/pom.xml ./02_services/01_ingestion/pom.xml
COPY 02_services/06_execution_gateway/pom.xml ./02_services/06_execution_gateway/pom.xml
# Download deps first for layer caching
RUN mvn -q dependency:go-offline 2>/dev/null || true
COPY 02_services/01_ingestion/src ./02_services/01_ingestion/src
RUN mvn -q -DskipTests package -pl 02_services/01_ingestion -am

# Stage 3: Runtime
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=go-builder /arrow-bridge /app/arrow-bridge
COPY --from=java-builder /src/02_services/01_ingestion/target/ingestion.jar /app/ingestion.jar
COPY 02_services/01_ingestion/docker-entrypoint.sh /app/docker-entrypoint.sh
# 2026-08-22 single-pane: OTel Javaagent for JVM metrics (jvm.*) → collector.
# Agent is downloaded at build so image stays self-contained; same endpoint as gateway.
ADD --chmod=0444 https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.9.0/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar
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
ENTRYPOINT ["/app/docker-entrypoint.sh"]
