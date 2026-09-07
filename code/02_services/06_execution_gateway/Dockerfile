FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml /workspace/pom.xml
COPY common /workspace/common
COPY 02_services/01_ingestion /workspace/02_services/01_ingestion
COPY 02_services/06_execution_gateway/pom.xml /workspace/02_services/06_execution_gateway/pom.xml
RUN mvn -q -f /workspace/pom.xml -pl 02_services/06_execution_gateway -am dependency:go-offline -DskipTests || true
COPY 02_services/06_execution_gateway/src /workspace/02_services/06_execution_gateway/src
RUN mvn -q -f /workspace/pom.xml -pl 02_services/06_execution_gateway -am package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/02_services/06_execution_gateway/target/execution-gateway.jar /app/execution-gateway.jar
# 2026-08-22 single-pane: OTel Javaagent for JVM metrics (jvm.*) → collector:4318 (same as ingestion)
ADD --chmod=0444 https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.9.0/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar
ENV OTEL_SERVICE_NAME=execution-gateway
# 2026-09-07 live-proven (ingestion pass): the javaagent defaults to OTLP/HTTP,
# so this MUST be :4318. :4317 is gRPC-only and breaks every export with
# 'unexpected end of stream'. Verified 12/12 + 20/20 records on ingestion's rig.
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
ENV OTEL_METRICS_EXPORTER=otlp
ENV JAVA_TOOL_OPTIONS=-javaagent:/app/opentelemetry-javaagent.jar
USER 10001:10001
ENTRYPOINT ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar", "/app/execution-gateway.jar"]
