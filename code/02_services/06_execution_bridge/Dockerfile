# Build context MUST be the Maven/Java reactor root `code/`.
# Pinned digest from versions.pin (T0, foundation L548). The tag alone is
# floating; the digest makes the build reproducible and auditable.
ARG GO_BUILDER_IMAGE=golang:1.24.5-alpine@sha256:daae04ebad0c21149979cd8e9db38f565ecefd8547cf4a591240dc1972cf1399
FROM ${GO_BUILDER_IMAGE} AS go-builder
WORKDIR /src

# Keep the relative SDK replacement used by the standalone module valid in
# the image. No Arrow credentials are copied into the image.
COPY 02_services/06_execution_bridge/go-bridge/go.mod 02_services/06_execution_bridge/go-bridge/go.sum ./02_services/06_execution_bridge/go-bridge/
COPY 02_services/01_ingestion/go-bridge/third_party ./02_services/01_ingestion/go-bridge/third_party
WORKDIR /src/02_services/06_execution_bridge/go-bridge
RUN go mod download
COPY 02_services/06_execution_bridge/go-bridge/*.go ./
# TARGETARCH is set by BuildKit for the platform being built; the explicit default keeps the
# legacy builder (which does not set it) on today's amd64 behaviour instead of silently emitting
# an empty GOARCH. The hardcoded amd64 produced an x86_64 binary on every builder, including
# arm64 ones (P3-471).
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /execution-bridge .

FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
RUN addgroup -S execution && adduser -S -G execution execution
WORKDIR /app
COPY --from=go-builder /execution-bridge /app/execution-bridge
USER execution
ENV EXECUTION_BRIDGE_LISTEN_ADDR=0.0.0.0:8787
EXPOSE 8787
ENTRYPOINT ["/app/execution-bridge"]
