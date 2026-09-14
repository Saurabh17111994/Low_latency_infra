# -----------------------------------------------------------------------------------------------
# Nautilus execution service — Rust container (T8.4)
#
# Both base images are digest-pinned (`@sha256:`), not just version-tagged: a tag can be moved
# to different content by the registry, a digest cannot. The same refs are recorded in
# code/01_platform/04_scripts/versions.pin so the pin discipline has one source of record (P3-413).
#
# Multi-stage: pinned toolchain builder -> slim runtime.
#
# Env-gates (must hold at build time):
#   * Base image `rust:1.97.1-bookworm` must resolve (or be mirrored) — toolchain matches
#     in versions.pin.
#   * The Nautilus git dependencies are pinned by revision in Cargo.toml/Cargo.lock and are
#     fetched at build time (network + git). This does NOT copy a developer working tree or use a
#     floating branch — the rev is fixed to 74d57e7e055015a8d974a0cf047ac2e5139610b3.
#
# The runtime image boots the service HALTED (EXECUTION_ENABLED=false is the only accepted boot
# state); live execution additionally requires the gateway/Fluss bridge + broker reachability.
# -----------------------------------------------------------------------------------------------

# --- Builder: pinned toolchain ----------------------------------------------------------------
# `-bookworm` names the Debian release the toolchain links against. The runtime stage is
# debian:bookworm-slim, so an unsuffixed rolling `rust:1.97.1` could move the builder to a newer
# glibc than the runtime ships and break the binary at boot (P3-176).
FROM rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 AS builder
WORKDIR /build

# Dependencies are cached across builds by BuildKit cache mounts rather than by a
# dummy-source layer: the registry/git mounts keep the downloads, and the target
# mount keeps the compiled dependency rlibs, so a source-only rebuild recompiles
# just the service crate. The binary is copied OUT of the mounted target into the
# layer, because cache-mount contents are not part of the image.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin nautilus-execution-service \
 && cp /build/target/release/nautilus-execution-service /build/nautilus-execution-service

# --- Runtime: slim, non-root, fail-closed -----------------------------------------------------
FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171 AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/nautilus-execution-service /usr/local/bin/nautilus-execution-service

# Fail closed by default: the service always boots HALTED and is never trading-ready.
ENV EXECUTION_ENABLED=false \
    LOG_LEVEL=info

# Non-root execution.
USER nobody

EXPOSE 8787

ENTRYPOINT ["nautilus-execution-service"]
