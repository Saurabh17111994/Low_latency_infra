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

# Layer-cache the dependencies, then copy the real source and build:
#   * `cargo fetch --locked` runs in a plain layer (no cache mounts) and stores the complete lock
#     graph into the image, so the sources cargo needs are present and described by Cargo.lock.
#   * a stub `src/main.rs` then compiles the whole dependency closure in its own layer, which any
#     later change under src/ reuses.
# The target directory deliberately has NO cache mount: the mounts replace their target while the
# layer builds, and a target directory shared between builds can hold fingerprints from an
# earlier build while the artifacts they describe are gone, so cargo skips compiling a crate the
# current build needs. That is exactly what happened here — three builds that shared a target
# mount failed with `error[E0463]: can't find crate for `rstest`` while compiling
# `nautilus-model`, and every build without that mount (this tree, cold target and cold
# CARGO_HOME, on the host) compiled `rstest v0.26.1` first and succeeded.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
RUN cargo fetch --locked

# Stub-source dependency layer: `cargo build` compiles the dependency closure of the requested
# bin, and a stub main that references nothing still pulls the whole declared dependency set.
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs \
 && cargo build --release --locked --bin nautilus-execution-service

COPY src ./src
RUN cargo build --release --locked --bin nautilus-execution-service \
 && cp target/release/nautilus-execution-service ./nautilus-execution-service

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

# Non-root execution. `WORKDIR /app` is root-owned by default, so a non-root process has nowhere
# to write: the paper harness (src/t9paper) creates its output/audit directories with
# `create_dir_all`, and those runs would fail with EACCES in this image. /app is handed to the
# runtime user instead, and HOME is set because some runtime libraries look it up and `nobody`
# has no home directory (P3-414).
RUN chown -R nobody:nogroup /app
ENV HOME=/tmp
USER nobody

# The service binds 127.0.0.1:8787 by default (src/config.rs) and compose overrides that with
# EXECUTOR_LISTEN_ADDR=0.0.0.0:9190, which is the port NAUTILUS_PRIVATE_ENDPOINT points at. 8787
# is the bridge's port, so an unqualified `EXPOSE 8787` read as if the executor served it (P3-415).
EXPOSE 8787

ENTRYPOINT ["nautilus-execution-service"]
