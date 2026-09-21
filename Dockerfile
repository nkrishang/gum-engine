# syntax=docker/dockerfile:1
# Multi-stage build with cargo-chef so dependency compilation is cached as its own layer.
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# Line-table debug info (kept on for local profiling) triples the binary; production ships without it.
# Set before `cook` so dependencies and the final build share one profile and the cache layer is reused.
ENV CARGO_PROFILE_RELEASE_DEBUG=0
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p gum-engine
COPY . .
RUN cargo build --release -p gum-engine

# The runtime MUST be the same Debian release as the builder above (the cargo-chef image tracks Debian
# stable, currently 13 "trixie"). An older runtime has an older glibc than the binary was linked against
# and the process dies at startup with "GLIBC_2.xx not found".
FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 gum
WORKDIR /app
COPY --from=builder /app/target/release/gum-engine /usr/local/bin/gum-engine
COPY config ./config
USER gum
ENV GUM_CONFIG=/app/config/production.toml
# SIGTERM starts a graceful drain; see railway.toml for the matching drainingSeconds.
CMD ["gum-engine"]
