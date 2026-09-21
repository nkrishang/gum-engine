# syntax=docker/dockerfile:1
# Multi-stage build with cargo-chef so dependency compilation is cached as its own layer.
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json -p gum-engine
COPY . .
RUN cargo build --release -p gum-engine

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 gum
WORKDIR /app
COPY --from=builder /app/target/release/gum-engine /usr/local/bin/gum-engine
COPY config ./config
USER gum
ENV GUM_CONFIG=/app/config/production.toml
# SIGTERM starts a graceful drain; see railway.toml for the matching drainingSeconds.
CMD ["gum-engine"]
