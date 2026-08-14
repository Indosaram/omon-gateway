FROM rust:1.85-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY README.md LICENSE ./
RUN cargo build --locked --release --bin omon-gateway

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libopus0 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 omon \
    && useradd --system --uid 10001 --gid omon --home-dir /app --shell /usr/sbin/nologin omon \
    && mkdir -p /app/data /app/workspace \
    && chown -R omon:omon /app

WORKDIR /app
COPY --from=builder /build/target/release/omon-gateway /usr/local/bin/omon-gateway

ENV DATABASE_URL=sqlite:///app/data/omon_gateway.db \
    OMON_WORKSPACE_ROOT=/app/workspace \
    RUST_LOG=info

USER omon

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD test -r /proc/1/cmdline && grep -aq "omon-gateway" /proc/1/cmdline || exit 1

ENTRYPOINT ["/usr/local/bin/omon-gateway"]
