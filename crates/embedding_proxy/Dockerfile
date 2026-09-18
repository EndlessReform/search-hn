# Build with the crate directory as context. publish.sh supplies its standalone lockfile.
FROM rust:1.93.0-trixie@sha256:bbde3ca426faeebab3eb58b8005d3d6da70607817a14bb92a55c94611d4d905f AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docs ./docs
COPY fixtures ./fixtures
RUN cargo test --locked && cargo build --release --locked && strip target/release/embedding_proxy

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/embedding_proxy /usr/local/bin/embedding_proxy
USER 65532:65532
ENV RUST_LOG=info
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/embedding_proxy"]
