# Compile stage
FROM rust:1.78.0 as builder
# Set workdir in builder
WORKDIR /usr/src/app

COPY . .
RUN cargo build --release 

# Runtime stage
FROM debian:bookworm-slim
# Set logging
ENV RUST_LOG=info
# Install runtime dependencies for OpenSSL and Postgres
RUN apt-get update && \
    apt-get install -y libssl-dev libpq5 curl && \
    rm -rf /var/lib/apt/lists/*
# Copy in the backend binary
COPY --from=builder /usr/src/app/target/release/backend /usr/local/bin
EXPOSE 3000
CMD ["backend"]