# Compile stage
FROM rust:1.71.0 as builder
# Set workdir in builder
WORKDIR /usr/src/app

# Dependency compile step (broken out for caching)
# Copy Cargo.toml and Cargo.lock
COPY Cargo.toml Cargo.lock ./
# Create dummy files just to fulfill lib and bin crates
RUN echo "fn main() {}" > main.rs
RUN mkdir lib && echo "pub fn lib() {}" > lib/lib.rs
# This is just to cache dependencies
RUN cargo build --release 
# Remove the built files, they are outdated now
# RUN rm -rf target/release/*
RUN rm main.rs && rm -rf lib
COPY . .
RUN apt-get update && \
    apt-get install -y protobuf-compiler
RUN cargo build --release 

# Runtime stage
FROM debian:bullseye
# Set logging
ENV RUST_LOG=info
# Install runtime dependencies for OpenSSL and Postgres
RUN apt-get update && \
    apt-get install -y libssl1.1 libpq5 protobuf-compiler curl && \
    rm -rf /var/lib/apt/lists/*
# Copy in the backend binary
COPY --from=builder /usr/src/app/target/release/backend /usr/local/bin
EXPOSE 3000
CMD ["backend"]