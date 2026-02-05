# Build stage
FROM rust:latest AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy main for dependency caching
RUN mkdir -p src/bin benches && \
    echo "fn main() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/ingester.rs && \
    echo "fn main() {}" > src/bin/query.rs && \
    echo "fn main() {}" > src/bin/compactor.rs && \
    echo "fn main() {}" > src/bin/rebuild_metadata.rs && \
    echo "fn main() {}" > src/bin/backfill_levels.rs && \
    echo "fn main() {}" > src/bin/test_data_generator.rs && \
    echo "fn main() {}" > benches/write_throughput.rs && \
    echo "fn main() {}" > benches/query_latency.rs

# Build dependencies only
RUN cargo build --release && rm -rf src

# Copy actual source code
COPY src ./src
COPY tests ./tests
COPY benches ./benches

# Touch files to ensure rebuild
RUN touch src/lib.rs src/bin/*.rs

# Build release binaries
RUN cargo build --release --bins

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/target/release/cardinalsin-ingester /usr/local/bin/
COPY --from=builder /app/target/release/cardinalsin-query /usr/local/bin/
COPY --from=builder /app/target/release/cardinalsin-compactor /usr/local/bin/

# Create non-root user
RUN useradd -m -s /bin/bash cardinalsin
USER cardinalsin

# Default to query node
CMD ["cardinalsin-query"]
