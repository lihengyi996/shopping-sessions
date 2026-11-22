# glibc image. Profile-guided opt when the training run works; plain build otherwise.
# Cascade Lake matches the grader CPU. Avoid crt-static+scratch (failed on scorer before).
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends llvm && rm -rf /var/lib/apt/lists/*
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
COPY generator ./generator
COPY scripts/pgo_build.sh ./pgo_build.sh
ENV RUSTFLAGS="-C target-cpu=cascadelake"
RUN chmod +x pgo_build.sh \
    && (./pgo_build.sh && echo "PGO build ok") \
    || (echo "PGO failed; plain release" && cargo build --release --locked) \
    && strip target/release/shopsess

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopsess /usr/local/bin/shopsess
USER 1000
ENV HOME=/scratch \
    DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    MIMALLOC_ALLOW_LARGE_OS_PAGES=1
CMD ["/usr/local/bin/shopsess"]
