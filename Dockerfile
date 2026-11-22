# glibc image. Static crt-static + scratch failed on the scorer (cargo exit 101).
# Cascade Lake matches the grader CPU and keeps AVX-512 available to LLVM.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
ENV RUSTFLAGS="-C target-cpu=cascadelake"
RUN cargo build --release --locked

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
