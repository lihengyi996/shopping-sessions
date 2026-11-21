# glibc build. musl was several times slower on the allocator-heavy pass.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
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
