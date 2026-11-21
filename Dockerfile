# Static musl binary: container start is just exec of one file.
FROM rust:1.85-alpine AS build
RUN apk add --no-cache musl-dev gcc
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM scratch
COPY --from=build /src/target/release/shopsess /shopsess
USER 1000:1000
ENV DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    MIMALLOC_ALLOW_LARGE_OS_PAGES=1 \
    MIMALLOC_ARENA_EAGER_COMMIT=1
CMD ["/shopsess"]
