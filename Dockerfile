# Static glibc binary for Cascade Lake (the scorer's CPU). Scratch starts faster
# than a distro image. musl was slower on the allocator-heavy path; this is
# glibc linked with crt-static, not alpine/musl.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
ENV RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static"
RUN cargo build --release --locked \
 && cp target/release/shopsess /shopsess \
 && chmod 0755 /shopsess

FROM scratch
COPY --from=build /shopsess /shopsess
ENV HOME=/scratch \
    DATA_DIR=/data \
    OUTPUT_DIR=/output \
    SCRATCH_DIR=/scratch \
    MIMALLOC_ALLOW_LARGE_OS_PAGES=1
CMD ["/shopsess"]
