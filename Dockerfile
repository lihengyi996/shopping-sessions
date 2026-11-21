# Native binary for the hot path. DuckDB only writes the Parquet tables.
# The scorer builds this on linux/amd64 and runs it as uid 1000.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
RUN cargo build --release

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=build /src/target/release/shopsess /usr/local/bin/shopsess
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /usr/local/bin/shopsess
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIPELINE_BIN=/usr/local/bin/shopsess
WORKDIR /app
CMD ["/app/run.sh"]
