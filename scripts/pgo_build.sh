#!/bin/sh
# Instrument -> train on a small synthetic set -> rebuild with profile-use.
set -eu
rustup component add llvm-tools-preview >/dev/null
(cd generator && cargo build --release --quiet)
./generator/target/release/generate --seed 5 --events 1000000 --clicks 250000 --output /tmp/pgo-data
RUSTFLAGS="${RUSTFLAGS:-} -C profile-generate=/tmp/pgo-prof" cargo build --release --locked
mkdir -p /tmp/pgo-out
DATA_DIR=/tmp/pgo-data OUTPUT_DIR=/tmp/pgo-out NCPU=4 ./target/release/shopsess
PROFDATA="$(command -v llvm-profdata || true)"
if [ -z "$PROFDATA" ]; then
  PROFDATA="$(find /usr -name llvm-profdata -type f 2>/dev/null | head -1 || true)"
fi
test -n "$PROFDATA"
"$PROFDATA" merge -o /tmp/pgo.profdata /tmp/pgo-prof
RUSTFLAGS="${RUSTFLAGS:-} -C profile-use=/tmp/pgo.profdata -C llvm-args=-pgo-warn-missing-function" \
  cargo build --release --locked
