#!/bin/sh
# Kept for local debugging; the scorer image runs /shopsess directly.
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
exec /shopsess
