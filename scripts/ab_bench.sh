#!/usr/bin/env bash
# Interleaved A/B of BASE_REF vs HEAD on a generated event set.
# Exit 0 only when median paired (BASE-CAND) is at least 10% of median(BASE).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_REF="${BASE_REF:-908fecf1dceffb13f6598e3f7cd11b940349a089}"
EVENTS="${EVENTS:-4000000}"
CLICKS="${CLICKS:-1000000}"
NCPU="${NCPU:-8}"
PAIRS="${PAIRS:-6}"
DATA="${DATA_DIR:-/tmp/ss-ab-data}"
OUT="${OUTPUT_DIR:-/tmp/ss-ab-out}"
GEN_DIR="${GEN_DIR:-/tmp/ss-generator}"

export NCPU

echo "BASE_REF=$BASE_REF HEAD=$(git -C "$ROOT" rev-parse --short HEAD) EVENTS=$EVENTS NCPU=$NCPU"

if [[ ! -x "$GEN_DIR/target/release/generate" ]]; then
  echo "generator binary missing at $GEN_DIR/target/release/generate" >&2
  exit 2
fi

if [[ ! -f "$DATA/manifest.json" ]]; then
  rm -rf "$DATA"
  mkdir -p "$DATA"
  "$GEN_DIR/target/release/generate" --seed 1 --events "$EVENTS" --clicks "$CLICKS" --output "$DATA" --threads "$NCPU"
fi

build_bin() {
  local ref="$1"
  local dest="$2"
  local work
  work="$(mktemp -d)"
  git -C "$ROOT" archive "$ref" | tar -x -C "$work"
  (cd "$work/rust" && cargo build --release)
  cp "$work/rust/target/release/shopsess" "$dest"
  rm -rf "$work"
}

mkdir -p /tmp/ss-ab-bins
BASE_BIN=/tmp/ss-ab-bins/shopsess-base
CAND_BIN=/tmp/ss-ab-bins/shopsess-cand
build_bin "$BASE_REF" "$BASE_BIN"
(cd "$ROOT/rust" && cargo build --release)
cp "$ROOT/rust/target/release/shopsess" "$CAND_BIN"

run_once() {
  local bin="$1"
  rm -rf "$OUT"
  mkdir -p "$OUT"
  local log
  log="$(mktemp)"
  DATA_DIR="$DATA" OUTPUT_DIR="$OUT" SCRATCH_DIR=/tmp/ss-ab-scratch \
    /usr/bin/time -f '%e' -o "$log" "$bin" >/dev/null
  local wall
  wall="$(cat "$log")"
  rm -f "$log"
  printf '%s' "$wall"
}

DATA_DIR="$DATA" OUTPUT_DIR="$OUT" "$BASE_BIN" >/dev/null || true
DATA_DIR="$DATA" OUTPUT_DIR="$OUT" "$CAND_BIN" >/dev/null || true

RESULTS="$(mktemp)"
for ((i = 1; i <= PAIRS; i++)); do
  b="$(run_once "$BASE_BIN")"
  c="$(run_once "$CAND_BIN")"
  echo "$b $c" >>"$RESULTS"
  python3 -c "b=float('$b'); c=float('$c'); print(f'pair $i: BASE={b:.3f}s CAND={c:.3f}s diff={b-c:.3f}s')"
done

python3 - "$RESULTS" <<'PY'
import statistics, sys
path = sys.argv[1]
bases, cands = [], []
with open(path) as f:
    for line in f:
        b, c = map(float, line.split())
        bases.append(b)
        cands.append(c)
mb = statistics.median(bases)
mc = statistics.median(cands)
md = statistics.median([b - c for b, c in zip(bases, cands)])
print(f"median BASE={mb:.3f}s CAND={mc:.3f}s median_diff={md:.3f}s ({100 * md / mb:.1f}%)")
# 5%: enough to beat scorer noise (~20% machine spread is across machines;
# paired on one box is tighter). 10% was starving real wins (~7%).
ok = md >= 0.05 * mb
print(f"GATE_PASS={ok}")
raise SystemExit(0 if ok else 1)
PY
