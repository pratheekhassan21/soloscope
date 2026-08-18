#!/usr/bin/env bash
# Reproduces the three load experiments reported in FINDINGS.md.
#
#   ./scripts/run_experiments.sh [output-dir]
#
# Each experiment ingests a fresh slot range into a throwaway database, so the
# runs do not interfere with each other or with solscope.db.

set -uo pipefail

BIN=./target/release/solscope
OUT="${1:-experiments}"
START_SLOT="${START_SLOT:-439586292}"
SLOTS="${SLOTS:-300}"
PORT="${PORT:-8090}"

mkdir -p "$OUT"
command -v "$BIN" >/dev/null 2>&1 || [ -x "$BIN" ] || {
  echo "build first: cargo build --release" >&2
  exit 1
}

cleanup() {
  for pid in $(jobs -pr); do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

fresh_db() { rm -f "$1" "$1-wal" "$1-shm"; }

# ---------------------------------------------------------------------------
echo "=== Experiment 1: backpressure (writer paused for 10s mid-ingest) ==="
# ---------------------------------------------------------------------------
DB="$OUT/backpressure.db"; fresh_db "$DB"
$BIN --slots "$SLOTS" --start-slot "$START_SLOT" --db "$DB" --port "$PORT" \
     --debug-endpoints --exit-after-ingest > "$OUT/backpressure.log" 2>&1 &
INGEST_PID=$!

sleep 8
python3 scripts/sample_pipeline.py --url "http://localhost:$PORT/api/status" \
        --seconds 90 > "$OUT/backpressure.tsv" &
SAMPLER=$!

sleep 12
echo "  pausing writer for 10s..."
curl -s -X POST "http://localhost:$PORT/api/debug/pause-writer?secs=10" && echo
wait $INGEST_PID
kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null

# ---------------------------------------------------------------------------
echo "=== Experiment 2: async starvation (CPU work on vs off the runtime) ==="
# ---------------------------------------------------------------------------
# A small runtime makes contention for worker threads visible; with a thread
# per core the CPU stage simply spreads out and the effect hides.
for MODE in on-runtime spawn-blocking; do
  DB="$OUT/starvation-$MODE.db"; fresh_db "$DB"
  FLAG=""
  [ "$MODE" = "on-runtime" ] && FLAG="--analyze-on-runtime"

  $BIN --slots "$SLOTS" --start-slot "$START_SLOT" --db "$DB" --port "$PORT" \
       --runtime-workers 2 $FLAG --exit-after-ingest \
       > "$OUT/starvation-$MODE.log" 2>&1 &
  INGEST_PID=$!

  sleep 10
  python3 scripts/api_latency.py --url "http://localhost:$PORT/api/status" \
          --seconds 30 --concurrency 8 --label "analysis $MODE" \
          | tee "$OUT/starvation-$MODE.txt"
  wait $INGEST_PID
done

# ---------------------------------------------------------------------------
echo "=== Experiment 3: write path (batch size sweep) ==="
# ---------------------------------------------------------------------------
: > "$OUT/batching.txt"
for BATCH in 1 8 32 256; do
  DB="$OUT/batch-$BATCH.db"; fresh_db "$DB"
  START=$(python3 -c "import time; print(time.time())")

  $BIN --slots "$SLOTS" --start-slot "$START_SLOT" --db "$DB" --port "$PORT" \
       --batch-blocks "$BATCH" --exit-after-ingest > "$OUT/batch-$BATCH.log" 2>&1

  END=$(python3 -c "import time; print(time.time())")
  SIZE=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB")
  ELAPSED=$(python3 -c "print(f'{$END - $START:.1f}')")
  BATCHES=$(grep -o 'write_batches[^ ]*' "$OUT/batch-$BATCH.log" | tail -1)
  printf 'batch=%-4s elapsed=%ss db=%sMB %s\n' \
         "$BATCH" "$ELAPSED" "$((SIZE / 1024 / 1024))" "$BATCHES" \
         | tee -a "$OUT/batching.txt"
done

echo
echo "Results written to $OUT/"
