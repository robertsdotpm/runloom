#!/usr/bin/env bash
# run.sh -- run the canary as an R1 soak subject (docs/dev/RELIABILITY_PROGRAM.md
# R6): start the server (self-sampling to a CSV) + the client fleet, let them run
# for a duration, then run the slope oracle on the sample CSV and write a REPORT.
#
# This is the highest-fidelity reliability test: a REAL service under REAL load,
# judged by the same flat-slope oracle as any soak.  "runloom served continuously
# for N with flat gauges" is the launch-credibility claim, and this measures it.
#
# Usage:
#   examples/canary/run.sh [seconds] [warmup_seconds]
#   # default 600s run, 120s warmup (the pool takes ~30s to reach steady state;
#   # a real multi-day canary uses the standard 600s soak warmup, plenty here).
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
SECS="${1:-600}"
WARMUP="${2:-120}"
# sample interval: 30s is right for a multi-day canary; a SHORT verification run
# needs a fine interval to collect enough post-warmup samples for a slope fit.
INTERVAL="${3:-30}"
OUT="$ROOT/docs/dev/soak/canary_$(date +%Y%m%d_%H%M%S 2>/dev/null || echo run)"
mkdir -p "$OUT"
CSV="$OUT/canary.csv"

# raise the fd ceiling so the churn bursts don't hit EMFILE (best-effort).
sudo -n prlimit --pid $$ --nofile=1048576:1048576 2>/dev/null || true

echo "[canary] soak ${SECS}s -> $OUT"
CANARY_CRASH_FILE="$OUT/canary_crash.txt" RUNLOOM_WATCHDOG=120 \
  PYTHON_GIL=0 "$PY" examples/canary/server.py \
    --csv "$CSV" --interval "$INTERVAL" --seconds "$SECS" >"$OUT/server.log" 2>&1 &
SRV=$!
sleep 3   # let the listeners come up
PYTHON_GIL=0 "$PY" examples/canary/client.py \
    --seconds "$SECS" >"$OUT/client.log" 2>&1 &
CLI=$!

wait "$CLI" 2>/dev/null
kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null

echo "[canary] running slope oracle (warmup ${WARMUP}s) ..."
"$PY" tools/soak/oracle.py "$CSV" --warmup "$WARMUP" | tee "$OUT/REPORT.txt"
