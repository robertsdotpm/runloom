#!/usr/bin/env bash
# lifefuzz_forever.sh -- march the lifefuzz fuzzer through the seed space FOREVER
# in fixed batches, hunting a hang or a crash over millions of runs (the soak-scale
# version of a single `lifefuzz.py sweep`).
#
# Each seed is a PURE FUNCTION of itself: build_spec(seed) fixes the workload and
# --mn-seed fixes the baton schedule, so every run is a distinct point in
# workload x schedule space AND any hang/crash reduces to one integer seed --
# reproducible with `lifefuzz.py repro <seed> --mn-seed <seed>` and minimizable
# with `shrink`.  The sweep already carries the hang watchdog, crash/signal
# detection, and the conservation/completion/parked/self_check oracle net; this
# wrapper just runs it batch after batch and keeps a permanent ledger, so "did it
# ever hang or crash over N million runs" is one file read whenever you next look.
#
# KIND: "grammar" (default) drives the resource-typed op-sequence generator (each
# seed a DIFFERENT program structure -- the #17 grammar); "mixed" leaves build_spec
# on its core/aio rotation.
#
# Usage:
#   tools/lifefuzz/lifefuzz_forever.sh [batch] [kind] [timeout] [seed0] [workers]
#   tools/lifefuzz/lifefuzz_forever.sh 100000 grammar
# Detach (survives logout, not reboot); low nice so it never starves the fleet:
#   setsid nice -n 12 tools/lifefuzz/lifefuzz_forever.sh 100000 grammar >/dev/null 2>&1 &
# Stop:
#   pkill -f lifefuzz_forever.sh; pkill -f 'lifefuzz.py sweep'
set +e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
BATCH="${1:-100000}"
KIND="${2:-grammar}"
TIMEOUT="${3:-20}"
SEED0="${4:-1}"
WORKERS="${5:-}"                     # empty -> sweep's default (cpu-2)
SUM="$ROOT/docs/dev/soak/forever_lifefuzz_${KIND}_SUMMARY.txt"
cd "$ROOT" || exit 1

export PYTHON_GIL=0
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
if [ "$KIND" = "grammar" ]; then
  export LIFEFUZZ_KIND=grammar
else
  unset LIFEFUZZ_KIND                 # "mixed" -> core/aio rotation
fi

wk_args=()
[ -n "$WORKERS" ] && wk_args=(--workers "$WORKERS")

echo "== lifefuzz forever started: kind=$KIND batch=$BATCH timeout=${TIMEOUT}s seed0=$SEED0 $(date '+%F %T') ==" >> "$SUM"
seed="$SEED0"
cum_runs=0
cum_find=0
while true; do
  start="$(date '+%F %T')"
  # workload seed == mn_seed (sweep pairs seed0+i with mn_seed+i), so a finding's
  # printed seed is the full repro key.
  out="$(nice -n 12 "$PY" tools/lifefuzz/lifefuzz.py sweep "$BATCH" \
         "${wk_args[@]}" --seed0 "$seed" --timeout "$TIMEOUT" --mn-seed "$seed" 2>&1)"
  line="$(printf '%s\n' "$out" | grep -m1 'sweep done:' || echo 'sweep done: (no summary -- wrapper/interpreter crashed?)')"
  find="$(printf '%s\n' "$line" | sed -nE 's/.*, ([0-9]+) findings/\1/p')"
  [ -z "$find" ] && find=0
  cum_runs=$((cum_runs + BATCH))
  cum_find=$((cum_find + find))
  echo "batch [$seed,$((seed + BATCH)))  $start -> $(date '+%F %T')  |  $line  (cum: runs=$cum_runs findings=$cum_find)" >> "$SUM"
  # A hang/crash is the whole point: keep going, but record the repro one-liners
  # (the per-seed JSON is already saved under tools/lifefuzz/corpus/).
  if [ "$find" -gt 0 ]; then
    printf '%s\n' "$out" | grep -E '!! FINDING|repro:' >> "$SUM"
  fi
  seed=$((seed + BATCH))
  sleep 2
done
