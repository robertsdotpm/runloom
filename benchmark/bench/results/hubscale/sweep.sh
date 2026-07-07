#!/usr/bin/env bash
# Experiment 1 (zero-code control): hub-count sweep on p207 (scheduler-pure
# channel ping-pong).  For each hub count we capture BOTH throughput
# (ops_per_sec, from the harness) and the flat perf self-time top (the
# _Py_DecRefShared / _PyEval / runloom_netpoll_wake_pump shares).
#
# Hypothesis under test (HUB_SCALING.md): _Py_DecRefShared % stays ~FLAT across
# hub counts (every pair is always split, so the per-op cross-hub decref cost is
# constant -> it is a LEVEL cost, not a SLOPE cost), while ops_per_sec scales
# sub-linearly and runloom_netpoll_wake_pump % GROWS with hub count (the shared
# wake-eventfd thundering herd, R2).
#
# perf needs sudo here (perf_event_paranoid=4).  Pin to a fixed core set so the
# comparison is apples-to-apples and never oversubscribes (64-core box).
set -u
cd "$(git rev-parse --show-toplevel)"
PY=/home/x/.pyenv/versions/3.14.4t/bin/python3
FUNCS="${FUNCS:-64000}"
DUR="${DUR:-8}"
CORES="${CORES:-0-47}"
FREQ="${FREQ:-1999}"
OUT=bench/results/hubscale
mkdir -p "$OUT"

for H in 8 16 32; do
  echo "=================== hubs=$H funcs=$FUNCS dur=${DUR}s cores=$CORES ==================="
  DATA="/tmp/p207_h${H}.data"
  sudo -n perf record -o "$DATA" -e task-clock -F "$FREQ" -- \
    taskset -c "$CORES" env PYTHONPATH=src PYTHON_GIL=0 \
    "$PY" big_100/p207_park_wake_pingpong.py \
      --funcs "$FUNCS" --duration "$DUR" --rounds 0 --hubs "$H" \
    > "$OUT/p207_h${H}.out" 2> "$OUT/p207_h${H}.log"
  # throughput line
  grep -E "ops_per_sec|^  ops |elapsed_s|VERDICT|peak_goroutines|mem_rss_mb" "$OUT/p207_h${H}.log" | sed 's/^/  /'
  # flat self-time top (no children = real self time per symbol)
  echo "  --- flat self-time top 18 ---"
  sudo -n perf report -i "$DATA" --stdio --no-children -g none 2>/dev/null \
    | grep -vE '^#|^$' | head -18 | sed 's/^/  /' | tee "$OUT/p207_h${H}.flat"
  sudo -n rm -f "$DATA"
  echo
done
echo "DONE. artifacts in $OUT/"
