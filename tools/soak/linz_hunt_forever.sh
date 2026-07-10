#!/usr/bin/env bash
# linz_hunt_forever.sh -- generative LINEARIZABILITY fault hunt, forever.
#
# Loops the linearizability battery (tools/lincheck/linz/battery.py) over
# ever-advancing seed ranges across every primitive (chan/mutex/rwmutex/
# semaphore/waitgroup/event).  Each seeded run records a real concurrent history
# on the M:N scheduler and checks it against the sequential reference spec with
# the pure-Python WGL checker.  Any NOT-LINEARIZABLE verdict is a genuine
# correctness bug in the primitive, reproducible from ONE integer:
#   python tools/lincheck/linz/battery.py <primitive> --seeds S S+1 -v
# A same-seed observable divergence in a native family (chan/rwmutex/semaphore/
# waitgroup) is a determinism regression; the Co* family (mutex/event) is
# linearizability-only (wake order not seed-governed -- see battery.py).
#
# Niced to 19 (alongside the rr fleet / simfd hunt) so it never starves
# big100/cserve.  Log: ${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/linz_hunt/.
set +e
cd "$(dirname "$0")/../.." || exit 9
DIR="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/linz_hunt"
mkdir -p "$DIR"
SUMMARY="$DIR/SUMMARY.txt"
PY="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
export RUNLOOM_PYTHON="$PY"
N="${HUNT_BATCH:-40}"                 # seeds per primitive per round
seed0="${HUNT_SEED0:-0}"
round=0
echo "[$(date -u +%FT%TZ)] linz hunt START seed0=$seed0 batch=$N" >> "$SUMMARY"
while true; do
  round=$((round + 1))
  for prim in chan mutex rwmutex semaphore waitgroup event; do
    log="$DIR/${prim}_round${round}_seed${seed0}.log"
    nice -n 19 "$PY" tools/lincheck/linz/battery.py "$prim" \
        --seeds "$seed0" "$((seed0 + N))" > "$log" 2>&1
    rc=$?
    done_line=$(grep -E "== battery:" "$log" | tail -1)
    ts=$(date -u +%FT%TZ)
    if [ "$rc" != "0" ]; then
      echo "[$ts] *** FINDING *** prim=$prim seeds [$seed0,$((seed0+N))) rc=$rc -> ${done_line:-crash}  (log: $log)" >> "$SUMMARY"
    else
      echo "[$ts] clean prim=$prim seeds [$seed0,$((seed0+N))) -> ${done_line:-no-summary}" >> "$SUMMARY"
      rm -f "$log"                    # keep only finding logs
    fi
  done
  seed0=$((seed0 + N))
done
