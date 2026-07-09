#!/usr/bin/env bash
# simfd_hunt_forever.sh -- concentrated deterministic byte-plane FAULT HUNT.
#
# Loops forced LIFEFUZZ_KIND=simfd and =simfd_dgram sweeps over ever-advancing
# seed ranges, forever.  The sim byte plane is a pure function of the seed, so any
# finding (conservation miss / reap-tally mismatch / self_check fail / crash) is a
# real netpoll/wake bug reproducible from ONE integer: LIFEFUZZ_KIND=<kind>
# tools/lifefuzz/lifefuzz.py run <seed>.  Niced to 19 (alongside the rr fleet) so
# it never starves big100/cserve.  Log: docs/dev/soak/simfd_hunt/.
set +e
cd "$(dirname "$0")/../.." || exit 9
DIR=docs/dev/soak/simfd_hunt
mkdir -p "$DIR"
SUMMARY="$DIR/SUMMARY.txt"
PY="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
export PYTHONPATH="src:tools/dst:tools/lifefuzz:${PYTHONPATH:-}"
export PYTHONHASHSEED=0          # mn kinds: trace digests comparable across repro runs
N="${HUNT_BATCH:-400}"
WORKERS="${HUNT_WORKERS:-4}"
seed0="${HUNT_SEED0:-1}"
round=0
echo "[$(date -u +%FT%TZ)] simfd hunt START seed0=$seed0 batch=$N workers=$WORKERS" >> "$SUMMARY"
while true; do
  round=$((round + 1))
  for kind in simfd simfd_dgram simfd_mn simfd_dgram_mn; do
    log="$DIR/${kind}_round${round}_seed${seed0}.log"
    LIFEFUZZ_KIND="$kind" nice -n 19 "$PY" tools/lifefuzz/lifefuzz.py sweep "$N" \
        --workers "$WORKERS" --seed0 "$seed0" --timeout 25 > "$log" 2>&1
    done_line=$(grep -E "sweep done:" "$log" | tail -1)
    findings=$(echo "$done_line" | grep -oE "[0-9]+ findings" | grep -oE "[0-9]+")
    ts=$(date -u +%FT%TZ)
    if [ "${findings:-0}" != "0" ] && [ -n "$findings" ]; then
      echo "[$ts] *** FINDING *** kind=$kind seed0=$seed0 -> $done_line  (log: $log)" >> "$SUMMARY"
    else
      echo "[$ts] clean kind=$kind seeds [$seed0,$((seed0+N))) -> ${done_line:-no-summary}" >> "$SUMMARY"
      rm -f "$log"                    # keep only finding logs
    fi
    seed0=$((seed0 + N))
  done
done
