#!/usr/bin/env bash
# conformance_forever.sh -- run the upstream-stdlib conformance suites
# (Pillars B + C) in a loop, FOREVER, so a regression that only shows up under
# sustained free-threaded contention (a monkey Co* lock race, an asyncio-loop
# wake lost under load) becomes VISIBLE instead of a one-shot green.
#
# Each round runs, back to back:
#   B: tools/conformance/run_asyncio.py         (CPython test_asyncio vs RunloomEventLoop)
#   C: tools/conformance/run_free_threading.py  (CPython test_free_threading vs monkey.patch)
# Both exit nonzero ONLY on a GENUINE (non-known-failure) red test -- known
# divergences are subtracted from their *_known_failures.txt, so a nonzero rc
# here is a real regression.
#
# Unlike the net_echo soak (which must STAY crashed to stay visible), this is a
# repeat-until-red harness: a clean round is expected, so it loops; the FIRST
# red round is recorded to SUMMARY.txt with the failing pillar + round + a saved
# full log, and the loop STOPS there so the failure is preserved for triage
# (set CONFORMANCE_KEEP_GOING=1 to keep looping past a red round instead).
#
# Niced to 19 (alongside the rr / simfd fleet) so it never starves big100/cserve.
# Log dir: ${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/conformance/
#
# Launch detached:  setsid nice -n 19 tools/soak/conformance_forever.sh >/dev/null 2>&1 &
# Watch:            tail -f ${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/conformance/SUMMARY.txt
# Stop:             kill $(cat ${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/conformance/PID)
set +e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT" || exit 9
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
DIR="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/conformance"
mkdir -p "$DIR"
SUMMARY="$DIR/SUMMARY.txt"
echo "$$" > "$DIR/PID"

# PYTHON_TLBC=0 up front so runloom does NOT self-re-exec (keeps one stable pid).
export PYTHON_GIL=0 PYTHON_TLBC=0 PYTHONPATH="$ROOT/src"

# Per-pillar wall-clock ceiling; a hang trips it and is recorded as a red round
# (timeout, rc=124) rather than wedging the loop forever.
TMO="${CONFORMANCE_TIMEOUT:-1200}"

round=0
echo "[$(date -u +%FT%TZ)] conformance_forever START pid=$$ py=$PY" >> "$SUMMARY"
while true; do
  round=$((round + 1))
  red=0
  for pillar in asyncio free_threading; do
    log="$DIR/${pillar}_round${round}.log"
    timeout "$TMO" nice -n 19 "$PY" "tools/conformance/run_${pillar}.py" > "$log" 2>&1
    rc=$?
    ts=$(date -u +%FT%TZ)
    total=$(grep -E "^TOTAL " "$log" | tail -1)
    if [ "$rc" -eq 0 ]; then
      echo "[$ts] clean round=$round pillar=$pillar -> ${total:-no-summary}" >> "$SUMMARY"
      rm -f "$log"                       # keep only red logs
    else
      red=1
      if [ "$rc" -eq 124 ]; then
        echo "[$ts] *** RED (TIMEOUT rc=124) *** round=$round pillar=$pillar (log: $log)" >> "$SUMMARY"
      else
        echo "[$ts] *** RED rc=$rc *** round=$round pillar=$pillar -> ${total:-no-summary}  (log: $log)" >> "$SUMMARY"
      fi
    fi
  done
  if [ "$red" -ne 0 ] && [ "${CONFORMANCE_KEEP_GOING:-0}" != "1" ]; then
    echo "[$(date -u +%FT%TZ)] STOP after first red round=$round (set CONFORMANCE_KEEP_GOING=1 to continue)" >> "$SUMMARY"
    break
  fi
done
rm -f "$DIR/PID"
