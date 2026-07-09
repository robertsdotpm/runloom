#!/usr/bin/env bash
# rr_forever.sh -- run the rr-chaos lost-wake hunt CONTINUOUSLY (24/7), not just
# the nightly 2h duty slot.  Interleaving bugs fall to accumulated schedule-
# hours; rr serializes each recording onto ~1 core, so on a big box this buys
# ~12x the nightly accrual for a rounding-error of CPU.
#
# Loops 1h iterations of tools/soak/rr_chaos.sh; per iteration appends one line
# to docs/dev/soak/forever_rrchaos_SUMMARY.txt and inboxes any findings.  Kept
# finding-traces are CAPPED per day (RR_FOREVER_TRACE_CAP, default 8): past the
# cap the log line still lands in the inbox but the trace is dropped -- a
# repeat-hang day must not fill the disk.  Clean traces are deleted per-seed by
# rr_chaos.sh, so steady-state disk is one in-flight trace.
#
# Detach:  setsid nice -n 10 tools/soak/rr_forever.sh >/dev/null 2>&1 &
# Stop:    pkill -f 'soak/rr_forever.sh'; pkill -f 'rr record --chaos'
# (Dies on reboot -- on the per-reboot relaunch checklist with the cserve soak.)
set +e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
CAP="${RR_FOREVER_TRACE_CAP:-8}"
SUM="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/forever_rrchaos_SUMMARY.txt"
cd "$ROOT"

echo "== rr-chaos forever loop started $(date '+%F %T') (1h iterations, trace cap $CAP/day) ==" >> "$SUM"
i=0
while true; do
  i=$((i+1)); DATE="$(date +%F)"
  ART="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/inbox_artifacts/rr_forever/$DATE"
  mkdir -p "$ART"
  bash tools/soak/rr_chaos.sh 3600 "$ART" > "$ART/iter_${i}.log" 2>&1
  runs="$(grep -oE '^rr-chaos: [0-9]+ runs, [0-9]+ findings' "$ART/iter_${i}.log" | tail -1)"
  skip="$(grep -m1 '^rr-chaos SKIP' "$ART/iter_${i}.log")"
  echo "iter $i  $(date '+%F %T')  |  ${runs:-${skip:-(no summary -- crashed?)}}" >> "$SUM"
  # inbox findings; enforce the per-day kept-trace cap
  grep -E "^FINDING " "$ART/iter_${i}.log" | while read -r _ kind rest; do
    "$PY" tools/soak/inbox.py --add --kind "$kind" --title "rr-forever $rest" \
        --artifact "$ART/iter_${i}.log" --date "$DATE" 2>/dev/null
  done
  kept="$(ls -1d "$ART"/rr-chaos-*_seed* 2>/dev/null | wc -l)"
  if [ "$kept" -gt "$CAP" ]; then
    ls -1dt "$ART"/rr-chaos-*_seed* | tail -n +"$((CAP+1))" | while read -r d; do
      echo "trace-cap: dropping $d" >> "$SUM"; rm -rf "$d"
    done
  fi
  # if rr can't record (vPMU dead again), don't spin: nap and re-probe hourly
  [ -n "$skip" ] && sleep 3600
  sleep 10
done
