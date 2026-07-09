#!/usr/bin/env bash
# rr_fleet.sh -- EXHAUSTIVE parallel rr-chaos lost-wake hunt.  Replaces the
# single-worker rr_forever with W concurrent `rr record --chaos` recorders, each
# pinned to ~1 core with a DISJOINT seed stream, running forever.  rr serializes
# one recording onto ~1 core, so on a big box this multiplies accrued chaos
# schedule-hours (and thus rare-hang discovery rate) by ~W.
#
# Every finding comes with a DETERMINISTICALLY REPLAYABLE rr trace, so a
# load-induced false timeout is filterable after the fact: a real lost-wake
# replays to the same hang on an idle box; a starvation artifact does not.  We
# still keep the per-run timeout GENEROUS so contention rarely fakes a hang.
#
# Knobs (env):
#   RR_FLEET_WORKERS   parallel recorders          (default: nproc/3, capped 24)
#   RR_FLEET_TIMEOUT   per-run outer timeout s      (default: 120 -- rr+chaos+load)
#   RR_FLEET_INNER     lifefuzz --timeout s         (default: 30)
#   RR_FLEET_TRACE_CAP kept finding-traces/day      (default: 64)
#   RUNLOOM_PYTHON     interpreter
#
# Findings -> tools/soak/inbox.py (flock-serialised) + docs/dev/soak/inbox_artifacts/rr_fleet/<date>/.
# Detach:  setsid nice -n 10 tools/soak/rr_fleet.sh >/dev/null 2>&1 &
# Stop:    kill <this-pid>   (traps TERM/INT and reaps the whole fleet)
set +e
# Deprioritize the whole fleet below big100/foreground: W parallel `rr --chaos`
# recorders are the box's heaviest CPU sink, and starving big100 hides bugs (its
# 1M runs need CPU to reach the scale/coverage that exposes faults).  Self-renice
# to 19 -- increasing niceness is always permitted and inherited by every child
# recorder -- so big100 wins the CPU when runnable.  Supersedes the `nice -n 10`
# launch hint above and survives restarts/reboots.
renice -n 19 $$ >/dev/null 2>&1
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
NCPU="$(nproc 2>/dev/null || echo 8)"
W="${RR_FLEET_WORKERS:-$(( NCPU/3 < 24 ? NCPU/3 : 24 ))}"; [ "$W" -ge 1 ] || W=1
TMO="${RR_FLEET_TIMEOUT:-120}"
INNER="${RR_FLEET_INNER:-30}"
CAP="${RR_FLEET_TRACE_CAP:-64}"
cd "$ROOT"
OUTBASE="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/inbox_artifacts/rr_fleet"
SUM="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/forever_rrfleet_SUMMARY.txt"
LOCK="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/.rr_fleet_inbox.lock"
STAT="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/.rr_fleet_stats"; mkdir -p "$STAT"

# --- availability gate (once) ------------------------------------------------
if ! command -v rr >/dev/null 2>&1; then echo "rr not installed -- exit" | tee -a "$SUM"; exit 0; fi
G="$(mktemp -d)"
if ! _RR_TRACE_DIR="$G" rr record /bin/true >/dev/null 2>&1; then
  rm -rf "$G"; echo "== rr cannot record on this host (vPMU -- see docs/dev/rr_vpmu_status.md); exit ==" | tee -a "$SUM"; exit 0
fi
rm -rf "$G"

echo "== rr-chaos FLEET started $(date '+%F %T'): W=$W workers, per-run ${TMO}s, inner ${INNER}s ==" >> "$SUM"

worker() {                                       # $1 = worker id
  local w="$1" n=0
  local TR="$STAT/traces_w$w"; mkdir -p "$TR"
  while true; do
    local DATE; DATE="$(date +%F)"
    local ART="$OUTBASE/$DATE"; mkdir -p "$ART"
    # disjoint global index -> distinct seed per (worker, run)
    local gi=$(( n * W + w )); n=$(( n + 1 ))
    local seed=$(( (gi * 1103515245 + 12345) % 2000000000 ))
    local log="$ART/w${w}_seed${seed}.log"
    # Set the interpreter env in OUR shell (inherited by rr's child) and record
    # "$PY" DIRECTLY -- do NOT put `env ...` inside `rr record`.  Under --chaos,
    # a recorded `env -> execve(python3)` occasionally wedges in the exec (glibc
    # ENOEXEC -> shell-script retry) for the whole run and gets SIGTERM'd at the
    # outer timeout: a false rr-chaos-HANG that never reaches pygo.  PYTHON_TLBC=0
    # likewise avoids an in-recording TLBC re-exec (os.execv) wedging the same way.
    _RR_TRACE_DIR="$TR" PYTHON_GIL=0 PYTHONPATH="$ROOT/src" PYTHON_TLBC=0 \
        timeout -k 5 "$TMO" \
        rr record --chaos \
        "$PY" tools/lifefuzz/lifefuzz.py run "$seed" --timeout "$INNER" \
        >"$log" 2>&1
    local rc=$?
    if [ "$rc" -eq 0 ]; then
      rm -f "$log"; rm -rf "$TR"/*/ 2>/dev/null          # clean run: drop log + trace
    else
      local kind="rr-chaos-fail"; [ "$rc" -ge 124 ] && kind="rr-chaos-HANG"
      local latest; latest="$(ls -1dt "$TR"/*/ 2>/dev/null | head -1)"
      local keep="$ART/${kind}_seed${seed}"
      [ -n "$latest" ] && mv "$latest" "$keep" 2>/dev/null
      echo "$(date -Iseconds) $kind seed=$seed rc=$rc w=$w trace=$keep" >> "$ART/findings.txt"
      # serialise the inbox write (INBOX.md is a single shared file)
      flock "$LOCK" "$PY" tools/soak/inbox.py --add --kind "$kind" \
          --title "rr-fleet seed=$seed rc=$rc (replay: rr replay $keep -- confirm on an IDLE box: real lost-wake replays to the hang, starvation does not)" \
          --artifact "$log" --date "$DATE" 2>/dev/null
      # cap kept traces/day across the fleet (best-effort; rm of gone dir is harmless)
      local kept; kept="$(ls -1d "$ART"/rr-chaos-*_seed* 2>/dev/null | wc -l)"
      [ "$kept" -gt "$CAP" ] && ls -1dt "$ART"/rr-chaos-*_seed* | tail -n +"$((CAP+1))" | xargs -r rm -rf
    fi
    echo "$n" > "$STAT/w${w}.runs"                        # per-worker run counter (low-contention)
  done
}

pids=()
for w in $(seq 0 $((W-1))); do worker "$w" & pids+=($!); done
trap 'echo "== fleet stopping $(date "+%F %T") ==" >> "$SUM"; kill "${pids[@]}" 2>/dev/null; pkill -P $$ 2>/dev/null; exit 0' TERM INT

# hourly heartbeat: total runs + findings today
while true; do
  sleep 3600
  local_runs=0; for f in "$STAT"/w*.runs; do [ -f "$f" ] && local_runs=$(( local_runs + $(cat "$f" 2>/dev/null || echo 0) )); done
  DATE="$(date +%F)"; finds=$(wc -l < "$OUTBASE/$DATE/findings.txt" 2>/dev/null || echo 0)
  echo "$(date '+%F %T')  fleet W=$W  total_runs~=$local_runs  findings_today=$finds" >> "$SUM"
done
