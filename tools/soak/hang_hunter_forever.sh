#!/usr/bin/env bash
# hang_hunter_forever.sh -- run the hang_hunter M:N hang/crash hunter in
# back-to-back bounded iterations, FOREVER, as a standalone continuous runner
# (like rr_fleet.sh / big100_forever.sh) instead of just one duty_cycle stage.
#
# Each iteration hunts for HH_ITER seconds -- randomized realistic M:N workloads,
# self-triaging hangs/crashes -- then every real finding (a *.txt with a KIND:
# line: HANG/CRASH) is flock-serialised into tools/soak/inbox.py.  Load-gated +
# niced so it never fights the other soak loops or foreground work.
#
# Knobs (env):
#   HH_ITER        per-iteration hunt seconds        (default 1800)
#   HH_LOAD_FRAC   skip an iter while 1-min load > FRAC*cores (default 0.8)
#   HH_JOBS        parallel hunt jobs (0=auto)       (default 0)
#   RUNLOOM_PYTHON interpreter
#
# Findings -> tools/soak/inbox.py + docs/dev/soak/inbox_artifacts/hang_hunter_forever/<date>/.
# Detach:  setsid nice -n 10 tools/soak/hang_hunter_forever.sh >/dev/null 2>&1 &
# Stop:    kill <this-pid>
set +e
# Deprioritize below big100/foreground: self-renice to 19 (always permitted,
# inherited by every hunt job; supersedes the launch/inner nice hints and
# survives restarts/reboots) so big100's 1M runs get CPU to reach bug-exposing
# scale instead of being starved by the hunt fleet.
renice -n 19 $$ >/dev/null 2>&1
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
NCPU="$(nproc 2>/dev/null || echo 8)"
HH_ITER="${HH_ITER:-1800}"
HH_LOAD_FRAC="${HH_LOAD_FRAC:-0.8}"
HH_JOBS="${HH_JOBS:-0}"
OUTBASE="$ROOT/docs/dev/soak/inbox_artifacts/hang_hunter_forever"
SUM="$ROOT/docs/dev/soak/forever_hanghunter_SUMMARY.txt"
LOCK="$ROOT/docs/dev/soak/.hang_hunter_inbox.lock"

load_ok() {
  local l1; l1="$(cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo 0)"
  awk -v l="$l1" -v c="$NCPU" -v f="$HH_LOAD_FRAC" 'BEGIN{exit !(l < c*f)}'
}

echo "== hang_hunter FOREVER started $(date '+%F %T'): iter=${HH_ITER}s load-gate ${HH_LOAD_FRAC}x${NCPU} jobs=${HH_JOBS} ==" >> "$SUM"
iter=0
while true; do
  iter=$((iter + 1))
  if ! load_ok; then sleep 30; continue; fi          # self-throttle vs the other loops
  DATE="$(date +%F)"
  OUT="$OUTBASE/$DATE/iter${iter}"; mkdir -p "$OUT"
  # PYTHON_TLBC=0: hang_hunter's spawned M:N workloads must not TLBC-re-exec on
  # ft-3.14 (see runloom.run/_tlbc_reexec_if_needed); PYTHON_GIL=0 for M:N.
  nice -n 10 env PYTHON_GIL=0 PYTHON_TLBC=0 PYTHONPATH="$ROOT/src" \
      "$PY" -m tools.hang_hunter.daemon --duration "$HH_ITER" \
      --load-frac "$HH_LOAD_FRAC" --jobs "$HH_JOBS" --python "$PY" \
      --report-dir "$OUT" >"$OUT/run.log" 2>&1
  # inbox real findings only (a KIND: line = HANG/CRASH; status/summary files
  # have none), flock-serialised against the shared INBOX.md.
  finds=0
  for rep in "$OUT"/*.txt; do
    [ -e "$rep" ] || continue
    kind="$(grep -m1 -oE 'KIND: [A-Z]+' "$rep" | awk '{print $2}')"
    [ -n "$kind" ] || continue
    flock "$LOCK" "$PY" tools/soak/inbox.py --add --kind "$kind" \
        --title "hang_hunter $(basename "$rep")" --artifact "$rep" --date "$DATE" 2>/dev/null
    finds=$((finds + 1))
  done
  echo "$(date '+%F %T')  iter=$iter  findings=$finds  (out=$OUT)" >> "$SUM"
done
