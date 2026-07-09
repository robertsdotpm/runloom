#!/bin/bash
# big100_forever.sh -- run EVERY big_100 program at N=1,000,000 through the
# harness, FOREVER, collecting results.  Built from the proven sweep_1m.sh +
# ramp_campaign.sh patterns, adapted to the MAIN repo (tests/big_100 + ../src).
#
# Per program (all safety nets from the sweeps):
#   * systemd --user --scope MemoryMax=24G / MemorySwapMax=0 / TasksMax=200000
#     -- no single 1M run can OOM / fork-bomb / swap-thrash the box.
#   * timeout -k 10 $TMO -- a hung run is SIGTERM'd, then SIGKILL'd 10s later.
#   * prlimit --nofile=8388608 -- tens of thousands of sockets fit.
#   * RUNLOOM_GON_* -- the 1M bulk/fresh goroutine + stack-arena config.
#   * pkill straggler reap after each run.
# Classification: PASS / VFAIL(<verdict>) / CRASH(rc) / TIMEOUT.
#
# Results (append-only, survives restarts):
#   docs/dev/soak/big100_forever/results.tsv   one row per (iter, program)
#   docs/dev/soak/big100_forever/SUMMARY.txt   one line per completed iteration
#
# Detach:  setsid nice -n 10 tests/big_100/big100_forever.sh >/dev/null 2>&1 &
# Stop:    pkill -f big100_forever.sh
# Env:     BIG100_TMO (per-prog timeout s, default 300); BIG100_FUNCS (default 1000000)
set +e
cd "$(dirname "$0")" || exit 2                    # tests/big_100
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null

PY="$HOME/.pyenv/versions/3.14.4t/bin/python3"
FUNCS="${BIG100_FUNCS:-1000000}"
TMO="${BIG100_TMO:-300}"
GON="RUNLOOM_HARNESS_GON=1 RUNLOOM_GON_BULK=1 RUNLOOM_GON_FRESH=1 RUNLOOM_STACK_ARENA_N=1300000"
# TLBC mitigation: start ft-3.14 children with thread-local bytecode OFF so each
# program runs clean instead of re-exec'ing itself (runloom.run does the re-exec
# as a fallback).  Honor RUNLOOM_TLBC=1 (used to re-arm the p565/p524 guards).
TLBC=""; [ "${RUNLOOM_TLBC:-}" = "1" ] || TLBC="PYTHON_TLBC=0"

OUT="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/big100_forever"
mkdir -p "$OUT"
RES="$OUT/results.tsv"
SUM="$OUT/SUMMARY.txt"
[ -f "$RES" ] || printf "iso_time\titer\tprogram\tverdict\texit\telapsed_s\tfuncs\tpeak_g\tworker_exits\tfails\n" > "$RES"

# Detect the memory/task-capped transient-scope wrapper.  It must be inlined at
# the call site (NOT a shell function): the run is `env VARS <cmd> ...`, and env
# can only exec a real binary -- a function named after `env` yields rc=127.
have_scope=0
systemd-run --user --scope -q -p TasksMax=100 -- true 2>/dev/null && have_scope=1
SCOPE=""
[ "$have_scope" = 1 ] && SCOPE="systemd-run --user --scope -q -p MemoryMax=24G -p MemorySwapMax=0 -p TasksMax=200000 --"

echo "== big100-forever started $(date '+%F %T')  funcs=$FUNCS tmo=${TMO}s scope=$have_scope ==" >> "$SUM"
iter=0
while true; do
  iter=$((iter + 1))
  ti=$(date +%s); pass=0; vfail=0; crash=0; tout=0; scale=0; n=0
  # Program set for this iteration.  BIG100_FOCUS=1 -> only NEW + recently-buggy
  # programs (focus_list.py: git-added in the last BIG100_FOCUS_DAYS days UNION
  # results.tsv real-bug verdicts).  Recomputed each iter so a newly-found bug
  # joins the set automatically; empty/absent -> fall back to the full glob.
  if [ "${BIG100_FOCUS:-0}" = "1" ]; then
    mapfile -t PROGS < <("$PY" focus_list.py 2>/dev/null)
    [ "${#PROGS[@]}" -gt 0 ] || PROGS=( p[0-9]*.py )
    echo "$(date -Iseconds) iter=$iter FOCUS: ${#PROGS[@]} progs (new + buggy, last ${BIG100_FOCUS_DAYS:-7}d)" >> "$SUM"
  else
    PROGS=( p[0-9]*.py )
  fi
  for pf in "${PROGS[@]}"; do
    [ -f "$pf" ] || continue
    prog="${pf%.py}"; n=$((n + 1))
    t0=$(date +%s)
    out=$(env PYTHON_GIL=0 $TLBC PYTHONPATH=../src $GON \
          $SCOPE timeout -k 10 "$TMO" "$PY" "$pf" \
          --funcs "$FUNCS" --duration 5 --rounds 0 --hubs 8 2>&1)
    rc=$?
    pkill -9 -f "$pf" 2>/dev/null            # reap stragglers ("$pf" is the unique filename; never matches this script)
    dt=$(( $(date +%s) - t0 ))
    verdict=$(printf '%s\n' "$out" | grep -oE "VERDICT +: [A-Z]+"            | head -1 | grep -oE "[A-Z]+$")
    funcs=$(printf '%s\n'   "$out" | grep -oE "funcs +: [0-9]+"              | head -1 | grep -oE "[0-9]+$")
    peak=$(printf '%s\n'    "$out" | grep -oE "peak_goroutines +: [0-9]+"    | head -1 | grep -oE "[0-9]+$")
    exits=$(printf '%s\n'   "$out" | grep -oE "worker_exits +: [0-9]+/[0-9]+"| head -1 | grep -oE "[0-9]+/[0-9]+")
    fails=$(printf '%s\n'   "$out" | grep -oE "failures +: [0-9]+"           | head -1 | grep -oE "[0-9]+$")
    if   [ "$rc" = "124" ];        then cls="TIMEOUT";           tout=$((tout + 1))
    elif [ "$rc" = "4" ];          then cls="SCALE_LIMIT";       scale=$((scale + 1))
    elif [ "$rc" != "0" ];         then cls="CRASH(rc=$rc)";     crash=$((crash + 1))
    elif [ "$verdict" = "PASS" ];  then cls="PASS";              pass=$((pass + 1))
    else                                cls="VFAIL(${verdict:-none})"; vfail=$((vfail + 1)); fi
    printf "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$(date -Iseconds)" "$iter" "$prog" "$cls" "$rc" "$dt" \
      "${funcs:-?}" "${peak:-?}" "${exits:-?}" "${fails:-?}" >> "$RES"
  done
  printf "%s iter=%d done: %d progs  PASS=%d VFAIL=%d CRASH=%d TIMEOUT=%d SCALE=%d  (%dm%02ds)\n" \
    "$(date -Iseconds)" "$iter" "$n" "$pass" "$vfail" "$crash" "$tout" "$scale" \
    "$(( ($(date +%s) - ti) / 60 ))" "$(( ($(date +%s) - ti) % 60 ))" >> "$SUM"
done
