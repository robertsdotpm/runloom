#!/bin/bash
# Run a chunk of the 1M survival sweep, FOREGROUND.
# Usage: sweep_1m.sh <per_prog_timeout_s> "<extra-args>" prog1 prog2 ...
#   PASS     exit 0 + VERDICT PASS within timeout
#   VFAIL    exit 0 but VERDICT not PASS (correctness/conservation)
#   CRASH    nonzero exit (segv/abort)
#   TIMEOUT  killed by `timeout` -> FAILURE
set +e
cd /home/x/projects/pygo-big100/big_100
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
GON="RUNLOOM_HARNESS_GON=1 RUNLOOM_GON_BULK=1 RUNLOOM_GON_FRESH=1 RUNLOOM_STACK_ARENA_N=1300000"
PY=$HOME/.pyenv/versions/3.13.13t/bin/python3
RES=/tmp/sweep_1m_results.txt
tmo="$1"; extra="$2"; shift 2
for prog in "$@"; do
  [ -f "$prog.py" ] || { printf "%-26s MISSING\n" "$prog" | tee -a "$RES"; continue; }
  t0=$(date +%s)
  # -k 10: if it ignores SIGTERM at the timeout, SIGKILL 10s later -> never hangs.
  out=$(env PYTHON_GIL=0 PYTHONPATH=../src $GON timeout -k 10 "$tmo" "$PY" "$prog.py" \
        --funcs 1000000 --duration 5 --rounds 0 --hubs 8 $extra 2>&1)
  rc=$?
  pkill -9 -f "$prog.py" 2>/dev/null    # belt-and-braces: reap any stragglers
  dt=$(( $(date +%s) - t0 ))
  verdict=$(printf '%s\n' "$out" | grep -oE "VERDICT +: [A-Z]+" | head -1 | grep -oE "[A-Z]+$")
  funcs=$(printf '%s\n' "$out"  | grep -oE "funcs +: [0-9]+" | head -1 | grep -oE "[0-9]+$")
  exits=$(printf '%s\n' "$out"  | grep -oE "worker_exits +: [0-9]+/[0-9]+" | head -1 | grep -oE "[0-9]+/[0-9]+")
  peak=$(printf '%s\n' "$out"   | grep -oE "peak_goroutines +: [0-9]+" | head -1 | grep -oE "[0-9]+$")
  fails=$(printf '%s\n' "$out"  | grep -oE "failures +: [0-9]+" | head -1 | grep -oE "[0-9]+$")
  if   [ "$rc" = "124" ]; then cls="TIMEOUT"
  elif [ "$rc" != "0" ];  then cls="CRASH(rc=$rc)"
  elif [ "$verdict" = "PASS" ]; then cls="PASS"
  else cls="VFAIL(${verdict:-none})"; fi
  printf "%-26s %-13s %4ss funcs=%-8s exits=%-15s peak=%-8s fails=%-3s\n" \
    "$prog" "$cls" "$dt" "${funcs:-?}" "${exits:-?}" "${peak:-?}" "${fails:-?}" | tee -a "$RES"
done
