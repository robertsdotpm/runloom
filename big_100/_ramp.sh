#!/bin/bash
set +e
cd "$(dirname "$0")"
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
scale="$1"; tmo="$2"; arena="$3"; shift 3
GON="RUNLOOM_HARNESS_GON=1 RUNLOOM_GON_BULK=1 RUNLOOM_GON_FRESH=1 RUNLOOM_STACK_ARENA_N=$arena"
PY=$HOME/.pyenv/versions/3.13.13t/bin/python3
RES="_scale_logs/results_${scale}.txt"
: > "$RES"
for prog in "$@"; do
  [ -f "$prog.py" ] || { printf "%-30s MISSING\n" "$prog" | tee -a "$RES"; continue; }
  log="_scale_logs/${prog}_${scale}.log"
  t0=$(date +%s)
  env PYTHON_GIL=0 PYTHONPATH=../src $GON timeout -k 10 "$tmo" "$PY" "$prog.py" \
      --funcs "$scale" --duration 5 --rounds 0 --hubs 8 > "$log" 2>&1
  rc=$?
  pkill -9 -f "$prog.py" 2>/dev/null
  dt=$(( $(date +%s) - t0 ))
  verdict=$(grep -oE "VERDICT +: [A-Z]+" "$log" | head -1 | grep -oE "[A-Z]+$")
  exits=$(grep -oE "worker_exits +: [0-9]+/[0-9]+" "$log" | head -1 | grep -oE "[0-9]+/[0-9]+")
  peak=$(grep -oE "peak_goroutines +: [0-9]+" "$log" | head -1 | grep -oE "[0-9]+$")
  fails=$(grep -oE "failures +: [0-9]+" "$log" | head -1 | grep -oE "[0-9]+$")
  if   [ "$rc" = "124" ] || [ "$rc" = "137" ]; then cls="TIMEOUT"
  elif [ "$rc" != "0" ];  then cls="CRASH(rc=$rc)"
  elif [ "$verdict" = "PASS" ]; then cls="PASS"
  else cls="VFAIL(${verdict:-none})"; fi
  printf "%-30s %-14s %4ss exits=%-15s peak=%-9s fails=%-4s\n" \
    "$prog" "$cls" "$dt" "${exits:-?}" "${peak:-?}" "${fails:-?}" | tee -a "$RES"
done
echo "=== DONE $scale ===" | tee -a "$RES"
