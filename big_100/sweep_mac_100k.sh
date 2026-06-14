#!/bin/bash
# sweep_mac_100k.sh -- run every big_100 program at N=100000 goroutines on macOS,
# SEQUENTIALLY (8-core box, --hubs 8 => one program packs the machine).  No
# systemd / per-job IP slots (those are the Linux orchestrator's job); each run
# uses the default 127.1.0.1..127.8.0.1 lo0 aliases, and sequential => no
# cross-job port collisions.
#
# Classification is VERDICT-FIRST so the known mn_fini teardown hang (process
# lingers in teardown after the workload finished) does NOT mask a real PASS:
# if the program printed "VERDICT : PASS" the run counts as PASS even if the
# wrapper timeout then SIGKILLs the slow teardown.
#
# Usage: sweep_mac_100k.sh [funcs] [hubs] [duration] [timeout] [from] [to]
set +e
cd "$HOME/pygo-macwin" || exit 2
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null   # no-op on macOS
ulimit -n 400000 2>/dev/null
PY="$HOME/.pyenv/versions/3.13.13t/bin/python3"
FUNCS="${1:-100000}"; HUBS="${2:-8}"; DUR="${3:-15}"; TMO="${4:-120}"
FROM="${5:-0}"; TO="${6:-999}"
RES=/tmp/big100_100k_results.txt
LOGD=/tmp/big100_100k_logs
mkdir -p "$LOGD"; : > "$RES"
export PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON_QUIET=1
echo "big_100 @ funcs=$FUNCS hubs=$HUBS dur=${DUR}s tmo=${TMO}s (mac, sequential)" | tee -a "$RES"
pass=0; total=0; bad=""
for prog in big_100/p[0-9]*.py; do
  name=$(basename "$prog" .py)
  num=$(echo "$name" | grep -oE "^p[0-9]+" | grep -oE "[0-9]+" | sed 's/^0*//'); num=${num:-0}
  [ "$num" -lt "$FROM" ] && continue
  [ "$num" -gt "$TO" ] && continue
  total=$((total+1))
  log="$LOGD/$name.log"
  t0=$(date +%s)
  gtimeout -k 10 "$TMO" "$PY" "$prog" --funcs "$FUNCS" --hubs "$HUBS" \
      --duration "$DUR" --rounds 1 --hang-timeout 60 --drain-timeout 60 \
      > "$log" 2>&1
  rc=$?
  pkill -9 -f "$name.py" 2>/dev/null
  dt=$(( $(date +%s) - t0 ))
  verdict=$(grep -oE "VERDICT *: *[A-Z]+" "$log" | tail -1 | grep -oE "[A-Z]+$")
  exits=$(grep -oE "worker_exits *: *[0-9]+/[0-9]+" "$log" | tail -1 | grep -oE "[0-9]+/[0-9]+")
  peak=$(grep -oE "peak_goroutines *: *[0-9]+" "$log" | tail -1 | grep -oE "[0-9]+$")
  fails=$(grep -oE "failures *: *[0-9]+" "$log" | tail -1 | grep -oE "[0-9]+$")
  ops=$(grep -oE "ops_per_sec *: *[0-9.]+" "$log" | tail -1 | grep -oE "[0-9.]+$")
  # VERDICT-first classification.
  if   [ "$verdict" = "PASS" ]; then cls="PASS"; pass=$((pass+1))
  elif [ "$rc" -gt 128 ] 2>/dev/null; then cls="CRASH(rc=$rc)"
  elif [ "$rc" = "124" ] || [ "$rc" = "137" ]; then cls="TIMEOUT"
  elif [ "$rc" = "3" ]; then cls="HANG"
  elif [ -n "$verdict" ]; then cls="VFAIL($verdict)"
  else cls="FAIL(rc=$rc)"; fi
  [ "$cls" = "PASS" ] || bad="$bad $name"
  printf "%-26s %-15s %4ss exits=%-13s peak=%-8s fails=%-4s ops/s=%s\n" \
    "$name" "$cls" "$dt" "${exits:-?}" "${peak:-?}" "${fails:-?}" "${ops:-?}" | tee -a "$RES"
done
echo "==== big_100 @${FUNCS}: $pass/$total PASS ====" | tee -a "$RES"
[ -n "$bad" ] && echo "NON-PASS:$bad" | tee -a "$RES"
