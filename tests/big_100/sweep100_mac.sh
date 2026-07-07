#!/bin/bash
# sweep100_mac.sh -- run the FIRST 100 big_100 programs (by number) on macOS,
# sequentially, at a given scale, classifying each VERDICT-first.
#
# Usage: sweep100_mac.sh [funcs] [hubs] [duration] [timeout] [drain]
#   defaults: 100000 8 10 240 90
#
# Per-program FRESH IP window (offset 8*num) so a closed connection's 4-tuple
# never collides with the next program via TIME_WAIT (needs the lo0 aliases).
# Heavy programs at 100k need a roomy drain-timeout (~90s) -- their teardown of
# hundreds of thousands of fibers takes ~40s.  VERDICT-first so a program that
# printed VERDICT: PASS but whose watchdog fired at the teardown boundary still
# counts PASS.
set +e
cd "$HOME/pygo-macwin" || exit 2
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
ulimit -n 400000 2>/dev/null
sudo -n sysctl -w kern.ipc.somaxconn=200000 net.inet.tcp.msl=1000 >/dev/null 2>&1
PY="$HOME/.pyenv/versions/3.14.4t/bin/python3"
FUNCS="${1:-100000}"; HUBS="${2:-8}"; DUR="${3:-10}"; TMO="${4:-240}"; DRAIN="${5:-90}"
RES=/tmp/sweep100_results.txt
LOGD=/tmp/sweep100_logs
mkdir -p "$LOGD"; : > "$RES"
export PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON_QUIET=1 BIG100_BACKLOG=200000
echo "big_100 first-100 @ funcs=$FUNCS hubs=$HUBS dur=${DUR}s drain=${DRAIN}s (mac)" | tee -a "$RES"
pass=0; total=0; bad=""
for prog in big_100/p[0-9]*.py; do
  name=$(basename "$prog" .py)
  num=$(echo "$name" | sed -E 's/^p0*([0-9]+).*/\1/')
  [ "$num" -gt 100 ] 2>/dev/null && continue
  total=$((total+1))
  lo=$((8*num)); hi=$((8*num+7))
  log="$LOGD/$name.log"
  gtimeout -k 10 "$TMO" "$PY" "$prog" --funcs "$FUNCS" --hubs "$HUBS" \
      --duration "$DUR" --rounds 1 --hang-timeout "$DRAIN" --drain-timeout "$DRAIN" \
      --ip-start-offset "$lo" --ip-end-offset "$hi" > "$log" 2>&1
  rc=$?
  pkill -9 -f "$name.py" 2>/dev/null
  verdict=$(grep -oE "VERDICT *: *[A-Z]+" "$log" | tail -1 | grep -oE "[A-Z]+$")
  exited=$(grep -oE "worker_exits *: *[0-9]+/[0-9]+" "$log" | tail -1)
  if   [ "$verdict" = "PASS" ]; then cls="PASS"; pass=$((pass+1))
  elif [ "$rc" = "124" ] || [ "$rc" = "137" ]; then cls="TIMEOUT"
  elif [ "$rc" -gt 128 ] 2>/dev/null; then cls="CRASH(rc=$rc)"
  elif [ "$rc" = "3" ]; then cls="HANG"
  elif [ -n "$verdict" ]; then cls="VFAIL($verdict)"
  else cls="FAIL(rc=$rc)"; fi
  [ "$cls" = "PASS" ] || bad="$bad $name"
  printf "%-26s %-14s %s\n" "$name" "$cls" "$exited" | tee -a "$RES"
done
echo "==== big_100 first-100 @${FUNCS}: $pass/$total PASS ====" | tee -a "$RES"
[ -n "$bad" ] && echo "NON-PASS:$bad" | tee -a "$RES"
echo "SWEEP100_DONE" | tee -a "$RES"
