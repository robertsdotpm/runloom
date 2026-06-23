#!/bin/bash
# sweep_mac.sh -- run every big_100 program on macOS, SEQUENTIALLY, robustly.
#
# Usage: sweep_mac.sh [funcs] [hubs] [duration] [timeout]
#
# Design notes (learned the hard way):
#   * FRESH IP WINDOW PER PROGRAM (--ip-start-offset 8k..8k+7): each program's
#     servers bind unique 127/8 loopback IPs, so a closed connection's 4-tuple
#     never collides with the next program's via TIME_WAIT.  Needs ~800 lo0
#     aliases provisioned (see provision_loopback.sh) + kern.ipc.somaxconn high
#     + a short net.inet.tcp.msl.  Without this, consecutive socket-storm runs
#     contaminate each other and falsely HANG.
#   * FILE REDIRECT, not $(...) capture: a subprocess-spawning program
#     (process_tree, subproc_net) leaves orphaned grandchildren that inherit the
#     capture pipe; $(...) then blocks FOREVER after gtimeout kills the parent.
#     Redirect each run to a log file and grep the file -- the shell never waits
#     on a pipe held by a grandchild.
#   * pkill stragglers between programs so leaked grandchildren can't pile up.
#   * VERDICT-FIRST classification so the known mn_fini teardown hang (process
#     lingers after the workload printed VERDICT: PASS) doesn't mask a real pass.
set +e
cd "$HOME/pygo-macwin" || exit 2
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
ulimit -n 400000 2>/dev/null
sudo -n sysctl -w kern.ipc.somaxconn=200000 net.inet.tcp.msl=1000 >/dev/null 2>&1
PY="$HOME/.pyenv/versions/3.13.13t/bin/python3"
FUNCS="${1:-20000}"; HUBS="${2:-8}"; DUR="${3:-10}"; TMO="${4:-80}"
RES=/tmp/big100_results.txt
LOGD=/tmp/big100_logs
mkdir -p "$LOGD"; : > "$RES"
export PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON_QUIET=1 BIG100_BACKLOG=200000
echo "big_100 @ funcs=$FUNCS hubs=$HUBS dur=${DUR}s (mac, fresh-IP windows)" | tee -a "$RES"
pass=0; total=0; bad=""; k=0
for prog in big_100/p[0-9]*.py; do
  name=$(basename "$prog" .py)
  total=$((total+1))
  lo=$((8*k)); hi=$((8*k+7)); k=$((k+1))
  log="$LOGD/$name.log"
  gtimeout -k 10 "$TMO" "$PY" "$prog" --funcs "$FUNCS" --hubs "$HUBS" \
      --duration "$DUR" --rounds 1 --hang-timeout 40 --drain-timeout 40 \
      --ip-start-offset "$lo" --ip-end-offset "$hi" > "$log" 2>&1
  rc=$?
  pkill -9 -f "$name.py" 2>/dev/null     # reap leaked grandchildren
  verdict=$(grep -oE "VERDICT *: *[A-Z]+" "$log" | tail -1 | grep -oE "[A-Z]+$")
  exited=$(grep -oE "worker_exits *: *[0-9]+/[0-9]+|exited=[0-9]+/[0-9]+" "$log" | tail -1)
  if   [ "$verdict" = "PASS" ]; then cls="PASS"; pass=$((pass+1))
  elif [ "$rc" -gt 128 ] 2>/dev/null; then cls="CRASH(rc=$rc)"
  elif [ "$rc" = "124" ] || [ "$rc" = "137" ]; then cls="TIMEOUT"
  elif [ "$rc" = "3" ]; then cls="HANG"
  elif [ -n "$verdict" ]; then cls="VFAIL($verdict)"
  else cls="FAIL(rc=$rc)"; fi
  [ "$cls" = "PASS" ] || bad="$bad $name"
  printf "%-26s %-15s %s\n" "$name" "$cls" "$exited" | tee -a "$RES"
done
echo "==== big_100 @${FUNCS}: $pass/$total PASS ====" | tee -a "$RES"
[ -n "$bad" ] && echo "NON-PASS:$bad" | tee -a "$RES"
