#!/bin/bash
# seq_mac.sh -- run big_100 programs on macOS SEQUENTIALLY, stopping at the
# first non-PASS so a bug can be fixed before moving on (the "don't advance
# until the last one works" discipline).
#
# Usage: seq_mac.sh [START] [END] [funcs] [hubs] [duration] [timeout]
#   START/END are program numbers (default 1..100).  Resume after a fix with the
#   same START -- already-passing programs before it are skipped.
#
# Fresh IP window per program (offset = 8*prognum) so a closed connection's
# 4-tuple never collides with the next program's via TIME_WAIT (needs the lo0
# aliases from provision_loopback.sh; 8*100+7 = 807 < 808 provisioned).
set +e
cd "$HOME/pygo-macwin" || exit 2
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
ulimit -n 400000 2>/dev/null
sudo -n sysctl -w kern.ipc.somaxconn=200000 net.inet.tcp.msl=1000 >/dev/null 2>&1
PY="$HOME/.pyenv/versions/3.14.4t/bin/python3"
START="${1:-1}"; END="${2:-100}"; FUNCS="${3:-20000}"; HUBS="${4:-8}"
DUR="${5:-10}"; TMO="${6:-90}"
LOGD=/tmp/big100_seq; mkdir -p "$LOGD"
export PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON_QUIET=1 BIG100_BACKLOG=200000
echo "seq @ funcs=$FUNCS hubs=$HUBS dur=${DUR}s start=p$START end=p$END"
for prog in big_100/p[0-9]*.py; do
  name=$(basename "$prog" .py)
  num=$(echo "$name" | sed -E 's/^p0*([0-9]+).*/\1/')
  [ "$num" -lt "$START" ] 2>/dev/null && continue
  [ "$num" -gt "$END" ] 2>/dev/null && continue
  lo=$((8*num)); hi=$((8*num+7))
  log="$LOGD/$name.log"
  gtimeout -k 10 "$TMO" "$PY" "$prog" --funcs "$FUNCS" --hubs "$HUBS" \
      --duration "$DUR" --rounds 1 --hang-timeout 40 --drain-timeout 40 \
      --ip-start-offset "$lo" --ip-end-offset "$hi" > "$log" 2>&1
  rc=$?
  pkill -9 -f "$name.py" 2>/dev/null
  verdict=$(grep -oE "VERDICT *: *[A-Z]+" "$log" | tail -1 | grep -oE "[A-Z]+$")
  exited=$(grep -oE "worker_exits *: *[0-9]+/[0-9]+" "$log" | tail -1)
  if   [ "$verdict" = "PASS" ]; then cls="PASS"
  elif [ "$rc" -gt 128 ] 2>/dev/null; then cls="CRASH(rc=$rc)"
  elif [ "$rc" = "124" ] || [ "$rc" = "137" ]; then cls="TIMEOUT"
  elif [ "$rc" = "3" ]; then cls="HANG"
  elif [ -n "$verdict" ]; then cls="VFAIL($verdict)"
  else cls="FAIL(rc=$rc)"; fi
  printf "%-26s %-15s %s\n" "$name" "$cls" "$exited"
  if [ "$cls" != "PASS" ]; then
    echo "==== STOP at $name ($cls) -- log tail: $log ===="
    tail -40 "$log"
    exit 1
  fi
done
echo "==== ALL PASS p$START..p$END @${FUNCS} ===="
