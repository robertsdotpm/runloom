#!/usr/bin/env bash
# net_echo_forever.sh -- launch the over-the-internet pygo echo soak
# (tools/soak/net_echo_forever.py) as ONE long-lived process.
#
# Deliberately does NOT restart on exit: a crash must STAY crashed so it is
# VISIBLE -- that is the whole point.  An hourly-restart / supervisor wrapper
# would hide a real runtime crash behind a fresh iteration.  So this runs the
# soak exactly once; when it exits it records the exit code + timestamp to
# net_echo_forever/EXITED and stops.  rc tells you what happened:
#     rc=0    clean stop (SIGTERM/SIGINT -> STOP flag)
#     rc=139  SIGSEGV  -- a real runtime CRASH (segfault)
#     rc=134  SIGABRT  -- abort() / assert
#     rc=137  SIGKILL  -- OOM-killer or manual kill -9
#   else      unhandled Python exception (see the log tail + faulthandler dump)
#
# The soak's client fibers dial a remote TCP echo server (default
# ovh1.p2pd.net:7, the inetd echo service) OVER THE INTERNET and verify every
# byte echoed back, forever, exercising pygo netpoll against real WAN RTT/loss.
#
# Launch detached:  setsid nice -n 5 tools/soak/net_echo_forever.sh >/dev/null 2>&1 &
# Watch:            tail -f docs/dev/soak/net_echo_forever/net_echo.log
# Live fiber stacks:kill -USR1 $(cat docs/dev/soak/net_echo_forever/PID)
# Stop:             kill      $(cat docs/dev/soak/net_echo_forever/PID)
set +e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
OUT="$ROOT/docs/dev/soak/net_echo_forever"
mkdir -p "$OUT"
LOG="$OUT/net_echo.log"

echo "=== net_echo_forever START $(date '+%F %T') -> ${RUNLOOM_ECHO_HOST:-ovh1.p2pd.net}:${RUNLOOM_ECHO_PORT:-7} ===" >> "$LOG"
# PYTHON_TLBC=0 up front so runloom does NOT self-re-exec (keeps one stable pid).
env PYTHON_GIL=0 PYTHON_TLBC=0 PYTHONPATH="$ROOT/src" \
    "$PY" "$ROOT/tools/soak/net_echo_forever.py" >> "$LOG" 2>&1 &
child=$!
echo "$child" > "$OUT/PID"
wait "$child"
rc=$?
echo "=== net_echo_forever EXITED rc=$rc $(date '+%F %T') -- NO RESTART (crash/stop stays visible) ===" >> "$LOG"
echo "$(date -Iseconds) rc=$rc" >> "$OUT/EXITED"
rm -f "$OUT/PID"
