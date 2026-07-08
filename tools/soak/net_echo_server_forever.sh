#!/usr/bin/env bash
# net_echo_server_forever.sh -- launch the over-the-internet pygo echo SERVER
# (tools/soak/net_echo_server_forever.py) as ONE long-lived process.  Runs on
# the public box (e.g. ovh1.p2pd.net); a remote pygo client soaks it over the
# internet.
#
# Same crash-visible contract as net_echo_forever.sh: NO restart on exit -- a
# crash must STAY crashed so it is visible.  Records the exit code + timestamp to
# net_echo_server_forever/EXITED (rc=139 SIGSEGV, 134 SIGABRT, 137 SIGKILL/OOM,
# 0 clean stop, else unhandled exception).
#
# Launch detached:  RUNLOOM_PYTHON=~/py314t/bin/python3.14t setsid nice -n 5 \
#                     tools/soak/net_echo_server_forever.sh >/dev/null 2>&1 &
# Watch:            tail -f docs/dev/soak/net_echo_server_forever/net_echo_srv.log
# Live fiber stacks:kill -USR1 $(cat docs/dev/soak/net_echo_server_forever/PID)
# Stop:             kill      $(cat docs/dev/soak/net_echo_server_forever/PID)
set +e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
OUT="$ROOT/docs/dev/soak/net_echo_server_forever"
mkdir -p "$OUT"
LOG="$OUT/net_echo_srv.log"

echo "=== net_echo_server_forever START $(date '+%F %T') bind=${RUNLOOM_ECHO_BIND:-::}:${RUNLOOM_ECHO_PORT:-7777} ===" >> "$LOG"
env PYTHON_GIL=0 PYTHON_TLBC=0 PYTHONPATH="$ROOT/src" \
    "$PY" "$ROOT/tools/soak/net_echo_server_forever.py" >> "$LOG" 2>&1 &
child=$!
echo "$child" > "$OUT/PID"
wait "$child"
rc=$?
echo "=== net_echo_server_forever EXITED rc=$rc $(date '+%F %T') -- NO RESTART (crash/stop stays visible) ===" >> "$LOG"
echo "$(date -Iseconds) rc=$rc" >> "$OUT/EXITED"
rm -f "$OUT/PID"
