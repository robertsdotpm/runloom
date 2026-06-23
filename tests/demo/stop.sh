#!/usr/bin/env bash
# stop.sh -- stop the supervisor (and thus the server + client) cleanly.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pid=$(cat "$HERE/run/supervisor.pid" 2>/dev/null || true)
if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
    echo "stopping supervisor pid $pid"
    kill -TERM "$pid"
    for i in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.3; done
    kill -KILL "$pid" 2>/dev/null || true
fi
# Belt and suspenders: reap any stragglers that belong to this demo dir.
for p in $(pgrep -f "$HERE/site.py" ; pgrep -f "$HERE/burst_client.py"); do
    kill -KILL "$p" 2>/dev/null || true
done
echo "stopped"
