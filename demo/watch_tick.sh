#!/usr/bin/env bash
# watch_tick.sh -- one autonomous watch interval.  Blocks until a new incident
# appears, the supervisor dies, or MAX_WAIT elapses, then prints a status
# summary and exits.  Run in the background; the agent inspects the output when
# it returns, handles any incident, and re-launches this for the next interval.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN="$HERE/run"; INC="$RUN/incidents"
MAX_WAIT="${1:-1500}"

baseline=$(ls "$INC"/INCIDENT-* 2>/dev/null | wc -l | tr -d ' ')
reqs_start=$(curl -s --max-time 4 http://127.0.0.1:8080/stats 2>/dev/null \
             | python3 -c "import sys,json;print(json.load(sys.stdin)['requests_served'])" 2>/dev/null || echo "?")
waited=0; reason=""
while [ "$waited" -lt "$MAX_WAIT" ]; do
    sleep 8; waited=$((waited + 8))
    cur=$(ls "$INC"/INCIDENT-* 2>/dev/null | wc -l | tr -d ' ')
    if [ "$cur" -gt "$baseline" ]; then reason="NEW INCIDENT(S): $baseline -> $cur"; break; fi
    sp=$(cat "$RUN/supervisor.pid" 2>/dev/null || echo "")
    if [ -z "$sp" ] || ! kill -0 "$sp" 2>/dev/null; then reason="SUPERVISOR DOWN (pid='$sp')"; break; fi
done
[ -z "$reason" ] && reason="HEALTHY TICK (${MAX_WAIT}s elapsed, no event)"

reqs_now=$(curl -s --max-time 4 http://127.0.0.1:8080/stats 2>/dev/null \
           | python3 -c "import sys,json;print(json.load(sys.stdin)['requests_served'])" 2>/dev/null || echo "?")
echo "================ WATCH RESULT: $reason ================"
echo "requests_served: ${reqs_start} -> ${reqs_now}   (waited ${waited}s)"
echo "--- status.txt ---";        cat "$RUN/status.txt" 2>/dev/null
echo "--- last 3 client bursts ---"; grep '^\[burst' "$RUN/client.log" 2>/dev/null | tail -3
echo "--- supervisor.log (last 6) ---"; tail -6 "$RUN/supervisor.log" 2>/dev/null
echo "--- incidents (newest 5) ---"; ls -t "$INC"/INCIDENT-* 2>/dev/null | head -5
