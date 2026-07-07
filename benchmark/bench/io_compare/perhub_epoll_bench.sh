#!/usr/bin/env bash
# Per-hub epoll (RUNLOOM_PERHUB_EPOLL) throughput A/B via the io_compare loadgen.
#
# THE methodology that exposes the netpoll win: an EXTERNAL Go loadgen saturating
# an ISOLATED server_runloom.py.  In-process client+server benches bottleneck on
# the client / Python overhead, not netpoll, and read ~neutral -- see
# docs/dev/PERHUB_EPOLL.md "Bench methodology".  High -n (saturate) + high H (build
# the shared-epoll ep->lock contention the per-hub backend removes).
#
# Usage: bench/io_compare/perhub_epoll_bench.sh [N_CONNS] [HUBS...]
#   e.g. bench/io_compare/perhub_epoll_bench.sh 4000 16 32 48
# Env:  PYTHON=<3.13t python3>   REPS=<n, default 1>
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-/home/x/.pyenv/versions/3.14.4t/bin/python3}"
LG="bench/io_compare/loadgen"
NCONNS="${1:-4000}"; shift || true
HUBS=("$@"); [ ${#HUBS[@]} -eq 0 ] && HUBS=(16 32 48)
REPS="${REPS:-1}"

# Build the loadgen if absent (needs `go`).
[ -x "$LG" ] || { (cd bench/io_compare && go build -o loadgen loadgen.go) \
    || { echo "ERROR: need Go to build $LG"; exit 1; }; }
# Raise this shell's fd ceiling so the server + loadgen can hold N*2 fds.
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null

run() {  # hubs mode port -> prints rps
    local H=$1 M=$2 PORT=$3 SRV
    RUNLOOM_SYSMON_QUIET=1 PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_PER_G_TSTATE=0 \
        RUNLOOM_PERHUB_EPOLL=$M \
        "$PY" bench/io_compare/server_runloom.py 127.0.0.1 "$PORT" 0 "$H" \
        >"/tmp/perhub_srv_$PORT.log" 2>&1 &
    SRV=$!
    sleep 2.5
    if ! kill -0 "$SRV" 2>/dev/null; then echo "SERVER_DIED"; return; fi
    "$LG" -addr "127.0.0.1:$PORT" -n "$NCONNS" -ramp 2 -warmup 2 -measure 6 2>&1 \
        | grep -oE "rps=[0-9]+" | grep -oE "[0-9]+"
    kill -KILL "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null
}

echo "loadgen -n $NCONNS, server_runloom (isolated), OFF(=0) vs ON(default) per-hub epoll"
printf "%-6s | %-12s | %-12s | %s\n" "hubs" "OFF rps" "ON rps" "ON/OFF"
p=9600
for H in "${HUBS[@]}"; do
    for rep in $(seq 1 "$REPS"); do
        o=$(run "$H" 0 $((p++))); n=$(run "$H" 1 $((p++)))
        r=$(awk -v a="$o" -v b="$n" \
            'BEGIN{if(a+0>0)printf "%.2fx (+%.0f%%)",b/a,(b-a)*100/a; else print "?"}')
        printf "%-6s | %-12s | %-12s | %s\n" "$H" "$o" "$n" "$r"
    done
done
