#!/usr/bin/env sh
# Run a bpftrace script while a runloom workload runs, then print the maps.
#
# bpftrace uses kprobes/tracepoints (not the PMU), so latency histograms work
# on this VM even though perf's hardware counters don't (finding F5). Needs
# sudo (kernel tracing). The workload runs long enough (bump --reps) to overlap
# bpftrace's ~1s BPF attach.
#
# Usage:  bench/profile/bpftrace_driver.sh <workload> [run_workload opts]
#   e.g.  bench/profile/bpftrace_driver.sh mn --n 256 --iter 2000 --hubs 8 --reps 6
#         BT=bench/profile/futexlat.bt bench/profile/bpftrace_driver.sh pingpong --n 200000 --reps 4
set -eu
BT="${BT:-bench/profile/futexlat.bt}"
WORKLOAD="${1:-mn}"
[ "$#" -gt 0 ] && shift || true
PY="${PYTHON:-python3}"
OUT="$(mktemp)"

command -v bpftrace >/dev/null 2>&1 || { echo "bpftrace not installed"; exit 1; }

sudo bpftrace "$BT" > "$OUT" 2>/dev/null &
BTPID=$!
sleep 1.2   # let the BPF program compile + attach before work starts

env PYTHONPATH=src PYTHON_GIL=0 "$PY" -m bench.profile.run_workload \
    "$WORKLOAD" "$@" --quiet
sleep 0.3

sudo kill -INT "$BTPID" 2>/dev/null || true
wait "$BTPID" 2>/dev/null || true
cat "$OUT"
rm -f "$OUT"
