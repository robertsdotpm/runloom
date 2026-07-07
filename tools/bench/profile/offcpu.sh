#!/usr/bin/env bash
# offcpu.sh -- off-CPU / scheduler-latency profiling of a runloom workload.
#
# For a scheduler, where goroutines *block* and how long they wait to be
# rescheduled matters more than where CPU burns.  On-CPU profilers miss this
# entirely.  Two complementary views, both skip cleanly if unavailable:
#
#   1. perf sched  -- OS run-queue latency: time a thread is runnable but not
#                     running (the kernel-level analogue of park->wake delay).
#   2. bpftrace    -- off-CPU time histogram (sched_switch): how long threads
#                     stay blocked.  (Brendan Gregg's off-CPU analysis.)
#
# Goroutine-level park->wake latency (below the OS thread) lives in runloom's own
# event ring -- run the workload with RUNLOOM_DEBUG=ring,gstate and read
# _diag_dump; this script measures the OS-thread layer underneath it.
#
# perf needs kernel.perf_event_paranoid <= 1; bpftrace needs root.  Both are
# commonly restricted, so this skips with instructions rather than failing.
#
# Install: apt-get install linux-tools-common bpftrace
# Run:     tools/bench/profile/offcpu.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python3
export PYTHON_GIL=0
PARANOID="$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)"

# ---- preferred: perf sched run-queue latency -----------------------------
if command -v perf >/dev/null 2>&1 && [ "$PARANOID" -le 1 ]; then
    echo "[offcpu] perf sched record on target.py (run-queue latency)"
    REC="$(mktemp /tmp/runloom_perfsched.XXXXXX)"
    if perf sched record -o "$REC" -- "$PY" "$HERE/target.py" >/dev/null 2>&1; then
        perf sched latency -i "$REC" | head -40
        "$(command -v safe-rm || echo rm)" -f "$REC"
        exit 0
    fi
    "$(command -v safe-rm || echo rm)" -f "$REC"
    echo "[offcpu] perf sched failed; trying bpftrace"
fi

# ---- fallback: bpftrace off-CPU histogram (needs root) -------------------
if command -v bpftrace >/dev/null 2>&1 && [ "$(id -u)" = "0" ]; then
    echo "[offcpu] bpftrace off-CPU time histogram during target.py"
    "$PY" "$HERE/target.py" &
    PID=$!
    bpftrace -p "$PID" -e '
      tracepoint:sched:sched_switch /args->prev_pid != 0/ { @off[args->prev_pid] = nsecs; }
      tracepoint:sched:sched_switch /@off[args->next_pid]/ {
          @offcpu_us = hist((nsecs - @off[args->next_pid]) / 1000);
          delete(@off[args->next_pid]);
      }
      END { clear(@off); }' 2>/dev/null &
    BT=$!
    wait "$PID"
    kill "$BT" 2>/dev/null
    wait "$BT" 2>/dev/null
    exit 0
fi

echo "[offcpu] skipping -- need perf with perf_event_paranoid<=1 (have $PARANOID),"
echo "         or bpftrace as root.  (sudo sysctl kernel.perf_event_paranoid=1)"
exit 0
