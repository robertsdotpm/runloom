#!/usr/bin/env bash
# Build + run the stalled-hub repro both ways. Logs to stall_test.log so
# results survive the laggy interactive channel.
set -u
cd /home/x/projects/runloom/tests_c
LOG=stall_test.log
exec > "$LOG" 2>&1
echo "=== stall test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.13.13t
SO=/home/x/projects/runloom/src/runloom_c.cpython-313t-x86_64-linux-gnu.so

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter \
   -I"$PY/include/python3.13t" -I../src/runloom_c \
   test_stall_steal.c -o test_stall_steal \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,/home/x/projects/runloom/src -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_stall_steal 2>&1 || { echo "BUILD FAILED"; exit 1; }

# NB: RUNLOOM_HANDOFF + RUNLOOM_PREEMPT now default ON (free-threaded 3.13+), so the
# RED baseline sets them =0 explicitly to show the un-recovered problem.
echo "--- RUN default mode, recovery OFF (RUNLOOM_HANDOFF=0 RUNLOOM_PREEMPT=0) -- expect RED ---"
RUNLOOM_PER_G_TSTATE=0 RUNLOOM_HANDOFF=0 RUNLOOM_PREEMPT=0 timeout 30 ./test_stall_steal; echo "exit rc=$?"

echo "--- RUN per-g-tstate ON (RUNLOOM_PER_G_TSTATE=1) -- recovers via global runq ---"
RUNLOOM_PER_G_TSTATE=1 timeout 30 ./test_stall_steal; echo "exit rc=$?"

# Group B: stalled-hub tstate handoff.  The DETACHED staller (a well-behaved
# Py_BEGIN_ALLOW_THREADS blocking call) is the handoff-recoverable class; the
# rescue M adopts the hub's freed tstate and drains its stranded gs.
echo "--- RUN DETACHED staller + RUNLOOM_HANDOFF=1, default mode -- expect GREEN 64/64 ---"
STALL_ALLOW_THREADS=1 RUNLOOM_HANDOFF=1 timeout 30 ./test_stall_steal; echo "exit rc=$?"

# Control: the raw-usleep staller keeps its tstate ATTACHED (CPU/raw-syscall
# class), which a tstate handoff CANNOT recover -- must stay RED even with the
# handoff on (the rescue correctly refuses to adopt a non-DETACHED wedge).
# (RUNLOOM_PREEMPT=0 here: this staller is a raw-usleep *C* goroutine with no
# Python frames, so preemption can't touch it anyway -- isolate the handoff.)
echo "--- RUN ATTACHED staller + RUNLOOM_HANDOFF=1 RUNLOOM_PREEMPT=0 -- expect RED (not handoff-recoverable) ---"
RUNLOOM_HANDOFF=1 RUNLOOM_PREEMPT=0 timeout 30 ./test_stall_steal; echo "exit rc=$?  (RED is correct here)"

echo "=== stall test end $(date -Is) ==="
