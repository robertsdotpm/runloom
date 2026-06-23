#!/usr/bin/env bash
# Build + run the MULTI-wedge stalled-hub repro (Group B rescue-thread pool).
# Same binary, two pool sizes: POOL=1 reproduces the old single-thread limit
# (RED), POOL>=wedged is the pool (GREEN).  Logs to stall_pool_test.log.
set -u
cd /home/x/projects/runloom/tests_c
LOG=stall_pool_test.log
exec > "$LOG" 2>&1
echo "=== stall pool test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.13.13t
SO=/home/x/projects/runloom/src/runloom_c.cpython-313t-x86_64-linux-gnu.so

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter \
   -I"$PY/include/python3.13t" -I../src/runloom_c \
   test_stall_pool.c -o test_stall_pool \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,/home/x/projects/runloom/src -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_stall_pool 2>&1 || { echo "BUILD FAILED"; exit 1; }

# The pool must recover EVERY wedged hub.  Run a few times each (timing varies).
echo "--- RUN POOL=1 (old single-thread behaviour) -- expect RED (FAIL) ---"
for r in 1 2 3 4 5; do
    RUNLOOM_HANDOFF=1 RUNLOOM_HANDOFF_POOL=1 RUNLOOM_SYSMON_MS=20 timeout 30 ./test_stall_pool
    echo "  run $r exit rc=$?"
done

echo "--- RUN POOL=4 (rescue pool) -- expect GREEN 64/64 (PASS) ---"
for r in 1 2 3 4 5 6 7 8 9 10; do
    RUNLOOM_HANDOFF=1 RUNLOOM_HANDOFF_POOL=4 RUNLOOM_SYSMON_MS=20 timeout 30 ./test_stall_pool
    echo "  run $r exit rc=$?"
done

echo "--- RUN default pool (RUNLOOM_HANDOFF=1, pool=min(hubs,4)=4) -- expect GREEN ---"
for r in 1 2 3; do
    RUNLOOM_HANDOFF=1 RUNLOOM_SYSMON_MS=20 timeout 30 ./test_stall_pool
    echo "  run $r exit rc=$?"
done

echo "=== stall pool test end $(date -Is) ==="
