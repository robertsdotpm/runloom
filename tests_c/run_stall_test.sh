#!/usr/bin/env bash
# Build + run the stalled-hub repro both ways. Logs to stall_test.log so
# results survive the laggy interactive channel.
set -u
cd /home/x/projects/pygo/tests_c
LOG=stall_test.log
exec > "$LOG" 2>&1
echo "=== stall test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.13.13t
SO=/home/x/projects/pygo/src/pygo_core.cpython-313t-x86_64-linux-gnu.so

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter \
   -I"$PY/include/python3.13t" -I../src/pygo_core \
   test_stall_steal.c -o test_stall_steal \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,/home/x/projects/pygo/src -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_stall_steal 2>&1 || { echo "BUILD FAILED"; exit 1; }

echo "--- RUN default mode (PYGO_PER_G_TSTATE=0) ---"
PYGO_PER_G_TSTATE=0 timeout 30 ./test_stall_steal; echo "exit rc=$?"

echo "--- RUN per-g-tstate ON (PYGO_PER_G_TSTATE=1) -- expect RED today ---"
PYGO_PER_G_TSTATE=1 timeout 30 ./test_stall_steal; echo "exit rc=$?"

echo "=== stall test end $(date -Is) ==="
