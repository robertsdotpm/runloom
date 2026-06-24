#!/usr/bin/env bash
# Build + run the MULTI-wedge stalled-hub repro (Group B rescue-thread pool).
# Same binary, two pool sizes: POOL=1 reproduces the old single-thread limit
# (RED), POOL>=wedged is the pool (GREEN).  Logs to stall_pool_test.log.
set -u
cd "$(dirname "$0")"                       # tests/tests_c (was a hardcoded dead clone path)
SRC="$(cd ../../src && pwd)"               # repo src/ (post-reorg: tests/tests_c -> ../../src)
LOG=stall_pool_test.log
exec > "$LOG" 2>&1
echo "=== stall pool test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.13.13t
SO="$SRC/runloom_c.cpython-313t-x86_64-linux-gnu.so"

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter \
   -I"$PY/include/python3.13t" -I"$SRC/runloom_c" \
   test_stall_pool.c -o test_stall_pool \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,"$SRC" -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_stall_pool 2>&1 || { echo "BUILD FAILED"; exit 1; }

# The rescue-thread pool this exercised was removed (2026-06).  The surviving
# invariant is no-lost-wake: every worker eventually runs even with multiple
# wedged hubs.  Run a few times; any non-PASS is a regression.
echo "--- RUN no-lost-wake check (multi-wedge, RUNLOOM_HANDOFF=1) -- expect PASS 64/64 ---"
rc=0
for r in 1 2 3 4 5; do
    PYTHON_GIL=0 RUNLOOM_HANDOFF=1 RUNLOOM_SYSMON_MS=20 timeout 30 ./test_stall_pool || rc=1
    echo "  run $r exit rc=$?"
done
echo "=== stall pool test end $(date -Is) rc=$rc ==="
exit $rc
