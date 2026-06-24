#!/usr/bin/env bash
# Build + run the stalled-hub repro both ways. Logs to stall_test.log so
# results survive the laggy interactive channel.
set -u
cd "$(dirname "$0")"                       # tests/tests_c (was a hardcoded dead clone path)
SRC="$(cd ../../src && pwd)"               # repo src/ (post-reorg: tests/tests_c -> ../../src)
LOG=stall_test.log
exec > "$LOG" 2>&1
echo "=== stall test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.13.13t
SO="$SRC/runloom_c.cpython-313t-x86_64-linux-gnu.so"

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter \
   -I"$PY/include/python3.13t" -I"$SRC/runloom_c" \
   test_stall_steal.c -o test_stall_steal \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,"$SRC" -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_stall_steal 2>&1 || { echo "BUILD FAILED"; exit 1; }

# The handoff-rescue / per-g-tstate recovery these modes exercised was removed
# (2026-06; work-stealing steals only FRESH fibers, so a stalled hub no longer
# drains mid-stall).  The surviving invariant is no-lost-wake: every woken worker
# eventually runs even behind a stalled hub.  Run a few times; any non-PASS is a
# regression.
echo "--- RUN no-lost-wake check (DETACHED staller, RUNLOOM_HANDOFF=1) -- expect PASS 64/64 ---"
rc=0
for r in 1 2 3 4 5; do
    PYTHON_GIL=0 STALL_ALLOW_THREADS=1 RUNLOOM_HANDOFF=1 RUNLOOM_PREEMPT=1 RUNLOOM_SYSMON_MS=20 \
        timeout 30 ./test_stall_steal || rc=1
    echo "  run $r exit rc=$?"
done
echo "=== stall test end $(date -Is) rc=$rc ==="
exit $rc
