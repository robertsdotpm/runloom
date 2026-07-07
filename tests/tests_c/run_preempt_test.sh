#!/usr/bin/env bash
# Build + run the ATTACHED/CPU preemption repro (RUNLOOM_PREEMPT).  Same binary,
# env discriminator: no RUNLOOM_PREEMPT -> RED (CPU-bound Python g monopolises its
# hub); RUNLOOM_PREEMPT=1 -> GREEN (eval-frame wrapper yields it).  Logs to
# preempt_test.log.
set -u
cd "$(dirname "$0")"                       # tests/tests_c (was a hardcoded dead clone path)
SRC="$(cd ../../src && pwd)"               # repo src/ (post-reorg: tests/tests_c -> ../../src)
LOG=preempt_test.log
exec > "$LOG" 2>&1
echo "=== preempt test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.14.4t
SO="$SRC/runloom_c.cpython-313t-x86_64-linux-gnu.so"

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter -Wno-unused-result \
   -I"$PY/include/python3.13t" -I"$SRC/runloom_c" \
   test_preempt.c -o test_preempt \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,"$SRC" -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_preempt 2>&1 || { echo "BUILD FAILED"; exit 1; }

# A CPU-stalled hub no longer drains mid-stall (the handoff-rescue pool was
# removed 2026-06; work-stealing steals only FRESH fibers).  The surviving, real
# invariant is no-lost-wake: every woken worker eventually runs even behind the
# CPU-bound Python staller.  Run a few times under preemption; any non-PASS is a
# regression (a permanently stranded worker = a lost wake).
echo "--- RUN no-lost-wake check (RUNLOOM_PREEMPT=1) -- expect PASS 64/64 ---"
rc=0
for r in 1 2 3 4 5; do
    PYTHON_GIL=0 RUNLOOM_PREEMPT=1 RUNLOOM_SYSMON_MS=20 timeout 30 ./test_preempt || rc=1
    echo "  run $r exit rc=$?"
done
echo "=== preempt test end $(date -Is) rc=$rc ==="
exit $rc
