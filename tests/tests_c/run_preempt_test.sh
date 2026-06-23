#!/usr/bin/env bash
# Build + run the ATTACHED/CPU preemption repro (RUNLOOM_PREEMPT).  Same binary,
# env discriminator: no RUNLOOM_PREEMPT -> RED (CPU-bound Python g monopolises its
# hub); RUNLOOM_PREEMPT=1 -> GREEN (eval-frame wrapper yields it).  Logs to
# preempt_test.log.
set -u
cd /home/x/projects/runloom/tests_c
LOG=preempt_test.log
exec > "$LOG" 2>&1
echo "=== preempt test $(date -Is) ==="
PY=/home/x/.pyenv/versions/3.13.13t
SO=/home/x/projects/runloom/src/runloom_c.cpython-313t-x86_64-linux-gnu.so

echo "--- build ---"
cc -g -O2 -Wall -Wextra -Wno-unused-parameter -Wno-unused-result \
   -I"$PY/include/python3.13t" -I../src/runloom_c \
   test_preempt.c -o test_preempt \
   -L"$PY/lib" -Wl,-rpath,"$PY/lib" -lpython3.13t -pthread \
   -Wl,-rpath,/home/x/projects/runloom/src -Wl,--no-as-needed "$SO"
echo "build rc=$?"
ls -la test_preempt 2>&1 || { echo "BUILD FAILED"; exit 1; }

# NB: RUNLOOM_PREEMPT now defaults ON (free-threaded 3.13+), so the RED baseline
# sets RUNLOOM_PREEMPT=0 explicitly (also HANDOFF=0 -- irrelevant here, the staller
# is ATTACHED, but keep the baseline free of all recovery).
echo "--- RUN recovery OFF (RUNLOOM_PREEMPT=0 RUNLOOM_HANDOFF=0) -- expect RED (FAIL) ---"
for r in 1 2 3; do
    RUNLOOM_PREEMPT=0 RUNLOOM_HANDOFF=0 timeout 30 ./test_preempt; echo "  run $r exit rc=$?"
done

echo "--- RUN RUNLOOM_PREEMPT=1 (eval-frame wrapper) -- expect GREEN 64/64 ---"
for r in 1 2 3 4 5 6 7 8 9 10; do
    RUNLOOM_PREEMPT=1 RUNLOOM_SYSMON_MS=20 timeout 30 ./test_preempt; echo "  run $r exit rc=$?"
done

echo "=== preempt test end $(date -Is) ==="
