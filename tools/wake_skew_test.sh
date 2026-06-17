#!/bin/sh
# wake_skew_test.sh -- Layer-3 wake-protocol test policy.
#
# Runs the wake-sensitive test suite under conditions that make a (hypothetical)
# park/wake regression manifest DETERMINISTICALLY in test rather than as a
# 1-in-a-billion production hang.  Three levers (docs/dev/wake_protocol/README.md,
# Layer 3):
#
#   1. cache-line padding ON  -- the runloom_hub_t padding (B4) is the DEFAULT
#      build, so false-sharing no longer hides a missing fence.  (No flag needed.)
#   2. wake-skew injection    -- build with -DRUNLOOM_WAKE_SKEW so a few
#      sched_yield()s widen the park/wake Dekker store..fence..load window; a
#      logical handshake race that would normally interleave once in a billion
#      now interleaves on nearly every park.  Intensity = RUNLOOM_WAKE_SKEW (yields
#      per skew point, default 4).
#   3. TSan (separate)        -- tools/run_sanitizers_ext.sh / check_all exttsan
#      exposes the fences the hardware otherwise hides; run that too.
#
# A lost-wake regression shows up here as a HANG -> a per-test timeout catches it
# (a wedged 0.5s test cannot legitimately take 60s).  Exit non-zero on any
# hang/failure.
#
# Usage: tools/wake_skew_test.sh [skew_intensity] [reps]
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
PY="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
SKEW="${1:-8}"
REPS="${2:-3}"
TIMEOUT="${WAKE_SKEW_TIMEOUT:-60}"

echo "== Layer-3 wake-skew test policy =="
echo "   padding: ON (default build)   skew: RUNLOOM_WAKE_SKEW=$SKEW   reps: $REPS   per-test timeout: ${TIMEOUT}s"

echo "-- building with -DRUNLOOM_WAKE_SKEW --"
RUNLOOM_EXTRA_CFLAGS="-DRUNLOOM_WAKE_SKEW" PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/tmp/wake_skew_build.log 2>&1 \
    || { echo "BUILD FAILED -- see /tmp/wake_skew_build.log"; exit 1; }

# The wake-sensitive surface: park/wake, M:N scheduling, blockpool offload,
# channels, netpoll readiness, timed parks, and the cross-thread wakers.
TESTS="test_mn_park test_mn test_unpark_many test_timeout_no_waker
       test_critical_section_park test_event_inmem_park test_blocking
       test_scheduler_channel_compat test_cov_netpoll test_adv_sched
       test_swarm_mn_sched test_swarm_blockpool_diag_crash"

fail=0
rep=1
while [ "$rep" -le "$REPS" ]; do
    echo "-- rep $rep/$REPS --"
    for t in $TESTS; do
        [ -f "tests/$t.py" ] || continue
        # RUNLOOM_STEAL_WOKEN=1 exercises the global-runq wake_state path too.
        if timeout "$TIMEOUT" env RUNLOOM_WAKE_SKEW="$SKEW" RUNLOOM_STEAL_WOKEN=1 \
               PYTHON_GIL=0 "$PY" tests/run_isolated.py "tests/$t.py" >/tmp/wake_skew_$t.log 2>&1; then
            : # passed
        else
            rc=$?
            if [ "$rc" -eq 124 ]; then
                echo "   HANG (timeout ${TIMEOUT}s): $t  -- a lost-wake regression? see /tmp/wake_skew_$t.log"
            else
                echo "   FAIL (rc=$rc): $t  -- see /tmp/wake_skew_$t.log"
            fi
            fail=$((fail + 1))
        fi
    done
    rep=$((rep + 1))
done

if [ "$fail" -ne 0 ]; then
    echo "WAKE-SKEW: $fail test run(s) hung/failed under skew=$SKEW."
    exit 1
fi
echo "WAKE-SKEW OK: all wake-sensitive tests made progress under skew=$SKEW x$REPS reps."
