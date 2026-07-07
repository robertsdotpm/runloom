#!/usr/bin/env bash
# check_ctxcheck.sh -- build with RUNLOOM_CTXCHECK=1 and run a fast slice under
# it, so the lock-order rank checker AND the park/yield-safety assert (item 10)
# actually run in CI.  Before this, the rank checker in runloom_lockrank.h was
# never compiled by any lane (setup.py had no flag; coverage showed the lines
# elided) -- it caught nothing.
#
# FAILS if the checker fires (a lock-order inversion, or a yield while holding a
# ranked lock / inside a no-yield region) OR if any test fails under the build.
# Rebuilds without the flag at the end so later phases get the normal .so.
#
# Env: CTX_TESTS=<space-separated test names> to override the slice.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
cd "$ROOT" || exit 2

# A slice that exercises the park/yield seams the assert guards: scheduler,
# channels, netpoll, aio, cross-thread.  Kept fast; the point is coverage of the
# yield sites, not the whole matrix.
TESTS="${CTX_TESTS:-test_mn test_mn_park test_adv_sched test_chan test_adv_chan \
  test_tcpconn test_adv_netpoll test_netpoll_conformance test_concurrency \
  test_aio test_adv_sync test_differential_asyncio}"

# A -D flag change does NOT retrigger setuptools recompilation (it only checks
# source mtimes), so force a clean object/.so sweep before each build or the
# flag silently no-ops and we test the wrong binary.
force_clean() { rm -f src/runloom_c/*.so 2>/dev/null;
                find build -name '*.o' -delete 2>/dev/null; }

echo "== ctxcheck: rebuild with RUNLOOM_CTXCHECK=1 =="
force_clean
if ! RUNLOOM_CTXCHECK=1 PYTHON_GIL=0 "$PY" setup.py build_ext --inplace \
        >/tmp/runloom_ctxcheck_build.log 2>&1; then
    echo "ctxcheck BUILD FAILED (see /tmp/runloom_ctxcheck_build.log)"
    tail -20 /tmp/runloom_ctxcheck_build.log
    exit 1
fi

echo "== ctxcheck: run slice, report mode (collect every fire in one pass) =="
ERR=/tmp/runloom_ctxcheck_run.err
PYTHON_GIL=0 PYTHONPATH=src "$PY" tests/run_isolated.py -j"${CTX_JOBS:-6}" $TESTS \
    >/tmp/runloom_ctxcheck_run.out 2>"$ERR"
test_rc=$?
tail -3 /tmp/runloom_ctxcheck_run.out

fires=$(grep -cE "runloom-lockrank|runloom-ctxcheck" "$ERR" 2>/dev/null)
rc=0
if [ "${fires:-0}" -gt 0 ]; then
    echo "ctxcheck FAIL: checker fired $fires time(s):"
    grep -E "runloom-lockrank|runloom-ctxcheck" "$ERR" | sort | uniq -c
    rc=1
fi
if [ "$test_rc" != "0" ]; then
    echo "ctxcheck FAIL: tests did not pass under the CTXCHECK build (rc=$test_rc)"
    rc=1
fi
[ "$rc" = 0 ] && echo "ctxcheck OK: no lock-order / unsafe-park violations; slice green."

echo "== ctxcheck: rebuild WITHOUT the flag (restore normal .so) =="
force_clean
RUNLOOM_CTXCHECK=0 PYTHON_GIL=0 "$PY" setup.py build_ext --inplace \
    >/tmp/runloom_ctxcheck_restore.log 2>&1 || {
        echo "WARN: restore build failed -- the in-place .so is still a CTXCHECK build"
        echo "      (see /tmp/runloom_ctxcheck_restore.log); rebuild before benching."; }
exit $rc
