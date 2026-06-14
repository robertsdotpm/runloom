#!/bin/bash
# kqueue_cov.sh -- scoped BRANCH coverage of the macOS kqueue netpoll backend.
#
# Builds runloom_c instrumented (-fprofile-arcs -ftest-coverage -O0), runs the
# kqueue + netpoll test corpus to accumulate .gcda counters, then gcov -b -c the
# netpoll + tcp translation units and summarizes BRANCH coverage of the kqueue
# source files (the .inc fragments gcov emits via #include).  Leaves an
# instrumented .so -- rebuild a normal one with `setup.py build_ext --inplace
# --force` afterwards.
set -u
cd "$HOME/pygo-macwin" || exit 2
PY="$HOME/.pyenv/versions/3.13.13t/bin/python3"
export PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON_QUIET=1
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
ulimit -n 400000 2>/dev/null

OBJ=build/temp.kqcov
COVOUT=build/kqcov
rm -rf "$OBJ" "$COVOUT" src/runloom_c*.so build/lib.* ./*.gcov 2>/dev/null
mkdir -p "$COVOUT"

echo "[kqcov] building instrumented (-O0 --coverage) ..."
RUNLOOM_DEBUG=1 \
RUNLOOM_EXTRA_CFLAGS="-fprofile-arcs -ftest-coverage" \
RUNLOOM_EXTRA_LDFLAGS="-fprofile-arcs -ftest-coverage" \
"$PY" setup.py build_ext --inplace --build-temp "$OBJ" > "$COVOUT/build.log" 2>&1 \
  || { echo "[kqcov] BUILD FAILED"; tail -25 "$COVOUT/build.log"; exit 1; }

GCNODIR="$(dirname "$(find "$OBJ" -name 'netpoll.gcno' | head -1)")"
echo "[kqcov] gcno dir: $GCNODIR"

TESTS="
tests/test_kqueue_register.py tests/test_kqueue_readiness.py
tests/test_kqueue_eof_error.py tests/test_kqueue_wake_all.py
tests/test_kqueue_cancel.py tests/test_kqueue_mn_selfpipe.py
tests/test_kqueue_faults_ext.py tests/test_kqueue_crash_race.py
tests/test_kqueue_faultinject.py tests/test_netpoll_conformance.py
tests/test_netpoll_arming.py tests/test_netpoll_faultinject.py
tests/test_netpoll_pending_wake_recheck.py tests/test_unpark_many.py
tests/test_mn_park.py
"
echo "[kqcov] running kqueue + netpoll corpus ..."
gtimeout -k 10 900 "$PY" -m pytest $TESTS -q -p no:cacheprovider --no-header \
  > "$COVOUT/tests.log" 2>&1
echo "[kqcov] tests exit=$? :: $(tail -1 "$COVOUT/tests.log")"

echo "[kqcov] collecting gcov -b -c ..."
gcov -b -c -o "$GCNODIR" src/runloom_c/netpoll.c     >/dev/null 2>&1
gcov -b -c -o "$GCNODIR" src/runloom_c/runloom_tcp.c >/dev/null 2>&1
mv -f ./*.gcov "$COVOUT/" 2>/dev/null

echo "[kqcov] BRANCH coverage of kqueue source:"
"$PY" tools/kqueue_cov_parse.py "$COVOUT"
echo "[kqcov] per-file .gcov in $COVOUT/"
echo "[kqcov] NOTE: instrumented .so left in src/. Rebuild normal:"
echo "        $PY setup.py build_ext --inplace --force"
