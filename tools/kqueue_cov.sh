#!/bin/bash
# kqueue_cov.sh -- scoped BRANCH coverage of the macOS kqueue netpoll backend.
#
# Builds runloom_c instrumented (-fprofile-arcs -ftest-coverage -O0), then runs
# the kqueue + netpoll corpus ONE module per pytest process (a single 15-module
# process is too slow at -O0 and a timeout-kill flushes no .gcda; per-module each
# clean exit ACCUMULATES its counters).  Then gcov -b -c + branch summary of the
# kqueue source.  Leaves an instrumented .so -- rebuild normal with
# `setup.py build_ext --inplace --force` afterwards.
set -u
cd "$HOME/pygo-macwin" || exit 2
PY="$HOME/.pyenv/versions/3.14.4t/bin/python3"
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

MODS="test_kqueue_register test_kqueue_readiness test_kqueue_eof_error
test_kqueue_wake_all test_kqueue_cancel test_kqueue_diag_signal
test_kqueue_mn_selfpipe test_kqueue_faults_ext test_kqueue_faultinject
test_netpoll_conformance test_netpoll_arming test_netpoll_faultinject
test_netpoll_pending_wake_recheck test_unpark_many test_mn_park
test_kqueue_crash_race"

for m in $MODS; do
  to=300; [ "$m" = test_kqueue_crash_race ] && to=700
  gtimeout -k 10 "$to" "$PY" -m pytest tests/$m.py -q -p no:cacheprovider --no-header \
    > "$COVOUT/$m.log" 2>&1
  echo "[kqcov] $m exit=$? :: $(tail -1 "$COVOUT/$m.log")"
done

rm -f ./*.gcov
gcov -b -c -o "$GCNODIR" src/runloom_c/netpoll.c     >/dev/null 2>&1
gcov -b -c -o "$GCNODIR" src/runloom_c/runloom_tcp.c >/dev/null 2>&1
mv -f ./*.gcov "$COVOUT/" 2>/dev/null

echo "=== BRANCH coverage of kqueue source ==="
"$PY" tools/kqueue_cov_parse.py "$COVOUT"
