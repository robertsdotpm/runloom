#!/bin/bash
# kqueue_cov_run.sh -- run the kqueue+netpoll corpus against an ALREADY-BUILT
# instrumented extension, ONE module per pytest process so each clean exit
# flushes + ACCUMULATES its .gcda (a single 15-module process is too slow at -O0
# and a timeout-kill flushes nothing).  Then gcov -b -c + branch summary.
set -u
cd "$HOME/pygo-macwin" || exit 2
PY="$HOME/.pyenv/versions/3.14.4t/bin/python3"
export PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_SYSMON_QUIET=1
sudo -n prlimit --pid $$ --nofile=8388608:8388608 2>/dev/null
ulimit -n 400000 2>/dev/null

OBJ=build/temp.kqcov
COVOUT=build/kqcov
mkdir -p "$COVOUT"
GCNODIR="$(dirname "$(find "$OBJ" -name 'netpoll.gcno' | head -1)")"
[ -z "$GCNODIR" ] && { echo "no .gcno -- run kqueue_cov.sh first to build instrumented"; exit 1; }
echo "[run] gcno dir: $GCNODIR"
find "$OBJ" -name '*.gcda' -delete 2>/dev/null     # fresh accumulation

MODS="test_kqueue_register test_kqueue_readiness test_kqueue_eof_error
test_kqueue_wake_all test_kqueue_cancel test_kqueue_mn_selfpipe
test_kqueue_faults_ext test_kqueue_faultinject
test_netpoll_conformance test_netpoll_arming test_netpoll_faultinject
test_netpoll_pending_wake_recheck test_unpark_many test_mn_park
test_kqueue_crash_race"

for m in $MODS; do
  to=300; [ "$m" = test_kqueue_crash_race ] && to=700
  gtimeout -k 10 "$to" "$PY" -m pytest tests/$m.py -q -p no:cacheprovider --no-header \
    > "$COVOUT/$m.log" 2>&1
  echo "[run] $m exit=$? :: $(tail -1 "$COVOUT/$m.log")"
done

rm -f ./*.gcov
gcov -b -c -o "$GCNODIR" src/runloom_c/netpoll.c     >/dev/null 2>&1
gcov -b -c -o "$GCNODIR" src/runloom_c/runloom_tcp.c >/dev/null 2>&1
mv -f ./*.gcov "$COVOUT/" 2>/dev/null

echo "=== BRANCH coverage of kqueue source ==="
"$PY" tools/kqueue_cov_parse.py "$COVOUT"
