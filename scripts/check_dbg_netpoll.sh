#!/usr/bin/env bash
# check_dbg_netpoll.sh -- run the netpoll/mn/aio suite with the RUNLOOM_DBG_NETPOLL
# stale-arm tripwire armed everywhere (item 7, increment 2).
#
# The tripwire validates the arm-cache-vs-kernel skip path INLINE (one
# EPOLL_CTL_MOD per predicted skip), turning a silent stale-arm hang -- the
# stale-cache-vs-kernel class -- into a loud, self-healing diagnosis at the exact
# offending park.  It is a pure runtime env var (no rebuild).  Targeted fd-reuse
# tests already assert the self-heal; this lane runs the BROAD suite under it so
# stale-arm drift ANYWHERE surfaces, the way the ctxcheck lane runs the suite
# under the lock-rank checker.
#
# Excludes the tests that DELIBERATELY create stale arms (they'd log expected
# heals); a heal in the normal suite is drift.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
cd "$ROOT" || exit 2

TESTS="${DBGNP_TESTS:-test_tcpconn test_tcp_scenarios test_aio test_aio_net \
  test_netpoll_conformance test_mn test_mn_park test_concurrency \
  test_monkey_offload test_adv_tcpconn test_netpoll_arming}"
TESTS="$(for t in $TESTS; do printf '%s.py ' "$t"; done)"

echo "== dbg-netpoll: run broad suite with RUNLOOM_DBG_NETPOLL=1 (inline stale-arm check) =="
RUNLOOM_DBG_NETPOLL=1 PYTHON_GIL=0 PYTHONPATH=src "$PY" tests/run_isolated.py \
    -j"${DBGNP_JOBS:-6}" $TESTS
rc=$?
if [ "$rc" = 0 ]; then
    echo "== dbg-netpoll OK: no stale-arm-induced failure across the broad suite =="
else
    echo "== dbg-netpoll FAIL (rc=$rc): a stale arm / cache-vs-kernel drift was "
    echo "   caught inline -- a wait_fd skipped the re-ADD on a fd the kernel no "
    echo "   longer has registered. =="
fi
exit $rc
