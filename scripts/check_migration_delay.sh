#!/usr/bin/env bash
# check_migration_delay.sh -- exercise the tstate-MIGRATION perturbation net
# (Gap 2 of the interleaving-verification audit).
#
# RUNLOOM_DLY_SNAP_SAVE / SNAP_LOAD / HANDOFF_ADOPT were declared enum slots with
# ZERO call sites -- an advertised-but-dead widener exactly on the windows a
# handle-substrate migration (item 3) touches.  They are now wired
# (runloom_sched_pystate.c.inc snap/load, mn_sched_hub_main.c.inc cross-hub
# adopt, mn_sched_mn_api.c.inc g-resurrection window).
#
# NOTE on RUNLOOM_DELAY: it is a numeric SEED, not a site selector -- setting it
# enables the delay injector at EVERY wired site (runloom_diag.c: strtoull(seed)
# + a global on-flag), which is even more thorough than perturbing one window.
# The migration/resurrection sites fire alongside WORLD_YIELD/CORO_*; a
# snap/load/adopt/resurrect reorder then surfaces as a failure/hang instead of a
# 1-in-a-million production crash.  Pure runtime env vars (no rebuild).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
cd "$ROOT" || exit 2

TESTS="${MIGDLY_TESTS:-test_mn test_mn_park test_concurrency test_freethread_stress \
  test_swarm_mn_sched test_swarm_coro_stack test_gc_fibers test_stack_frames}"
TESTS="$(for t in $TESTS; do printf '%s.py ' "$t"; done)"

echo "== migration-delay: mn/pystate stress with ALL delay sites armed (seed ${MIGDLY_SEED:-1}) =="
RUNLOOM_DELAY="${MIGDLY_SEED:-1}" \
RUNLOOM_DELAY_MAX_NS="${MIGDLY_MAX_NS:-2000}" \
PYTHON_GIL=0 PYTHONPATH=src "$PY" tests/run_isolated.py -j"${MIGDLY_JOBS:-4}" $TESTS
rc=$?
if [ "$rc" = 0 ]; then
    echo "== migration-delay OK: snap/load/adopt windows survive perturbation =="
else
    echo "== migration-delay FAIL (rc=$rc): a snap-save/load or cross-hub adopt "
    echo "   reorder surfaced under RUNLOOM_DELAY -- a migration-window race. =="
fi
exit $rc
