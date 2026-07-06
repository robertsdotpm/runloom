#!/usr/bin/env bash
# run_steal_cbmc.sh -- drive sched_steal_cbmc.c: the work-stealing consume path's
# refcount safety (the piece chase_lev_real.c + sched_qref_cbmc.c did not span
# together -- TWO potential consumers of one deque g, queue ref dropped once).
set -u
H="$(dirname "$0")/sched_steal_cbmc.c"
U="--unwind 3 --unwinding-assertions"
fail=0
run() { local label="$1" expect="$2"; shift 2
  local got; got="$(cbmc "$@" $U "$H" 2>&1 | grep -oE 'VERIFICATION (SUCCESSFUL|FAILED)' | head -1)"
  if [ "$got" = "VERIFICATION $expect" ]; then echo "  as-expected [$expect]  $label"
  else echo "  UNEXPECTED  $label -> ${got:-<none>} (wanted $expect)"; fail=1; fi
}
echo "== work-stealing consume: owner-take vs thief-steal, queue ref dropped once =="
run "exactly-once claim -> no UAF, refcount>=0"            SUCCESSFUL
run "BUG_DOUBLE_CONSUME (both consume -> double-decref UAF)" FAILED -DBUG_DOUBLE_CONSUME
[ $fail -eq 0 ] && echo "STATUS: steal consume ref-safe (Chase-Lev exactly-once + queue-ref protocol)" \
                || echo "STATUS: steal consume harness drifted"
exit $fail
