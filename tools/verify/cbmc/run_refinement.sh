#!/usr/bin/env bash
# run_refinement.sh -- drive netpoll_refinement_cbmc.c and check every config
# behaves as audited: the faithful default VERIFIES (the two-ledger refinement
# invariant holds across the whole fd lifecycle) and each out-of-band negative
# control FAILS (teeth -- the letter is really modelled).  A control that
# VERIFIES is a VACUOUS assertion, the exact trap the demonic harness fell into.
set -u
H="$(dirname "$0")/netpoll_refinement_cbmc.c"
U="--unwind 6 --unwinding-assertions"
fail=0
run() { local label="$1" expect="$2"; shift 2
  local got; got="$(cbmc "$H" "$@" $U 2>&1 | grep -oE 'VERIFICATION (SUCCESSFUL|FAILED)' | head -1)"
  if [ "$got" = "VERIFICATION $expect" ]; then echo "  as-expected [$expect]  $label"
  else echo "  UNEXPECTED  $label -> ${got:-<none>} (wanted $expect)"; fail=1; fi
}
echo "== two-ledger refinement: arm cache vs kernel epoll, demonic OOB alphabet =="
run "faithful default -> refinement invariant holds"          SUCCESSFUL
run "BUG_CLOSE_NO_INVALIDATE (close leaves stale arm)"        FAILED -DBUG_CLOSE_NO_INVALIDATE
run "BUG_FORK_NO_RESET (child keeps parent cache)"            FAILED -DBUG_FORK_NO_RESET
run "BUG_REUSE_NO_CLEAR (reused fd keeps stale arm)"          FAILED -DBUG_REUSE_NO_CLEAR
run "BUG_MODESWITCH_NO_REPUMP (fd left in unpumped epoll)"    FAILED -DBUG_MODESWITCH_NO_REPUMP
[ $fail -eq 0 ] && echo "STATUS: refinement harness behaves as audited (default GREEN, all controls RED)" \
                || echo "STATUS: refinement harness drifted -- a control went vacuous or the model regressed"
exit $fail
