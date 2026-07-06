#!/usr/bin/env bash
# run_demonic.sh -- demonic-oracle netpoll-arm harness. After the 2026-07-06
# adversarial audit, the DEFAULT is the FAITHFUL model of register() and FAILS,
# exposing a suspected lost-wake window (migration DEL-ok + ADD-fail). This is a
# LEAD, not a settled proof: it stays RED until either the runtime is shown to
# recover the window (then model that recovery faithfully) or the code is fixed.
set -u
H="$(dirname "$0")/netpoll_arm_demonic_cbmc.c"
U="--unwind 4 --unwinding-assertions"
fail=0
run() { local label="$1" expect="$2"; shift 2
  local got; got="$(cbmc "$H" "$@" $U 2>&1 | grep -oE 'VERIFICATION (SUCCESSFUL|FAILED)' | head -1)"
  if [ "$got" = "VERIFICATION $expect" ]; then echo "  as-expected [$expect]  $label"
  else echo "  UNEXPECTED  $label -> ${got:-<none>} (wanted $expect)"; fail=1; fi
}
echo "== demonic-oracle netpoll arm/re-arm (kernel-independent) =="
run "faithful register() -> exposes the DEL-ok+ADD-fail window"      FAILED
run "BUG_ARM_DROP control (2026-07-02 migration bug)"                FAILED -DBUG_ARM_DROP
run "ASSUME_ADD_RECOVERY -> window closes IFF that recovery is real" SUCCESSFUL -DASSUME_ADD_RECOVERY
[ $fail -eq 0 ] && echo "STATUS: harness behaves as audited (default RED = open lead)" || echo "STATUS: harness drifted from audited behaviour"
exit $fail
