#!/usr/bin/env bash
# run_demonic.sh -- the demonic-oracle netpoll-arm proof + its teeth.
# Proves (default) the arm/re-arm logic is lost-wake-free for EVERY epoll_ctl
# return; the two -D controls MUST fail (a passing control = a toothless proof).
set -u
H="$(dirname "$0")/netpoll_arm_demonic_cbmc.c"
U="--unwind 4 --unwinding-assertions"
pass=0
run() { # <label> <expect SUCCESS|FAILED> <extra defs...>
  local label="$1" expect="$2"; shift 2
  local got; got="$(cbmc "$H" "$@" $U 2>&1 | grep -oE 'VERIFICATION (SUCCESSFUL|FAILED)' | head -1)"
  if [ "$got" = "VERIFICATION $expect" ]; then echo "  OK   $label -> $got"
  else echo "  FAIL $label -> ${got:-<none>} (expected VERIFICATION $expect)"; pass=1; fi
}
echo "== demonic-oracle netpoll arm/re-arm (kernel-independent lost-wake proof) =="
run "shipped fix + recovery (no lost wake, all epoll_ctl returns)" SUCCESSFUL
run "control BUG_ARM_DROP (2026-07-02 migration bug)"             FAILED -DBUG_ARM_DROP
run "control NO_ADD_RECOVERY (isolates the ADD-fail dependency)"  FAILED -DNO_ADD_RECOVERY
[ $pass -eq 0 ] && echo "PASS: proof holds + both controls have teeth" || echo "FAILED"
exit $pass
