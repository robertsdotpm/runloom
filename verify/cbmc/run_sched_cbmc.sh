#!/usr/bin/env bash
# run_sched_cbmc.sh -- CBMC harnesses for the SINGLE-THREADED runloom_sched.c data
# structures: the per-sched ready FIFO ring (wraparound + grow) and the per-g
# tstate save/restore (completeness + cross-g isolation).  Each correct harness
# must be SUCCESSFUL; each negative control must be FAILED (proving teeth).
# Prints "N passed, M failed".
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
echo "-- CBMC (bounded) runloom_sched.c data structures --"
if ! command -v cbmc >/dev/null 2>&1; then
  echo "  (cbmc not found -- skipping; apt-get install cbmc)"; exit 0
fi
pass=0; fail=0
UNWIND="${RUNLOOM_CBMC_UNWIND:-10}"

want_ok()  { # file [flags]
  printf '  [cbmc] %-44s ' "$(basename "$1") ${2:-} (expect SUCCESSFUL)"
  if cbmc "$HERE/$1" $2 --unwind "$UNWIND" --unwinding-assertions 2>&1 \
        | grep -q "VERIFICATION SUCCESSFUL"; then echo PASS; pass=$((pass+1));
  else echo FAIL; fail=$((fail+1)); fi; }
want_bug() { # file flags label
  printf '  [cbmc] %-44s ' "$(basename "$1") $2 ($3)"
  if cbmc "$HERE/$1" $2 --unwind "$UNWIND" --unwinding-assertions 2>&1 \
        | grep -q "VERIFICATION FAILED"; then echo "PASS (correctly trips)"; pass=$((pass+1));
  else echo "FAIL (bug not caught!)"; fail=$((fail+1)); fi; }

# ready FIFO ring: FIFO order / no loss / no dup across wraparound + grow
want_ok  sched_readyring_cbmc.c ""
want_bug sched_readyring_cbmc.c "-DBUG_GROW_NOOFFSET" "grow drops head offset -> reorder/loss"
want_bug sched_readyring_cbmc.c "-DBUG_NO_CAPCHECK"   "push skips full check -> overwrite"

# per-g tstate save/restore: completeness + cross-g isolation
want_ok  sched_pystate_cbmc.c ""
want_bug sched_pystate_cbmc.c "-DBUG_DROP_FIELD" "load forgets a field -> cross-g leak"

# default-path queue-membership + refcount: a stale wake racing completion must
# not leave a queue entry pointing at a freed g (the per-hub-kqueue arm64 UAF).
# The FIX (try_incref before touching g) is correct; BOTH the naive queue ref
# (incref AFTER the in_sub_queue CAS) and the original no-ref code have the UAF.
want_ok  sched_qref_cbmc.c ""
want_bug sched_qref_cbmc.c "-DBUG_INCREF_AFTER_CAS" "naive queue ref (incref after CAS) -> stale-wake UAF window"
want_bug sched_qref_cbmc.c "-DBUG_NO_QUEUE_REF"     "no queue ref -> stale-wake-after-completion UAF (original)"

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
