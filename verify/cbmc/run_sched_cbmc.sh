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

# per-g tstate snapshot REFERENCE OWNERSHIP (companion to the completeness proof):
# every owned ref snap acquires is released exactly once by load XOR clear; the
# immortal-context skip and the raw delete_later chain are the bug-prone cases.
want_ok  snap_refown_cbmc.c ""
want_bug snap_refown_cbmc.c "-DBUG_LOAD_FORGETS_FIELD"      "load forgets to release a field -> leak"
want_bug snap_refown_cbmc.c "-DBUG_INCREF_IMMORTAL"        "snap increfs an immortal context -> the no-op decref can't release it -> leak"
want_bug snap_refown_cbmc.c "-DBUG_DELETE_LATER_REFCOUNTED" "the raw delete_later chain is refcounted -> dying-object corruption"

# g slab recycle field-clear: every pre-id byte is cleared/overwritten by the
# two-part scrub (no stale pass_index/arena/wake_state across recycling).  The
# coverage loop runs offsetof(id) (~88) iterations, so it needs a larger unwind
# than the shared default; the bound is a compile-time constant so the unwinding
# assertion still holds.  Negative control inserts a field into the [state,arena)
# gap that the scrub misses.
SLAB_UNWIND=256
want_ok_slab()  {
  printf '  [cbmc] %-44s ' "g_slab_recycle_cbmc.c (expect SUCCESSFUL)"
  if cbmc "$HERE/g_slab_recycle_cbmc.c" --unwind "$SLAB_UNWIND" --unwinding-assertions 2>&1 \
        | grep -q "VERIFICATION SUCCESSFUL"; then echo PASS; pass=$((pass+1));
  else echo FAIL; fail=$((fail+1)); fi; }
want_bug_slab() {
  printf '  [cbmc] %-44s ' "g_slab_recycle_cbmc.c -DBUG_GAP_AFTER_STATE (gap leaks a stale byte)"
  if cbmc "$HERE/g_slab_recycle_cbmc.c" -DBUG_GAP_AFTER_STATE --unwind "$SLAB_UNWIND" --unwinding-assertions 2>&1 \
        | grep -q "VERIFICATION FAILED"; then echo "PASS (correctly trips)"; pass=$((pass+1));
  else echo "FAIL (bug not caught!)"; fail=$((fail+1)); fi; }
want_ok_slab
want_bug_slab

# datastack chunk-pool alias: runloom's pool reuses _PyStackChunk.previous as its
# free-list link -- the SAME field CPython's data-stack chain walks + frees.  A
# pooled chunk must never be reachable from the live datastack_chunk via ->previous
# (else PopFrame arena-frees / re-owns a pooled chunk = double-owned UAF).  Two
# guards: pool_get SEVERs `previous`, pool_install ROOT-SKIPs to data[1].
want_ok  chunk_pool_alias_cbmc.c ""
want_bug chunk_pool_alias_cbmc.c "-DBUG_NO_SEVER"     "pool_get keeps the free-list link -> live chain walks into the pool"
want_bug chunk_pool_alias_cbmc.c "-DBUG_NO_ROOT_SKIP" "install starts at data[0] -> the root chunk is popped -> walk-to-previous fires"

# max-fibers admission slot: every counted admit is released exactly once across the
# spawn exit paths (rejected/uncounted/coro-fail/tstate-fail/success) -> live_g back
# to 0 at quiescence, never above the cap.  Overlapping in-flight fibers exercise the
# rejection path (where a missed back-out leaks a slot).
want_ok  fiber_admit_cbmc.c ""
want_bug fiber_admit_cbmc.c "-DBUG_NO_BACKOUT"     "over-limit admit doesn't back out -> live_g leaks (cap ratchets down)"
want_bug fiber_admit_cbmc.c "-DBUG_DOUBLE_RELEASE" "release ignores limit_counted -> an uncounted fiber underflows the slot"
want_bug fiber_admit_cbmc.c "-DBUG_BULK_COUNTED"   "a bulk go_n fiber wrongly counted -> phantom release / underflow"

# channel PyObject ref conservation: a sent value takes one Py_INCREF and is
# released exactly once -- recv-consume / close-drop (parked sender) / free-drain
# (buffer) -- never leaked, never over-freed.
want_ok  chan_refflow_cbmc.c ""
want_bug chan_refflow_cbmc.c "-DBUG_CLOSE_NO_SENDER_DROP" "close forgets the parked-sender Py_DECREF -> value leaks"
want_bug chan_refflow_cbmc.c "-DBUG_FREE_NO_BUFFER_DRAIN" "final decref frees without draining the buffer -> values leak"
want_bug chan_refflow_cbmc.c "-DBUG_DOUBLE_CONSUME"       "a consumed value is dropped again -> refcount negative (over-free)"

# Drift-guard: the proof (and the real scrub's part-2 start) assume `arena`
# immediately follows the atomic `state` byte -- a field inserted into that gap
# would silently leak across recycling (the stale-pass_index class).  Fail if a
# field declaration appears between `state` and `arena` in the real header.
ROOT="$(cd "$HERE/../.." && pwd)"
HDR="$ROOT/src/runloom_c/runloom_sched.h"
printf '  [cbmc] %-44s ' "drift-guard: state->arena adjacency in struct"
if [ -f "$HDR" ]; then
  gap="$(awk '/unsigned char state;/{f=1;next} /unsigned char arena;/{f=0} f' "$HDR" \
         | grep -E '^[[:space:]]+[A-Za-z_].*;[[:space:]]*$' | grep -vE '^[[:space:]]*(\*|/\*|//)')"
  if [ -z "$gap" ]; then echo "PASS (no field inserted in [state,arena) gap)"; pass=$((pass+1));
  else echo "FAIL (field inserted between state and arena -- update the proof!)"; echo "$gap"; fail=$((fail+1)); fi
else echo "SKIP (header not found)"; fi

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
