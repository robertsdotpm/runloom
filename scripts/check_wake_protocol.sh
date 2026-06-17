#!/bin/sh
# check_wake_protocol.sh -- Layer-2 wake-protocol regression lint.
#
# The wake-protocol methodology (docs/dev/wake_protocol/) makes lost/dropped wakes
# structurally catchable: every live `wake_state` TRANSITION drives an explicit
# state AND carries a RUNLOOM_WS_NOTE(from,to), so a -DRUNLOOM_FSM_VALIDATE build
# aborts loudly on an illegal transition.  This lint fails the build if a NEW
# wake_state mutation lands WITHOUT its NOTE -- exactly the T6-class hazard (an
# unmodeled PARKED->RUNNING timer-claim that had no NOTE, invisible to VALIDATE).
#
# Invariant (robust to line distance / multi-line CAS, unlike a line window):
# in every TU that mutates wake_state, the number of RUNLOOM_WS_NOTE() calls must
# be >= the number of atomic wake_state TRANSITIONS.  A removed/forgotten NOTE
# drops the count below the transition count and fails the build.  Spawn-time
# INITIAL-state sets (a fresh g is "born RUNNING", no from-state to NOTE) are
# allow-listed by file.
#
# Run by scripts/check_all.sh; exits non-zero on a violation.
# Usage: scripts/check_wake_protocol.sh

set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
SRC="$ROOT/src/runloom_c"
MUT_RE='__atomic_(store_n|compare_exchange_n)\(&[A-Za-z_>.-]*wake_state'
violations=0

# Files that set wake_state only as a fresh g's INITIAL state (not a transition).
is_init_only() {
    case "$1" in
        *mn_sched_init_fini.c.inc) return 0 ;;
        *) return 1 ;;
    esac
}

# Every live TU (not the CBMC harness) that mutates wake_state.
files=$(grep -rlE "$MUT_RE" "$SRC"/*.c.inc "$SRC"/*.c 2>/dev/null | grep -v 'cbmc' || true)

for f in $files; do
    is_init_only "$f" && continue
    trans=$(grep -cE "$MUT_RE" "$f" || true)
    notes=$(grep -cE 'RUNLOOM_WS_NOTE\(' "$f" || true)
    if [ "$notes" -lt "$trans" ]; then
        printf 'WAKE-PROTOCOL LINT: %s has %s wake_state transition(s) but only %s RUNLOOM_WS_NOTE(): an unwitnessed transition.\n' \
               "$(basename "$f")" "$trans" "$notes"
        violations=$((violations + 1))
    fi
done

if [ "$violations" -ne 0 ]; then
    printf '\n%s TU(s) have an unwitnessed wake_state transition.\n' "$violations"
    printf 'Add RUNLOOM_WS_NOTE(from,to) at each new transition (or allow-list a\n'
    printf 'true INITIAL-state set). Rationale: docs/dev/wake_protocol/README.md\n'
    printf '(Layer 2); this lint exists because the T6 timer-claim had no NOTE.\n'
    exit 1
fi

echo "wake-protocol lint OK: every live wake_state transition is NOTE-witnessed."
