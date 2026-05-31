#!/usr/bin/env bash
# run_litmus.sh -- run the herd7 C11 weak-memory litmus tests and check each
# reaches its expected outcome.  These probe the FENCE PLACEMENT (the actual
# C11 memory_order on the netpoll commit / wake-list paths), which the Spin
# models (sequentially consistent) deliberately do NOT cover.
#
# Needs herd7 (herdtools7).  Install: `opam install herdtools7`.
#
# Outcomes:
#   commit_cas_then_publish  Sometimes  -- the BUG: relying on the commit-CAS
#       acquire ALONE (no pool->lock), the aborting g CAN read a stale ready_out
#       on a weak model.  This MUST be reachable -> proves the lock is needed.
#   commit_lock_publish      Never      -- the FIX: the pool->lock round-trip
#       (release unlock / acquire lock) makes the publish visible.  Forbidden.
#   wakelist_mpsc            Never      -- cross-thread wake_list handoff under
#       wake_list_lock carries the g's state correctly.  Forbidden.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
HERD="${HERD7:-herd7}"
command -v "$HERD" >/dev/null 2>&1 || HERD="$HOME/.opam/herd/bin/herd7"

pass=0; fail=0
green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

if ! command -v "$HERD" >/dev/null 2>&1; then
    echo "  (herd7 not found -- skipping litmus;  opam install herdtools7)"
    exit 0
fi

# name, expected-observation (Never|Sometimes|Always), description
check() {
    local name="$1" want="$2" desc="$3"
    printf '  [herd7] %-26s ' "$name"
    local obs
    obs="$("$HERD" "$HERE/$name.litmus" 2>/dev/null \
           | awk '/^Observation/ {print $3}')"
    if [ "$obs" = "$want" ]; then
        green "PASS"; echo " ($obs) -- $desc"; pass=$((pass+1))
    else
        red "FAIL"; echo " (got '$obs', want '$want') -- $desc"; fail=$((fail+1))
    fi
}

echo "-- herd7 (C11/RC11 weak-memory litmus) --"
check commit_cas_then_publish Sometimes \
      "commit-CAS acquire ALONE allows a stale ready_out read (lock is needed)"
check commit_lock_publish Never \
      "pool->lock round-trip makes the publish visible (no stale read)"
check wakelist_mpsc Never \
      "cross-thread wake_list handoff carries g state (no stale read)"

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
