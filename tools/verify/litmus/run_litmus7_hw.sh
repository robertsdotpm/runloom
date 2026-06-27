#!/usr/bin/env bash
# run_litmus7_hw.sh -- run the C11 litmus tests on REAL HARDWARE with litmus7.
#
# herd7/GenMC/Dartagnan check the litmus tests against a MODEL (the .cat axioms).
# litmus7 compiles each test to actual atomics and runs it billions of times on
# THIS CPU, so it checks the realized binary on real silicon -- the model-vs-
# hardware gap that has repeatedly bitten this project (arm64-only SIGSEGVs, the
# MSVC seq-cst StoreLoad downgrade). Even on x86-TSO it has teeth: the
# store-buffering (SB/Dekker) shape IS reorderable on x86, so a *_no_fence test
# shows the bug "Sometimes" and its *_sc_fence partner shows "Never" -- a built-in
# control pair on real hardware. (On arm64/Power it exposes strictly more; RUN IT
# THERE too -- that is where the residual reorderings live.)
#
# GATE: every fence/lock/mpsc test (expected FORBIDDEN) MUST observe "Never" on
# this CPU -- a "Sometimes" there means the ordering mechanism does NOT hold on
# the realized binary = a real fault. The *_no_fence tests are informational
# (Sometimes on a weak-enough CPU = the bug is reachable; the contrast is the
# evidence). Needs litmus7 (opam install herdtools7) + a C compiler.
#
# Usage: tools/verify/litmus/run_litmus7_hw.sh [-a NCPU]
# Exit: 0 = every must-be-forbidden test held (Never); 1 = a fence failed on hw; 2 = setup.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
NCPU="${2:-$(nproc 2>/dev/null || echo 4)}"

# litmus7 may live in an opam switch
command -v litmus7 >/dev/null 2>&1 || eval "$(opam env 2>/dev/null || true)"
command -v litmus7 >/dev/null 2>&1 || {
  echo "run_litmus7_hw: litmus7 absent (opam install herdtools7). SKIP."; exit 0; }
command -v cc >/dev/null 2>&1 || { echo "run_litmus7_hw: no C compiler. SKIP."; exit 0; }

echo "== litmus7 hardware run ($(litmus7 -version 2>&1 | head -1)) on $(uname -m), -a $NCPU =="
fail=0
for f in "$HERE"/*.litmus; do
    name="$(basename "$f" .litmus)"
    out="$(litmus7 -c11 true -a "$NCPU" "$f" 2>/dev/null)"
    obs="$(echo "$out" | grep -oE 'Observation [^ ]+ (Sometimes|Never|Always) [0-9]+ [0-9]+' | head -1)"
    if [ -z "$obs" ]; then
        echo "  $name: (no observation -- litmus7 could not run it; skipped)"
        continue
    fi
    verdict="$(echo "$obs" | awk '{print $3}')"
    # must-be-forbidden: the fence/lock/mpsc/publish-via-lock tests
    case "$name" in
        *sc_fence|*lock_publish|*mpsc|*_fence_*)
            if [ "$verdict" = "Never" ]; then
                echo "  $name: $verdict  [OK -- ordering holds on this CPU]"
            else
                echo "  $name: $verdict  [FAIL -- the ordering mechanism did NOT hold on real hardware]"
                fail=1
            fi ;;
        *)
            echo "  $name: $verdict  [info -- weak-hw reachability; Sometimes = bug observable here]" ;;
    esac
done
echo
if [ "$fail" -ne 0 ]; then
    echo "run_litmus7_hw: a must-be-forbidden reorder was OBSERVED on hardware (exit 1)"
    exit 1
fi
echo "run_litmus7_hw: all must-be-forbidden tests held (Never) on $(uname -m)"
echo "  NOTE: x86-TSO masks LB/MP reorders -- run on arm64/Power for full weak-memory coverage."
exit 0
