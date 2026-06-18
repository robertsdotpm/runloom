#!/usr/bin/env bash
# check_all_fast.sh -- the routine PRE-MERGE gate.
#
# Runs the quick correctness phases PLUS a fast formal-verification lane: every
# Spin model + every cheap CBMC proof, parallelised across the box's cores, but
# SKIPPING the 3 genuinely slow CBMC proofs (the Chase-Lev concurrent deque
# ~148s, the INV_race disjointness monitor ~5-10min, and the timer min-heap
# ~76s).  Those run only in check_all_extensive.  Net: ~all formal coverage as a
# sub-minute smoke gate on top of the test suite.
#
# AGENTS: run this before proposing a merge.  Run check_all_extensive before a
# risky/large merge, or periodically -- and the self-hosted CI runner exercises
# the full matrix post-merge.
#
# Usage:  scripts/check_all_fast.sh
# Env:    same as check_all.sh (PYTHON=..., VERIFY_JOBS=N, ...)
exec "$(dirname "$0")/check_all.sh" tests mn replay lincheck dst ctest verify-fast
