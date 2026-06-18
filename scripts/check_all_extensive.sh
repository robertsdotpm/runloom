#!/usr/bin/env bash
# check_all_extensive.sh -- the thorough lane: EVERY correctness layer including
# the static analyzer, all C sanitizers, the whole-ext ThreadSanitizer run, and
# the COMPLETE formal-verification suite (all Spin models + every CBMC proof,
# including the 3 slow ones check_all_fast skips).  The verify phase is now
# parallelised (VERIFY_JOBS, default nproc), so this is much faster than the old
# serial run -- but its wall-clock floor is the single slowest proof (the
# INV_race disjoint monitor, a few minutes), which no parallelism removes.
#
# Run this before a risky/large merge or periodically; check_all_fast is the
# routine pre-merge gate.
#
# Usage:  scripts/check_all_extensive.sh
# Env:    same as check_all.sh (PYTHON=..., VERIFY_JOBS=N, ...)
exec "$(dirname "$0")/check_all.sh" all
