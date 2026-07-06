#!/usr/bin/env bash
# sweep_all.sh [--limit N] [TU...] -- run the counted-exhaustive fault sweep
# across EVERY instrumentable TU, not just netpoll (item 4).
#
# For each TU: build_faultsites.sh instruments every fallible call site with a
# runtime-selectable realistic-errno fault, then fault_sweep.py enables one site
# at a time and runs that TU's affinity test subset.  A site is HANDLED if a test
# fails/hangs when its call is forced to fail; UNCHECKED (a survivor) if nothing
# noticed -- an error path that is silently swallowed or has no test.  The
# survivors are the error paths worth reading.
#
# The full sweep is HOURS (build + one test-subset run per site x hundreds of
# sites x several TUs) -- meant for the periodic/nightly lane, not per-commit.
# --limit N caps sites per TU for a fast smoke that proves the pipeline end to
# end.  Resumable: fault_sweep.py checkpoints per site, so a re-run continues.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
MAIN="$(cd "$HERE/../../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"

LIMIT=""
if [ "${1:-}" = "--limit" ]; then LIMIT="--limit $2"; shift 2; fi
TUS=("$@")
[ ${#TUS[@]} -eq 0 ] && TUS=(netpoll mn_sched io_uring runloom_tcp)

echo "== counted fault sweep across ${#TUS[@]} TU(s): ${TUS[*]} =="
[ -n "$LIMIT" ] && echo "   (smoke mode: $LIMIT sites per TU)"
rc=0
for tu in "${TUS[@]}"; do
    echo "--- [$tu] instrument every fallible call site ---"
    if ! bash "$HERE/build_faultsites.sh" "$tu" > "/tmp/runloom_fs_${tu}_build.log" 2>&1; then
        echo "   BUILD/INSTRUMENT FAILED for $tu (see /tmp/runloom_fs_${tu}_build.log)"
        tail -8 "/tmp/runloom_fs_${tu}_build.log"
        rc=1
        continue
    fi
    echo "--- [$tu] sweep sites ---"
    # non-zero exit from fault_sweep just means survivors were found; the report
    # is the deliverable, so don't fail the whole driver on survivors alone.
    "$PY" "$HERE/fault_sweep.py" "$tu" $LIMIT 2>&1 | tail -4
    WT="${RUNLOOM_MUT_WORKTREE:-$HOME/projects/pygo-mutants}"
    rep="$WT/src/runloom_c/${tu}.unchecked_errors.txt"
    [ -f "$rep" ] && echo "   survivors report: $rep"
done
echo "== sweep_all done (rc=$rc) =="
exit $rc
