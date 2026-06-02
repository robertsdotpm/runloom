#!/usr/bin/env bash
# run_sweep.sh -- generate the park/wake memory_order lattice and run every
# variant through herd7, then report the WEAKEST fence that forbids the lost
# wakeup.  Automated necessity+sufficiency proof for the seq_cst StoreLoad
# fence (src/pygo_core/pygo_sched.c:1363), generalising the two hand-written
# parkwake_{no_fence,sc_fence}.litmus endpoints to the full 12-point lattice.
#
# Needs herd7 (herdtools7).  Install: `opam install herdtools7`.
# Run: verify/litmus/sweep/run_sweep.sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python3
HERD="${HERD7:-herd7}"
command -v "$HERD" >/dev/null 2>&1 || HERD="$HOME/.opam/herd/bin/herd7"

green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

if ! command -v "$HERD" >/dev/null 2>&1; then
    echo "  (herd7 not found -- skipping fence sweep;  opam install herdtools7)"
    exit 0
fi

GEN="$(mktemp -d /tmp/pygo_fencesweep.XXXXXX)"
"$PY" "$HERE/gen_sweep.py" "$GEN" >/dev/null

echo "-- park/wake fence-order sweep (herd7, RC11) --"
printf '   %-9s %-9s %-9s  %s\n' store load fence observation
sc_all_never=1     # are ALL seq_cst-fence rows Never?
weaker_any_never=0 # is ANY non-seq_cst row Never?
rc=0
for f in "$GEN"/parkwake_sweep_*.litmus; do
    base="$(basename "$f" .litmus)"      # parkwake_sweep_<store>_<load>_<fence>
    rest="${base#parkwake_sweep_}"
    store="${rest%%_*}"; rest2="${rest#*_}"
    load="${rest2%%_*}"; fence="${rest2#*_}"
    obs="$("$HERD" "$f" 2>/dev/null | awk '/^Observation/ {print $3}')"
    printf '   %-9s %-9s %-9s  ' "$store" "$load" "$fence"
    if [ "$obs" = "Never" ]; then
        green "Never"; echo "  (forbids the lost wakeup)"
        [ "$fence" != "scfence" ] && weaker_any_never=1
    else
        red "${obs:-?}"; echo "  (lost wakeup REACHABLE)"
        [ "$fence" = "scfence" ] && sc_all_never=0
    fi
done

"$(command -v safe-rm || echo rm)" -rf "$GEN" 2>/dev/null
echo "----------------------------------------------------------"
if [ "$sc_all_never" = 1 ] && [ "$weaker_any_never" = 0 ]; then
    echo "  CONCLUSION: the seq_cst StoreLoad fence is NECESSARY and SUFFICIENT --"
    echo "  it is the only fence that forbids the lost wakeup, and it does so even"
    echo "  with relaxed store/load (release/acquire and release fences do not)."
else
    echo "  UNEXPECTED lattice (sc_all_never=$sc_all_never weaker_any_never=$weaker_any_never)"
    echo "  -- a weaker fence forbade it, or seq_cst did not.  Investigate."
    rc=1
fi
exit $rc
