#!/usr/bin/env bash
# check_lin.sh -- linearizability pipeline for runloom channels.
#
#   1. record a concurrent send/recv/close history from a real M:N run
#      -- twice: once with plain recv consumers, once with select() consumers;
#   2. check both against the sequential FIFO-channel spec with Porcupine
#      (expect LINEARIZABLE).  The select run proves select-recv linearizes
#      identically to recv while driving chan.c's multi-waiter Phase-2 path;
#   3. teeth: corrupt the history (phantom delivery) and re-check
#      (expect NOT LINEARIZABLE);
#   4. run the stateful Hypothesis model of the channel API (send/recv/close
#      plus a genuine two-channel select rule).
#
# Usage:  tools/lincheck/check_lin.sh
# Env:    PYTHON=...  interpreter (default: free-threaded 3.13t if present)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.14.4t/bin/python3" python3.13t python3; do
        command -v "$cand" >/dev/null 2>&1 && { PYTHON="$cand"; break; }
    done
fi
export PYTHON PYTHON_GIL=0
RM="$(command -v safe-rm || echo rm)"
HIST="$(mktemp /tmp/runloom_hist.XXXX.json)"
HSEL="$(mktemp /tmp/runloom_hist_sel.XXXX.json)"
BAD="$(mktemp /tmp/runloom_hist_bad.XXXX.json)"
rc=0

echo "== 1a. record concurrent history -- plain recv consumers (real M:N) =="
PYTHONPATH="$ROOT/src" "$PYTHON" "$HERE/record_history.py" "$HIST" 4 3 8 2 0 || rc=1
echo "== 1b. record concurrent history -- select() consumers (real M:N) =="
# All 3 consumers receive via select() over [ch, never-ready idle chan]: drives
# chan.c's multi-waiter Phase-2 install/abort/cleanup, recorded as recv events.
PYTHONPATH="$ROOT/src" "$PYTHON" "$HERE/record_history.py" "$HSEL" 4 3 8 2 3 || rc=1

echo "== 2. Porcupine check both histories (expect LINEARIZABLE) =="
# SKIP, don't fail, when the checker cannot be built -- the same contract every
# other optional verifier in tools/verify/run_verify.sh follows (spin, cbmc,
# herd7, genmc, Dartagnan all print a skip line and continue).  This one used to
# `exit 2` instead, so a runner without Go on PATH turned the whole verification
# phase red: that is exactly what the macOS CI legs hit ("go: command not found"
# -> "go build failed"), while Linux passed because its image ships Go.
#
# CI installs Go explicitly (see .github/workflows/ci.yml) so this skip does NOT
# quietly cost coverage there -- it is here for local runs on a box without Go.
if [ ! -x "$HERE/porcupine/lincheck" ]; then
    if ! command -v go >/dev/null 2>&1; then
        echo "  (go not found -- skipping Porcupine linearizability check;  https://go.dev/dl/)"
        SKIP_PORCUPINE=1
    elif ! ( cd "$HERE/porcupine" && go build -o lincheck . ); then
        echo "  (go build failed -- skipping Porcupine linearizability check)"
        SKIP_PORCUPINE=1
    fi
fi
if [ -z "${SKIP_PORCUPINE:-}" ]; then
    echo "  -- plain --";  "$HERE/porcupine/lincheck" "$HIST" || rc=1
    echo "  -- select --"; "$HERE/porcupine/lincheck" "$HSEL" || rc=1
fi

echo "== 3. teeth: phantom delivery (expect NOT LINEARIZABLE) =="
"$PYTHON" - "$HIST" "$BAD" <<'PY'
import json, sys
h = json.load(open(sys.argv[1]))
for e in h["events"]:
    if e["op"] == "recv" and e["result"] == "ok":
        e["value"] = 999999          # never sent
        break
json.dump(h, open(sys.argv[2], "w"))
PY
# MUST be guarded by the same skip as step 2.  This test passes when the checker
# EXITS NON-ZERO, so a missing binary -- which also exits non-zero, as "command
# not found" -- would take the else branch and print ">>> OK: corrupted history
# correctly rejected".  A teeth check that reports teeth precisely when the tool
# is absent is worse than no teeth check at all.
if [ -n "${SKIP_PORCUPINE:-}" ]; then
    echo "  (skipped with the Porcupine check above)"
elif "$HERE/porcupine/lincheck" "$BAD"; then
    echo "  >>> FAIL: checker accepted a corrupted history (no teeth)"; rc=1
else
    echo "  >>> OK: corrupted history correctly rejected"
fi

# pytest exit 5 = "no tests collected", which is what a module-level
# pytest.importorskip produces -- and hypothesis is genuinely absent below 3.14
# (its transitive deps have no free-threaded wheels, so the CI installer does a
# best-effort `pip install -q hypothesis` and carries on).  Treating 5 as a
# failure is what kept the 3.13 legs red: first as an ImportError collection
# ERROR, then -- once the modules were changed to skip instead -- as a bare
# exit 5.  tests/run_isolated.py already draws this distinction; a real
# collection/import error still exits 2 and still fails here.
run_optional_pytest() {
    "$PYTHON" -m pytest "$@" -q -p no:cacheprovider
    local prc=$?
    [ "$prc" -eq 0 ] || [ "$prc" -eq 5 ] || rc=1
    [ "$prc" -eq 5 ] && echo "  (no tests collected -- optional dependency absent; skipped)"
    return 0
}

echo "== 4. stateful Hypothesis model of the channel API =="
PYTHONPATH="$ROOT/src" run_optional_pytest "$HERE/stateful_chan.py"

echo "== 5. generative linearizability battery (all primitives, seeded DST) =="
# The abstract generalization of big_100: record a real concurrent history per
# (primitive, seed) on the M:N scheduler and check it against the sequential
# reference spec with the pure-Python WGL checker.  Bounded sweep here; the
# unbounded hunt is tools/soak/linz_hunt_forever.sh.
PYTHON_GIL=0 PYTHONPATH="$ROOT/src" "$PYTHON" "$HERE/linz/battery.py" --seeds 0 8 || rc=1

echo "== 6. stateful Hypothesis models of Lock + weighted Semaphore =="
PYTHONPATH="$ROOT/src" run_optional_pytest "$HERE/linz/stateful_sync.py"

$RM -f "$HIST" "$HSEL" "$BAD"
echo "== linearizability pipeline rc=$rc =="
exit $rc
