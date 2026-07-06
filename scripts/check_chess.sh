#!/usr/bin/env bash
# check_chess.sh -- wire the CHESS/PCT coverage-theorem tools into CI (Gap 3).
#
# tools/mn_controlled/ ships a real systematic-concurrency stack -- PCT (depth-d
# probability bound on the REAL grant path) and CHESS-style exhaustive/greybox
# exploration -- but NOTHING re-ran it: zero CI references, and chess_compose.py
# was broken (KeyError 'mkeys', drift vs chess_explore).  So a park/wake rewrite
# (item 1) got sampled + replay-checked but never BRACKETED by an exhaustive /
# probability-bounded search over the interleaving space.
#
# This lane gates on grep-verified OUTCOMES (not exit codes -- some of these
# tools exit 0 even on INCONCLUSIVE, the audit's finding), so a teeth regression
# fails loudly:
#   * pct_find.py must still self-report RESULT: PASS (PCT finds+replays its
#     depth-2 bug while depth-1 finds nothing -- the guarantee machinery works);
#   * chess_compose.py must still FIND its planted bug (teeth intact);
#   * chess_conserve / chess_chan run clean over the REAL chan primitives.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
MC="$ROOT/tools/mn_controlled"
cd "$ROOT" || exit 2

rc=0
run() {  # run <script.py> <grep-that-must-match> <label>
    local script="$1" want="$2" label="$3"; shift 3
    printf '  [chess] %-26s ' "$label"
    local out
    out="$(PYTHON_GIL=0 PYTHONPATH=src timeout "${CHESS_TIMEOUT:-180}" \
           "$PY" "$MC/$script" "$@" 2>&1)"
    if printf '%s' "$out" | grep -qiE "$want"; then
        echo "OK"
    else
        echo "FAIL (wanted /$want/)"; printf '%s\n' "$out" | tail -3; rc=1
    fi
}

echo "== CHESS/PCT coverage-theorem gate =="
run pct_find.py       "RESULT: PASS"                 "pct depth-2 find+replay"
run chess_compose.py  "produced a bug|found@c|BUG"   "compose finds planted bug"
run chess_conserve.py "OK|recvd="                    "conserve (real chan) clean"
run chess_chan.py     "OK|recvd=|PASS"               "chan (real chan) clean"

[ "$rc" = 0 ] && echo "== chess OK: PCT guarantee + CHESS exploration have teeth and run ==" \
              || echo "== chess FAIL: a coverage-theorem tool regressed =="
exit $rc
