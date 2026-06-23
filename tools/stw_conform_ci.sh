#!/usr/bin/env bash
# stw_conform_ci.sh -- the check_all_extensive entry point for the STW (M2) trace
# conformance: conform the REAL CPython stop-the-world handshake against
# tools/verify/tla/RunloomCPythonSTW.tla under TLC (see tools/stw_trace_conform_demo.sh,
# docs/dev/ft_conformance/STW_FINDINGS.md).
#
# The demo needs an INSTRUMENTED --with-pydebug interpreter + a runloom_c built
# against its ABI.  This wrapper makes "always run if possible" true: it sets that
# up idempotently, then runs the demo.  It SKIPS CLEANLY (exit 0) whenever the
# pieces aren't available (no pydebug tree, no java, patch won't apply) -- so it is
# safe in the gate; it only actually runs where the pydebug oracle lives (the dev
# box / the self-hosted CI runner).
#
#   * The instrumentation is env-gated (RUNLOOM_STW_TRACE): an instrumented interp
#     behaves IDENTICALLY to a pristine one for every other pydebug use, so we
#     instrument once and leave it (no rebuild churn each run).
#   * The pydebug-ABI ext (...-313td...so) coexists with the stock ext
#     (...-313t...so) by ABI tag, so building it does NOT disturb the normal build.
#
# Usage:  tools/stw_conform_ci.sh
# Env:    RUNLOOM_PYDEBUG_PYTHON  the --with-pydebug free-threaded interp
#                                 (default: /home/x/projects/cpython-pydebug/python)
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PYD="${RUNLOOM_PYDEBUG_PYTHON:-/home/x/projects/cpython-pydebug/python}"
PATCH="$ROOT/tools/verify/cpython_patches/pystate_stw_trace.patch"

skip() { echo "== STW (M2) trace conformance =="; echo "  SKIP: $1"; exit 0; }

command -v java >/dev/null 2>&1 || skip "java not found (TLC needs it)"
[ -x "$PYD" ] || skip "no pydebug interp at $PYD (set RUNLOOM_PYDEBUG_PYTHON)"
"$PYD" -c "import sys; assert hasattr(sys,'gettotalrefcount')" >/dev/null 2>&1 \
    || skip "$PYD is not a --with-pydebug build"
[ -f "$PATCH" ] || skip "instrumentation patch missing ($PATCH)"

PYDTREE="$(cd "$(dirname "$PYD")" && pwd)"
PYSTATE="$PYDTREE/Python/pystate.c"
[ -f "$PYSTATE" ] || skip "pydebug source tree not found ($PYSTATE)"

# 1) ensure the interp is instrumented (idempotent -- the emit is a no-op when
#    RUNLOOM_STW_TRACE is unset, so leaving it instrumented is harmless).
if ! grep -q runloom_stw "$PYSTATE"; then
    echo "== STW (M2) trace conformance =="
    echo "  instrumenting pydebug pystate.c (reproducible, env-gated patch) + rebuilding"
    patch -p1 -d "$PYDTREE" --forward <"$PATCH" >/tmp/stwci_patch.log 2>&1 \
        || skip "instrumentation patch did not apply (CPython moved?) -- left pristine"
    ( cd "$PYDTREE" && make -j"$(nproc)" ) >/tmp/stwci_pyd.log 2>&1 \
        || skip "pydebug rebuild failed (see /tmp/stwci_pyd.log)"
fi

# 2) build the ext against the pydebug ABI (incremental; separate ABI tag, so the
#    stock .so is untouched).
PYTHON_GIL=0 "$PYD" setup.py build_ext --inplace >/tmp/stwci_ext.log 2>&1 \
    || skip "ext build against the pydebug ABI failed (see /tmp/stwci_ext.log)"

# 3) run the end-to-end conformance demo (real handshake CONFORMS + negative
#    control NON-CONFORMING).  Its exit code is this phase's result.
exec env RUNLOOM_PYDEBUG_PYTHON="$PYD" bash tools/stw_trace_conform_demo.sh
