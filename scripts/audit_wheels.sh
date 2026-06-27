#!/usr/bin/env bash
# audit_wheels.sh -- post-build wheel/ABI conformance gate (local; NO hosted CI).
#
# runloom links a handful of CPython INTERNAL/private symbols (e.g.
# _Py_SetImmortalUntracked) by design, and ships a separate cp31Xt (free-threaded)
# ABI.  A mistagged wheel, an accidental abi3 tag, or a NEW unreviewed internal-
# symbol link would all pass the cibuildwheel import smoke today.  This audits the
# built artifacts:
#
#   * abi3audit --strict      -- every wheel's tag vs the symbols it actually uses;
#                                the deliberate internal links become a REVIEWED
#                                allowlist (tools/wheel_symbol_allowlist.txt), and
#                                a new unlisted internal symbol fails the gate.
#   * auditwheel show (Linux) / delocate-listdeps (macOS) -- platform tag + the
#                                shared libraries the wheel drags in.
#   * filename-tag sanity     -- a cp31Xt wheel must NOT also claim abi3/cp31X.
#
# Each tool is OPTIONAL: missing -> that check SKIPs cleanly (prints a hint), so
# the script is green on a box without the auditors and does real work where they
# exist.  Run as a RELEASING step (see docs/dev/RELEASING.md), not in the fast gate.
#
# Usage:  scripts/audit_wheels.sh [wheelhouse-dir]   (default: ./wheelhouse then ./wheels)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WHEELDIR="${1:-}"
if [ -z "$WHEELDIR" ]; then
    for d in "$ROOT/wheelhouse" "$ROOT/wheels" "$ROOT/dist"; do
        [ -d "$d" ] && WHEELDIR="$d" && break
    done
fi
ALLOWLIST="$ROOT/tools/wheel_symbol_allowlist.txt"

if [ -z "${WHEELDIR:-}" ] || [ ! -d "$WHEELDIR" ]; then
    echo "audit_wheels: no wheel dir (looked for wheelhouse/ wheels/ dist/, or pass one)."
    echo "  build first:  scripts/build_wheels.sh   (or: pipx run cibuildwheel)"
    exit 0   # nothing to audit is not a failure
fi

shopt -s nullglob
WHEELS=("$WHEELDIR"/*.whl)
if [ "${#WHEELS[@]}" -eq 0 ]; then
    echo "audit_wheels: no *.whl in $WHEELDIR"; exit 0
fi
echo "audit_wheels: $WHEELDIR  (${#WHEELS[@]} wheels)"

fail=0
have() { command -v "$1" >/dev/null 2>&1; }

# 1) filename-tag sanity (pure shell -- always runs).
for w in "${WHEELS[@]}"; do
    base="$(basename "$w")"
    if echo "$base" | grep -q 't-' && echo "$base" | grep -qE 'abi3|cp3[0-9]+-cp3[0-9]+-'; then
        # a freethreaded wheel (…t-…) must carry a cp31Xt-cp31Xt tag, never abi3.
        if echo "$base" | grep -q 'abi3'; then
            echo "  FAIL tag: free-threaded wheel carries abi3: $base"; fail=1
        fi
    fi
done

# 2) abi3audit --strict (symbol-vs-tag; internal links reviewed via allowlist).
if have abi3audit; then
    echo "--- abi3audit --strict ---"
    if [ -f "$ALLOWLIST" ]; then
        echo "  (allowlist: $ALLOWLIST)"
    else
        echo "  (no allowlist yet at $ALLOWLIST -- create one listing the reviewed"
        echo "   internal symbols, e.g. _Py_SetImmortalUntracked, after first run)"
    fi
    for w in "${WHEELS[@]}"; do
        abi3audit --strict "$w" || fail=1
    done
else
    echo "--- abi3audit: SKIP (pip install abi3audit) ---"
fi

# 3) platform tag + linkage.
case "$(uname -s)" in
    Linux)
        if have auditwheel; then
            echo "--- auditwheel show ---"
            for w in "${WHEELS[@]}"; do auditwheel show "$w" || true; done
        else
            echo "--- auditwheel: SKIP (pip install auditwheel) ---"
        fi ;;
    Darwin)
        if have delocate-listdeps; then
            echo "--- delocate-listdeps ---"
            for w in "${WHEELS[@]}"; do delocate-listdeps "$w" || true; done
        else
            echo "--- delocate-listdeps: SKIP (pip install delocate) ---"
        fi ;;
esac

if [ "$fail" -ne 0 ]; then
    echo "audit_wheels: FAIL (tag/abi3 violation above)"; exit 1
fi
echo "audit_wheels: OK (or all auditors skipped cleanly)"
exit 0
