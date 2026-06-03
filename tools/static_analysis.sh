#!/usr/bin/env bash
# static_analysis.sh -- static analysis of the C core.
#
# Coverage / sanitizers / formal methods all need a path to be EXERCISED.
# Static analysis reasons about paths the tests never hit -- error and cleanup
# branches (an alloc-fail, an EINTR retry, a cross-thread-wake race) where the
# classic memory-safety bugs (NULL deref, double-free, use-after-free,
# leak-on-error, uninitialised read) hide.
#
#   gcc -fanalyzer   AUTHORITATIVE gate.  Symbolic execution over the real
#                    preprocessed source, so it understands GCC __atomic_*
#                    builtins, the Python C-API macros, and noreturn
#                    (Py_FatalError) -- low false-positive on this codebase.
#                    Any [-Wanalyzer] warning fails the phase.
#   cppcheck         ADVISORY second opinion.  High false-positive rate here
#                    (it can't parse the atomic builtins / some Python macros),
#                    so it is reported, never gating.
#
# Usage:  tools/static_analysis.sh        (or: scripts/check_all.sh static)
# Env:    PYTHON=...   interpreter (for the Python C headers)
#         GCC=...      compiler for -fanalyzer (default: gcc)
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
    for c in "$HOME/.pyenv/versions/3.13.13t/bin/python3" python3.13t python3; do
        command -v "$c" >/dev/null 2>&1 && { PYTHON="$c"; break; }
    done
fi
PYINC="$("$PYTHON" -c 'import sysconfig; print(sysconfig.get_path("include"))')"

OSDEF="-D_GNU_SOURCE"
case "$(uname -s)" in
    Darwin) OSDEF="-D_XOPEN_SOURCE=600 -D_DARWIN_C_SOURCE -Wno-deprecated-declarations" ;;
    *BSD)   OSDEF="-D_BSD_SOURCE" ;;
esac
CFLAGS="-std=gnu11 -fno-strict-aliasing -O2 -Wall -Wextra -Wno-unused-parameter \
        $OSDEF -I$PYINC -Isrc/runloom_c"

# Core translation units; skip the Windows-only IOCP backend and the .S asm.
FILES=$(ls src/runloom_c/*.c | grep -v netpoll_iocp)

rc=0

# ---------------- gcc -fanalyzer (authoritative gate) ----------------
GCC="${GCC:-gcc}"
if echo 'int main(void){return 0;}' | "$GCC" -fanalyzer -x c - -o /dev/null 2>/dev/null; then
    echo "== gcc -fanalyzer (authoritative) =="
    nwarn=0
    for f in $FILES; do
        log="$("$GCC" -c -fanalyzer $CFLAGS "$f" -o /dev/null 2>&1)"
        k="$(printf '%s' "$log" | grep -c '\[-Wanalyzer' || true)"
        if [ "${k:-0}" -gt 0 ]; then
            echo "  $(basename "$f"): $k warning(s)"
            printf '%s\n' "$log" | grep -A4 '\[-Wanalyzer' | sed 's/^/    /'
            nwarn=$((nwarn + k))
        fi
    done
    if [ "$nwarn" -gt 0 ]; then
        echo "  FAIL: $nwarn analyzer warning(s)"; rc=1
    else
        echo "  clean -- no analyzer warnings across $(echo $FILES | wc -w) files"
    fi
else
    echo "== gcc -fanalyzer: not available (needs GCC 10+); skipped =="
fi

# ---------------- cppcheck (advisory) ----------------
if command -v cppcheck >/dev/null 2>&1; then
    echo "== cppcheck (advisory; high FP rate on Python C-API + atomic builtins) =="
    cppcheck --enable=warning,performance,portability --inconclusive \
        --std=c11 --language=c --platform=unix64 -I"$PYINC" -Isrc/runloom_c \
        --suppress=internalAstError --suppress=missingInclude \
        --suppress=missingIncludeSystem --suppress=unmatchedSuppression \
        --suppress="*:$PYINC/*" \
        `# confirmed false positives (see comments in the named files):` \
        --suppress=shiftTooManyBits:src/runloom_c/coro.c \
        --suppress=nullPointerRedundantCheck:src/runloom_c/runloom_sched.c \
        --quiet $FILES 2>&1 | sed 's/^/  /' | head -50
    echo "  (advisory -- triage manually; not gating)"
else
    echo "== cppcheck: not installed (advisory; skipped) =="
fi

echo
[ "$rc" -eq 0 ] && echo "static analysis: PASS" || echo "static analysis: FAIL"
exit $rc
