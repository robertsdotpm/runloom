#!/usr/bin/env bash
# static_analysis.sh -- static + security analysis of the C core.
#
# Coverage / sanitizers / formal methods all need a path to be EXERCISED.
# Static analysis reasons about paths the tests never hit -- error and cleanup
# branches (an alloc-fail, an EINTR retry, a cross-thread-wake race) where the
# classic memory-safety bugs (NULL deref, double-free, use-after-free,
# leak-on-error, uninitialised read) and the security-relevant ones (buffer
# overflow, tainted index/size, unbounded string copy) hide.
#
# Two GATES (fail the phase) and four ADVISORIES (reported, never gating):
#
#   seclint          GATE.  Greps for the classically-dangerous unbounded
#                    functions (gets/strcpy/strcat/sprintf/vsprintf/alloca) that
#                    have zero call-sites today -- so the gate stays a "don't
#                    reintroduce these" tripwire.  Bounded variants
#                    (strncpy/snprintf/sscanf) are NOT banned.
#   gcc -fanalyzer   GATE.  Symbolic execution over the real preprocessed
#                    source, so it understands GCC __atomic_* builtins, the
#                    Python C-API macros, and noreturn (Py_FatalError) -- low
#                    false-positive here.  Includes the taint checker
#                    (-fanalyzer-checker=taint) for tainted-array-index /
#                    tainted-size buffer overflows.  Any [-Wanalyzer] fails.
#
#   gcc hardening    ADVISORY.  -Warray-bounds=2 / -Wstringop-* / -Wformat-
#                    security / _FORTIFY_SOURCE=3 compile-time bounds + format
#                    checks, filtered to the security warning classes (the
#                    idiomatic C-API noise -- METH_KEYWORDS casts, tp_free field
#                    init -- is suppressed from the report).
#   clang analyzer   ADVISORY.  A SECOND, independent symbolic engine
#                    (clang --analyze) with the dedicated security checkers:
#                    security.insecureAPI.* (unbounded copies) and
#                    alpha.security.ArrayBoundV2 + alpha.security.taint (the
#                    buffer-overflow / tainted-bound checkers).
#   clang-tidy       ADVISORY.  cert-* (CERT C secure-coding rules -- the
#                    coverage the cppcheck `cert` addon would give, which Debian/
#                    Ubuntu does not package) + bugprone-* AST matchers.
#   cppcheck         ADVISORY.  Cheap third opinion; high FP rate here (can't
#                    parse the atomic builtins / some Python macros).
#
# Every per-file pass fans out across $STATIC_JOBS cores (default: nproc), so
# the whole phase is bounded by the slowest single file, not their sum -- adding
# the clang/cert/hardening passes does not lengthen wall-clock on a many-core
# box.  This phase runs in check_all_extensive (== check_all.sh all), NOT in the
# routine check_all_fast pre-merge gate.
#
# Usage:  tools/static_analysis.sh        (or: scripts/check_all.sh static)
# Env:    PYTHON=...       interpreter (for the Python C headers)
#         GCC=...          compiler for -fanalyzer (default: gcc)
#         CLANG=...        clang for the analyzer pass (default: clang-18/clang)
#         CLANG_TIDY=...   clang-tidy binary (default: clang-tidy-18/clang-tidy)
#         STATIC_JOBS=N    parallel fan-out width (default: nproc)
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
NFILES=$(echo $FILES | wc -w)

JOBS="${STATIC_JOBS:-$(nproc 2>/dev/null || echo 4)}"

# Per-file logs land here; one file per pass-per-TU so the parallel fan-out
# never interleaves output.  Cleaned on exit (safe-rm per repo convention).
TMP="$(mktemp -d "${TMPDIR:-/tmp}/runloom_static.XXXXXX")"
cleanup() { case "$TMP" in /tmp/*|"${TMPDIR:-/tmp}"/*) command -v safe-rm >/dev/null 2>&1 && safe-rm -rf "$TMP" || rm -rf "$TMP" ;; esac; }
trap cleanup EXIT

rc=0

# ------------------------------------------------------------------ parallel --
# analyze_file PASS FILE -- run one tool over one TU, output to $TMP/PASS.<tu>.log.
# Dispatched by $PASS so a single exported function serves every per-file pass.
analyze_file() {
    f="$2"; b="$(basename "$f")"; out="$TMP/$1.$b.log"
    case "$1" in
        gate)   "$GCC" -c -fanalyzer $TAINT $CFLAGS "$f" -o /dev/null    >"$out" 2>&1 ;;
        harden) "$GCC" -c $HARDEN $CFLAGS "$f" -o /dev/null              >"$out" 2>&1 ;;
        clang)  "$CLANG" --analyze -Xclang -analyzer-output=text $CLANG_CHK \
                         $CFLAGS -c "$f"                                 >"$out" 2>&1 ;;
        tidy)   "$CLANG_TIDY" "$f" -checks="$TIDY_CHK" --quiet -- $CFLAGS >"$out" 2>&1 ;;
    esac
    return 0
}
export -f analyze_file
export GCC CLANG CLANG_TIDY CFLAGS TAINT HARDEN CLANG_CHK TIDY_CHK TMP

run_parallel() {  # $1 = PASS name; fans $FILES across $JOBS cores
    printf '%s\n' $FILES | xargs -P"$JOBS" -n1 -I'{}' \
        bash -c 'analyze_file "$1" "$2"' _ "$1" '{}'
}

# ------------------------------------------------------ seclint (GATE) --------
echo "== seclint (banned unbounded functions; gate) =="
BANNED='gets|strcpy|strcat|sprintf|vsprintf|alloca|wcscpy|wcscat'
hits="$(grep -rnE "\\b(${BANNED})[[:space:]]*\(" \
        src/runloom_c --include='*.c' --include='*.h' --include='*.inc' 2>/dev/null \
        | grep -vE '^[^:]+:[0-9]+:[[:space:]]*[*]' || true)"
if [ -n "$hits" ]; then
    echo "  FAIL: banned unbounded function(s) reintroduced -- use a bounded variant:"
    printf '%s\n' "$hits" | sed 's/^/    /'
    rc=1
else
    echo "  clean -- no banned unbounded calls ($BANNED)"
fi

# ------------------------------------------------ gcc -fanalyzer (GATE) -------
GCC="${GCC:-gcc}"
if echo 'int main(void){return 0;}' | "$GCC" -fanalyzer -x c - -o /dev/null 2>/dev/null; then
    # Enable the taint checker iff this gcc supports it (tainted index/size/alloc).
    TAINT=""
    if echo 'int main(void){return 0;}' | "$GCC" -fanalyzer -fanalyzer-checker=taint -x c - -o /dev/null 2>/dev/null; then
        TAINT="-fanalyzer-checker=taint"
    fi
    export GCC TAINT
    echo "== gcc -fanalyzer${TAINT:+ +taint} (authoritative gate; ${JOBS}-way) =="
    run_parallel gate
    nwarn=0
    for f in $FILES; do
        b="$(basename "$f")"; log="$TMP/gate.$b.log"
        [ -f "$log" ] || continue
        k="$(grep -c '\[-Wanalyzer' "$log" || true)"
        if [ "${k:-0}" -gt 0 ]; then
            echo "  $b: $k warning(s)"
            grep -A4 '\[-Wanalyzer' "$log" | sed 's/^/    /'
            nwarn=$((nwarn + k))
        fi
    done
    if [ "$nwarn" -gt 0 ]; then echo "  FAIL: $nwarn analyzer warning(s)"; rc=1
    else echo "  clean -- no analyzer warnings across $NFILES files"; fi
else
    echo "== gcc -fanalyzer: not available (needs GCC 10+); skipped =="
fi

# ----------------------------------------- gcc hardening (ADVISORY) -----------
# Compile-time bounds / format / fortify checks, filtered to the security
# classes only -- the codebase is clean here today; idiomatic C-API warnings
# (-Wcast-function-type from METH_KEYWORDS, -Wmissing-field-initializers on
# tp_free) are NOT security and are dropped from the report.
HARDEN="-O2 -Warray-bounds=2 -Wstringop-overflow=4 -Wstringop-truncation \
        -Wformat=2 -Wformat-security -Wnull-dereference -Wuse-after-free=3 \
        -D_FORTIFY_SOURCE=3"
export HARDEN
if command -v "$GCC" >/dev/null 2>&1; then
    echo "== gcc hardening warnings (advisory; bounds/format/fortify; ${JOBS}-way) =="
    run_parallel harden
    SECW='\[-W(array-bounds|stringop-overflow|stringop-truncation|format-security|format-overflow|format-truncation|null-dereference|use-after-free)'
    sec="$(cat "$TMP"/harden.*.log 2>/dev/null | grep -hE "warning:.*$SECW" || true)"
    if [ -n "$sec" ]; then printf '%s\n' "$sec" | sed 's/^/  /' | head -40
    else echo "  clean -- no bounds/format/fortify warnings"; fi
    echo "  (advisory -- not gating)"
else
    echo "== gcc hardening: gcc not available; skipped =="
fi

# --------------------------------- clang static analyzer (ADVISORY) ----------
CLANG="${CLANG:-}"; [ -z "$CLANG" ] && { for c in clang-18 clang; do command -v "$c" >/dev/null 2>&1 && { CLANG="$c"; break; }; done; }
if [ -n "$CLANG" ] && command -v "$CLANG" >/dev/null 2>&1; then
    # Independent symbolic engine; dedicated security + buffer-overflow checkers.
    # Disable security.insecureAPI.DeprecatedOrUnsafeBufferHandling: it demands
    # the C11 Annex K *_s bounds functions (memset_s/fprintf_s) that glibc does
    # not implement, so it flags every memset/memcpy/fprintf -- pure noise.  The
    # rest of security.insecureAPI (strcpy/sprintf/gets/rand/...) stays on.
    CLANG_CHK="-Xclang -analyzer-checker=core,security,unix,alpha.security.ArrayBoundV2,alpha.security.taint \
               -Xclang -analyzer-disable-checker=security.insecureAPI.DeprecatedOrUnsafeBufferHandling"
    export CLANG CLANG_CHK
    echo "== clang --analyze (advisory; security + ArrayBoundV2 + taint; ${JOBS}-way) =="
    run_parallel clang
    cw="$(cat "$TMP"/clang.*.log 2>/dev/null | grep -hE 'warning:' || true)"
    if [ -n "$cw" ]; then
        printf '%s\n' "$cw" | sed 's/^/  /' | head -40
        echo "  ($(printf '%s\n' "$cw" | wc -l) clang-analyzer warning(s); advisory -- not gating)"
    else echo "  clean -- no clang-analyzer warnings"; fi
else
    echo "== clang --analyze: clang not installed (advisory; skipped) =="
fi

# ------------------------------------------ clang-tidy cert/bugprone (ADV) ----
CLANG_TIDY="${CLANG_TIDY:-}"; [ -z "$CLANG_TIDY" ] && { for c in clang-tidy-18 clang-tidy; do command -v "$c" >/dev/null 2>&1 && { CLANG_TIDY="$c"; break; }; done; }
if [ -n "$CLANG_TIDY" ] && command -v "$CLANG_TIDY" >/dev/null 2>&1; then
    # CERT C secure-coding rules (the cppcheck `cert` addon is not packaged) +
    # bugprone AST matchers.  Drop the non-security chatter: reserved-identifier
    # /dcl37/dcl51 fire on the necessary `#define _POSIX_C_SOURCE`, and
    # easily-swappable-parameters is a design smell, not a security finding.
    TIDY_CHK='-*,cert-*,bugprone-*,-bugprone-reserved-identifier,-cert-dcl37-c,-cert-dcl51-cpp,-bugprone-easily-swappable-parameters'
    export CLANG_TIDY TIDY_CHK
    echo "== clang-tidy (advisory; cert-* + bugprone-*; ${JOBS}-way) =="
    run_parallel tidy
    tw="$(cat "$TMP"/tidy.*.log 2>/dev/null | grep -hE 'warning:.*\[(cert|bugprone)-' || true)"
    if [ -n "$tw" ]; then
        printf '%s\n' "$tw" | sort -u | sed 's/^/  /' | head -40
        echo "  ($(printf '%s\n' "$tw" | sort -u | wc -l) unique cert/bugprone finding(s); advisory -- not gating)"
    else echo "  clean -- no cert/bugprone findings"; fi
else
    echo "== clang-tidy: not installed (advisory; skipped) =="
fi

# ---------------------------------------------- cppcheck (ADVISORY) ----------
if command -v cppcheck >/dev/null 2>&1; then
    echo "== cppcheck (advisory; ${JOBS}-way; high FP rate on Python C-API + atomic builtins) =="
    cppcheck -j "$JOBS" --enable=warning,performance,portability --inconclusive \
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
