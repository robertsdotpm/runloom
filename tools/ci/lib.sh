# lib.sh -- shared helpers for tools/ci/*.  Sourced, never executed.
#
# Deliberately POSIX-sh compatible and dependency-free: this runs on the dev
# Mac, on the Linux release host over SSH, and on whatever box someone points
# at it.  No bash arrays, no GNU-only flags, no jq.

# ---- portability ------------------------------------------------------------

# nproc does not exist on macOS; sysctl does not exist on Linux.
rl_nproc() {
    if command -v nproc >/dev/null 2>&1; then nproc
    elif command -v sysctl >/dev/null 2>&1; then sysctl -n hw.ncpu
    else echo 4
    fi
}

# CLAUDE.md: deletions go through safe-rm where it exists.
rl_rm() {
    if command -v safe-rm >/dev/null 2>&1; then safe-rm "$@"; else rm "$@"; fi
}

# shasum (macOS) vs sha256sum (Linux); both print "<hash>  <file>".
rl_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
    else shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

# Release-artifact platform tag, e.g. darwin-arm64 / linux-x86_64.
rl_platform() {
    _os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    _arch="$(uname -m)"
    case "$_arch" in
        aarch64) _arch=arm64 ;;
        amd64)   _arch=x86_64 ;;
    esac
    echo "${_os}-${_arch}"
}

# ---- output -----------------------------------------------------------------

rl_log()  { printf '\033[1m[ci]\033[0m %s\n' "$*" >&2; }
rl_step() { printf '\n\033[1m========== %s ==========\033[0m\n' "$*" >&2; }
rl_warn() { printf '\033[33m[ci:WARN]\033[0m %s\n' "$*" >&2; }
rl_die()  { printf '\033[31m[ci:FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# rl_ci_summary <markdown-line> -- surface a result on the GitHub Actions run
# page (Step Summary), so the CPython/runloom test outcomes are visible
# at-a-glance instead of buried in the build-job log.  No-op off CI.
rl_ci_summary() {
    [ -n "${GITHUB_STEP_SUMMARY:-}" ] && printf '%s\n\n' "$*" >> "$GITHUB_STEP_SUMMARY"
    return 0
}

# ---- version matrix (VERSION-driven) ----------------------------------------
#
# The CI's primary key is a full major.minor.patch VERSION.  Everything else --
# the series, the patch pair, the sha256, the interpreter basename, the test
# exclusions -- is DERIVED from it here, so callers only ever pass a version.

# rl_validate_version 3.14.4 -> ok, else die.  Guards every downstream eval/awk.
rl_validate_version() {
    case "$1" in
        [0-9]*.[0-9]*.[0-9]*)
            case "$1" in *[!0-9.]*) rl_die "bad version '$1' (non-numeric)";; esac ;;
        *) rl_die "bad version '$1' -- expected major.minor.patch, e.g. 3.14.4" ;;
    esac
}

# rl_series_of_version 3.14.4 -> 314   (major concat minor; the patch-set id)
rl_series_of_version() {
    rl_validate_version "$1"
    printf '%s' "$1" | awk -F. '{print $1 $2}'
}

# rl_majmin_of_version 3.14.4 -> 3.14
rl_majmin_of_version() {
    rl_validate_version "$1"
    printf '%s' "$1" | awk -F. '{print $1 "." $2}'
}

# rl_sha_of_version 3.14.4 -> pinned sha256, or "" if not pinned.
# Reads the RL_CI_PINS table from versions.env (must be sourced first).
rl_sha_of_version() {
    printf '%s\n' "${RL_CI_PINS:-}" | awk -v v="$1" '$1==v {print $2; exit}'
}

# rl_patches_of_version 3.14.4 -> the two patch FILENAMES for its series.
# Dies if either file is missing -- a version whose patch pair does not exist
# cannot be built, and saying so here beats a confusing apply failure later.
rl_patches_of_version() {
    _s="$(rl_series_of_version "$1")"
    _a="cpython${_s}t-tstate-alloc-home.patch"
    _e="cpython${_s}t-tstate-exec-home.patch"
    _d="${RL_CI_PATCHDIR:-$(cd "$(dirname "$0")/../.." && pwd)/src/patches}"
    [ -f "$_d/$_a" ] || rl_die "no alloc-home patch for series $_s ($_d/$_a) -- version $1 has no patch set; port one and name it cpython${_s}t-tstate-alloc-home.patch"
    [ -f "$_d/$_e" ] || rl_die "no exec-home patch for series $_s ($_d/$_e) -- version $1 has no patch set"
    printf '%s %s' "$_a" "$_e"
}

# rl_exclude_of_version 3.14.4 -> $PY314_TEST_EXCLUDE (upstream-only exclusions).
rl_exclude_of_version() {
    _s="$(rl_series_of_version "$1")"
    eval "printf '%s' \"\${PY${_s}_TEST_EXCLUDE:-}\""
}

# rl_pybin_basename 3.14.4 -> python3.14   (the free-threaded 't' is appended by
# the caller, which knows whether the install produced python3.14 or python3.14t).
rl_pybin_basename() {
    printf 'python%s' "$(rl_majmin_of_version "$1")"
}

# ---- install prefix ---------------------------------------------------------
#
# rl_resolve_prefix <version> <platform> <work>
#
# One resolution order, used by both the build and the packager, because a
# CPython install is NOT relocatable: it resolves its stdlib against the prefix
# it was CONFIGURED with.  If these two disagreed, the packager would tar an
# empty or stale directory.
#
#   RL_CI_PREFIX          explicit, wins outright (single-version use)
#   RL_CI_RELEASE_PREFIX  release builds: <base>/<version>, stable across hosts
#                         and distinct per version
#   otherwise             per-version scratch under the work dir
rl_resolve_prefix() {
    if [ -n "${RL_CI_PREFIX:-}" ]; then
        printf '%s' "$RL_CI_PREFIX"
    elif [ -n "${RL_CI_RELEASE_PREFIX:-}" ]; then
        printf '%s/%s' "$RL_CI_RELEASE_PREFIX" "$1"
    else
        printf '%s/prefix-%s-%s' "$3" "$1" "$2"
    fi
}

# ---- the patch gate ---------------------------------------------------------
#
# These three live here, not inline in the callers, because BOTH the fast
# `ci.sh --patches-only` lane and the real build run them.  If the fast gate and
# the real build could drift apart, the fast gate would be worse than useless --
# it would report clean on something the build then applies differently.

# rl_fetch_cpython <version> <sha256> <tarball-path>
rl_fetch_cpython() {
    _ver="$1"; _sha="$2"; _tar="$3"
    if [ -f "$_tar" ] && [ "$(rl_sha256 "$_tar")" = "$_sha" ]; then
        rl_log "cached tarball matches pin, reusing"
        return 0
    fi
    rl_rm -f "$_tar"
    curl -fsSL "${RL_CI_PY_MIRROR}/${_ver}/Python-${_ver}.tgz" -o "$_tar" \
        || rl_die "download failed: Python-${_ver}.tgz"
    _got="$(rl_sha256 "$_tar")"
    [ "$_got" = "$_sha" ] || rl_die "sha256 MISMATCH for Python-${_ver}.tgz
  pinned: $_sha
  got:    $_got
Either the pin in tools/ci/versions.env is stale or the download is not what it
claims to be.  Do not 'fix' this by pasting the new hash in without checking
python.org."
    rl_log "sha256 OK: $_got"
}

# rl_apply_patches <srcdir> <patchdir> <version> <patch...>
#
# ZERO FUZZ, and that is the whole point.  Fuzzy application here is not a
# convenience, it is a correctness hazard: the 3.13 alloc-home patch's two
# llist_insert_tail hunks will fuzz into 3.14 against superficially-similar
# context and introduce a double alloc-home hop that no test catches.  If a hunk
# needs fuzz, the patch needs a port -- see the 'WHAT CHANGED' section in
# src/patches/cpython314t-tstate-alloc-home.patch for the shape of one.
rl_apply_patches() {
    _src="$1"; _pdir="$2"; _ver="$3"; shift 3
    for _p in "$@"; do
        _pf="$_pdir/$_p"
        [ -f "$_pf" ] || rl_die "missing patch file: $_pf"
        rl_log "applying $_p"
        if ! ( cd "$_src" && patch -p1 -F0 --forward --no-backup-if-mismatch < "$_pf" ); then
            rl_die "$_p does NOT apply cleanly to Python-${_ver}.
This is the hard gate.  Do NOT retry with -F3.  Port the patch, commit it as a
new version-specific file, and update tools/ci/versions.env."
        fi
    done
    _rej="$(find "$_src" -name '*.rej' | head -20)"
    [ -z "$_rej" ] || rl_die "reject files present after a supposedly clean apply:
$_rej"
}

# rl_verify_witnesses <srcdir>
#
# The feature flags are armed in pyconfig.h unconditionally, so a half-patched
# tree ADVERTISES both features while lacking them.  These greps are the only
# thing standing between that and a released interpreter.
rl_verify_witnesses() {
    _src="$1"
    grep -q '_Py_TID_ASM' "$_src/Include/object.h" \
        || rl_die "exec-home witness missing: _Py_TID_ASM not in Include/object.h"
    grep -q '_PyThreadStateImpl_AllocHome' "$_src/Include/internal/pycore_tstate.h" \
        || rl_die "alloc-home witness missing: _PyThreadStateImpl_AllocHome not in pycore_tstate.h"
    grep -q 'Py_NO_INLINE' "$_src/Python/pystate.c" \
        || rl_die "exec-home witness missing: Py_NO_INLINE not applied to _PyThreadState_GetCurrent()"
    rl_log "witnesses present: _Py_TID_ASM, _PyThreadStateImpl_AllocHome, Py_NO_INLINE"
}

# ---- guards -----------------------------------------------------------------

# The exec-home patch works by keeping _PyThreadState_GetCurrent() a real
# cross-TU call.  LTO folds it back in and silently reintroduces the UAF the
# patch exists to prevent, with no build error and no test failure -- the
# resulting interpreter just corrupts memory under migration.  Refuse loudly.
rl_reject_lto() {
    case " $* " in
        *" --with-lto"*|*"--enable-optimizations"*|*"-flto"*)
            rl_die "refusing to build with LTO/PGO: it re-inlines _PyThreadState_GetCurrent() and silently undoes the exec-home patch (see src/patches/cpython314t-tstate-exec-home.patch, CAVEATS)" ;;
    esac
}
