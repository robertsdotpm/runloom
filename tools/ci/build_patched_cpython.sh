#!/usr/bin/env bash
# build_patched_cpython.sh -- fetch a PINNED CPython release, apply runloom's
# migration patches at ZERO FUZZ, build it free-threaded with both feature flags,
# and install it into a self-contained prefix.
#
# Argument is a full major.minor.patch VERSION; the patch set, sha256 and test
# exclusions are all derived from it (see lib.sh resolvers + versions.env).
#
# Contract, in order of how badly each bites if wrong:
#   1. THE PATCHES MUST APPLY CLEANLY.  -F0, --forward, zero .rej.  Fuzzy
#      application is a correctness hazard: the 3.13 alloc-home patch's two
#      llist_insert_tail hunks fuzz into 3.14 and introduce a double alloc-home
#      hop no test catches.  A hunk that needs fuzz means the patch needs a port.
#   2. THE FEATURE MUST REACH EXTENSION MODULES.  CPPFLAGS covers only CPython's
#      own build; extensions include the INSTALLED pyconfig.h.  alloc-home adds a
#      field to _PyThreadStateImpl, so a mismatch shifts struct offsets SILENTLY.
#   3. NO LTO, NO PGO.  exec-home keeps _PyThreadState_GetCurrent() a real
#      cross-TU call; LTO inlines it back and reintroduces the UAF with no error.
#   4. THE PATCH MUST ACTUALLY BE IN THERE.  Flags are armed in pyconfig.h
#      regardless of whether hunks landed; compile-time witnesses are grepped.
#
# Usage:  tools/ci/build_patched_cpython.sh <version>     # e.g. 3.14.4
# Env:    RL_CI_WORK    scratch root      (default ~/.cache/runloom-ci)
#         RL_CI_PREFIX  install prefix    (default $RL_CI_WORK/prefix-<version>-<platform>)
#         RL_CI_JOBS    make parallelism  (default: all cores)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=lib.sh
. "$HERE/lib.sh"
# shellcheck source=versions.env
. "$HERE/versions.env"

VERSION="${1:-}"
[ -n "$VERSION" ] || rl_die "usage: $0 <version>   (e.g. 3.14.4)"
rl_validate_version "$VERSION"

export RL_CI_PATCHDIR="$ROOT/src/patches"
SERIES="$(rl_series_of_version "$VERSION")"
SHA256="$(rl_sha_of_version "$VERSION")"
PATCHES="$(rl_patches_of_version "$VERSION")"
[ -n "$SHA256" ] || rl_die "version $VERSION is not pinned in tools/ci/versions.env (RL_CI_PINS). Add a 'VERSION  SHA256' row -- the CI never fetches an unpinned tarball."

WORK="${RL_CI_WORK:-$HOME/.cache/runloom-ci}"
PLATFORM="$(rl_platform)"
PREFIX="$(rl_resolve_prefix "$VERSION" "$PLATFORM" "$WORK")"
SRC="$WORK/src-$VERSION-$PLATFORM"
TARBALL="$WORK/Python-$VERSION.tgz"
JOBS="${RL_CI_JOBS:-$(rl_nproc)}"
PATCHDIR="$RL_CI_PATCHDIR"

mkdir -p "$WORK"
rl_log "version $VERSION -> series $SERIES, patches: $PATCHES"

# ---- 1..4: fetch, extract, apply at zero fuzz, verify the hunks landed ------
# All in lib.sh so `ci.sh --patches-only` runs the IDENTICAL gate without a build.

rl_step "fetch CPython $VERSION"
rl_fetch_cpython "$VERSION" "$SHA256" "$TARBALL"

rl_step "extract to $SRC"
rl_rm -rf "$SRC"
mkdir -p "$SRC"
tar xzf "$TARBALL" -C "$SRC" --strip-components=1

rl_step "apply patches (zero fuzz)"
# shellcheck disable=SC2086
rl_apply_patches "$SRC" "$PATCHDIR" "$VERSION" $PATCHES

rl_step "verify patches landed"
rl_verify_witnesses "$SRC"

# ---- 5. configure -----------------------------------------------------------

CPPDEFS=""
for d in $RL_CI_FEATURE_DEFINES; do CPPDEFS="$CPPDEFS -D$d"; done
CONFIGURE_EXTRA="${RL_CI_CONFIGURE_EXTRA:-}"
rl_reject_lto "$CONFIGURE_EXTRA"

rl_step "configure (free-threaded, both features, no LTO)"
(
    cd "$SRC"
    env ac_cv_buggy_getaddrinfo=no ./configure \
        --prefix="$PREFIX" \
        --disable-gil \
        CPPFLAGS="${CPPDEFS# }" \
        $CONFIGURE_EXTRA
) > "$WORK/configure-$VERSION.log" 2>&1 \
    || { tail -40 "$WORK/configure-$VERSION.log" >&2; rl_die "configure failed (full log: $WORK/configure-$VERSION.log)"; }

# CPPFLAGS reaches CPython's own TUs only.  Extension modules -- runloom's
# included -- include the INSTALLED pyconfig.h, and alloc-home changes the
# _PyThreadStateImpl layout.  Both must agree or offsets shift silently.
rl_step "arm feature defines in pyconfig.h (for extension modules)"
for d in $RL_CI_FEATURE_DEFINES; do
    grep -q "define $d" "$SRC/pyconfig.h" || printf '#define %s 1\n' "$d" >> "$SRC/pyconfig.h"
    rl_log "pyconfig.h: $d"
done

# ---- 6. build + install -----------------------------------------------------

rl_step "make -j$JOBS"
( cd "$SRC" && make -j"$JOBS" ) > "$WORK/make-$VERSION.log" 2>&1 \
    || { tail -60 "$WORK/make-$VERSION.log" >&2; rl_die "build failed (full log: $WORK/make-$VERSION.log)"; }

rl_step "install to $PREFIX"
rl_rm -rf "$PREFIX"
( cd "$SRC" && make install ) > "$WORK/install-$VERSION.log" 2>&1 \
    || { tail -40 "$WORK/install-$VERSION.log" >&2; rl_die "install failed (full log: $WORK/install-$VERSION.log)"; }

# ---- 7. post-install assertions --------------------------------------------

PYBIN="$PREFIX/bin/$(rl_pybin_basename "$VERSION")t"
[ -x "$PYBIN" ] || PYBIN="$PREFIX/bin/$(rl_pybin_basename "$VERSION")"
[ -x "$PYBIN" ] || rl_die "no interpreter installed under $PREFIX/bin"

rl_step "verify installed interpreter"
INSTALLED_CFG="$("$PYBIN" -c 'import sysconfig; print(sysconfig.get_paths()["include"])')/pyconfig.h"
for d in $RL_CI_FEATURE_DEFINES; do
    grep -q "define $d" "$INSTALLED_CFG" \
        || rl_die "installed pyconfig.h is MISSING $d ($INSTALLED_CFG).
Extension modules would compile against a different _PyThreadStateImpl layout
than the interpreter -- links fine, corrupts at runtime."
done
rl_log "installed pyconfig.h carries both defines"

"$PYBIN" -c '
import sys
assert not sys._is_gil_enabled(), "GIL is ENABLED -- --disable-gil did not take"
print("interpreter:", sys.version.split()[0], "| free-threaded:", not sys._is_gil_enabled())
' || rl_die "installed interpreter failed its smoke check"

"$PYBIN" -m ensurepip --upgrade > /dev/null 2>&1 || true
# A fresh prefix has no setuptools (unbundled since 3.12), and setup.py needs it.
rl_step "seed build dependencies"
"$PYBIN" -m pip install -q --upgrade pip setuptools wheel \
    || rl_die "could not install setuptools/wheel into $PREFIX -- setup.py cannot run without them"
rl_log "pip/setuptools/wheel installed"

printf '%s\n' "$PYBIN" > "$WORK/pybin-$VERSION-$PLATFORM.txt"
rl_step "BUILD OK -- $PYBIN"
