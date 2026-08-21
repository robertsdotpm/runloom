#!/usr/bin/env bash
# package_release.sh -- turn a built+tested prefix into a release artifact.
#
# Produces, under dist/patched-cpython/:
#   runloom-cpython-<version>-<platform>.tar.gz   the whole install prefix
#   runloom-cpython-<version>-<platform>.json     what it is and how it was made
#   SHA256SUMS                                    appended, one line per artifact
#
# RELOCATABILITY: the artifact extracts and RUNS at any path -- modern CPython's
# getpath resolves sys.prefix, the stdlib, and the include/ headers relative to
# the interpreter binary, so `tar x` anywhere then run bin/pythonX.Yt.  (Verified:
# sys.prefix and sysconfig.get_path("include") follow the extraction dir.)  The
# ONE stale value is sysconfig.get_config_var("prefix"), baked to the build path;
# it is cosmetic for running and only matters to some tools that compile
# extensions against the install.  The .json still records build_prefix for
# provenance, not because extraction must land there.
#
# Usage:  tools/ci/package_release.sh <version>
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=lib.sh
. "$HERE/lib.sh"
# shellcheck source=versions.env
. "$HERE/versions.env"

VERSION="${1:-}"
[ -n "$VERSION" ] || rl_die "usage: $0 <version>"
rl_validate_version "$VERSION"

export RL_CI_PATCHDIR="$ROOT/src/patches"
PATCHES="$(rl_patches_of_version "$VERSION")"
SHA256="$(rl_sha_of_version "$VERSION")"
WORK="${RL_CI_WORK:-$HOME/.cache/runloom-ci}"
PLATFORM="$(rl_platform)"
PREFIX="$(rl_resolve_prefix "$VERSION" "$PLATFORM" "$WORK")"
[ -d "$PREFIX" ] || rl_die "no install prefix at $PREFIX -- build first"

OUT="$ROOT/dist/patched-cpython"
mkdir -p "$OUT"
BASE="runloom-cpython-$VERSION-$PLATFORM"
TARBALL="$OUT/$BASE.tar.gz"
META="$OUT/$BASE.json"

rl_step "package $BASE"

# Tar the prefix with its parent stripped, so it extracts as ./<basename>.
( cd "$(dirname "$PREFIX")" && tar czf "$TARBALL" "$(basename "$PREFIX")" )

# Record the exact provenance: which upstream release, which patches (by
# content hash, so a silently edited patch is detectable), which toolchain.
PYBIN="$(cat "$WORK/pybin-$VERSION-$PLATFORM.txt" 2>/dev/null || echo "")"
{
    printf '{\n'
    printf '  "artifact": "%s",\n' "$BASE.tar.gz"
    printf '  "cpython_version": "%s",\n' "$VERSION"
    printf '  "cpython_sha256": "%s",\n' "$SHA256"
    printf '  "platform": "%s",\n' "$PLATFORM"
    printf '  "uname": "%s",\n' "$(uname -srm)"
    printf '  "build_prefix": "%s",\n' "$PREFIX"
    printf '  "free_threaded": true,\n'
    printf '  "features": ['
    sep=""
    for d in $RL_CI_FEATURE_DEFINES; do printf '%s"%s"' "$sep" "$d"; sep=", "; done
    printf '],\n'
    printf '  "runloom_commit": "%s",\n' "$(cd "$ROOT" && git rev-parse HEAD 2>/dev/null || echo unknown)"
    printf '  "patches": [\n'
    first=1
    for p in $PATCHES; do
        [ "$first" -eq 1 ] || printf ',\n'
        first=0
        printf '    {"file": "%s", "sha256": "%s"}' \
            "$p" "$(rl_sha256 "$ROOT/src/patches/$p")"
    done
    printf '\n  ],\n'
    printf '  "interpreter": "%s",\n' "$PYBIN"
    printf '  "artifact_sha256": "%s"\n' "$(rl_sha256 "$TARBALL")"
    printf '}\n'
} > "$META"

# Replace rather than append this artifact's line, so re-running the packager
# does not leave two conflicting hashes for the same filename in SHA256SUMS.
(
    cd "$OUT"
    if [ -f SHA256SUMS ]; then
        grep -v "  $BASE\.tar\.gz\$" SHA256SUMS > SHA256SUMS.tmp || true
        mv SHA256SUMS.tmp SHA256SUMS
    fi
    printf '%s  %s\n' "$(rl_sha256 "$BASE.tar.gz")" "$BASE.tar.gz" >> SHA256SUMS
    sort -k2 -o SHA256SUMS SHA256SUMS
)

rl_log "$(du -h "$TARBALL" | cut -f1)  $TARBALL"
rl_log "metadata: $META"
