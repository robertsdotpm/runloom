#!/usr/bin/env bash
# ci.sh -- the patched-CPython CI, for THIS host.
#
# For each pinned series (tools/ci/versions.env): fetch the exact upstream
# release, apply runloom's migration patches at zero fuzz, build it
# free-threaded with both feature flags, and require BOTH CPython's own stdlib
# suite AND runloom's suite to pass clean against it -- then package the result.
#
# THE CONTRACT is deliberately simple: each pinned release is one whose patches
# apply cleanly AND whose suites are fully green.  If a release does not pass,
# pin one that does (or exclude a genuine upstream-only test in versions.env
# with a reason).  There is no expected-failure list and nothing carries a
# runloom-caused failure forward.
#
# NO HOSTED CI, BY POLICY (CLAUDE.md): there is no .github/workflows here and
# none is to be added.  This script IS the CI -- run it locally before a merge,
# and on the release host to cut artifacts.  tools/ci/release_matrix.sh fans it
# out over SSH to produce the macOS + Linux artifact set.
#
# Usage:
#   tools/ci/ci.sh                          # full matrix (RL_CI_VERSIONS): build+test+package
#   tools/ci/ci.sh --versions="3.14.4 ..."  # explicit version list
#   tools/ci/ci.sh --no-package             # gate only, no artifacts
#   tools/ci/ci.sh --patches-only           # JUST the zero-fuzz apply check (seconds)
#   tools/ci/ci.sh --print-sha <url>        # helper for pinning versions.env
#
# Env: RL_CI_WORK, RL_CI_PREFIX, RL_CI_JOBS, RL_CI_VERSIONS  (see versions.env, lib.sh)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=lib.sh
. "$HERE/lib.sh"

# --print-sha is a pure helper; handle before sourcing the matrix.
if [ "${1:-}" = "--print-sha" ]; then
    [ -n "${2:-}" ] || rl_die "usage: $0 --print-sha <url>"
    tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
    curl -fsSL "$2" -o "$tmp" || rl_die "download failed"
    rl_sha256 "$tmp"
    exit 0
fi

# shellcheck source=versions.env
. "$HERE/versions.env"

DO_PACKAGE=yes
PATCHES_ONLY=no
for a in "$@"; do
    case "$a" in
        --versions=*)   RL_CI_VERSIONS="${a#--versions=}" ;;
        --no-package)   DO_PACKAGE=no ;;
        --patches-only) PATCHES_ONLY=yes ;;
        *)              rl_die "unknown argument: $a" ;;
    esac
done

PLATFORM="$(rl_platform)"
rl_step "runloom patched-CPython CI on $PLATFORM"
rl_log "versions: $RL_CI_VERSIONS"

# ---- fast lane: the patch gate on its own ----------------------------------
# The single most valuable check here, seconds rather than the ~20 min a build
# takes.  Worth running on its own after touching anything under src/patches/.
if [ "$PATCHES_ONLY" = yes ]; then
    # shellcheck disable=SC2086
    exec "$HERE/check_patches.sh" $RL_CI_VERSIONS
fi

overall=0
built=""
for v in $RL_CI_VERSIONS; do
    rl_validate_version "$v"
    rl_step "VERSION $v"

    if ! "$HERE/build_patched_cpython.sh" "$v"; then
        rl_warn "$v: BUILD FAILED"; overall=1; continue
    fi
    if ! "$HERE/test_patched_cpython.sh" "$v"; then
        rl_warn "$v: RELEASE GATE FAILED"; overall=1
        # Deliberately do NOT package a build that failed the release gate.
        continue
    fi
    if [ "$DO_PACKAGE" = yes ]; then
        if ! "$HERE/package_release.sh" "$v"; then
            rl_warn "$v: PACKAGING FAILED"; overall=1; continue
        fi
    fi
    built="$built $v"
done

rl_step "SUMMARY ($PLATFORM)"
rl_log "versions completed:$([ -n "$built" ] && echo "$built" || echo " none")"
if [ "$DO_PACKAGE" = yes ] && [ -d "$ROOT/dist/patched-cpython" ]; then
    ls -1 "$ROOT/dist/patched-cpython" 2>/dev/null | sed 's/^/    /' >&2
fi
[ "$overall" -eq 0 ] || rl_die "CI FAILED"
rl_step "CI OK"
