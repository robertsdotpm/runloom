#!/usr/bin/env bash
# check_patches.sh -- the fast lane: prove the pinned patches still apply at
# ZERO FUZZ to their pinned CPython releases, without building anything.
#
# Seconds instead of the ~20 minutes a full build takes, and it catches the
# failure mode that matters most: a patch that has silently stopped matching its
# target.  Run it after touching anything under src/patches/ or bumping a pin.
#
# It calls the SAME lib.sh helpers the real build calls -- rl_fetch_cpython,
# rl_apply_patches, rl_verify_witnesses -- so a pass here means the build's own
# patch stage will pass too.  Everything happens in a throwaway tree.
#
# Usage:  tools/ci/check_patches.sh [version...]    # default: RL_CI_VERSIONS
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=lib.sh
. "$HERE/lib.sh"
# shellcheck source=versions.env
. "$HERE/versions.env"

export RL_CI_PATCHDIR="$ROOT/src/patches"
WORK="${RL_CI_WORK:-$HOME/.cache/runloom-ci}"
mkdir -p "$WORK"

VERSIONS="${*:-$RL_CI_VERSIONS}"
SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/rl-patchcheck.XXXXXX")"
trap 'rl_rm -rf "$SCRATCH"' EXIT

for v in $VERSIONS; do
    rl_validate_version "$v"
    SHA256="$(rl_sha_of_version "$v")"
    PATCHES="$(rl_patches_of_version "$v")"
    [ -n "$SHA256" ] || rl_die "version $v is not pinned in tools/ci/versions.env (RL_CI_PINS)"

    rl_step "$v (series $(rl_series_of_version "$v")) -- $PATCHES"
    rl_fetch_cpython "$v" "$SHA256" "$WORK/Python-$v.tgz"

    src="$SCRATCH/$v"
    mkdir -p "$src"
    tar xzf "$WORK/Python-$v.tgz" -C "$src" --strip-components=1

    # shellcheck disable=SC2086
    rl_apply_patches "$src" "$RL_CI_PATCHDIR" "$v" $PATCHES
    rl_verify_witnesses "$src"
    rl_log "$v: patches apply cleanly"
done

rl_step "PATCH GATE OK"
