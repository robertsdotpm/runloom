#!/usr/bin/env bash
# fetch_prebuilt_cpython.sh -- provision a patched interpreter from the newest
# matching GitHub Release instead of building it.
#
# This is the "prebuilt" half of provide-cpython: when nothing that DEFINES the
# interpreter changed (see changed_needs_build.sh), a released tarball built from
# the SAME patch set is byte-for-byte what a fresh build would produce, so we can
# skip the ~15-min build AND the full CPython stdlib suite (both already ran when
# that release was cut) and go straight to the runloom side.
#
# It is a drop-in substitute for build_patched_cpython.sh: it extracts the
# interpreter into the SAME prefix (rl_resolve_prefix) and writes the SAME
# breadcrumb ($WORK/pybin-<version>-<platform>.txt), so test_patched_cpython.sh
# runs against it with no changes.
#
# SAFETY: it only accepts a release whose recorded provenance matches the CURRENT
# tree -- same cpython_version AND every current patch file's sha256 present in
# the artifact's .json -- and verifies the tarball against the release's
# SHA256SUMS.  So even if the coarse "did the interpreter change?" diff upstream
# is wrong, a stale/mismatched interpreter is refused here, never tested against.
#
# Exits NONZERO without provisioning if no matching, verified artifact is found;
# the caller (provide-cpython) treats that as "fall back to a full build".
#
# Usage: tools/ci/fetch_prebuilt_cpython.sh <version>
# Env:   RL_CI_WORK, RL_CI_RELEASE_PREFIX / RL_CI_PREFIX  (as build_patched_cpython.sh)
#        RL_CI_RELEASE_REPO   owner/repo to pull releases from (default $GITHUB_REPOSITORY)
#        RL_CI_RELEASE_MAX    how many recent releases to scan  (default 12)
#        GH_TOKEN             gh auth (github.token in CI)
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

command -v gh >/dev/null 2>&1 || rl_die "gh CLI not available -- cannot fetch a prebuilt release"
REPO="${RL_CI_RELEASE_REPO:-${GITHUB_REPOSITORY:-}}"
[ -n "$REPO" ] || rl_die "no release repo (set RL_CI_RELEASE_REPO or GITHUB_REPOSITORY)"

export RL_CI_PATCHDIR="$ROOT/src/patches"
PLATFORM="$(rl_platform)"
WORK="${RL_CI_WORK:-$HOME/.cache/runloom-ci}"
PREFIX="$(rl_resolve_prefix "$VERSION" "$PLATFORM" "$WORK")"
BASE="runloom-cpython-$VERSION-$PLATFORM"
MAX="${RL_CI_RELEASE_MAX:-12}"

# The patch hashes this tree would build from; the release's .json must carry
# every one of them or it predates the current patches (stale) -> reject.
PATCHES="$(rl_patches_of_version "$VERSION")"
WANT_HASHES=""
for p in $PATCHES; do
    WANT_HASHES="$WANT_HASHES $(rl_sha256 "$ROOT/src/patches/$p")"
done

mkdir -p "$WORK"
DL="$WORK/prebuilt-$VERSION-$PLATFORM"
rl_rm -rf "$DL"; mkdir -p "$DL"

rl_step "look for a prebuilt $BASE in the last $MAX releases of $REPO"

# Newest-first list of release tags (published releases only; drafts excluded).
TAGS="$(gh release list --repo "$REPO" --limit "$MAX" \
          --json tagName,createdAt,isDraft \
          --jq 'sort_by(.createdAt) | reverse | .[] | select(.isDraft==false) | .tagName' \
        2>/dev/null || true)"
[ -n "$TAGS" ] || rl_die "no releases found on $REPO -- build from source"

FOUND=""
for tag in $TAGS; do
    rl_log "candidate release: $tag"
    rl_rm -f "$DL"/* 2>/dev/null || true
    if ! gh release download "$tag" --repo "$REPO" --dir "$DL" --clobber \
            --pattern "$BASE.tar.gz" \
            --pattern "$BASE.json" \
            --pattern "SHA256SUMS" >/dev/null 2>&1; then
        rl_log "  $tag has no $BASE artifact"
        continue
    fi
    [ -f "$DL/$BASE.tar.gz" ] || { rl_log "  $tag: tarball missing after download"; continue; }
    [ -f "$DL/$BASE.json" ]   || { rl_log "  $tag: metadata (.json) missing"; continue; }

    # 1. integrity: tarball sha256 must match the release's SHA256SUMS line.
    WANT_SHA=""
    [ -f "$DL/SHA256SUMS" ] && WANT_SHA="$(awk -v f="$BASE.tar.gz" '$2==f {print $1; exit}' "$DL/SHA256SUMS")"
    GOT_SHA="$(rl_sha256 "$DL/$BASE.tar.gz")"
    if [ -z "$WANT_SHA" ] || [ "$WANT_SHA" != "$GOT_SHA" ]; then
        rl_warn "  $tag: sha256 mismatch/absent (want=${WANT_SHA:-<none>} got=$GOT_SHA) -- skipping"
        continue
    fi

    # 2. provenance: same version AND built from exactly the current patches.
    META="$DL/$BASE.json"
    if ! grep -q "\"cpython_version\": \"$VERSION\"" "$META"; then
        rl_warn "  $tag: version does not match $VERSION -- skipping"
        continue
    fi
    prov_ok=1
    for h in $WANT_HASHES; do
        grep -q "\"sha256\": \"$h\"" "$META" || prov_ok=0
    done
    if [ "$prov_ok" != 1 ]; then
        rl_warn "  $tag: patch provenance does not match current src/patches -- stale, skipping"
        continue
    fi

    FOUND="$tag"
    break
done
[ -n "$FOUND" ] || rl_die "no release in the last $MAX with a verified, provenance-matching $BASE.tar.gz -- build from source"

rl_step "extract $BASE.tar.gz (release $FOUND) into $PREFIX"
# The tarball unpacks as ./<basename-of-the-build-prefix>; extract into a scratch
# dir and move the single top-level directory into our prefix, so this works
# regardless of what the packaging host's prefix basename was.
X="$DL/x"; rl_rm -rf "$X"; mkdir -p "$X"
tar xzf "$DL/$BASE.tar.gz" -C "$X"
TOP="$(cd "$X" && ls -1 | head -1)"
[ -n "$TOP" ] || rl_die "extracted tarball is empty"
rl_rm -rf "$PREFIX"
mkdir -p "$(dirname "$PREFIX")"
mv "$X/$TOP" "$PREFIX"

PYBIN="$PREFIX/bin/$(rl_pybin_basename "$VERSION")t"
[ -x "$PYBIN" ] || PYBIN="$PREFIX/bin/$(rl_pybin_basename "$VERSION")"
[ -x "$PYBIN" ] || rl_die "no interpreter under $PREFIX/bin after extract"

# Same post-conditions build_patched_cpython.sh asserts on a fresh install: the
# feature defines must be armed in the INSTALLED pyconfig.h (extension modules
# compile against it) and the interpreter must actually be free-threaded.
rl_step "verify prebuilt interpreter"
INSTALLED_CFG="$("$PYBIN" -c 'import sysconfig; print(sysconfig.get_paths()["include"])')/pyconfig.h"
for d in $RL_CI_FEATURE_DEFINES; do
    grep -q "define $d" "$INSTALLED_CFG" \
        || rl_die "prebuilt pyconfig.h is MISSING $d ($INSTALLED_CFG) -- refusing a mismatched interpreter"
done
"$PYBIN" -c '
import sys
assert not sys._is_gil_enabled(), "GIL is ENABLED in the prebuilt -- wrong artifact"
print("prebuilt interpreter:", sys.version.split()[0], "| free-threaded:", not sys._is_gil_enabled())
' || rl_die "prebuilt interpreter failed its smoke check"
rl_log "installed pyconfig.h carries both defines; interpreter is free-threaded"

printf '%s\n' "$PYBIN" > "$WORK/pybin-$VERSION-$PLATFORM.txt"
rl_ci_summary "♻️ **CPython $VERSION** ($PLATFORM): reused prebuilt from release \`$FOUND\` (skipped source build + stdlib suite)"
rl_step "PREBUILT OK ($FOUND) -- $PYBIN"
