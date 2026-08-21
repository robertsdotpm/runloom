#!/usr/bin/env sh
# release_matrix.sh -- produce the macOS + Linux patched-interpreter artifact set.
#
# There is no hosted CI here by policy (CLAUDE.md), and a patched CPython cannot
# be cross-built anyway -- each OS must build its own.  So this does what
# scripts/collect_wheels.sh already does for wheels: run the real CI on this
# machine, run it over SSH on the other platform's host, and shovel the
# artifacts back into dist/patched-cpython/.
#
# Every host builds from a FRESH git clone of a configurable repo URL + ref, in
# a throwaway directory deleted on exit -- artifacts always come from exactly
# the committed code you intend to release, never a mutated local tree.
#
# Configure in an UNTRACKED file (never committed): scripts/release_hosts.env
# (same file scripts/collect_wheels.sh reads; copy scripts/release_hosts.env.example):
#   RUNLOOM_REPO_URL   git URL to build from   (PROMPTED if unset)
#   RUNLOOM_REF        branch/tag/sha          (default: main)
#   RELEASE_SSH_HOSTS  space-separated "<target>|<base-dir>|<kind>" entries.
#                      Only kind=posix hosts are used here -- there is no
#                      free-threaded MSVC migration target, and the exec-home
#                      patch explicitly does not cover MSVC's thread-id
#                      intrinsics, so Windows is skipped rather than shipped
#                      half-patched.
#                      Each host needs: git, curl, a C toolchain, make.
#
# RELEASE PREFIX: CPython installs are not relocatable, so release artifacts
# should be built at a stable path rather than a per-user cache.  Override with
# RL_CI_RELEASE_PREFIX (default /opt/runloom-cpython); it is passed through to
# every host.  The host must be able to create it (or pre-create it writable).
#
# Usage:  tools/ci/release_matrix.sh
#         RL_CI_VERSIONS="3.14.4" tools/ci/release_matrix.sh     # one version
set -eu

cd "$(dirname "$0")/../.."
ROOT="$(pwd)"
OUT="$ROOT/dist/patched-cpython"
mkdir -p "$OUT"

if [ -f scripts/release_hosts.env ]; then
    # shellcheck disable=SC1091
    . scripts/release_hosts.env
fi

ask() {   # ask VAR "prompt" "default"   -- no TTY means take the default, never hang
    eval "cur=\${$1:-}"; [ -n "$cur" ] && return
    def="$3"; ans=""
    [ -t 0 ] && { printf '%s [%s]: ' "$2" "$def" >&2; read -r ans || true; }
    [ -z "$ans" ] && ans="$def"
    eval "$1=\$ans"
}
ask RUNLOOM_REPO_URL "Repo URL to build from" ""
ask RUNLOOM_REF      "Ref to build (branch/tag/sha)" "main"
[ -z "${RELEASE_SSH_HOSTS+set}" ] && ask RELEASE_SSH_HOSTS \
    "Remote build hosts 'target|dir|kind ...' (blank = this machine only)" ""
: "${RELEASE_SSH_HOSTS:=}"
: "${RL_CI_RELEASE_PREFIX:=/opt/runloom-cpython}"
: "${RL_CI_VERSIONS:=}"

STAMP="$(date +%Y%m%d-%H%M%S)-$$"
REMOTE_CLEANUP=""
cleanup() {
    printf '%s\n' "$REMOTE_CLEANUP" | while IFS='|' read -r t d; do
        [ -z "$t" ] && continue
        ssh "$t" "rm -rf '$d'" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT INT TERM

say() { printf '\n\033[1m[release] %s\033[0m\n' "$*" >&2; }

# ---- this machine -----------------------------------------------------------

say "building on this host ($(uname -s) $(uname -m))"
# RL_CI_RELEASE_PREFIX (not RL_CI_PREFIX) so each version lands in its own
# <base>/<version> and they do not overwrite one another.
RL_CI_RELEASE_PREFIX="$RL_CI_RELEASE_PREFIX" RL_CI_VERSIONS="${RL_CI_VERSIONS:-}" tools/ci/ci.sh \
    || { printf '[release] LOCAL BUILD FAILED\n' >&2; exit 1; }

# ---- remote posix hosts -----------------------------------------------------

for entry in $RELEASE_SSH_HOSTS; do
    target="$(printf '%s' "$entry" | cut -d'|' -f1)"
    basedir="$(printf '%s' "$entry" | cut -d'|' -f2)"
    kind="$(printf '%s' "$entry" | cut -d'|' -f3)"
    [ -z "$target" ] && continue
    if [ "$kind" = windows ]; then
        say "SKIP $target (windows): no free-threaded MSVC migration target; exec-home does not cover MSVC thread-id intrinsics"
        continue
    fi
    [ -n "$RUNLOOM_REPO_URL" ] || { printf '[release] RUNLOOM_REPO_URL unset -- cannot build remotely\n' >&2; exit 1; }

    workdir="$basedir/rl-ci-$STAMP"
    REMOTE_CLEANUP="$REMOTE_CLEANUP
$target|$workdir"

    say "building on $target ($workdir)"
    ssh "$target" "sh -eu -c '
        mkdir -p \"$workdir\"
        cd \"$workdir\"
        git clone --depth 1 --branch \"$RUNLOOM_REF\" \"$RUNLOOM_REPO_URL\" repo
        cd repo
        RL_CI_VERSIONS=\"${RL_CI_VERSIONS:-}\" RL_CI_RELEASE_PREFIX=\"$RL_CI_RELEASE_PREFIX\" tools/ci/ci.sh
    '" || { printf '[release] REMOTE BUILD FAILED on %s\n' "$target" >&2; exit 1; }

    say "collecting artifacts from $target"
    scp -q "$target:$workdir/repo/dist/patched-cpython/*" "$OUT/" \
        || printf '[release] warning: nothing collected from %s\n' "$target" >&2
done

# ---- manifest ---------------------------------------------------------------

say "artifact set in $OUT"
ls -1 "$OUT" | sed 's/^/    /' >&2

have_mac=no; have_linux=no
for f in "$OUT"/*.tar.gz; do
    case "$f" in
        *darwin*) have_mac=yes ;;
        *linux*)  have_linux=yes ;;
    esac
done
[ "$have_mac"   = yes ] || printf '[release] WARNING: no darwin artifact in the set\n' >&2
[ "$have_linux" = yes ] || printf '[release] WARNING: no linux artifact in the set\n' >&2
say "done"
