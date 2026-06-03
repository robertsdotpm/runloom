#!/usr/bin/env sh
# collect_wheels.sh -- build wheels for EVERY platform from a clean, configurable
# git checkout and gather them, plus the sdist, into ./dist for one
# `twine upload dist/*`.
#
# No hosted CI: this orchestrates cibuildwheel locally (this machine's platform)
# and over SSH on your other build hosts (a Mac, a Windows box), then rsyncs
# their wheels back here.  cibuildwheel cannot cross-build macOS/Windows from
# Linux, so each OS builds its own -- this just automates the shovelling.
#
# Every host builds from a FRESH `git clone` (of a configurable repo URL + ref)
# in a unique throwaway directory that is deleted on exit -- so wheels always
# come from exactly the committed code you intend to release, never a mutated
# or stale tree.  Configure in an UNTRACKED file (never committed):
# scripts/release_hosts.env  (copy scripts/release_hosts.env.example):
#
#   RUNLOOM_REPO_URL  git URL to build from   (default: the GitHub repo)
#   RUNLOOM_REF       branch/tag/sha to build (default: main)
#   RELEASE_SSH_HOSTS space-separated "<ssh-target>:<base-dir>" entries, one per
#                     non-Linux platform (your Mac, your Windows box).  A fresh
#                     build dir is created under <base-dir> and removed on exit.
#                     Each host needs git, a C toolchain, the CPython matrix,
#                     and `pip install "runloom[dev]"` (build + cibuildwheel>=2.20).
#                     The remote shell must be POSIX-ish (bash / git-bash).
#
# Usage:
#   scripts/collect_wheels.sh            # build + collect into dist/, twine check
#   scripts/collect_wheels.sh --upload   # ...then twine upload dist/*
#   RUNLOOM_REF=v0.0.1 scripts/collect_wheels.sh
set -eu

cd "$(dirname "$0")/.."
ROOT="$(pwd)"
PY="${PYTHON:-python3}"

UPLOAD=no
[ "${1:-}" = "--upload" ] && UPLOAD=yes

if [ -f scripts/release_hosts.env ]; then
    # shellcheck disable=SC1091
    . scripts/release_hosts.env
fi

# Prompt for anything not already provided (by env or release_hosts.env).
# Non-interactive (no TTY): silently take the default, so CI/automation
# never hangs.
ask() {   # ask VAR "prompt" "default"
    eval "cur=\${$1:-}"
    [ -n "$cur" ] && return
    def="$3"; ans=""
    if [ -t 0 ]; then
        printf '%s [%s]: ' "$2" "$def" >&2
        read -r ans || true
    fi
    [ -z "$ans" ] && ans="$def"
    eval "$1=\$ans"
}
ask RUNLOOM_REPO_URL "Repo URL to build from" "https://github.com/robertsdotpm/runloom.git"
ask RUNLOOM_REF      "Ref to build (branch/tag/sha)" "main"
# Empty RELEASE_SSH_HOSTS is a valid answer (build only this machine), so only
# prompt when it was never set at all.
if [ -z "${RELEASE_SSH_HOSTS+set}" ]; then
    ask RELEASE_SSH_HOSTS "Remote build hosts 'target:dir target:dir' (blank = this machine only)" ""
fi
: "${RELEASE_SSH_HOSTS:=}"

STAMP="$(date +%Y%m%d-%H%M%S)-$$"
LOCAL_WORK=""
REMOTE_CLEANUP=""   # newline-separated "<ssh-target>|<dir>" of dirs to delete

cleanup() {
    [ -n "$LOCAL_WORK" ] && rm -rf "$LOCAL_WORK" 2>/dev/null || true
    printf '%s\n' "$REMOTE_CLEANUP" | while IFS='|' read -r t d; do
        [ -n "$t" ] && ssh "$t" "rm -rf '$d'" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT INT TERM

echo ">> repo: $RUNLOOM_REPO_URL   ref: $RUNLOOM_REF"
rm -rf wheelhouse dist
mkdir -p wheelhouse dist

# ---- local platform (Linux: manylinux via docker; QEMU for aarch64) ------
LOCAL_WORK="$(mktemp -d)"
echo ">> [local: $(uname -s)] fresh clone + cibuildwheel ..."
git clone --depth 1 --branch "$RUNLOOM_REF" "$RUNLOOM_REPO_URL" "$LOCAL_WORK/src"
( cd "$LOCAL_WORK/src" && "$PY" -m cibuildwheel --output-dir "$ROOT/wheelhouse" )

# ---- remote platforms (Mac, Windows) over SSH ----------------------------
for entry in $RELEASE_SSH_HOSTS; do
    target="${entry%%:*}"
    base="${entry#*:}"
    bdir="$base/runloom-build-$STAMP"
    REMOTE_CLEANUP="$REMOTE_CLEANUP
$target|$bdir"
    echo ">> [$target] fresh clone $RUNLOOM_REF in $bdir + cibuildwheel ..."
    ssh "$target" "rm -rf '$bdir' && git clone --depth 1 --branch '$RUNLOOM_REF' '$RUNLOOM_REPO_URL' '$bdir' && cd '$bdir' && python -m cibuildwheel --output-dir wheelhouse"
    echo ">> [$target] pulling wheels back ..."
    rsync -az "$target:$bdir/wheelhouse/" wheelhouse/
done

# ---- sdist (built from the same fresh checkout) + gather -----------------
echo ">> building sdist from $RUNLOOM_REF ..."
( cd "$LOCAL_WORK/src" && "$PY" -m build --sdist --outdir "$ROOT/dist" )
cp wheelhouse/*.whl dist/

echo ">> twine check ..."
"$PY" -m twine check dist/*

n_whl=$(find dist -name '*.whl' | wc -l | tr -d ' ')
echo
echo ">> dist/ ready: ${n_whl} wheels + 1 sdist (from $RUNLOOM_REF). Throwaway build dirs cleaned."
if [ "$UPLOAD" = yes ]; then
    "$PY" -m twine upload dist/*
else
    echo "   upload with:  $PY -m twine upload dist/*    (or re-run with --upload)"
fi
