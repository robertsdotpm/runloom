#!/usr/bin/env sh
# bootstrap_compiler.sh
#
# Best-effort: install a C compiler suitable for building pygo if one
# isn't already on the system.  Idempotent; safe to re-run.
#
# Strategy: detect distro / OS via uname + /etc/os-release, then pick
# the package manager's "give me a C compiler" command.  We prefer the
# system compiler because (a) it matches glibc/libc ABI, (b) it's the
# one users debug with, (c) on container hosts it's usually already
# half-installed.
#
# Exit codes:
#   0  success (compiler present, or installed)
#   1  unsupported / cannot determine package manager
#   2  install attempt failed
#
# Usage:
#   ./scripts/bootstrap_compiler.sh
#   sudo ./scripts/bootstrap_compiler.sh        # for distros that need root
set -eu

have() { command -v "$1" >/dev/null 2>&1; }
log()  { printf '%s\n' "[bootstrap] $*"; }

if have cc || have gcc || have clang; then
    cc="$(command -v cc 2>/dev/null || command -v gcc 2>/dev/null || command -v clang)"
    log "compiler already present: $cc"
    exit 0
fi

OS="$(uname -s)"
log "no C compiler found; OS=$OS"

# Detect privilege escalation helper.  Order: explicit sudo, doas (OpenBSD),
# pkexec (some Linuxes), nothing.
if [ "$(id -u 2>/dev/null || echo 0)" = "0" ]; then
    SUDO=""
elif have sudo; then
    SUDO="sudo"
elif have doas; then
    SUDO="doas"
else
    SUDO=""
    log "warn: not root and no sudo/doas; install may fail"
fi

run() {
    log "+ $SUDO $*"
    $SUDO "$@"
}

# -------------------- Linux --------------------
if [ "$OS" = "Linux" ]; then
    # /etc/os-release is the modern standard.
    if [ -r /etc/os-release ]; then
        . /etc/os-release
    fi
    case "${ID:-}${ID_LIKE:-}" in
        *debian*|*ubuntu*|*mint*|*pop*|*kali*|*raspbian*)
            run apt-get update -qq
            run apt-get install -y --no-install-recommends gcc make libc6-dev || exit 2
            ;;
        *fedora*|*rhel*|*centos*|*rocky*|*alma*|*amzn*)
            run dnf install -y gcc make glibc-devel || run yum install -y gcc make glibc-devel || exit 2
            ;;
        *arch*|*manjaro*|*endeavour*)
            run pacman -Sy --noconfirm --needed gcc make || exit 2
            ;;
        *suse*|*opensuse*)
            run zypper install -y gcc make glibc-devel || exit 2
            ;;
        *alpine*)
            run apk add --no-cache gcc musl-dev make || exit 2
            ;;
        *gentoo*)
            # Gentoo always has gcc via @system; if it's somehow missing,
            # emerge would take ages.  Don't try.
            log "Gentoo with no gcc is unusual; please 'emerge sys-devel/gcc' manually"
            exit 1
            ;;
        *void*)
            run xbps-install -y gcc make musl-devel libc-devel || exit 2
            ;;
        *)
            # Unknown distro -- try the most common managers anyway.
            for mgr in apt-get dnf yum pacman zypper apk; do
                if have "$mgr"; then
                    log "trying $mgr (unknown distro)"
                    case "$mgr" in
                        apt-get) run apt-get install -y gcc make ;;
                        dnf|yum) run "$mgr" install -y gcc make ;;
                        pacman)  run pacman -Sy --noconfirm gcc make ;;
                        zypper)  run zypper install -y gcc make ;;
                        apk)     run apk add --no-cache gcc musl-dev make ;;
                    esac && break
                fi
            done
            ;;
    esac

# -------------------- macOS --------------------
elif [ "$OS" = "Darwin" ]; then
    # macOS ships clang via Command Line Tools.  Trigger CLT install if
    # missing; xcode-select pops a GUI dialog.
    if have xcode-select; then
        log "triggering Command Line Tools install"
        xcode-select --install || true
        log "follow the GUI prompt, then re-run this script"
    else
        log "macOS without xcode-select?  install Xcode from the App Store"
        exit 1
    fi

# -------------------- BSDs --------------------
elif [ "$OS" = "FreeBSD" ]; then
    run pkg install -y gcc lang/gcc || exit 2
elif [ "$OS" = "OpenBSD" ]; then
    log "OpenBSD ships clang in the base system; if it's truly missing"
    log "your install is broken -- reinstall xbase from the install media."
    exit 1
elif [ "$OS" = "NetBSD" ]; then
    run pkgin -y install gcc || exit 2
elif [ "$OS" = "DragonFly" ]; then
    run pkg install -y gcc || exit 2

# -------------------- Solaris / illumos --------------------
elif [ "$OS" = "SunOS" ]; then
    # Solaris uses pkg(5), illumos derivatives use pkgin or pkg(7).
    if have pkg; then
        run pkg install developer/gcc || exit 2
    elif have pkgin; then
        run pkgin -y install gcc || exit 2
    else
        log "no Solaris/illumos package manager found"
        exit 1
    fi

# -------------------- Haiku, AIX, others --------------------
elif [ "$OS" = "Haiku" ]; then
    run pkgman install gcc make || exit 2

else
    log "unknown OS '$OS'; install gcc or clang manually"
    exit 1
fi

if have cc || have gcc || have clang; then
    log "compiler installed successfully"
    exit 0
fi
log "install attempted but no compiler is on PATH"
exit 2
