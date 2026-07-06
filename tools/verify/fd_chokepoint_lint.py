#!/usr/bin/env python3
"""fd_chokepoint_lint.py -- enforce the netpoll registration single-writer
chokepoint (item 7, first increment).

The stale-cache-vs-kernel bug class (10 in the appendix) is fed by the
registration-mutating syscall being scattered: a path mutates the kernel epoll
without updating runloom's fd_armed cache (or vice versa), and a parker hangs or
a reused fd wakes on a stale arm.  The structural fix is ONE writer -- a single
TU allowed to call epoll_ctl -- so cache+kernel are mutated under one lock in one
place.

Today epoll_ctl is called from a small allowlist of netpoll TUs (down from the
sprawl the class came from).  This lint RATCHETS that: it FAILS if epoll_ctl
appears in a file NOT on the allowlist, so the write surface can only SHRINK
toward the single chokepoint (netpoll_register), never spread again.  Removing a
file from ALLOWED as its calls are consolidated is the migration; adding one is
a regression the lint catches.

House style: %/.format, prints kept.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SRC = os.path.join(ROOT, "src", "runloom_c")

# The ONLY files permitted to issue the registration-mutating syscall.  The end
# state is a single entry (netpoll_register); shrink this list as call sites are
# funnelled, never grow it.
ALLOWED = {
    "netpoll_register.c.inc",   # the chokepoint (target: the sole writer)
    "netpoll_init.c.inc",       # pool/epoll bring-up (ADD of the wake eventfd)
    "netpoll_wake_iouring.c.inc",  # io_uring/wake-eventfd arm (EPOLLEXCLUSIVE path)
}

# match a real call, not a mention in a comment or the man-page prose.
CALL_RE = re.compile(r"(?<![A-Za-z_])epoll_ctl\s*\(")
COMMENT_RE = re.compile(r"^\s*(\*|/\*|//)")


def call_sites():
    sites = {}
    for name in os.listdir(SRC):
        if not (name.endswith(".c") or name.endswith(".c.inc")):
            continue
        path = os.path.join(SRC, name)
        for i, line in enumerate(open(path, errors="replace"), 1):
            if COMMENT_RE.match(line):
                continue
            # drop trailing line comments before matching
            code = line.split("//", 1)[0]
            if CALL_RE.search(code):
                sites.setdefault(name, []).append(i)
    return sites


def main():
    sites = call_sites()
    offenders = {f: ls for f, ls in sites.items() if f not in ALLOWED}
    total = sum(len(ls) for ls in sites.values())
    print("[fd-chokepoint] epoll_ctl call sites: %d calls in %d file(s) "
          "(allowlist has %d)" % (total, len(sites), len(ALLOWED)))
    for f in sorted(sites):
        tag = "OK" if f in ALLOWED else "OFF-ALLOWLIST"
        print("    %-30s %2d calls  [%s]" % (f, len(sites[f]), tag))
    # also surface allowlist entries that no longer call it -> shrink the list
    stale = sorted(a for a in ALLOWED if a not in sites)
    if stale:
        print("  allowlist entries with NO calls (remove them -- the surface "
              "shrank): %s" % ", ".join(stale))
    if offenders:
        print("[fd-chokepoint] FAIL: epoll_ctl called outside the chokepoint "
              "allowlist -- kernel registration must stay single-writer:")
        for f in sorted(offenders):
            print("  %s:%s" % (f, ",".join(str(x) for x in offenders[f])))
        print("  route the registration through netpoll_register (which owns the "
              "fd_armed cache under runloom_pool.lock), or add a justified "
              "allowlist entry in %s." % os.path.relpath(__file__, ROOT))
        return 1
    print("[fd-chokepoint] OK: kernel registration stays within the chokepoint "
          "allowlist.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
