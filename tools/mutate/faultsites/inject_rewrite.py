#!/usr/bin/env python3
"""inject_rewrite.py <flat.c> <out.c> <sites.json> -- [clang args...]

Systematic first-order fault injection by AST instrumentation: find EVERY call
to a fallible libc/syscall function in the (flattened) TU and wrap it so its
return can be forced to a realistic failure at RUNTIME, one site at a time, via
env RUNLOOM_FI_ENABLED.  No hand-picked site list -- the site set is exactly
"every fallible call clang's AST sees", which is the point (removes the human
judgment the compiled-in RUNLOOM_FAULT_* sites carry).

Each matched call `f(args)` -> `RUNLOOM_FI_<T>(id, ERRNO, f(args))`, where the
macro returns the failure value (-1 / NULL / MAP_FAILED) and sets errno when
site `id` is enabled, else evaluates the real call.  Build ONCE; then a sweep
enables id=0,1,2,... and runs the tests -- a site whose forced failure the suite
doesn't notice is an unchecked/mishandled error path.

Emits sites.json: [{"id":N,"func":"recv","errno":"ECONNRESET","loc":"file:line"}].
libclang is pointed at the system libclang-18 .so.
"""
import json
import sys
import clang.cindex as CI

CI.Config.set_library_file("/usr/lib/llvm-18/lib/libclang.so.1")

# fallible function -> (errno macro, failure-value kind).  kind: "int" -> -1,
# "ptr" -> (void*)0, "map" -> MAP_FAILED.  A representative HARD error each
# (EAGAIN/EINTR are transient and legitimately retried, so not injected here).
FALLIBLE = {
    "recv": ("ECONNRESET", "int"),   "recvfrom": ("ECONNRESET", "int"),
    "send": ("EPIPE", "int"),        "sendto": ("EPIPE", "int"),
    "sendmsg": ("EPIPE", "int"),     "recvmsg": ("ECONNRESET", "int"),
    "read": ("EIO", "int"),          "write": ("EIO", "int"),
    "accept": ("ECONNABORTED", "int"), "accept4": ("ECONNABORTED", "int"),
    "connect": ("ECONNREFUSED", "int"), "socket": ("EMFILE", "int"),
    "bind": ("EADDRINUSE", "int"),   "listen": ("EADDRINUSE", "int"),
    "setsockopt": ("EINVAL", "int"), "getsockopt": ("EINVAL", "int"),
    "getsockname": ("EBADF", "int"), "epoll_ctl": ("ENOMEM", "int"),
    "epoll_wait": ("EINTR", "int"),  "epoll_create1": ("EMFILE", "int"),
    "eventfd": ("EMFILE", "int"),    "eventfd_write": ("EINVAL", "int"),
    "timerfd_create": ("EMFILE", "int"), "timerfd_settime": ("EINVAL", "int"),
    "pipe": ("EMFILE", "int"),       "pipe2": ("EMFILE", "int"),
    "dup": ("EMFILE", "int"),        "dup2": ("EMFILE", "int"),
    "fcntl": ("EINVAL", "int"),      "close": ("EIO", "int"),
    "poll": ("EINVAL", "int"),       "select": ("EINVAL", "int"),
    "malloc": ("ENOMEM", "ptr"),     "calloc": ("ENOMEM", "ptr"),
    "realloc": ("ENOMEM", "ptr"),    "mmap": ("ENOMEM", "map"),
    "posix_memalign": ("ENOMEM", "int"),
    "io_uring_setup": ("ENOMEM", "int"), "io_uring_enter": ("EAGAIN", "int"),
    "io_uring_register": ("ENOMEM", "int"),
    "pthread_create": ("EAGAIN", "int"), "pthread_mutex_init": ("ENOMEM", "int"),
    "sysconf": ("EINVAL", "int"),    "getrandom": ("EINTR", "int"),
}
MACRO = {"int": "RUNLOOM_FI_I", "ptr": "RUNLOOM_FI_P", "map": "RUNLOOM_FI_M"}


def find_sites(flat_path, clang_args):
    idx = CI.Index.create()
    tu = idx.parse(flat_path, args=list(clang_args) + ["-std=gnu11"],
                   options=CI.TranslationUnit.PARSE_DETAILED_PROCESSING_RECORD)
    hard = [d.spelling for d in tu.diagnostics if d.severity >= 3]
    if hard:
        sys.stderr.write("WARN: %d clang errors parsing flat TU (sites in "
                         "un-parseable regions are missed):\n  %s\n"
                         % (len(hard), "\n  ".join(hard[:5])))
    sites = []
    seen = set()
    main = flat_path

    def walk(n):
        if n.kind == CI.CursorKind.CALL_EXPR and n.spelling in FALLIBLE:
            e = n.extent
            # only the flattened file itself (skip anything from a real header)
            if e.start.file and e.start.file.name == main:
                key = (e.start.offset, e.end.offset)
                if key not in seen and e.end.offset > e.start.offset:
                    seen.add(key)
                    err, kind = FALLIBLE[n.spelling]
                    sites.append({"start": e.start.offset, "end": e.end.offset,
                                  "func": n.spelling, "errno": err,
                                  "macro": MACRO[kind], "line": e.start.line})
        for c in n.get_children():
            walk(c)
    walk(tu.cursor)
    return sites


PRELUDE = r'''
/* ==== runloom systematic fault-injection prelude (inject_rewrite.py) ==== */
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#ifndef MAP_FAILED
#define MAP_FAILED ((void *) -1)
#endif
static int __rfi_ready = 0;
static uint64_t __rfi_bits[%(nwords)d];
static void __rfi_init(void) {
    const char *e = getenv("RUNLOOM_FI_ENABLED");
    if (e) {
        const char *p = e;
        while (*p) {
            long id = strtol(p, (char **)&p, 10);
            if (id >= 0 && id < %(nsites)d) __rfi_bits[id >> 6] |= (uint64_t)1 << (id & 63);
            while (*p == ',' || *p == ' ') p++;
        }
    }
    __rfi_ready = 1;   /* benign idempotent race across hub threads */
}
static inline int __rfi_on(int id) {
    if (!__rfi_ready) __rfi_init();
    return (int)((__rfi_bits[id >> 6] >> (id & 63)) & 1);
}
/* the wrapped call is ONE macro arg -- its own commas are paren-protected */
#define RUNLOOM_FI_I(id, err, call) (__rfi_on(id) ? (errno = (err), -1) : (call))
#define RUNLOOM_FI_P(id, err, call) (__rfi_on(id) ? (errno = (err), (void *)0) : (call))
#define RUNLOOM_FI_M(id, err, call) (__rfi_on(id) ? (errno = (err), MAP_FAILED) : (call))
/* ==================================================================== */
'''


def rewrite(flat_path, out_path, sites_path, clang_args, mapfn=None):
    src = open(flat_path, "rb").read()
    sites = find_sites(flat_path, clang_args)
    for i, s in enumerate(sites):
        s["id"] = i
    # point-insertions: prefix at start, suffix ')' at end; apply high->low so
    # lower offsets stay valid (handles nested fallible calls correctly).
    ins = []
    for s in sites:
        ins.append((s["end"], 1, b")"))
        ins.append((s["start"], 0, ("%s(%d, %s, " % (s["macro"], s["id"], s["errno"])).encode()))
    ins.sort(key=lambda t: (t[0], t[1]), reverse=True)
    buf = bytearray(src)
    for off, _pref, text in ins:
        buf[off:off] = text
    nwords = (len(sites) // 64) + 1
    prelude = (PRELUDE % {"nwords": nwords, "nsites": len(sites)}).encode()
    open(out_path, "wb").write(prelude + bytes(buf))

    spans = json.load(open(mapfn)) if mapfn else []
    def to_loc(flat_line):
        for sp in spans:
            if sp["flat_lo"] <= flat_line <= sp["flat_hi"]:
                import os
                return "%s:%d" % (os.path.basename(sp["src"]),
                                  sp["src_off"] + (flat_line - sp["flat_lo"]))
        return "flat:%d" % flat_line
    out = [{"id": s["id"], "func": s["func"], "errno": s["errno"],
            "loc": to_loc(s["line"])} for s in sites]
    json.dump(out, open(sites_path, "w"))
    print("instrumented %d fallible call sites -> %s" % (len(sites), out_path))
    from collections import Counter
    for fn, c in Counter(s["func"] for s in sites).most_common(12):
        print("  %-16s %d" % (fn, c))
    return len(sites)


if __name__ == "__main__":
    if "--" not in sys.argv:
        sys.exit("usage: inject_rewrite.py <flat.c> <out.c> <sites.json> [--mapfile M] -- <clang args>")
    dd = sys.argv.index("--")
    pos = sys.argv[1:dd]
    cargs = sys.argv[dd + 1:]
    mapfn = None
    if "--mapfile" in pos:
        mi = pos.index("--mapfile"); mapfn = pos[mi + 1]; pos = pos[:mi] + pos[mi + 2:]
    rewrite(pos[0], pos[1], pos[2], cargs, mapfn)
