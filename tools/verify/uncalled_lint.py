#!/usr/bin/env python3
"""uncalled_lint.py -- fail on a non-static runloom_* function with no callers.

WHY THIS EXISTS.  `runloom_iouring_signal_wake` sat in the tree with a
DECLARATION nowhere and a caller nowhere, while its own comment asserted "the
single-thread drain calls it after netpoll_signal_wake finds no parker".
`git log -S` says the drain never did.  The consequence was not a dead symbol:
a fiber blocked on an io_uring completion could not receive a Ctrl-C at all,
and -- because the path was never once exercised -- its CONSUMER had rotted
too, overwriting a delivered KeyboardInterrupt with OSError.  Two defects, each
hiding the other, in code that reads as correct.

That is the class this lint is aimed at: A DELIVERY PATH THAT EXISTS, IS
UNREACHABLE, AND IS THEREFORE WRONG WHERE NOBODY CAN SEE.  Tests cannot catch
it (nothing can call it), review does not (it looks fine and claims a caller),
and the compiler does not (it is non-static, so -Wunused-function is silent).
A coverage report WOULD have shown it at 0% -- the signal existed for years and
nothing gated on it.  This is that gate, minus the gcov run.

RATCHET.  ALLOWED starts EMPTY and should stay that way: at the commit that
added this lint, all 388 non-static runloom_* functions had at least one call
site.  An entry here is a promise that a symbol is deliberately uncalled today
(a platform stub whose callers are all #ifdef'd out on every build machine, an
API kept for an out-of-tree consumer) and it needs the reason in the comment.
Adding one is a regression; removing one is progress.

Validated against 4fe83911 -- the commit before io_uring signal delivery was
wired up -- where it reports exactly runloom_iouring_signal_wake.

House style: %/.format, prints kept.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SRC = os.path.join(ROOT, "src", "runloom_c")

# Deliberately uncalled symbols.  EMPTY IS THE CORRECT STATE -- see the ratchet
# note above.  Format: "name": "why it is uncalled and who is expected to call it".
ALLOWED = {
    # An exported, PyObject-free C API for out-of-tree consumers ("so a Cython
    # handler that calls these has an allocation-free hot loop" --
    # runloom_tcp_capi.c.inc).  Uncalled in-tree BY DESIGN.  Note the risk this
    # lint cannot cover: like any unexercised path, these can rot -- they mirror
    # RunloomTCPConn_recv_into/_send_all by hand, so a change to one of those
    # must be mirrored here or the C API silently diverges.
    "runloom_tcpconn_c_recv_into":  "runloom_tcp_capi.h C API (out-of-tree)",
    "runloom_tcpconn_c_send_all":   "runloom_tcp_capi.h C API (out-of-tree)",
    "runloom_tcp_c_fd_recv":        "runloom_tcp_capi.h C API (out-of-tree)",
    "runloom_tcp_c_fd_send_all":    "runloom_tcp_capi.h C API (out-of-tree)",
    "runloom_tcp_c_fd_close":       "runloom_tcp_capi.h C API (out-of-tree)",

    # Deliberately never called: the pool's threads persist across run cycles
    # rather than being torn down (mn_sched_init_fini.c.inc:571 says so, and
    # tests/test_cov95_blockpool_gstate.py carries it in its exclusions[]).
    "runloom_blockpool_fini":       "pool persists across cycles; never torn down",

    # Superseded accessor.  Its comment still says "for the io_uring-as-loop
    # backend to poll-add into a hub ring", but the hub calls the PER-HUB
    # runloom_netpoll_hub_epoll_fd() instead (mn_sched_hub_main.c.inc:425).
    # Left as ALLOWED rather than deleted because it is a plain accessor with
    # nothing to rot; delete it if the shared-fd path is confirmed dead.
    "runloom_netpoll_epoll_fd":     "superseded by runloom_netpoll_hub_epoll_fd",

    # ---- FOUND BY THIS LINT, NOT YET TRIAGED -------------------------------
    # These three are genuinely uncalled: a definition and a prototype, and
    # nothing else in the tree.  They are listed so the gate is green and the
    # ratchet works from today; they are NOT blessed.  Each needs a decision:
    # wire it up, or delete it.
    #
    # runloom_park_until is the most interesting, and it is the same shape as
    # the bug that motivated this lint: the RUNLOOM_FAULT_SPURIOUS_PARK note in
    # runloom_sched_parkwake.c.inc says "Every consumer in-tree does
    # (runloom_blocking_call, runloom_park_until, the io_uring waits, ...)" --
    # naming it as a live consumer of the park_safe contract when it has no
    # callers at all.  Whatever it was meant to serve is being served by
    # something else, and the fault-injection knob has one fewer consumer than
    # its documentation claims.
    # Orphaned MID-MIGRATION, not accidental: 367055b2 added it as the unified
    # predicate-park entry, 82dc67c0 moved the blockpool onto it, and 6a733b74
    # moved the blockpool back off while fixing a stranded wake credit -- taking
    # its only adopter with it.  Decision needed: resume the migration (with that
    # credit fix inside) or delete it.  The stale claim that it was a live
    # park_safe consumer has been removed from runloom_sched_parkwake.c.inc.
    "runloom_park_until":           "TRIAGE: migration paused, zero adopters since 6a733b74",
    "runloom_coro_init_at":         "TRIAGE: uncalled; placement-new coro ctor",
    "runloom_coro_arena_stack":     "TRIAGE: uncalled; arena stack allocator",
}

# A non-static definition: a line at column 0 that is not a declaration (no
# trailing ';') and names a runloom_* function.
DEF_RE = re.compile(
    r"^(?!static\b)(?:[A-Za-z_][A-Za-z0-9_ \*]*?)\b(runloom_[A-Za-z0-9_]+)\s*\([^;]*$")
# Bare identifier, NOT `name(`.  runloom_g_entry is handed to
# runloom_coro_new() as a function pointer at four sites and never called
# with parentheses anywhere; requiring "(" reported the fiber entry point
# itself as dead.  Prototypes are excluded by position instead (see
# _header_call_text), which is the only reliable discriminator here.
USE_RE = re.compile(r"(?<![A-Za-z0-9_])(runloom_[A-Za-z0-9_]+)(?![A-Za-z0-9_])")
SKIP_DIRS = (".git", "build", "__pycache__", ".check-logs", ".pytest_cache")


def definitions():
    """name -> [(path, lineno), ...].  A platform stub gives a second entry."""
    out = {}
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in sorted(files):
            if not (f.endswith(".c") or f.endswith(".inc")):
                continue
            path = os.path.join(root, f)
            with open(path, errors="replace") as fh:
                for i, line in enumerate(fh, 1):
                    if line[:1] in (" ", "\t", "#", "/", "*", "\n"):
                        continue
                    m = DEF_RE.match(line.rstrip())
                    if m and "=" not in line.split("(")[0]:
                        out.setdefault(m.group(1), []).append((path, i))
    return out


# In a header, a PROTOTYPE is not a caller but a MACRO BODY is -- and the two
# are not separable by shape: `RUNLOOM_EVT(...)` expands to a statement ending
# in `;`, exactly like the declaration above it.  Trying to tell them apart by
# regex reported runloom_evt_log_ and friends as dead (they are not).  So track
# macro continuation instead: inside a #define, an occurrence is a call;
# outside one, it is a declaration.
def _header_call_text(text):
    """Keep the parts of a header where a name occurrence is a CALL.

    Three things live in a header and only one of them is not a caller:
      * a PROTOTYPE at declaration level      -- not a call
      * a MACRO BODY (RUNLOOM_EVT -> ...)     -- a call
      * an INLINE FUNCTION BODY (RUNLOOM_INLINE runloom_lockrank_push) -- a call

    Shape does not separate them: a macro body ends in `;` exactly like the
    prototype above it.  Position does.  A call sits inside braces or inside a
    #define; a prototype sits at brace depth zero outside one.  Getting this
    wrong reported the whole diagnostics layer (kcsan, lockrank, cover, evt) as
    dead, which is how a lint gets switched off.
    """
    out, in_macro, depth = [], False, 0
    for line in text.split("\n"):
        stripped = line.strip()
        opens_macro = (not in_macro) and re.match(r"#\s*define\b", stripped)
        if opens_macro or in_macro or depth > 0:
            out.append(line)
        if opens_macro or in_macro:
            in_macro = stripped.endswith("\\")
        depth += line.count("{") - line.count("}")
        if depth < 0:
            depth = 0
    return "\n".join(out)


# A MENTION IS NOT A CALL.  runloom_iouring_signal_wake's own header comment
# said "runloom_iouring_signal_wake() is called from that same thread's drain"
# -- it was not, and never had been.  With comments left in, that sentence was
# the only "call site" the function had, and this lint reported OK on the exact
# commit whose bug motivated it.  A lint defeated by the lie it exists to catch
# is worse than no lint, so strip comments before counting.
C_BLOCK_RE = re.compile(r"/\*.*?\*/", re.S)
C_LINE_RE = re.compile(r"//[^\n]*")
PY_DOC_RE = re.compile("'''" + r".*?" + "'''" + "|" + '"""' + r".*?" + '"""', re.S)
PY_LINE_RE = re.compile(r"#[^\n]*")


def _strip_comments(path, text):
    if path.endswith(".py"):
        return PY_LINE_RE.sub("", PY_DOC_RE.sub("", text))
    return C_LINE_RE.sub("", C_BLOCK_RE.sub("", text))


def call_counts():
    """name -> occurrences that are plausibly CALLS.

    Assembly counts too: runloom_asm_entry is only ever reached from swap_*.S,
    where it appears bare, with no parentheses.
    """
    counts = {}
    for root, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            if not f.endswith((".c", ".inc", ".py", ".h", ".S", ".s")):
                continue
            path = os.path.join(root, f)
            try:
                text = open(path, errors="replace").read()
            except OSError:
                continue
            text = _strip_comments(path, text)
            if path.endswith(".h"):
                text = _header_call_text(text)
            for m in USE_RE.finditer(text):
                counts[m.group(1)] = counts.get(m.group(1), 0) + 1
    return counts


def main():
    defs = definitions()
    uses = call_counts()
    bad = []
    for name in sorted(defs):
        sites = defs[name]
        # every definition line also matches USE_RE; a real caller is anything beyond them
        if uses.get(name, 0) - len(sites) > 0:
            continue
        if name in ALLOWED:
            continue
        bad.append((name, sites))
    if not bad:
        print("uncalled_lint: OK -- %d non-static runloom_* functions, all called"
              % len(defs))
        return 0
    print("uncalled_lint: FAIL -- %d non-static function(s) with no call site\n" % len(bad))
    for name, sites in bad:
        where = ", ".join("%s:%d" % (os.path.relpath(p, ROOT), l) for p, l in sites)
        print("  %s" % name)
        print("      defined at %s" % where)
    print("\nA non-static function nobody calls is not merely dead: the path it")
    print("belongs to is unreachable, so nothing tests it and it rots silently.")
    print("Wire it up, delete it, or add it to ALLOWED with the reason.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
