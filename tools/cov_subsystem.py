"""Aggregate gcov line coverage for a SUBSYSTEM whose code is split across a
<stem>.c translation unit plus several <stem>_*.c.inc fragments (gcov emits one
.gcov per source file, so cov_summary.py -- which only reads *.c.gcov -- misses
the .inc fragments where the real code lives).

Honors LCOV-style exclusion markers IN THE SOURCE so the "coverable surface" is
reported honestly:
  /* LCOV_EXCL_LINE <reason> */   on a line  -> that line excluded from total
  /* LCOV_EXCL_START <reason> */ ... /* LCOV_EXCL_STOP */  -> the block excluded
Excluded lines are subtracted from BOTH total and covered, and counted+reported
separately, so coverage = covered_coverable / total_coverable.

Usage:  cov_subsystem.py <covdir>
"""
import os
import sys

SRC = "src/runloom_c"
# One group per .c TRANSLATION UNIT: the .c plus the .c.inc fragments it
# #includes (gcov emits a separate .gcov per fragment).  The "C file" coverable
# target the campaign reports is the per-TU total here.  netpoll_iocp.c (IOCP)
# and any kqueue-only paths do not compile on the Linux epoll build, so they
# emit no .gcov and are silently skipped.
GROUPS = {
    "mn_sched.c -- M:N scheduler": [
        "mn_sched.c", "mn_sched_init_fini.c.inc", "mn_sched_hub_main.c.inc",
        "mn_sched_hub_resume_preempt.c.inc", "mn_sched_handoff.c.inc",
        "mn_sched_sysmon.c.inc", "mn_sched_hubinfo.c.inc",
        "mn_sched_mn_api.c.inc", "mn_sched_runq.c.inc",
    ],
    "netpoll.c -- epoll default backend": [
        "netpoll.c", "netpoll_init.c.inc", "netpoll_register.c.inc",
        "netpoll_wait_fd.c.inc", "netpoll_pump.c.inc", "netpoll_pump_helpers.c.inc",
        "netpoll_parkers.c.inc", "netpoll_parker_link.c.inc",
        "netpoll_wake_iouring.c.inc", "netpoll_diag_fd.c.inc",
    ],
    "module.c -- Python module surface": [
        "module.c", "module_coro.c.inc", "module_tcp.c.inc", "module_io.c.inc",
        "module_fdio.c.inc", "module_g.c.inc", "module_chan.c.inc", "module_go.c.inc",
        "module_run.c.inc", "module_introspect.c.inc", "module_crash.c.inc",
        "module_advice.c.inc", "module_select.c.inc", "module_machinecode.c.inc",
        "module_init.c.inc",
    ],
    "runloom_sched.c -- single-thread scheduler": [
        "runloom_sched.c", "runloom_sched_pystate.c.inc", "runloom_sched_datastack.c.inc",
        "runloom_sched_core.c.inc", "runloom_sched_parkwake.c.inc",
        "runloom_sched_drain.c.inc", "runloom_sched_preempt.c.inc",
    ],
    "runloom_tcp.c -- TCP/conn layer": [
        "runloom_tcp.c", "runloom_tcp_helpers.c.inc", "runloom_tcp_conn_io.c.inc",
        "runloom_tcp_conn_send.c.inc", "runloom_tcp_conn_net.c.inc",
        "runloom_tcp_type_init.c.inc",
    ],
    "io_uring.c -- io_uring backend": [
        "io_uring.c", "io_uring_l_sys.c.inc", "io_uring_l_buf.c.inc",
        "io_uring_l_do.c.inc", "io_uring_l_msclose.c.inc", "io_uring_l_ring.c.inc",
        "io_uring_l_loop.c.inc",
    ],
    "chan.c -- channels + select": [
        "chan.c", "chan_waiters.c.inc", "chan_ops.c.inc",
        "chan_select_helpers.c.inc", "chan_select_main.c.inc",
    ],
    "coro.c -- coroutine/stack engine": ["coro.c"],
    "runloom_introspect.c -- introspection": [
        "runloom_introspect.c", "runloom_introspect_frames.c.inc",
    ],
    "runloom_blockpool.c -- blocking-call offload": ["runloom_blockpool.c"],
    "runloom_gstate.c -- goroutine state": ["runloom_gstate.c"],
    "runloom_diag.c -- diagnostics/event ring": ["runloom_diag.c"],
    "runloom_crash.c -- crash handler": ["runloom_crash.c"],
    "runloom_stackadvice.c -- stack autosizer": ["runloom_stackadvice.c"],
    "runloom_iframe.c -- interp-frame helpers": ["runloom_iframe.c"],
    "cldeque.c -- Chase-Lev work deque": ["cldeque.c"],
    "fcontext.c -- context-switch trampoline": ["fcontext.c"],
    "netpoll_iocp.c -- IOCP backend (Windows; not on Linux)": ["netpoll_iocp.c"],
}


MANIFEST = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "coverage_exclusions.txt")


def _load_manifest():
    """frag_basename -> set of excluded line numbers, from coverage_exclusions.txt."""
    ex = {}
    if not os.path.exists(MANIFEST):
        return ex
    for raw in open(MANIFEST, errors="replace"):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 3)
        if len(parts) < 3:
            continue
        frag, rng = parts[0], parts[1]
        try:
            a, b = (int(x) for x in rng.split("-"))
        except ValueError:
            continue
        ex.setdefault(frag, set()).update(range(a, b + 1))
    return ex


_MANIFEST = _load_manifest()


def excluded_lines(stem):
    """Excluded line numbers for <stem>: the manifest PLUS any inline LCOV_EXCL."""
    ex = set(_MANIFEST.get(stem, ()))
    p = os.path.join(SRC, stem)
    if os.path.exists(p):
        in_block = False
        for i, raw in enumerate(open(p, errors="replace"), start=1):
            if "LCOV_EXCL_START" in raw:
                in_block = True
            if in_block:
                ex.add(i)
            if "LCOV_EXCL_STOP" in raw:
                in_block = False
            if "LCOV_EXCL_LINE" in raw:
                ex.add(i)
    return ex


def parse_gcov(stem, covdir):
    """(total_coverable, covered_coverable, excluded) honoring LCOV_EXCL."""
    p = os.path.join(covdir, stem + ".gcov")
    if not os.path.exists(p):
        return None
    ex = excluded_lines(stem)
    total = covered = excl = 0
    for raw in open(p, errors="replace"):
        parts = raw.split(":", 2)
        if len(parts) < 3:
            continue
        c = parts[0].strip()
        ln = parts[1].strip()
        if not ln.isdigit() or ln == "0":
            continue
        if c == "-":
            continue
        if int(ln) in ex:
            excl += 1
            continue
        total += 1
        if c not in ("#####", "====="):
            covered += 1
    return total, covered, excl


def main(covdir):
    per_tu = []          # (title, total, covered, excl, pct) for the per-file summary
    XT = XC = XX = 0     # whole-extension coverable totals
    for title, files in GROUPS.items():
        print("=" * 70)
        print(title)
        print("  {0:<40} {1:>6} {2:>6} {3:>7} {4:>5}".format("fragment", "lines", "cov", "pct", "excl"))
        print("  " + "-" * 68)
        gt = gc = gx = 0
        any_seen = False
        for stem in files:
            r = parse_gcov(stem, covdir)
            if r is None:
                continue
            total, covered, excl = r
            if total == 0 and excl == 0:
                continue
            any_seen = True
            gt += total; gc += covered; gx += excl
            pct = 100.0 * covered / total if total else 100.0
            print("  {0:<40} {1:>6} {2:>6} {3:>6.1f}% {4:>5}".format(stem, total, covered, pct, excl))
        print("  " + "-" * 68)
        gpct = 100.0 * gc / gt if gt else 100.0
        print("  {0:<40} {1:>6} {2:>6} {3:>6.1f}% {4:>5}".format(
            "COVERABLE TOTAL (excl LCOV_EXCL)", gt, gc, gpct, gx))
        print()
        if any_seen:
            per_tu.append((title, gt, gc, gx, gpct))
            XT += gt; XC += gc; XX += gx

    # ---- per-file (per-TU) summary, sorted worst-first, gate at >=95% ----
    print("=" * 70)
    print("PER-FILE (translation-unit) COVERABLE SUMMARY  -- gate: >=95%")
    print("  {0:<44} {1:>6} {2:>6} {3:>7}  flag".format("translation unit", "lines", "cov", "pct"))
    print("  " + "-" * 68)
    below = []
    for title, gt, gc, gx, gpct in sorted(per_tu, key=lambda x: x[4]):
        flag = "OK" if gpct >= 95.0 else "<95 !!"
        if gpct >= 99.95:
            flag = "100%"
        if gpct < 95.0:
            below.append((title, gpct))
        print("  {0:<44} {1:>6} {2:>6} {3:>6.1f}%  {4}".format(title, gt, gc, gpct, flag))
    print("  " + "-" * 68)
    XP = 100.0 * XC / XT if XT else 100.0
    print("  {0:<44} {1:>6} {2:>6} {3:>6.1f}%".format(
        "WHOLE EXTENSION (coverable)", XT, XC, XP))
    if below:
        print()
        print("  !! {0} translation unit(s) below 95% coverable:".format(len(below)))
        for title, gpct in below:
            print("       {0:<44} {1:>6.1f}%".format(title, gpct))
    else:
        print("  ALL translation units >= 95% coverable.")
    print()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "build/coverage"))
