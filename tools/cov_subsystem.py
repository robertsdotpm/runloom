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
GROUPS = {
    "M:N scheduler (mn_sched.c + fragments)": [
        "mn_sched.c", "mn_sched_init_fini.c.inc", "mn_sched_hub_main.c.inc",
        "mn_sched_hub_resume_preempt.c.inc", "mn_sched_handoff.c.inc",
        "mn_sched_sysmon.c.inc", "mn_sched_hubinfo.c.inc",
        "mn_sched_mn_api.c.inc", "mn_sched_runq.c.inc",
    ],
    "epoll netpoll (Linux default backend)": [
        "netpoll.c", "netpoll_init.c.inc", "netpoll_register.c.inc",
        "netpoll_wait_fd.c.inc", "netpoll_pump.c.inc", "netpoll_pump_helpers.c.inc",
        "netpoll_parkers.c.inc", "netpoll_parker_link.c.inc",
        "netpoll_wake_iouring.c.inc", "netpoll_diag_fd.c.inc",
    ],
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
    for title, files in GROUPS.items():
        print("=" * 70)
        print(title)
        print("  {0:<40} {1:>6} {2:>6} {3:>7} {4:>5}".format("fragment", "lines", "cov", "pct", "excl"))
        print("  " + "-" * 68)
        gt = gc = gx = 0
        for stem in files:
            r = parse_gcov(stem, covdir)
            if r is None:
                continue
            total, covered, excl = r
            if total == 0 and excl == 0:
                continue
            gt += total; gc += covered; gx += excl
            pct = 100.0 * covered / total if total else 100.0
            print("  {0:<40} {1:>6} {2:>6} {3:>6.1f}% {4:>5}".format(stem, total, covered, pct, excl))
        print("  " + "-" * 68)
        gpct = 100.0 * gc / gt if gt else 0.0
        print("  {0:<40} {1:>6} {2:>6} {3:>6.1f}% {4:>5}".format(
            "COVERABLE TOTAL (excl LCOV_EXCL)", gt, gc, gpct, gx))
        print()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "build/coverage"))
