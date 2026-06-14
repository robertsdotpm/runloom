"""Parse gcov -b -c .gcov reports for BRANCH coverage of the kqueue netpoll
source (the .inc fragments cov_summary.py skips).  No external deps.

A branch is COVERED if "taken N" with N>0; UNCOVERED if "never executed" or
"taken 0".  Reports per-file branch%, the kqueue-relevant total, and the
uncovered branch line numbers (the targets to chase to 100%)."""
import os
import re
import sys

# kqueue / netpoll source the tests are meant to cover (basenames as gcov emits).
TARGETS = [
    "netpoll.c", "netpoll_register.c.inc", "netpoll_pump.c.inc",
    "netpoll_pump_helpers.c.inc", "netpoll_wait_fd.c.inc",
    "netpoll_wake_iouring.c.inc", "netpoll_init.c.inc",
    "netpoll_parkers.c.inc", "netpoll_parker_link.c.inc",
    "netpoll_diag_fd.c.inc",
]

SRC_LINE = re.compile(r"^\s*(?:[#=]{5}|-|\d+\*?|\d+):\s*(\d+):")
BRANCH = re.compile(r"^branch\s+\d+\s+(.*)$")


def parse(path):
    total = cov = 0
    misses = []
    cur = 0
    with open(path, "r", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            m = SRC_LINE.match(line)
            if m:
                cur = int(m.group(1))
                continue
            b = BRANCH.match(line)
            if b:
                info = b.group(1)
                total += 1
                taken = re.search(r"taken\s+(\d+)", info)
                if "never executed" in info or (taken and int(taken.group(1)) == 0):
                    misses.append(cur)
                else:
                    cov += 1
    return total, cov, misses


def main(covdir):
    rows = []
    gt = gc = 0
    for name in TARGETS:
        p = os.path.join(covdir, name + ".gcov")
        if not os.path.exists(p):
            rows.append((name, 0, 0, []))
            continue
        total, cov, misses = parse(p)
        rows.append((name, total, cov, misses))
        gt += total
        gc += cov

    print("  {0:<30} {1:>7} {2:>7} {3:>7}".format("file", "branch", "cov", "pct"))
    print("  " + "-" * 56)
    for name, total, cov, misses in rows:
        pct = 100.0 * cov / total if total else 100.0
        flag = "" if total == 0 else ("" if cov == total else "  <-- gaps")
        print("  {0:<30} {1:>7} {2:>7} {3:>6.1f}%{4}".format(
            name, total, cov, pct, flag))
    print("  " + "-" * 56)
    gp = 100.0 * gc / gt if gt else 0.0
    print("  {0:<30} {1:>7} {2:>7} {3:>6.1f}%".format("TOTAL (kqueue)", gt, gc, gp))
    print()
    print("  UNCOVERED branch lines (chase to 100%):")
    any_miss = False
    for name, total, cov, misses in rows:
        if misses:
            any_miss = True
            # dedupe + sort the line numbers
            uniq = sorted(set(misses))
            print("    {0}: {1}".format(name, ", ".join(str(x) for x in uniq)))
    if not any_miss:
        print("    (none -- 100% branch coverage)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "build/kqcov"))
