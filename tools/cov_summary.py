"""Summarize gcov .gcov reports: per-file line coverage + uncovered error paths.

No external deps (gcovr is not installed).  Reads every *.gcov in the given
directory.  gcov line format is "<count>:<lineno>:<source>" where count is a
number (executed), "#####" or "=====" (never executed), or "-" (no code).
"""
import os
import re
import sys

# heuristic: uncovered lines that look like error / cleanup handling -- the
# class of path that is both most dangerous and least likely to be tested.
ERROR_HINT = re.compile(
    r"\b(errno|ENOMEM|EAGAIN|EINTR|EPERM|return\s+-1|return\s+NULL|"
    r"PyErr_|goto\s+(fail|err|cleanup|error)|perror|abort\(|"
    r"_FAIL|_ERROR|out_of_memory|oom)\b"
)


def parse_gcov(path):
    """Return (filename, total_code_lines, covered, uncovered_line_tuples)."""
    fname = os.path.basename(path)[:-5]  # strip ".gcov"
    total = covered = 0
    uncovered = []
    with open(path, "r", errors="replace") as fh:
        for raw in fh:
            parts = raw.split(":", 2)
            if len(parts) < 3:
                continue
            count = parts[0].strip()
            lineno = parts[1].strip()
            text = parts[2].rstrip("\n")
            if not lineno.isdigit() or lineno == "0":
                continue
            if count == "-":
                continue  # not executable code
            total += 1
            if count in ("#####", "====="):
                uncovered.append((int(lineno), text))
            else:
                covered += 1
    return fname, total, covered, uncovered


def main(covdir):
    reports = sorted(p for p in os.listdir(covdir) if p.endswith(".c.gcov"))
    if not reports:
        print("  no .c.gcov reports found in {0}".format(covdir))
        return 1

    rows = []
    grand_total = grand_cov = 0
    error_misses = []
    for r in reports:
        fname, total, covered, uncovered = parse_gcov(os.path.join(covdir, r))
        if total == 0:
            continue
        rows.append((fname, total, covered, uncovered))
        grand_total += total
        grand_cov += covered
        for ln, text in uncovered:
            if ERROR_HINT.search(text):
                error_misses.append((fname, ln, text.strip()))

    rows.sort(key=lambda x: (x[2] / x[1]) if x[1] else 1.0)  # worst % first

    print("  {0:<22} {1:>7} {2:>7} {3:>7}".format("file", "lines", "cov", "pct"))
    print("  " + "-" * 46)
    for fname, total, covered, uncovered in rows:
        pct = 100.0 * covered / total if total else 100.0
        print("  {0:<22} {1:>7} {2:>7} {3:>6.1f}%".format(fname, total, covered, pct))
    print("  " + "-" * 46)
    gpct = 100.0 * grand_cov / grand_total if grand_total else 0.0
    print("  {0:<22} {1:>7} {2:>7} {3:>6.1f}%".format("TOTAL", grand_total, grand_cov, gpct))

    print()
    print("  uncovered ERROR/CLEANUP lines (the priority targets): {0}".format(len(error_misses)))
    for fname, ln, text in error_misses[:60]:
        print("    {0}:{1}: {2}".format(fname, ln, text[:90]))
    if len(error_misses) > 60:
        print("    ... and {0} more".format(len(error_misses) - 60))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))
