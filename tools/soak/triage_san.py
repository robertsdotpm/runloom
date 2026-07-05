#!/usr/bin/env python3
"""Sanitizer-log triage + machine-day ledger (docs/dev/RELIABILITY_PROGRAM.md R2).

Two jobs, one file:

  triage_san.py <rundir> --tag {asan,tsan}
      Scan every <rundir>/<tag>.<pid> sanitizer report file (written by
      ASAN_OPTIONS/TSAN_OPTIONS log_path).  Dedupe reports by a signature hash
      of their top frames (hang_hunter's idea), so a race that fires 10,000
      times collapses to one entry.  Print each distinct report + a count, then
      the last line is PASS (no non-suppressed report) or FAIL.

  triage_san.py --ledger LEDGER --preset ... --build ... --workers N --dur ...
                --soak-rc RC --san VERDICT --report REPORT.md
      Append one row to the machine-day LEDGER, computing machine-days from
      workers x duration and folding the soak-oracle verdict with the sanitizer
      verdict into a single PASS/FAIL.  The running machine-day total is the
      project's quantitative MTBF claim.
"""
import argparse
import glob
import hashlib
import os
import re
import sys


# Lines that OPEN a sanitizer report (the first line of an ASan/TSan/UBSan
# finding).  We collect the report block and hash its frame lines.
_REPORT_OPEN = re.compile(
    r"(ERROR: AddressSanitizer|WARNING: ThreadSanitizer|"
    r"runtime error:|ERROR: LeakSanitizer|"
    r"heap-use-after-free|heap-buffer-overflow|double-free|"
    r"data race)")
_FRAME = re.compile(r"#\d+ 0x[0-9a-f]+ in (\S+)")


def _signature(block):
    """Hash the first few frame symbols of a report block -> a stable id."""
    syms = []
    for line in block:
        m = _FRAME.search(line)
        if m:
            syms.append(m.group(1))
        if len(syms) >= 4:
            break
    if not syms:
        syms = [block[0].strip()[:80]] if block else ["(empty)"]
    return hashlib.sha1("\n".join(syms).encode()).hexdigest()[:12], syms


def scan(rundir, tag):
    """Return (verdict, distinct_reports) for <rundir>/<tag>.* files."""
    files = sorted(glob.glob(os.path.join(rundir, "%s.*" % tag)))
    distinct = {}   # sig -> {count, syms, sample_file}
    for fpath in files:
        try:
            with open(fpath, errors="replace") as f:
                lines = f.readlines()
        except OSError:
            continue
        i = 0
        while i < len(lines):
            if _REPORT_OPEN.search(lines[i]):
                # collect until a blank line or the next SUMMARY
                block = []
                j = i
                while j < len(lines) and j < i + 40:
                    block.append(lines[j])
                    if j > i and (lines[j].strip() == ""
                                  or lines[j].startswith("SUMMARY")):
                        break
                    j += 1
                sig, syms = _signature(block)
                d = distinct.setdefault(sig, {"count": 0, "syms": syms,
                                              "file": os.path.basename(fpath)})
                d["count"] += 1
                i = j + 1
            else:
                i += 1
    verdict = "PASS" if not distinct else "FAIL"
    return verdict, distinct


def _fmt_dur(dur):
    """'--hours 24' / '--seconds 60' -> (label, seconds)."""
    parts = dur.split()
    if len(parts) != 2:
        return dur, 0.0
    flag, val = parts
    val = float(val)
    if "hours" in flag:
        return "%gh" % val, val * 3600
    if "minutes" in flag:
        return "%gm" % val, val * 60
    return "%gs" % val, val


_LEDGER_HEADER = (
    "# Soak matrix ledger — machine-days accumulated\n\n"
    "Each row is one `tools/soak/matrix.sh` preset run.  "
    "**Machine-days** = workers × duration; the running total is the "
    "project's quantitative MTBF claim (keep the README status line "
    "current once ≥ 30).  A row PASSes iff the slope oracle passed "
    "AND no non-suppressed sanitizer report appeared.\n\n"
    "| preset | build | dur | workers | mdays | soak | sanitizer | verdict | report |\n"
    "|---|---|---:|---:|---:|:-:|:-:|:-:|---|\n")

_ROW_RE = re.compile(r"^\| .+ \| .+ \| .+ \| \d+ \| [\d.]+ \|")


def append_ledger(args):
    """Read existing rows, add the new one, rewrite the whole ledger cleanly
    (header + all rows + a single running-total footer).  A full rewrite avoids
    the row-after-footer ordering bug an append-mode writer has."""
    label, secs = _fmt_dur(args.dur)
    machine_days = (args.workers * secs) / 86400.0
    soak_ok = (args.soak_rc == "0")
    san_ok = (args.san not in ("FAIL",))
    verdict = "PASS" if (soak_ok and san_ok) else "FAIL"

    row = "| %s | %s | %s | %d | %.3f | %s | %s | %s | %s |\n" % (
        args.preset, args.build, label, args.workers, machine_days,
        "✅" if soak_ok else "❌",
        args.san if args.san != "n/a" else "—",
        "✅ PASS" if verdict == "PASS" else "❌ FAIL",
        os.path.relpath(args.report, os.path.dirname(args.ledger))
        if args.report else "—")

    existing = []
    if os.path.exists(args.ledger):
        try:
            with open(args.ledger) as f:
                existing = [l for l in f if _ROW_RE.match(l)]
        except OSError:
            existing = []
    rows = existing + [row]
    total = 0.0
    for r in rows:
        m = re.match(r"\|.*?\|.*?\|.*?\|.*?\|\s*([\d.]+)\s*\|", r)
        if m:
            total += float(m.group(1))
    with open(args.ledger, "w") as f:
        f.write(_LEDGER_HEADER)
        f.writelines(rows)
        f.write("\n**Total: %.3f machine-days**\n" % total)
    print("ledger += %s (%.3f machine-days); verdict=%s" %
          (args.preset, machine_days, verdict))


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("rundir", nargs="?")
    ap.add_argument("--tag")
    ap.add_argument("--ledger")
    ap.add_argument("--preset")
    ap.add_argument("--build")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--dur", default="--seconds 0")
    ap.add_argument("--soak-rc", default="0")
    ap.add_argument("--san", default="n/a")
    ap.add_argument("--report", default="")
    args = ap.parse_args(argv)

    if args.ledger:
        append_ledger(args)
        return 0

    if args.rundir and args.tag:
        verdict, distinct = scan(args.rundir, args.tag)
        for sig, d in distinct.items():
            print("[%s] x%d  %s  (%s)" % (sig, d["count"],
                                          " <- ".join(d["syms"][:3]), d["file"]))
        print(verdict)
        return 0 if verdict == "PASS" else 1

    ap.error("give either <rundir> --tag, or --ledger ...")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
