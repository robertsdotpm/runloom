#!/usr/bin/env python3
"""Print the FOCUSED big_100 program set, one filename per line:

  (1) NEW  -- program files added/renamed into tests/big_100 in the last N days
              (git history), plus
  (2) BUGGY -- programs that hit a REAL bug in the forever loop's results.tsv in
              the last N days: verdict VFAIL / TIMEOUT / HANG, or CRASH with an
              exit code that is NOT a benign over-scale limit (4 = SCALE_LIMIT,
              137 = OOM box-kill).

big100_forever.sh calls this when BIG100_FOCUS=1 so an iteration re-runs only the
new + recently-risky programs instead of the whole ~300-file suite.  Window is
N = BIG100_FOCUS_DAYS days (default 7).  Only files that still exist on disk are
printed; prints nothing on error so the caller can fall back to the full glob.
"""
import datetime
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
DAYS = int(os.environ.get("BIG100_FOCUS_DAYS", "7"))
RES = os.path.join(ROOT, "docs", "dev", "soak", "big100_forever", "results.tsv")

# Benign over-scale exit codes -- a program hitting these is NOT "a bug found".
BENIGN_EXIT = {"4", "137"}       # 4 = SCALE_LIMIT, 137 = OOM box-kill


def new_files():
    """Program files first added/renamed into tests/big_100 within DAYS (git)."""
    out = set()
    try:
        r = subprocess.run(
            ["git", "-C", ROOT, "log", "--since={0} days ago".format(DAYS),
             "--diff-filter=AR", "--name-only", "--pretty=format:",
             "--", "tests/big_100/p[0-9]*.py"],
            capture_output=True, text=True, timeout=30)
        for line in r.stdout.splitlines():
            line = line.strip()
            if line.startswith("tests/big_100/") and line.endswith(".py"):
                out.add(os.path.basename(line))
    except Exception:
        pass
    return out


def buggy_programs():
    """Programs with a real-bug verdict in results.tsv within DAYS."""
    out = set()
    cutoff = (datetime.date.today()
              - datetime.timedelta(days=DAYS)).isoformat()   # YYYY-MM-DD
    try:
        with open(RES) as f:
            f.readline()                                      # header
            for line in f:
                c = line.rstrip("\n").split("\t")
                if len(c) < 5:
                    continue
                iso, prog, verdict, ex = c[0], c[2], c[3], c[4]
                if iso[:10] < cutoff:                         # iso date sorts lexically
                    continue
                v = verdict.upper()
                real_bug = ("VFAIL" in v or "HANG" in v or "TIMEOUT" in v
                            or ("CRASH" in v and ex not in BENIGN_EXIT))
                if real_bug and prog:
                    out.add(prog + ".py")
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return out


def main():
    focus = new_files() | buggy_programs()
    names = sorted(n for n in focus
                   if os.path.isfile(os.path.join(HERE, n)))
    for n in names:
        print(n)


if __name__ == "__main__":
    main()
