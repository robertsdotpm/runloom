#!/usr/bin/env python3
"""Snowboard-style PMC candidate extractor (QA-steal-V2 #11).

Snowboard (SOSP'21) logs EVERY memory access from sequential corpus runs, pairs
same-address write/read into Persistent-Memory-Communication (PMC) candidates, and
then generates ONLY the schedules that flip each communication's order.  The full
access log needs a custom load/store instrumentation pass (LLVM/PIN/DynamoRIO) --
the multi-day part.  This is the honest, buildable SLICE: reuse the tsan-gold
race-report corpus, where every `WARNING: ThreadSanitizer: data race` block already
IS a same-address write/read pair on two symbolized sites that TSan PROVED is
order-sensitive (the exact subset a PMC candidate wants -- minus the SYNCHRONIZED
communications, whose order-flip is Snowboard's extra edge and which race reports
by definition do not contain: that gap is why the full access log is still needed).

Each race -> one PMC candidate: (siteA file:line, siteB file:line, kinds W/R, size,
same-address, whether the two accesses were on DIFFERENT threads).  Candidates whose
either side lands in the work-stealing deque, the handle table, or the grace-period
(g-slab / datastack) reclaim path are flagged IN-SCOPE -- those are the cross-hub
communications the schedule generator (the #18/#12 follow-up) would target.

Usage:
  tools/snowboard/pmc_candidates.py [tsan-log ...]     # default: the gold corpus
"""
import glob
import os
import re
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_GLOB = os.path.join(ROOT, "docs", "dev", "soak", "matrix_tsan-gold-*", "tsan.*")

# tsan-gold frame + access formats (NOT the ASan format triage_san.py matches):
#   "    #3 <symbol> src/runloom_c/file.c.inc:706 (mod+0x..) (BuildId: ..)"
#   "  Write of size 8 at 0x.. by thread T5:"  /  "Previous read of size 8 .. T1 (mutexes: ..)"
FRAME = re.compile(r"^\s*#\d+\s+(\S+)\s+(src/\S+):(\d+)")
# TSan capitalizes the FIRST access ("Write"/"Read") and lowercases the second
# ("Previous write"/"Previous read"), so match case-insensitively.
ACCESS = re.compile(
    r"^\s*((?:Previous\s+)?(?:Atomic\s+)?(?:write|read))\s+of size\s+(\d+)"
    r"\s+at\s+(0x[0-9a-f]+)\s+by thread\s+(T\d+)", re.IGNORECASE)

# In-scope target subsystems (the cross-hub communications worth order-flipping).
TARGET_FILES = ("cldeque.c", "rl_handle.c")
# Grace-period / QSBR reclaim path (there is no standalone QSBR module -- it is the
# retain-forever g-slab + datastack-chunk reclaim in the scheduler core).
GRACE_SYMS = ("slab", "reclaim", "qsbr", "retire", "datastack", "chunk")


def _top_intree_site(frames):
    """First in-tree (src/runloom_c/...) frame of an access -> (file, line, sym)."""
    for sym, path, line in frames:
        if "src/runloom_c" in path:
            return (path.split("src/runloom_c/")[-1], int(line), sym)
    # fall back to the first src/ frame at all (e.g. a bare src/... path)
    return (frames[0][1], int(frames[0][2]), frames[0][0]) if frames else None


def parse_log(path):
    """Yield one dict per race: the two same-address accesses + their top sites."""
    try:
        lines = open(path, errors="replace").read().splitlines()
    except OSError:
        return
    i, n = 0, len(lines)
    while i < n:
        if "WARNING: ThreadSanitizer: data race" not in lines[i]:
            i += 1
            continue
        accesses = []
        i += 1
        while i < n and "WARNING: ThreadSanitizer" not in lines[i]:
            if lines[i].startswith("SUMMARY: ThreadSanitizer"):
                i += 1
                break
            m = ACCESS.match(lines[i])
            if m and len(accesses) < 2:
                kind, size, addr, thread = m.group(1), int(m.group(2)), m.group(3), m.group(4)
                frames = []
                i += 1
                while i < n:
                    fm = FRAME.match(lines[i])
                    if fm:
                        frames.append((fm.group(1), fm.group(2), fm.group(3)))
                        i += 1
                    else:
                        break
                accesses.append({"kind": kind, "size": size, "addr": addr,
                                 "thread": thread, "site": _top_intree_site(frames)})
                continue
            i += 1
        if len(accesses) == 2 and accesses[0]["site"] and accesses[1]["site"]:
            yield {"a": accesses[0], "b": accesses[1]}


def _in_scope(site):
    if site is None:
        return False
    fpath, line, sym = site
    if any(t in fpath for t in TARGET_FILES):
        return True
    return any(g in sym.lower() for g in GRACE_SYMS)


def candidate(race):
    a, b = race["a"], race["b"]
    sa = "%s:%d %s" % a["site"]
    sb = "%s:%d %s" % b["site"]
    # canonical order so (A,B) and (B,A) dedupe to one communication
    lo, hi = sorted([sa, sb])
    return {
        "siteA": lo, "siteB": hi,
        "kinds": "".join(sorted((a["kind"].split()[-1][0].upper(),
                                 b["kind"].split()[-1][0].upper()))),
        "size": a["size"], "same_addr": a["addr"] == b["addr"],
        "cross_thread": a["thread"] != b["thread"],
        "in_scope": _in_scope(a["site"]) or _in_scope(b["site"]),
    }


def main(argv):
    logs = argv or sorted(glob.glob(DEFAULT_GLOB))
    if not logs:
        print("no tsan-gold logs found (%s)" % DEFAULT_GLOB)
        return 2
    seen = {}
    for log in logs:
        for race in parse_log(log):
            c = candidate(race)
            seen[(c["siteA"], c["siteB"], c["kinds"])] = c
    cands = sorted(seen.values(), key=lambda c: (not c["in_scope"], c["siteA"]))
    in_scope = [c for c in cands if c["in_scope"]]
    print("PMC candidates from %d tsan-gold log(s): %d distinct communications, "
          "%d in-scope (deque/handle/grace-reclaim)\n" % (len(logs), len(cands), len(in_scope)))
    if cands:
        print("  %-3s %-5s %-6s %-6s  %s" % ("W/R", "sz", "xthr", "scope", "sites (A <-> B)"))
        for c in cands:
            print("  %-3s %-5d %-6s %-6s  %s  <->  %s"
                  % (c["kinds"], c["size"], "yes" if c["cross_thread"] else "no",
                     "IN" if c["in_scope"] else "-", c["siteA"], c["siteB"]))
    if not in_scope:
        print("\n>>> 0 in-scope candidates from the current corpus (%d log(s)): the soak "
              "workload rarely stresses steal / handle-reclaim under tsan-gold.  The real "
              "prerequisite is a TARGETED deque/handle-churn workload run under "
              "tsan-gold-smoke; re-run this extractor against it to grow the list.\n"
              ">>> And the bug-finding payoff (generate ONLY the schedules that flip each "
              "candidate's order) needs the controlled-baton to target a specific (siteA,"
              "siteB) pair -- the same per-op scheduler work as #18/#12." % len(logs))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
