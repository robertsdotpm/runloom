#!/usr/bin/env python3
"""Cluster a sweep's results/ into bug buckets and regenerate the auto-section
of BUGS.md.

The sweep already attributes each crash to a module and saves the child's
verbose stderr.  This turns that pile into a triage: it groups CRASH/HANG/ERROR/
LOADERR modules by a coarse *signature* (where the child was when it died), so
"690 SIGSEGVs" collapses into a handful of root-cause candidates to work
through at the end.

    python tests_stdlib/triage.py            # regenerate BUGS.md triage section
    python tests_stdlib/triage.py --print     # just print to stdout

Signature heuristic (cheap, no gdb): from the child's captured stderr,
  * no unittest output at all      -> "import/load-time crash"
  * a `Doctest:` line near the end -> "doctest execution (BUG-001 readline family)"
  * a `test_*( ... )` line at end  -> "mid-unittest-run crash"
  * otherwise                      -> the trimmed last line
"""
import argparse
import collections
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "results")
BUGS_MD = os.path.join(HERE, "BUGS.md")
BEGIN = "<!-- TRIAGE:BEGIN -->"
END = "<!-- TRIAGE:END -->"


def stderr_of(status, module):
    path = os.path.join(RESULTS, status, module + ".log")
    try:
        with open(path, errors="replace") as f:
            text = f.read()
    except OSError:
        return ""
    part = text.split("# --- child stderr", 1)[-1]
    # drop the rest of the header line ("(verbosity=2; ...) ---") so an
    # otherwise-empty stderr (import-time crash) reads as truly empty.
    return part.split("\n", 1)[-1] if "\n" in part else ""


def meaningful_lines(text):
    out = []
    for ln in text.splitlines():
        s = ln.strip()
        if not s or s.startswith("[RUNLOOM_SYSMON]") or s.startswith("# ---"):
            continue
        out.append(s)
    return out


def signature(status, module):
    ms = meaningful_lines(stderr_of(status, module))
    if not ms:
        return "import/load-time crash (no unittest output)"
    tail = ms[-4:]
    if any("Doctest:" in l for l in tail) or any(l.endswith("...") and "Doctest" in l for l in tail):
        return "doctest execution crash (BUG-001 readline/terminfo family)"
    last = ms[-1]
    # a unittest progress line that never reached '... ok/ERROR/FAIL'
    if (last.startswith("test") or last[:1].islower()) and "(" in last and ")" in last:
        return "mid-unittest-run crash"
    if last.startswith("Traceback") or "Error" in last or "Exception" in last:
        return "exception at load: " + last[:90]
    return "other: " + last[:90]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", dest="to_stdout", action="store_true")
    args = ap.parse_args()

    csv_path = os.path.join(RESULTS, "results.csv")
    rows = []
    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))

    status_counts = collections.Counter(r["status"] for r in rows)
    total = len(rows)

    # cluster the failure-ish statuses by signature
    clusters = collections.defaultdict(list)  # (status, sig) -> [modules]
    for r in rows:
        st = r["status"]
        if st in ("PASS", "FAIL"):
            continue
        sig = signature(st, r["module"])
        clusters[(st, sig)].append(r["module"])

    lines = []
    lines.append("**%d modules** | " % total + " ".join(
        "%s=%d" % (k, status_counts[k]) for k in sorted(status_counts)))
    lines.append("")
    lines.append("| status | signature | count | example modules |")
    lines.append("|--------|-----------|-------|-----------------|")
    for (st, sig), mods in sorted(clusters.items(), key=lambda kv: -len(kv[1])):
        ex = ", ".join("`%s`" % m for m in sorted(mods)[:6])
        if len(mods) > 6:
            ex += ", … (+%d)" % (len(mods) - 6)
        lines.append("| %s | %s | %d | %s |" % (st, sig, len(mods), ex))
    lines.append("")

    # FAILs are semantic (unittest failures/errors) — list separately, briefly.
    fails = sorted(r["module"] for r in rows if r["status"] == "FAIL")
    if fails:
        lines.append("**FAIL (%d)** — unittest failures/errors (semantic; some "
                     "may be -j env collisions, revisit individually):" % len(fails))
        lines.append("")
        lines.append(", ".join("`%s`" % m for m in fails))
        lines.append("")

    section = "\n".join(lines)

    if args.to_stdout:
        print(section)
        return 0

    with open(BUGS_MD, errors="replace") as f:
        md = f.read()
    pre, _, rest = md.partition(BEGIN)
    _, _, post = rest.partition(END)
    new = pre + BEGIN + "\n" + section + "\n" + END + post
    with open(BUGS_MD, "w") as f:
        f.write(new)
    print("BUGS.md triage section regenerated (%d modules, %d clusters)."
          % (total, len(clusters)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
