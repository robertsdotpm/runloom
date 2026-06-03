#!/usr/bin/env python3
"""Mutation testing for runloom's C core -- does the suite actually have teeth?

Coverage says which lines run; mutation says whether a test would NOTICE if
that line were wrong.  We introduce one small, compilable fault at a time
(flip a comparison, swap && / ||, negate an `if` condition), rebuild the
extension, and run a fast slice of the suite:

  * the mutant is KILLED  -> some test failed/hung -> the suite has teeth here;
  * the mutant SURVIVED   -> every test still passed -> a real test gap at that
                             exact line (the interesting output).

The surviving mutants are the product: each names a line whose behaviour no
test constrains.  Stillborn (won't compile) mutants are skipped, not scored.

Operators (chosen to almost always compile and be semantically meaningful):
  ROR  relational swap     ==<->!=   <=<->>=
  LCR  logical swap        && <-> ||
  COR  condition negation  if (C) -> if (!(C))

Safety: the target file is restored from an in-memory copy in a finally, plus
a final `git checkout` backstop.  Runs on a COPY of the worktree state only in
the sense that we never commit; the .c is patched in place and put back.

Usage:
  tools/mutate/mutate.py src/runloom_c/chan.c [--max 25] [--seed 1] [--json out]
  tools/mutate/mutate.py src/runloom_c/chan.c --list      # just enumerate
Env:
  PYTHON     interpreter (default: free-threaded 3.13t if present)
  TEST_CMD   shell command whose exit code decides kill/survive (default: a
             fast chan/sched pytest slice + a short fixed-seed mn_stress)
"""
import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def find_python():
    for c in (os.path.join(os.path.expanduser("~"), ".pyenv/versions/3.13.13t/bin/python3"),
              "python3.13t", "python3"):
        if os.path.sep in c:
            if os.path.exists(c):
                return c
        elif shutil.which(c):
            return c
    return sys.executable


PYTHON = os.environ.get("PYTHON") or find_python()

DEFAULT_TEST_CMD = (
    "PYTHON_GIL=0 PYTHONPATH=src {py} -m pytest "
    "tests/test_chan.py tests/test_chan_queue.py tests/test_sched_fairness.py "
    "-x -q -p no:cacheprovider --no-header "
    "&& PYTHON_GIL=0 PYTHONPATH=src {py} tools/mn_stress.py --iters 40 --seed 1"
).format(py=PYTHON)


def mask_code(src):
    """Return src with comment and string/char bytes replaced by spaces (same
    length, same offsets) so operator scans never match inside them."""
    out = list(src)
    i, n = 0, len(src)
    state = None  # None | 'line' | 'block' | 'str' | 'char'
    while i < n:
        c = src[i]
        two = src[i:i + 2]
        if state is None:
            if two == "//":
                state = "line"; out[i] = out[i + 1] = " "; i += 2; continue
            if two == "/*":
                state = "block"; out[i] = out[i + 1] = " "; i += 2; continue
            if c == '"':
                state = "str"; out[i] = " "; i += 1; continue
            if c == "'":
                state = "char"; out[i] = " "; i += 1; continue
            i += 1
        elif state == "line":
            if c == "\n":
                state = None
            else:
                out[i] = " "
            i += 1
        elif state == "block":
            if two == "*/":
                out[i] = out[i + 1] = " "; state = None; i += 2; continue
            if c != "\n":
                out[i] = " "
            i += 1
        elif state in ("str", "char"):
            q = '"' if state == "str" else "'"
            if c == "\\":
                out[i] = out[i + 1] = " "; i += 2; continue
            if c == q:
                out[i] = " "; state = None; i += 1; continue
            out[i] = " "; i += 1
    return "".join(out)


# (regex on masked text, function(matchtext)->replacement, kind)
SWAPS = {"==": "!=", "!=": "==", "<=": ">=", ">=": "<=", "&&": "||", "||": "&&"}
OP_RE = re.compile(r"==|!=|<=|>=|&&|\|\|")


def line_of(src, off):
    return src.count("\n", 0, off) + 1


def gen_mutants(src):
    """Yield dict(off, end, old, new, kind, line, snippet)."""
    masked = mask_code(src)
    # --- binary operator swaps (ROR / LCR) ---
    for m in OP_RE.finditer(masked):
        op = m.group(0)
        yield dict(off=m.start(), end=m.end(), old=op, new=SWAPS[op],
                   kind="ROR" if op not in ("&&", "||") else "LCR",
                   line=line_of(src, m.start()))
    # --- condition negation (COR): if (C) -> if (!(C)) ---
    for m in re.finditer(r"\bif\s*\(", masked):
        popen = m.end() - 1            # index of '('
        depth, j = 0, popen
        while j < len(masked):
            if masked[j] == "(":
                depth += 1
            elif masked[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if j >= len(masked):
            continue
        cond = src[popen + 1:j]
        if not cond.strip():
            continue
        yield dict(off=popen, end=j + 1, old="(" + cond + ")",
                   new="(!(" + cond + "))", kind="COR",
                   line=line_of(src, popen))


def attach_snippets(src, mutants):
    lines = src.splitlines()
    for mu in mutants:
        ln = mu["line"]
        mu["snippet"] = lines[ln - 1].strip() if 0 < ln <= len(lines) else ""
    return mutants


def build():
    r = subprocess.run([PYTHON, "setup.py", "build_ext", "--inplace"],
                       cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return r.returncode == 0


def run_tests(test_cmd, timeout):
    try:
        r = subprocess.run(test_cmd, cwd=ROOT, shell=True, timeout=timeout,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return "pass" if r.returncode == 0 else "fail"
    except subprocess.TimeoutExpired:
        return "timeout"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("target", help="C source file to mutate (rel to repo root)")
    ap.add_argument("--max", type=int, default=25, help="max mutants to run")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--timeout", type=float, default=90.0, help="per-mutant test timeout (s)")
    ap.add_argument("--json", help="write full results JSON here")
    ap.add_argument("--list", action="store_true", help="enumerate mutants and exit")
    args = ap.parse_args()

    rel = args.target if not os.path.isabs(args.target) else os.path.relpath(args.target, ROOT)
    path = os.path.join(ROOT, rel)
    with open(path) as f:
        orig = f.read()

    mutants = attach_snippets(orig, list(gen_mutants(orig)))
    random.Random(args.seed).shuffle(mutants)
    by_kind = {}
    for mu in mutants:
        by_kind[mu["kind"]] = by_kind.get(mu["kind"], 0) + 1
    print("[mutate] {0}: {1} candidate mutants {2}".format(rel, len(mutants), by_kind))

    if args.list:
        for mu in mutants[:args.max]:
            print("  L{0:<5} {1}: {2} -> {3}    | {4}".format(
                mu["line"], mu["kind"], mu["old"][:30], mu["new"][:30], mu["snippet"][:60]))
        return 0

    test_cmd = os.environ.get("TEST_CMD") or DEFAULT_TEST_CMD
    print("[mutate] python: {0}".format(PYTHON))
    print("[mutate] test cmd: {0}".format(test_cmd))

    # Baseline must be green, or every "kill" is noise.
    print("[mutate] verifying clean baseline ...", end=" ", flush=True)
    if not build():
        print("BUILD FAILED on pristine tree -- aborting"); return 2
    base = run_tests(test_cmd, args.timeout)
    if base != "pass":
        print("baseline test cmd did not pass ({0}) -- fix/select tests first".format(base)); return 2
    print("ok")

    run = mutants[:args.max]
    results = []
    killed = survived = stillborn = 0
    t0 = time.time()
    try:
        for idx, mu in enumerate(run, 1):
            mutated = orig[:mu["off"]] + mu["new"] + orig[mu["end"]:]
            with open(path, "w") as f:
                f.write(mutated)
            tag = "L{0} {1} {2}->{3}".format(mu["line"], mu["kind"], mu["old"][:12], mu["new"][:12])
            print("  [{0:>3}/{1}] {2:<46}".format(idx, len(run), tag), end=" ", flush=True)
            if not build():
                mu["result"] = "stillborn"; stillborn += 1; print("stillborn (won't compile)")
            else:
                outcome = run_tests(test_cmd, args.timeout)
                if outcome == "pass":
                    mu["result"] = "survived"; survived += 1; print("SURVIVED  <-- test gap")
                else:
                    mu["result"] = "killed"; killed += 1
                    print("killed ({0})".format(outcome))
            results.append(mu)
    finally:
        with open(path, "w") as f:
            f.write(orig)
        subprocess.run(["git", "checkout", "--", rel], cwd=ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        build()  # leave a clean, working .so

    scored = killed + survived
    score = (100.0 * killed / scored) if scored else 0.0
    print("\n[mutate] === {0} ===".format(rel))
    print("[mutate] ran {0} mutants in {1:.0f}s: {2} killed, {3} survived, {4} stillborn"
          .format(len(run), time.time() - t0, killed, survived, stillborn))
    print("[mutate] mutation score: {0:.1f}% ({1}/{2} non-stillborn killed)"
          .format(score, killed, scored))
    if survived:
        print("[mutate] SURVIVORS (uncaught faults -- a test gap at each line):")
        for mu in results:
            if mu["result"] == "survived":
                print("    L{0:<5} {1}: {2} -> {3}   | {4}".format(
                    mu["line"], mu["kind"], mu["old"][:24], mu["new"][:24], mu["snippet"][:56]))
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"target": rel, "killed": killed, "survived": survived,
                       "stillborn": stillborn, "score": score, "mutants": results}, f, indent=2)
        print("[mutate] full results -> {0}".format(args.json))
    return 0


if __name__ == "__main__":
    sys.exit(main())
