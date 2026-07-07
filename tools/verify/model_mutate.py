#!/usr/bin/env python3
"""model_mutate.py -- mutation-testing for the formal-verification corpus.

The 80-model suite ships a `-DBUG_*` / bug-`.cfg` negative control per model, so
each model proves "this ONE hand-injected bug is caught". What no control checks
is whether the model/property constrains anything BEYOND that single bug -- a
toothless invariant, a dead assertion, or (the FT headline) a memory-order fence
that the property never actually relies on. This harness audits exactly that:

  * MUTATE the model/source (not the property), one change per mutant, then re-run
    the engine.  A load-bearing model element, when broken, makes verification
    FAIL (the mutant is KILLED -> the property has teeth).  A mutant that still
    VERIFIES is a SURVIVOR -> the mutated element is not constrained by the
    property = a real gap to investigate.
  * Mutation score = killed / (killed + survived).  100% = every mutated element
    is load-bearing under the checked property.

Operators
  relop  (Spin .pml + C): flip one relational/equality operator on a logic line
         (== <-> !=, < <-> >=, <= <-> >, > <-> <=, >= <-> <).  Skips comments and
         `assert(...)` lines (those are the PROPERTY, not the model).
  moflip (C real-source under CBMC): downgrade ONE C11 memory order to relaxed
         (seq_cst/acquire/release/acq_rel -> relaxed).  THE critique's headline
         test: does the fence actually carry the proof?  A SURVIVOR means the
         fence is over-strong OR the property is too weak to notice the weakening.

Targets are registered below with their exact engine command (mirrors
run_verify.sh).  CBMC mutates a COPY of the production source -- never in place.

Usage:
    tools/verify/model_mutate.py                 # quick validation subset (fast)
    tools/verify/model_mutate.py --target wake_state
    tools/verify/model_mutate.py --target cldeque --max-mutants 4
    tools/verify/model_mutate.py --full          # every mutant of every target

Exit: 0 = no survivors (every mutated element is load-bearing); 1 = >=1 survivor
(a model element the property does not constrain -- audit it); 2 = baseline not
green / engine missing (can't mutation-test).
Wire into run_verify.sh as a periodic (not every-commit) phase -- it is the
meta-test that the headline asset has teeth.
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

# --- engine runners: each returns ("PASS"|"FAIL"|"BUILDFAIL", detail) ----------

def run_spin(pml_path, timeout):
    """spin -a; cc pan.c; ./pan -m500000 ; PASS iff 'errors: 0'."""
    work = tempfile.mkdtemp(prefix="mm_spin_")
    try:
        dst = os.path.join(work, "m.pml")
        shutil.copy(pml_path, dst)
        try:
            g = subprocess.run(["spin", "-a", "m.pml"], cwd=work,
                               capture_output=True, text=True, timeout=timeout)
            if g.returncode != 0:
                return "BUILDFAIL", "spin-gen: " + (g.stdout + g.stderr)[-200:]
            c = subprocess.run(["cc", "-O2", "-o", "pan", "pan.c"], cwd=work,
                               capture_output=True, text=True, timeout=timeout)
            if c.returncode != 0:
                return "BUILDFAIL", "cc: " + (c.stdout + c.stderr)[-200:]
            r = subprocess.run(["./pan", "-m500000"], cwd=work,
                               capture_output=True, text=True, timeout=timeout)
            out = r.stdout + r.stderr
        except subprocess.TimeoutExpired:
            return "BUILDFAIL", "timeout"
        m = re.search(r"errors:\s*(\d+)", out)
        if not m:
            return "BUILDFAIL", "no 'errors:' line"
        return ("PASS" if m.group(1) == "0" else "FAIL"), "errors: " + m.group(1)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def run_cbmc(cmd_tmpl, mutated_path, timeout):
    """cbmc ...; PASS iff 'VERIFICATION SUCCESSFUL'."""
    cmd = [mutated_path if tok == "{MUT}" else tok for tok in cmd_tmpl]
    try:
        r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                           timeout=timeout)
    except subprocess.TimeoutExpired:
        return "BUILDFAIL", "timeout"
    out = r.stdout + r.stderr
    if "VERIFICATION SUCCESSFUL" in out:
        return "PASS", "SUCCESSFUL"
    if "VERIFICATION FAILED" in out:
        return "FAIL", "FAILED"
    return "BUILDFAIL", (out[-200:] or "no verdict")


def _genmc_bin():
    for c in (os.environ.get("GENMC"), "/usr/local/bin/genmc"):
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return shutil.which("genmc")


def run_genmc(mutated_path, timeout, cap=4):
    """Run a mutated cldeque.c through the chase_lev_real.c harness under GenMC's
    RC11 WEAK-memory model.  Unlike CBMC (sequentially consistent, so relaxing an
    order is a no-op and every moflip mutant vacuously survives), GenMC actually
    explores the weak-memory executions -- so a SURVIVING mutant ("No errors were
    detected") means that memory order is genuinely not load-bearing under RC11
    (a proof hole worth a look), and a KILLED mutant (a race/violation) proves the
    barrier is necessary.  chase_lev_real.c does #include "cldeque.c", so the
    harness + header are co-located with the mutant in its temp dir and the
    include resolves to the MUTANT via own-directory search."""
    genmc = _genmc_bin()
    if not genmc:
        return "BUILDFAIL", "genmc not found (set GENMC=/path/to/genmc)"
    mutant_dir = os.path.dirname(mutated_path)        # already holds the mutant cldeque.c
    src_dir = os.path.join(ROOT, "src", "runloom_c")
    gdir = os.path.join(HERE, "genmc")
    try:
        shutil.copy(os.path.join(gdir, "chase_lev_real.c"), mutant_dir)
        hdr = os.path.join(src_dir, "cldeque.h")
        if os.path.isfile(hdr):
            shutil.copy(hdr, mutant_dir)
    except OSError as e:
        return "BUILDFAIL", "co-locate: " + str(e)
    cmd = [genmc, "--", "-I", src_dir,
           "-DRUNLOOM_CLDEQUE_CAP={0}".format(cap), "chase_lev_real.c"]
    try:
        r = subprocess.run(cmd, cwd=mutant_dir, capture_output=True, text=True,
                           timeout=timeout)
    except subprocess.TimeoutExpired:
        return "BUILDFAIL", "timeout"
    out = r.stdout + r.stderr
    if "No errors were detected" in out:
        return "PASS", "no RC11 error (mutant survived)"
    if re.search(r"(?i)\b(race|violation|error|assert|liveness)\b", out):
        return "FAIL", "RC11 error/race (mutant killed)"
    return "BUILDFAIL", (out[-200:] or "no verdict")


# --- mutation operators: yield (label, mutated_text) per mutant ----------------

RELOPS = [("==", "!="), ("!=", "=="), ("<=", ">"), (">=", "<"),
          ("<", ">="), (">", "<=")]
# longer ops first so '<=' is matched before '<'
RELOP_RE = re.compile(r"(==|!=|<=|>=|<|>)")
# Both atomic families: C11 `memory_order_*` literals AND the GCC/Clang
# `__ATOMIC_*` builtin constants (what src/runloom_c/cldeque.c actually uses via
# __atomic_load_n/etc).  Matching only the C11 form made the cldeque moflip sweep
# a silent no-op -- 0 mutants found, exit 0, zero teeth.
MO_RE = re.compile(r"memory_order_(?:seq_cst|acquire|release|acq_rel)"
                   r"|__ATOMIC_(?:SEQ_CST|ACQUIRE|RELEASE|ACQ_REL)")
_BUG_RE = re.compile(r"BUG", re.I)


def _inactive_lines(text):
    """0-based line indices that are NOT compiled in the baseline (no -D) build:
    inside a `#if 0` or a `#ifdef <BUGmacro>` true-branch / `#ifndef <BUGmacro>`
    else-branch.  Mutating those is meaningless -- they're the negative-control
    code, live only under -DBUG*, so a mutation there trivially SURVIVES and would
    be a false finding.  (Non-bug `#ifdef`s are assumed active to avoid over-skip.)"""
    inactive = set()
    stack = []   # each: {"on": bool, "known": bool}

    def cur_on():
        return all(e["on"] for e in stack)

    for i, raw in enumerate(text.split("\n")):
        s = raw.strip()
        if s.startswith("#"):
            d = s[1:].strip()
            if re.match(r"if\s+0\b", d):
                stack.append({"on": False, "known": True})
            elif d.startswith(("ifdef", "ifndef")) or d.startswith("if "):
                if _BUG_RE.search(d):
                    stack.append({"on": d.startswith("ifndef"), "known": True})
                else:
                    stack.append({"on": True, "known": False})
            elif d.startswith("else"):
                if stack and stack[-1]["known"]:
                    stack[-1]["on"] = not stack[-1]["on"]
            elif d.startswith("elif"):
                if stack and stack[-1]["known"]:
                    stack[-1]["on"] = False
            elif d.startswith("endif"):
                if stack:
                    stack.pop()
            continue
        if not cur_on():
            inactive.add(i)
    return inactive


def gen_relop(text, inactive):
    out = []
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if i in inactive:
            continue
        code = line.split("/*")[0].split("//")[0]
        if "assert(" in code or "assume(" in code:
            # assert(...) is the PROPERTY; __CPROVER_assume(...) is input
            # scaffolding -- neither is model logic, so mutating them is meaningless.
            continue
        if not code.strip() or code.lstrip().startswith(("*", "#")):
            continue
        for m in RELOP_RE.finditer(code):
            op = m.group(1)
            repl = dict(RELOPS)[op]
            new_code = code[:m.start()] + repl + code[m.end():]
            new_line = new_code + line[len(code):]   # re-attach trailing comment
            ml = lines[:]
            ml[i] = new_line
            out.append(("L{0}:{1}->{2}".format(i + 1, op, repl), "\n".join(ml)))
    return out


def gen_moflip(text, inactive):
    out = []
    for m in MO_RE.finditer(text):
        tok = m.group(0)
        # relax to the SAME family's relaxed form (regex never matches an
        # already-relaxed token, so this only picks the strengthened orders).
        repl = "__ATOMIC_RELAXED" if tok.startswith("__ATOMIC_") else "memory_order_relaxed"
        ln0 = text.count("\n", 0, m.start())
        if ln0 in inactive:
            continue
        mutated = text[:m.start()] + repl + text[m.end():]
        out.append(("L{0}:{1}->relaxed".format(ln0 + 1, tok), mutated))
    return out


# --- target registry -----------------------------------------------------------

TARGETS = {
    "wake_state": {
        "engine": "spin",
        "mutate": os.path.join(HERE, "spin", "wake_state.pml"),
        "ops": [gen_relop],
        "run": lambda mut, to: run_spin(mut, to),
        "timeout": 120,
    },
    "parked_safe": {
        "engine": "spin",
        "mutate": os.path.join(HERE, "spin", "parked_safe.pml"),
        "ops": [gen_relop],
        "run": lambda mut, to: run_spin(mut, to),
        "timeout": 120,
    },
    "io_classify": {   # fast CBMC target (<1s): validates the cbmc runner + relop
        "engine": "cbmc",
        "mutate": os.path.join(HERE, "cbmc", "io_classify_cbmc.c"),
        "ops": [gen_relop],
        "run": lambda mut, to: run_cbmc(
            ["cbmc", "{MUT}", "-I", os.path.join(ROOT, "src", "runloom_c")],
            mut, to),
        "timeout": 120,
    },
    "cldeque": {   # CBMC (SC) -- validates the moflip runner, but SC makes a
        # memory-order flip a NO-OP, so every moflip mutant vacuously SURVIVES
        # here.  Kept for the runner; use cldeque_genmc for real weak-memory teeth.
        # ON-DEMAND ONLY: ~242s per CBMC run, so a full MO sweep is overnight-scale.
        "engine": "cbmc",
        "mutate": os.path.join(ROOT, "src", "runloom_c", "cldeque.c"),
        "ops": [gen_moflip],
        "run": lambda mut, to: run_cbmc(
            ["cbmc", os.path.join(HERE, "cbmc", "cldeque_cbmc.c"), "{MUT}",
             "-I", os.path.join(ROOT, "src", "runloom_c"),
             "-I", os.path.join(HERE, "cbmc", "stubs"),
             "-DRUNLOOM_CLDEQUE_CAP=4"], mut, to),
        "timeout": 600,
    },
    "cldeque_genmc": {   # THE FT headline with TEETH: RC11 weak memory (not SC),
        # so a surviving moflip mutant means the order is genuinely not
        # load-bearing (a proof hole), and a killed one proves the barrier needed.
        # ON-DEMAND ONLY: minutes per GenMC run; run with --target cldeque_genmc.
        "engine": "genmc",
        "mutate": os.path.join(ROOT, "src", "runloom_c", "cldeque.c"),
        "ops": [gen_moflip],
        "run": lambda mut, to: run_genmc(mut, to),
        "timeout": 900,
    },
}

# quick-validation subset (fast): the spin models + the fast cbmc target.
# cldeque (the headline MO-flip sweep) is on-demand only -- 242s/run.
QUICK = {"wake_state": None, "parked_safe": None, "io_classify": None}


def mutate_target(name, spec, max_mutants, timeout):
    src = spec["mutate"]
    if not os.path.isfile(src):
        return None, "source missing: " + src
    with open(src, encoding="utf-8", errors="replace") as f:
        text = f.read()
    to = timeout or spec["timeout"]

    # baseline must be green or we can't mutation-test.
    work = tempfile.mkdtemp(prefix="mm_base_")
    base_path = os.path.join(work, os.path.basename(src))
    shutil.copy(src, base_path)
    status, detail = spec["run"](base_path, to)
    shutil.rmtree(work, ignore_errors=True)
    if status != "PASS":
        return None, "baseline NOT green ({0}: {1}) -- fix before mutating".format(status, detail)

    inactive = _inactive_lines(text)
    mutants = []
    for op in spec["ops"]:
        mutants.extend(op(text, inactive))
    if max_mutants:
        mutants = mutants[:max_mutants]

    killed = survived = skipped = 0
    survivors = []
    for label, mtext in mutants:
        work = tempfile.mkdtemp(prefix="mm_mut_")
        mpath = os.path.join(work, os.path.basename(src))
        with open(mpath, "w") as f:
            f.write(mtext)
        st, det = spec["run"](mpath, to)
        shutil.rmtree(work, ignore_errors=True)
        if st == "FAIL":
            killed += 1
        elif st == "PASS":
            survived += 1
            survivors.append(label)
        else:
            skipped += 1
    return {
        "engine": spec["engine"], "mutants": len(mutants),
        "killed": killed, "survived": survived, "skipped": skipped,
        "survivors": survivors,
    }, None


def main(argv):
    ap = argparse.ArgumentParser(description="mutation-test the formal models")
    ap.add_argument("--target", default=None, help="one of: " + ", ".join(TARGETS))
    ap.add_argument("--full", action="store_true", help="all mutants of all targets")
    ap.add_argument("--max-mutants", type=int, default=None)
    ap.add_argument("--timeout", type=int, default=None, help="per-run seconds")
    ap.add_argument("--json", default=None,
                    help="write per-target results (survivors, score) to this path")
    args = ap.parse_args(argv)
    json_results = {}

    if args.target:
        plan = {args.target: args.max_mutants}
    elif args.full:
        plan = {t: args.max_mutants for t in TARGETS}
    else:
        plan = dict(QUICK)
        if args.max_mutants:
            plan = {t: args.max_mutants for t in plan}

    print("model_mutate: {0}".format(", ".join(plan)))
    any_survivor = False
    any_error = False
    for name in plan:
        spec = TARGETS.get(name)
        if not spec:
            print("  {0}: unknown target".format(name)); any_error = True; continue
        res, err = mutate_target(name, spec, plan[name], args.timeout)
        if err:
            print("  {0:<14} SKIP -- {1}".format(name, err)); any_error = True; continue
        conclusive = res["killed"] + res["survived"]
        if res["mutants"] == 0:
            print("  {0:<14} [{1}] no mutable model logic (all relops in "
                  "assert/assume/scaffold) -- runner OK, nothing to score"
                  .format(name, res["engine"]))
            continue
        if conclusive == 0:
            print("  {0:<14} [{1}] {2} mutants ALL inconclusive (build-fail/timeout) "
                  "-- no teeth signal".format(name, res["engine"], res["mutants"]))
            continue
        score = 100.0 * res["killed"] / conclusive
        json_results[name] = {"score": score, "killed": res["killed"],
                              "survived": res["survived"],
                              "survivors": res["survivors"]}
        print("  {0:<14} [{1}] {2} mutants: {3} killed, {4} SURVIVED, "
              "{5} skipped  -> teeth {6:.0f}%".format(
                  name, res["engine"], res["mutants"], res["killed"],
                  res["survived"], res["skipped"], score))
        if res["survivors"]:
            any_survivor = True
            for s in res["survivors"]:
                print("      SURVIVOR (property does not constrain this): {0}".format(s))

    if args.json:
        import json as _json
        with open(args.json, "w") as f:
            _json.dump(json_results, f, indent=1, sort_keys=True)
        print("wrote {0}".format(args.json))

    if any_error:
        return 2
    print("\nmodel_mutate: {0}".format(
        "SURVIVORS found -- audit the listed elements (exit 1)" if any_survivor
        else "no survivors -- every mutated element is load-bearing (exit 0)"))
    return 1 if any_survivor else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
