#!/usr/bin/env python3
"""sweep.py <TU> [--tests ...] [--limit N] [--sample K] [--jobs J] [--timeout T]

Mutation sweep over a schemata-built TU (see build_target.sh).  For each mutant
id: enable it via DREDD_ENABLED_MUTATION and run the test subset; a mutant is
KILLED if any test fails (assertion) or HANGS (timeout -- the runloom bug class),
SURVIVED if the whole subset stays green.  Survivors name a line whose behaviour
NO test in the subset constrains -- the true "untested logic" list, mapped back
to the real .inc file:line via the flatten provenance map.

Runs in the isolated mutant worktree (RUNLOOM_MUT_WORKTREE).  Resumable: each
verdict is checkpointed to <TU>.sweep.jsonl; re-running skips done ids.
Parallel across mutants (--jobs); each job runs the subset serially.

Default test subset for a TU is auto-picked from tests/ by name affinity; pass
--tests to override.  --sample K takes a random-but-SEEDED K-subset of mutants
for a fast first signal; --limit N caps to the first N ids.

House style: %/.format only.
"""
import argparse
import json
import os
import random
import subprocess
import sys
import concurrent.futures as cf

WT = os.environ.get("RUNLOOM_MUT_WORKTREE",
                    os.path.expanduser("~/projects/pygo-mutants"))
PY = os.environ.get("RUNLOOM_PYTHON",
                    os.path.expanduser("~/.pyenv/versions/3.13.13t/bin/python3"))

# name-affinity test subsets: tests most likely to EXECUTE a TU's lines.  A real
# survivor must survive the WHOLE suite, but a curated subset is the cheap,
# high-signal first cut (a mutant this subset can't kill is a strong candidate).
AFFINITY = {
    "netpoll": ["test_tcpconn", "test_adv_netpoll", "test_adv_tcpconn",
                "test_tcp_scenarios", "test_stdlib_selectors_monkey",
                "test_aio_fd_reuse", "test_cov95_gap_runloom_tcp_c"],
    "mn_sched": ["test_mn", "test_adv_sched", "test_cov100_runq",
                 "test_cov100_resume_preempt", "test_stall_steal",
                 "test_sched_fairness"],
    "chan": ["test_chan", "test_adv_chan", "test_chan_queue",
             "test_scheduler_channel_compat"],
    "io_uring": ["test_iouring", "test_iouring_arming", "test_cov95_iouring_ring",
                 "test_iouring_recv_backpressure", "test_iouring_cancel_close"],
}


def load_ids(tu):
    j = json.load(open(os.path.join(WT, "src/runloom_c", tu + ".mutants.json")))
    ids = set()
    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "mutationId" and isinstance(v, int):
                    ids.add(v)
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(j)
    return sorted(ids)


def load_map(tu):
    p = os.path.join(WT, "src/runloom_c", tu + ".flat.map.json")
    spans = json.load(open(p)) if os.path.exists(p) else []
    # dredd reports flat file:line per mutant; build id->line from mutants.json
    j = json.load(open(os.path.join(WT, "src/runloom_c", tu + ".mutants.json")))
    id_line = {}
    def walk(o):
        if isinstance(o, dict):
            mid = o.get("mutationId")
            ln = o.get("line") or o.get("sourceLine")
            if isinstance(mid, int) and isinstance(ln, int):
                id_line[mid] = ln
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(j)
    def to_src(flat_line):
        for s in spans:
            if s["flat_lo"] <= flat_line <= s["flat_hi"]:
                base = os.path.basename(s["src"])
                return "%s:%d" % (base, s["src_off"] + (flat_line - s["flat_lo"]))
        return "flat:%d" % flat_line
    return {mid: to_src(ln) for mid, ln in id_line.items()}


def run_mutant(tu, mid, tests, timeout):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               DREDD_ENABLED_MUTATION=str(mid))
    argv = [PY, "tests/run_isolated.py", "-j1"] + [t + ".py" for t in tests]
    try:
        p = subprocess.run(argv, cwd=WT, env=env, capture_output=True,
                           text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return mid, "killed-hang"
    if "all green" in (p.stdout or ""):
        return mid, "survived"
    return mid, "killed-fail"


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("tu")
    ap.add_argument("--tests", nargs="*", default=None)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--jobs", type=int, default=12)
    ap.add_argument("--timeout", type=int, default=90)
    a = ap.parse_args(argv[1:])

    tests = a.tests or AFFINITY.get(a.tu)
    if not tests:
        sys.exit("no default test subset for %r -- pass --tests" % a.tu)
    ids = load_ids(a.tu)
    if a.sample:
        random.Random(1234).shuffle(ids)
        ids = sorted(ids[:a.sample])
    elif a.limit:
        ids = ids[:a.limit]

    ck = os.path.join(WT, "src/runloom_c", a.tu + ".sweep.jsonl")
    done = {}
    if os.path.exists(ck):
        for line in open(ck):
            try:
                r = json.loads(line)
                done[r["id"]] = r["verdict"]
            except Exception:
                pass
    todo = [i for i in ids if i not in done]
    print("[sweep] %s: %d mutants, %d done, %d to run, subset=%s"
          % (a.tu, len(ids), len(done), len(todo), ",".join(tests)), flush=True)

    idline = load_map(a.tu)
    ckf = open(ck, "a", buffering=1)
    n_k = sum(1 for v in done.values() if v.startswith("killed"))
    n_s = sum(1 for v in done.values() if v == "survived")
    with cf.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        futs = [ex.submit(run_mutant, a.tu, i, tests, a.timeout) for i in todo]
        for i, fut in enumerate(cf.as_completed(futs)):
            mid, verdict = fut.result()
            ckf.write(json.dumps({"id": mid, "verdict": verdict}) + "\n")
            if verdict == "survived":
                n_s += 1
            else:
                n_k += 1
            if (i + 1) % 25 == 0 or verdict == "survived":
                tag = "SURVIVED " + idline.get(mid, "?") if verdict == "survived" else verdict
                print("[sweep] %d/%d  killed=%d survived=%d   last: m%d %s"
                      % (i + 1, len(todo), n_k, n_s, mid, tag), flush=True)
    ckf.close()

    # final survivors report
    surv = [json.loads(l) for l in open(ck)]
    survivors = sorted(r["id"] for r in surv if r["verdict"] == "survived")
    rep = os.path.join(WT, "src/runloom_c", a.tu + ".survivors.txt")
    with open(rep, "w") as f:
        f.write("# %s mutation survivors (subset: %s)\n" % (a.tu, ",".join(tests)))
        f.write("# %d survivors / %d swept -- lines no subset test constrains\n\n"
                % (len(survivors), len(surv)))
        byline = {}
        for mid in survivors:
            byline.setdefault(idline.get(mid, "?"), []).append(mid)
        for loc in sorted(byline):
            f.write("%-40s  mutants %s\n" % (loc, byline[loc]))
    score = 100.0 * (len(surv) - len(survivors)) / len(surv) if surv else 0.0
    print("[sweep] DONE %s: mutation score %.1f%% (%d killed / %d), %d survivors"
          % (a.tu, score, len(surv) - len(survivors), len(surv), len(survivors)),
          flush=True)
    print("[sweep] survivors -> %s" % rep, flush=True)


if __name__ == "__main__":
    main(sys.argv)
