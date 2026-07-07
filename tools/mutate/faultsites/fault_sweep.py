#!/usr/bin/env python3
"""fault_sweep.py <TU> [--tests ...] [--jobs J] [--timeout T] [--limit N]

Exhaustive first-order fault sweep over a TU built by build_faultsites.sh.  For
each fallible call site id: enable ONLY it (RUNLOOM_FI_ENABLED=id), run the test
subset.  A site is HANDLED (killed) if a test fails or hangs when its call is
forced to fail; UNCHECKED (survived) if the whole subset stays green -- i.e. the
runtime swallowed a real error there and NO test noticed.  The survivors are the
error paths worth reading: either the error is silently ignored, or no test
covers it.

Resumable (<TU>.fisweep.jsonl).  Parallel across sites.  Survivors mapped to the
real .inc file:line via sites.json.  House style: %/.format only.
"""
import argparse
import json
import os
import subprocess
import sys
import concurrent.futures as cf

WT = os.environ.get("RUNLOOM_MUT_WORKTREE", os.path.expanduser("~/projects/pygo-mutants"))
PY = os.environ.get("RUNLOOM_PYTHON", os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3"))

# same name-affinity subsets as schemata/sweep.py -- tests that EXECUTE the TU.
AFFINITY = {
    "netpoll": ["test_tcpconn", "test_adv_netpoll", "test_adv_tcpconn",
                "test_tcp_scenarios", "test_netpoll_faultinject",
                "test_select_faultinject", "test_fd_io_faultinject",
                "test_aio_fd_reuse"],
    "mn_sched": ["test_mn", "test_adv_sched", "test_spawn_faultinject",
                 "test_cov100_runq", "test_stall_steal"],
    "io_uring": ["test_iouring", "test_iouring_faultinject",
                 "test_iouring_arming", "test_iouring_recv_backpressure"],
    "runloom_tcp": ["test_tcpconn", "test_tcp_faultinject", "test_adv_tcpconn",
                    "test_tcp_scenarios"],
}


def run_site(tu, sid, tests, timeout):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_FI_ENABLED=str(sid))
    argv = [PY, "tests/run_isolated.py", "-j1"] + [t + ".py" for t in tests]
    try:
        p = subprocess.run(argv, cwd=WT, env=env, capture_output=True,
                           text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return sid, "handled-hang"
    return sid, ("unchecked" if "all green" in (p.stdout or "") else "handled-fail")


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("tu")
    ap.add_argument("--tests", nargs="*", default=None)
    ap.add_argument("--jobs", type=int, default=12)
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args(argv[1:])

    tests = a.tests or AFFINITY.get(a.tu)
    if not tests:
        sys.exit("no default subset for %r -- pass --tests" % a.tu)
    sites = json.load(open(os.path.join(WT, "src/runloom_c", a.tu + ".fisites.json")))
    loc = {s["id"]: "%s %s(errno=%s)" % (s["loc"], s["func"], s["errno"]) for s in sites}
    ids = sorted(s["id"] for s in sites)
    if a.limit:
        ids = ids[:a.limit]

    ck = os.path.join(WT, "src/runloom_c", a.tu + ".fisweep.jsonl")
    done = {}
    if os.path.exists(ck):
        for line in open(ck):
            try:
                r = json.loads(line); done[r["id"]] = r["verdict"]
            except Exception:
                pass
    todo = [i for i in ids if i not in done]
    print("[fi-sweep] %s: %d sites, %d done, %d to run, subset=%s"
          % (a.tu, len(ids), len(done), len(todo), ",".join(tests)), flush=True)

    ckf = open(ck, "a", buffering=1)
    n_h = sum(1 for v in done.values() if v.startswith("handled"))
    n_u = sum(1 for v in done.values() if v == "unchecked")
    with cf.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        futs = [ex.submit(run_site, a.tu, i, tests, a.timeout) for i in todo]
        for k, fut in enumerate(cf.as_completed(futs)):
            sid, verdict = fut.result()
            ckf.write(json.dumps({"id": sid, "verdict": verdict}) + "\n")
            if verdict == "unchecked":
                n_u += 1
                print("[fi-sweep] UNCHECKED site %d: %s" % (sid, loc.get(sid, "?")), flush=True)
            else:
                n_h += 1
            if (k + 1) % 20 == 0:
                print("[fi-sweep] %d/%d  handled=%d unchecked=%d"
                      % (k + 1, len(todo), n_h, n_u), flush=True)
    ckf.close()

    rows = [json.loads(l) for l in open(ck)]
    unchecked = sorted(r["id"] for r in rows if r["verdict"] == "unchecked")
    rep = os.path.join(WT, "src/runloom_c", a.tu + ".unchecked_errors.txt")
    with open(rep, "w") as f:
        f.write("# %s -- fallible call sites whose forced failure NO test noticed\n" % a.tu)
        f.write("# subset: %s\n" % ",".join(tests))
        f.write("# %d unchecked / %d swept\n\n" % (len(unchecked), len(rows)))
        for sid in unchecked:
            f.write("%s\n" % loc.get(sid, "id %d" % sid))
    print("[fi-sweep] DONE %s: %d handled / %d, %d UNCHECKED error paths -> %s"
          % (a.tu, len(rows) - len(unchecked), len(rows), len(unchecked), rep), flush=True)


if __name__ == "__main__":
    main(sys.argv)
