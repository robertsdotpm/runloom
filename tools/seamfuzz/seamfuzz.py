"""seamfuzz -- targeted fuzzer for the runloom <-> CPython-3.14t-internals SEAM.

Every recent REAL bug lived on this seam: gilstate/tstate attach-detach, the
stop-the-world (STW) handshake, biased-refcount merge, preempt-mid-object-
destruction, mimalloc per-thread heap binding (p488, the per-g crashes, Group B
handoff).  ThreadSanitizer finds races there; this hammers the seam with four
composable "moves" and asserts on it directly, so it also finds the non-race
failures (assert-fires under a pydebug interp, self_check violations, UAF under
ASan, hangs).

The four moves -- each provably drives one seam machine (refs are src/runloom_c):
  * stw     -- fiber loops gc.collect(): free-threaded 3.14t gc.collect() is a
               full stop-the-world; drives the ATTACHED->SUSPENDED sibling
               census + world-yield detach (mn_sched_hub_main.c.inc:102).
  * xhub    -- object born on hub A, its SOLE ref sent through a channel to a
               fiber on hub B and dropped there: cross-hub last-decref routed to
               A's biased-refcount merge queue + tp_dealloc drain (iframe.c:120).
               This is the net-zero dealloc-side path the refleak suite lacks.
  * preempt -- a fiber churns a WeakKeyDictionary (brc arm) AND a deep __del__
               container chain (trashcan/delete_later arm) while a thread STW-
               collects, so preemption fires mid-tp_dealloc (the C5 gate,
               resume_preempt.c.inc:954).
  * foreign -- real OS threads hammer a runloom CoLock also held by a fiber:
               the foreign-thread cooperative path (module_chan.c.inc:458-482).

Oracle: nonzero exit / signal (SEGV/SIGABRT) / timeout(HANG) / _self_check(0)!=0.
Lanes (compose via env, no code change):
  * pydebug lane: SEAM_PYTHON=/path/to/--with-pydebug-python -> CPython internal
    asserts fire at the exact seam line (tstate ownership, gilstate, brc, heap).
  * ASan lane: run under tools/run_asan_ext.sh's preloaded libasan (runloom's own
    C-heap UAF).  * TSan lane: run under the tsan-gold interp.
  * widener: RUNLOOM_DELAY=<seed> + RUNLOOM_DELAY_MAX_NS arm the diag delay sites
    (set per-seed by the sweep) so timing-rare seam reorders become deterministic.

Usage:
  seamfuzz.py gen SEED
  seamfuzz.py run SEED [--timeout S]     run one program in-process (dev)
  seamfuzz.py worker SEED TIMEOUT        subprocess entry (internal)
  seamfuzz.py sweep [N] [--workers W]    N isolated subprocesses
"""
import argparse
import gc
import os
import random
import subprocess
import sys
import threading
import weakref

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, ROOT)

ALL_MOVES = ("stw", "xhub", "preempt", "foreign")
OK_MARK = "SEAMFUZZ_OK"
FINDING_PATTERNS = ("SELF_CHECK", "Traceback", "Assertion", "Fatal Python error")


class Fin(object):
    """tp_dealloc runs user Python -> a real finalizer at destruction time."""
    __slots__ = ("n", "__weakref__")

    def __init__(self, n):
        self.n = n

    def __del__(self):
        pass


def build_spec(seed):
    rng = random.Random(seed)
    moves = [m for m in ALL_MOVES if rng.random() < 0.6] or ["xhub"]
    return {
        "seed": seed,
        "nhub": rng.choice([2, 2, 4]),
        "moves": moves,
        "delay_max_ns": rng.choice([2000, 10000, 50000]),
        "iters": rng.choice([1500, 4000]),
    }


def worker_env(spec):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src") + os.pathsep + env.get("PYTHONPATH", "")
    env["RUNLOOM_DEBUG"] = "ring,gstate"
    env["RUNLOOM_DBG_GSTATE"] = "1"
    env["RUNLOOM_DELAY"] = str(spec["seed"])        # arm the diag delay sites
    env["RUNLOOM_DELAY_MAX_NS"] = str(spec["delay_max_ns"])
    env["SEAM_NHUB"] = str(spec["nhub"])
    env["SEAM_MOVES"] = ",".join(spec["moves"])
    env["SEAM_ITERS"] = str(spec["iters"])
    # NB: deliberately does NOT set RUNLOOM_ALLOW_UNSAFE_MIGRATION -- that forces
    # the documented-impossible per-g live-frame tstate migration, whose mimalloc
    # heap->thread_id abort is a KNOWN-dead-mode artifact, not a seam bug.
    return env


def run_moves(nhub, moves, iters):
    """Run the selected seam moves under one mn_init/mn_run/mn_fini envelope.
    Every move is bounded/self-terminating so mn_run() returns on its own."""
    import runloom_c
    gc.set_threshold(50, 5, 5)                      # amplify STW frequency

    def stw():
        for _ in range(iters):
            gc.collect()
            runloom_c.sched_yield_classic()

    ring = runloom_c.Chan(256)

    def producer():
        for i in range(iters):
            ring.send(Fin(i))                        # sole ref handed across
            runloom_c.sched_yield_classic()
        ring.close()

    def consumer():
        while True:
            v, ok = ring.recv()
            if not ok:
                break
            del v                                    # cross-hub last-decref -> tp_dealloc

    def preempt_dealloc():
        t = threading.Thread(target=lambda: [gc.collect() for _ in range(iters // 4)])
        t.start()
        for _ in range(3):
            d = weakref.WeakKeyDictionary()
            ks = []
            for i in range(iters * 4):
                k = Fin(i)
                ks.append(k)
                d[k] = Fin(i)
            deep = None
            for i in range(iters):
                deep = [deep, Fin(i)]                # trashcan/delete_later arm
            while ks:
                ks.pop()
            del deep
        t.join()

    runloom_c.mn_init(nhub)
    if "stw" in moves:
        runloom_c.mn_fiber(stw)
    if "xhub" in moves:
        runloom_c.mn_fiber(producer)
        runloom_c.mn_fiber(consumer)
    if "preempt" in moves:
        runloom_c.mn_fiber(preempt_dealloc, 8 << 20)   # roomy stack for the churn
    foreign_threads = []
    if "foreign" in moves:
        from runloom.monkey.locks import CoLock
        lk = CoLock()

        def foreign():
            for _ in range(iters):
                lk.acquire()
                lk.release()

        def fiber_hold():
            for _ in range(iters):
                lk.acquire()
                runloom_c.sched_yield()
                lk.release()

        foreign_threads = [threading.Thread(target=foreign) for _ in range(nhub)]
        for t in foreign_threads:
            t.start()
        runloom_c.mn_fiber(fiber_hold)
    runloom_c.mn_run()
    for t in foreign_threads:
        t.join()
    runloom_c.mn_fini()
    return runloom_c._self_check(0)


def run_program(spec, timeout=120.0):
    from tools.watchdog import run_guarded
    v = run_guarded(lambda: run_moves(spec["nhub"], spec["moves"], spec["iters"]),
                    seconds=timeout, label="seamfuzz seed={0}".format(spec["seed"]))
    if v:
        return False, "SELF_CHECK {0}".format(v)
    return True, "ok moves={0} nhub={1}".format(",".join(spec["moves"]), spec["nhub"])


def worker_main(seed, timeout):
    # env carries the per-seed knobs; nhub/moves/iters come through SEAM_*.
    spec = build_spec(seed)
    spec["nhub"] = int(os.environ.get("SEAM_NHUB", spec["nhub"]))
    spec["moves"] = os.environ.get("SEAM_MOVES", ",".join(spec["moves"])).split(",")
    spec["iters"] = int(os.environ.get("SEAM_ITERS", spec["iters"]))
    ok, reason = run_program(spec, timeout=timeout)
    if ok:
        print("{0} seed={1} {2}".format(OK_MARK, seed, reason))
        return 0
    print("SEAMFUZZ_FAIL seed={0} reason={1}".format(seed, reason))
    return 1


def run_worker_subprocess(seed, timeout):
    spec = build_spec(seed)
    env = worker_env(spec)
    py = os.environ.get("SEAM_PYTHON", sys.executable)   # point at pydebug interp for lane 2
    wrap = os.environ.get("SEAMFUZZ_WORKER_WRAP", "").split()
    argv = wrap + [py, os.path.abspath(__file__), "worker", str(seed), str(timeout)]
    try:
        p = subprocess.run(argv, env=env, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=timeout + 15)
    except subprocess.TimeoutExpired as e:
        out = (e.output or b"").decode("utf-8", "replace")
        return {"seed": seed, "signal": "HANG", "rc": None, "tail": out[-2000:], "moves": spec["moves"]}
    out = p.stdout.decode("utf-8", "replace")
    bad = p.returncode != 0 or OK_MARK not in out
    if not bad:
        bad = any(pat in out for pat in FINDING_PATTERNS)
    if bad:
        sig = "CRASH" if p.returncode and p.returncode < 0 else "FAIL"
        return {"seed": seed, "signal": sig, "rc": p.returncode, "tail": out[-2000:], "moves": spec["moves"]}
    return None


def sweep(n, workers, seed0, timeout):
    import concurrent.futures
    corpus = os.path.join(HERE, "corpus")
    os.makedirs(corpus, exist_ok=True)
    print("seamfuzz sweep: seeds [{0},{1}) workers={2} timeout={3}s python={4}"
          .format(seed0, seed0 + n, workers, timeout,
                  os.environ.get("SEAM_PYTHON", sys.executable)))
    findings = []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(run_worker_subprocess, seed0 + i, timeout): seed0 + i for i in range(n)}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            f = fut.result()
            if f is not None:
                findings.append(f)
                import json
                path = os.path.join(corpus, "seed_{0}.json".format(f["seed"]))
                with open(path, "w") as out:
                    json.dump(f, out, indent=2)
                print("\n  !! FINDING seed={0} signal={1} rc={2} moves={3} -> {4}"
                      .format(f["seed"], f["signal"], f["rc"], f.get("moves"), path))
                print("     repro: tools/seamfuzz/seamfuzz.py run {0}".format(f["seed"]))
            if done % 25 == 0 or done == n:
                sys.stdout.write("\r  progress {0}/{1} findings={2}   ".format(done, n, len(findings)))
                sys.stdout.flush()
    print("\nsweep done: {0} runs, {1} findings".format(n, len(findings)))
    return 1 if findings else 0


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd")
    g = sub.add_parser("gen")
    g.add_argument("seed", type=int)
    r = sub.add_parser("run")
    r.add_argument("seed", type=int)
    r.add_argument("--timeout", type=float, default=120.0)
    w = sub.add_parser("worker")
    w.add_argument("seed", type=int)
    w.add_argument("timeout", type=float)
    s = sub.add_parser("sweep")
    s.add_argument("n", type=int, nargs="?", default=200)
    s.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 2))
    s.add_argument("--seed0", type=int, default=1)
    s.add_argument("--timeout", type=float, default=120.0)
    args = p.parse_args(argv)

    if args.cmd == "gen":
        import json
        print(json.dumps(build_spec(args.seed), indent=2))
        return 0
    if args.cmd == "run":
        ok, why = run_program(build_spec(args.seed), timeout=args.timeout)
        print("seed={0} -> {1} ({2})".format(args.seed, "OK" if ok else "FAIL", why))
        return 0 if ok else 1
    if args.cmd == "worker":
        return worker_main(args.seed, args.timeout)
    if args.cmd == "sweep":
        return sweep(args.n, args.workers, args.seed0, args.timeout)
    p.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
