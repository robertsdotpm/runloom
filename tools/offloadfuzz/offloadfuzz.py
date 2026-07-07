"""offloadfuzz -- conservation + leak fuzzer for the blockpool OFFLOAD geometry.

Hammers runloom_c.blocking (submit -> park -> worker drains off-hub -> wake ->
result) at scale across many hubs, in submit shapes and shard/worker geometries
the existing soak never varies.  The offload/blockpool handoff is the most
bug-dense subsystem historically -- the stealable wake queue, sweeper/thief
handshake, Group A/B/C stall recovery, and the per-g crashes all lived in
runloom_blockpool.c / the handoff -- yet lifefuzz only touches it via a 50%
coin-flip single op, and monkey_offload_stress drives a DIFFERENT (monkey) pool
with a hang-only oracle.  Nothing fuzzes the submit/drain/park GEOMETRY at scale
with a conservation + leak assertion.

Oracle (per program, after the run quiesces):
  * ABSOLUTE submit conservation -- the fuzzer submits exactly N ops, each op
    returning a KNOWN per-op token; every submitter's returned tokens are checked
    against what it asked for (catches mis-delivery / cross-submitter SWAP that a
    global sum would hide), and the global token sum must match.  The runtime
    exposes only NET outstanding (stats()['blockpool_inflight']), never a
    completed odometer, so absolute submitted==completed must be counted here --
    that is the teeth a hang-only stressor lacks.
  * NET zero -- stats()['blockpool_inflight']==0 and no parked/pending/deque/
    sleeping residue after mn_run()/run() returns.
  * NO g / fd leak -- /proc/self/fd back to baseline (+slop for the pump eventfd).
  * _self_check(0)==0 -- walks every live sched/netpoll parker list.

Usage:
  offloadfuzz.py gen SEED                     print the spec for a seed
  offloadfuzz.py run SEED [--timeout S]       run one program in-process (dev)
  offloadfuzz.py worker SEED MNSEED TIMEOUT   subprocess entry (internal)
  offloadfuzz.py sweep [N] [--workers W]      N isolated subprocesses -> corpus/
"""
import argparse
import json
import os
import random
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, ROOT)

# blockpool geometry knobs (folded into the worker env, a pure function of seed).
# 1 = the legacy single global queue (the p23 convoy repro); 32 = RUNLOOM_BP_SHARDS_MAX.
SHARD_CHOICES = [1, 2, 4, 8, 32]
WORKER_CHOICES = [0, 1, 8, 256]         # 0 = default (nshard*3, floor 8)

# markers the subprocess parser keys on (mirrors lifefuzz's LIFEFUZZ_OK / MISMATCH)
OK_MARK = "OFFLOADFUZZ_OK"
FAIL_MARK = "OFFLOADFUZZ_FAIL"
FINDING_PATTERNS = ("CONSERVATION", "SWAP", "INFLIGHT_LEAK", "PARKED_LEAK",
                    "FD_LEAK", "SUBMITTER_LEAK", "SELF_CHECK", "Traceback")


def build_spec(seed):
    rng = random.Random(seed)
    scale = rng.random() < 0.15                       # ~15% of seeds go big
    mode = rng.choice(["mn", "mn", "mn", "st"])        # M:N offload is the gap
    nhubs = rng.choice([2, 3, 4, 6, 8]) if mode == "mn" else 1
    nsub = rng.randint(40, 256) if scale else rng.randint(1, 32)
    ops = rng.randint(400, 2000) if scale else rng.randint(1, 60)
    return {
        "seed": seed,
        "mode": mode,
        "nhubs": nhubs,
        "nsub": nsub,
        "ops": ops,
        # drain-timing variance: 0 = instant (complete-before-park likely),
        # large = slow (park-before-complete likely); mixed exercises both sides.
        "spin_max": rng.choice([0, 0, 64, 4096, 65536]),
        "mixed_dur": rng.random() < 0.5,
        "shards": rng.choice(SHARD_CHOICES),
        "workers": rng.choice(WORKER_CHOICES),
    }


def fd_count():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def run_program(spec, timeout=30.0):
    """Run one offload-geometry program under the watchdog; return (ok, reason)."""
    import runloom
    import runloom_c
    from tools.watchdog import run_guarded

    nsub, ops = spec["nsub"], spec["ops"]
    spin_max, mixed = spec["spin_max"], spec["mixed_dur"]

    # each op returns a known token; per-submitter + global checksums are the
    # absolute-conservation ground truth the runtime cannot give us.
    def token(sid, k):
        return (sid * 100000 + k) & 0x3FFFFFFF

    expect_sum = sum(token(sid, k) for sid in range(nsub) for k in range(ops))
    done = bytearray(nsub)          # single-owner slot per submitter (race-free)
    mism = [0] * nsub               # per-submitter mis-delivery count (catches swaps)
    sums = [0] * nsub               # per-submitter token sum

    def make_job(tok, dur):
        def job():
            if dur:
                # variable off-hub, GIL-releasing-then-reacquiring Python work
                s = 0
                for i in range(dur):
                    s += i
                return tok if s >= 0 else -1   # s>=0 always; keeps the spin live
            return tok
        return job

    def submitter(sid, n_ops, rec):
        acc = 0
        mm = 0
        for k in range(n_ops):
            tok = token(sid, k)
            dur = 0
            if spin_max:
                dur = spin_max if (not mixed or (k & 1)) else 0
            got = runloom.blocking(make_job(tok, dur))
            if not rec:
                continue
            if got != tok:
                mm += 1
            if isinstance(got, int):
                acc += got
        if rec:
            sums[sid] = acc
            mism[sid] = mm
            done[sid] = 1

    def cycle(n_sub, n_ops, rec):
        # mn_fiber/fiber take (callable, [stack_size]) -- the 2nd positional is a
        # stack size, NOT a forwarded arg, so bind sid via a default-arg closure.
        def root():
            spawn = runloom_c.mn_fiber if spec["mode"] == "mn" else runloom_c.fiber
            for sid in range(n_sub):
                spawn(lambda s=sid: submitter(s, n_ops, rec))
        if spec["mode"] == "mn":
            runloom_c.mn_init(spec["nhubs"])
            runloom_c.mn_fiber(root)
            completed = runloom_c.mn_run()
            st = dict(runloom_c.stats())        # snapshot BEFORE fini
            runloom_c.mn_fini()
        else:
            runloom_c.fiber(root)
            runloom_c.run()
            completed = None
            st = dict(runloom_c.stats())
        return completed, st

    def driver():
        # Warmup cycle first: the runtime lazily opens per-hub netpoll epoll/
        # eventfds + the pump pipe and spins up the process-global blockpool
        # worker threads, and REUSES them across mn_init/mn_fini cycles.  So an
        # fd leak is a SLOPE (growth after steady state), never an absolute count
        # -- warm up, snapshot the baseline, then assert the measured run adds none.
        cycle(min(nsub, 4), 1, False)
        base_fd = fd_count()
        completed, st = cycle(nsub, ops, True)
        return completed, st, base_fd, fd_count()

    completed, st, base_fd, end_fd = run_guarded(
        driver, seconds=timeout,
        label="offloadfuzz seed={0}".format(spec["seed"]))

    # ---- oracles ----
    if sum(done) != nsub:
        return False, "SUBMITTER_LEAK {0}/{1}".format(sum(done), nsub)
    if sum(mism) != 0:
        return False, "SWAP mis-delivered={0} (per-op token mismatch)".format(sum(mism))
    if sum(sums) != expect_sum:
        return False, "CONSERVATION sum={0} want={1}".format(sum(sums), expect_sum)
    if st.get("blockpool_inflight", 0):
        return False, "INFLIGHT_LEAK bp={0}".format(st["blockpool_inflight"])
    residue = (st.get("sleeping", 0) + st.get("netpoll_parked", 0)
               + st.get("running", 0) + st.get("mn_pending_total", 0)
               + st.get("mn_deque_depth", 0) + st.get("foreign_park_inflight", 0))
    if residue:
        return False, "PARKED_LEAK residue={0} stats={1}".format(residue, st)
    if end_fd > base_fd + 2:
        return False, "FD_LEAK {0}->{1}".format(base_fd, end_fd)
    v = runloom_c._self_check(0)
    if v:
        runloom_c._self_check(1)
        return False, "SELF_CHECK {0}".format(v)
    return True, "ok submitted={0} completed={1}".format(nsub * ops, completed)


# --------------------------------------------------------------------------- #
# subprocess sweep (mirrors tools/lifefuzz/lifefuzz.py structure)
def worker_main(seed, mn_seed, timeout):
    spec = build_spec(seed)
    ok, reason = run_program(spec, timeout=timeout)
    if ok:
        print("{0} seed={1} {2}".format(OK_MARK, seed, reason))
        return 0
    print("{0} seed={1} reason={2}".format(FAIL_MARK, seed, reason))
    return 1


def run_worker_subprocess(seed, mn_seed, timeout):
    """Run one program as an isolated subprocess.  Returns a finding dict or None."""
    from tools.lifefuzz.lifefuzz import worker_env
    spec = build_spec(seed)
    extra = {"RUNLOOM_BLOCKPOOL_SHARDS": str(spec["shards"])}
    if spec["workers"]:
        extra["RUNLOOM_BLOCKPOOL_WORKERS"] = str(spec["workers"])
    env = worker_env(seed, mn_seed, extra=extra)
    wrap = os.environ.get("OFFLOADFUZZ_WORKER_WRAP", "").split()
    argv = wrap + [sys.executable, os.path.abspath(__file__), "worker",
                   str(seed), str(mn_seed if mn_seed is not None else -1), str(timeout)]
    try:
        p = subprocess.run(argv, env=env, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=timeout + 10)
    except subprocess.TimeoutExpired as e:
        out = (e.output or b"").decode("utf-8", "replace")
        return {"seed": seed, "signal": "HANG", "rc": None, "tail": out[-2000:]}
    out = p.stdout.decode("utf-8", "replace")
    bad = p.returncode != 0 or OK_MARK not in out
    if not bad:
        bad = any(pat in out for pat in FINDING_PATTERNS)
    if bad:
        sig = "CRASH" if p.returncode and p.returncode < 0 else "FAIL"
        return {"seed": seed, "signal": sig, "rc": p.returncode, "tail": out[-2000:]}
    return None


def sweep(n, workers, seed0, timeout):
    import concurrent.futures
    corpus = os.path.join(HERE, "corpus")
    os.makedirs(corpus, exist_ok=True)
    print("offloadfuzz sweep: seeds [{0},{1}) workers={2} timeout={3}s"
          .format(seed0, seed0 + n, workers, timeout))
    findings = []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(run_worker_subprocess, seed0 + i, seed0 + i, timeout): seed0 + i
                for i in range(n)}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            f = fut.result()
            if f is not None:
                findings.append(f)
                path = os.path.join(corpus, "seed_{0}.json".format(f["seed"]))
                with open(path, "w") as out:
                    json.dump(f, out, indent=2)
                print("\n  !! FINDING seed={0} signal={1} rc={2} -> {3}"
                      .format(f["seed"], f["signal"], f["rc"], path))
                print("     repro: tools/offloadfuzz/offloadfuzz.py run {0}".format(f["seed"]))
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
    r.add_argument("--timeout", type=float, default=30.0)
    w = sub.add_parser("worker")
    w.add_argument("seed", type=int)
    w.add_argument("mn_seed", type=int)
    w.add_argument("timeout", type=float)
    s = sub.add_parser("sweep")
    s.add_argument("n", type=int, nargs="?", default=500)
    s.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 2))
    s.add_argument("--seed0", type=int, default=1)
    s.add_argument("--timeout", type=float, default=30.0)
    args = p.parse_args(argv)

    if args.cmd == "gen":
        print(json.dumps(build_spec(args.seed), indent=2))
        return 0
    if args.cmd == "run":
        ok, why = run_program(build_spec(args.seed), timeout=args.timeout)
        print("seed={0} -> {1} ({2})".format(args.seed, "OK" if ok else "FAIL", why))
        return 0 if ok else 1
    if args.cmd == "worker":
        mn = args.mn_seed if args.mn_seed >= 0 else None
        return worker_main(args.seed, mn, args.timeout)
    if args.cmd == "sweep":
        return sweep(args.n, args.workers, args.seed0, args.timeout)
    p.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
