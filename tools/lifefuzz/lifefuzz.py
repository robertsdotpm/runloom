"""lifefuzz -- a generative, seed-replayable LIFE-CYCLE fuzzer for runloom.

Where the existing fuzzers vary the SCHEDULE (tools/dst, tools/pct,
tools/mn_controlled) or the CONFIG (tools/combinatorial) over a mostly-fixed
workload, or hunt HANGS (tools/hang_hunter), this one mass-produces
structurally-DIVERSE programs that exercise the OBJECT-LIFE-CYCLE operations the
verify/ models specify -- channel ref churn, varied-stack goroutines, nested
spawn (snap/migration), timed parks, select+close races, undrained buffered
channels -- and runs each under the LIFE-CYCLE ORACLES those models point at:

  * token conservation         (every value sent is received exactly once)
  * goroutine completion        (mn_run's completed count == goroutines spawned)
  * parked-leak                 (sleeping / netpoll-parked drain to 0 after run)
  * scheduler self-check        (runloom_c._self_check)
  * the runtime DBG oracles     (RUNLOOM_DBG_GSTATE freed-state, RUNLOOM_DBG_MIGRATE)
  * a hang watchdog             (a lost wakeup becomes a TimeoutError, not a wedge)
  * ASan/TSan                   (if the ext was built with a sanitizer)

It reuses the proven conservation kernel from tools/mn_stress.py and COMPOSES
with the existing replay levers rather than duplicating them: each program is
`f(seed)`, the schedule is pinned by RUNLOOM_MN_SEED, so a finding reduces to a
single (seed, env) one-liner that replays the exact execution.  Always-
terminating by construction, so a hang is a real bug.

The design rationale + a map of which knob targets which verify/ model lives in
tools/lifefuzz/README.md.

CLI (house style: .format(), no f-strings):
  lifefuzz.py gen   SEED                 # print the generated program spec (JSON)
  lifefuzz.py run   SEED [--mn-seed S]   # run ONE program in-process (verbose)
  lifefuzz.py worker SEED MNSEED TIMEOUT # one-shot subprocess entry (sweep uses this)
  lifefuzz.py sweep [N] [--workers W] [--seed0 K] [--timeout T] [--mn-seed S]
  lifefuzz.py repro SEED [--mn-seed S]   # verbose single run with full env dump
  lifefuzz.py shrink SEED [--mn-seed S]  # delta-debug the spec to a minimal repro
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

# Stack sizes the depot allocator must juggle: tiny (forces grow / small-class),
# the default, and large pins (different size-class -> the size-mismatch reuse
# path that must fall through to munmap, model #1 stack_depot).
STACK_CHOICES = [None, 16 * 1024, 32 * 1024, 128 * 1024, 512 * 1024]
CHAN_CAPS = [0, 0, 1, 2, 8]           # 0 == unbuffered handoff (the contended path)

# Findings keywords the parent scans a worker's stderr for (beyond a nonzero exit).
FINDING_PATTERNS = (
    "[RUNLOOM_DBG", "AddressSanitizer", "ThreadSanitizer", "runtime error:",
    "Assertion", "self_check", "SELF_CHECK", "MISMATCH", "LEAK", "Traceback",
    "Fatal Python error", "Segmentation",
)


# --------------------------------------------------------------------------- #
#  Spec generation: a program is a pure function of its seed.                  #
# --------------------------------------------------------------------------- #
def build_spec(seed):
    """Deterministically derive a program spec from an integer seed."""
    rng = random.Random(seed)
    mode = rng.choice(["mn", "mn", "st"])           # bias toward the M:N path
    nchan = rng.randint(1, 5)
    ncons = rng.randint(1, 6)
    # Every channel needs >=1 range-consumer covering it or its tokens are never
    # drained; cap nchan at ncons (mirrors mn_stress 'stable' coverage rule).
    nchan = min(nchan, ncons)
    nprod = rng.randint(1, 8)
    spec = {
        "seed": seed,
        "mode": mode,
        "nhubs": rng.choice([2, 3, 4]) if mode == "mn" else 1,
        "nchan": nchan,
        "caps": [rng.choice(CHAN_CAPS) for _ in range(nchan)],
        "nprod": nprod,
        "per_prod": rng.randint(3, 30),
        "ncons": ncons,
        # consumer styles: range (for v in ch) drains one channel; select drains
        # across all (exercises the select+close lifecycle / Finding A class).
        "cons_select": [rng.random() < 0.4 for _ in range(ncons)],
        # per-goroutine pinned stack size -> stack-depot push/pop/flush diversity.
        "prod_stacks": [rng.choice(STACK_CHOICES) for _ in range(nprod)],
        "cons_stacks": [rng.choice(STACK_CHOICES) for _ in range(ncons)],
        # nested child goroutines spawned from inside a producer (snap/migration
        # under M:N + more stack-depot traffic).
        "nest": rng.randint(0, 3),
        # timed parks between sends -> deadline heap + park/wake + the freed-state
        # timer oracle.  Kept tiny so the program still terminates promptly.
        "timer_us": rng.choice([0, 0, 50, 200, 800]),
        # scratch buffered channels: filled with PyObjects then DROPPED undrained
        # -> Chan dealloc must release the buffered refs (model #8 chan_refflow
        # FREE_NO_BUFFER_DRAIN).
        "scratch": rng.randint(0, 4),
        "yield_mask": rng.choice([0, 1, 3, 7]),     # sched_yield every (n & mask)==0
    }
    return spec


def spawned_count(spec):
    """Total goroutines a spec spawns (for the completion oracle)."""
    n = spec["nprod"] + spec["ncons"] + 1           # + closer
    n += spec["nprod"] * spec["nest"]               # nested children
    n += spec["scratch"]                            # scratch-channel goroutines
    return n


def sent_checksum(spec):
    """(count, sum) of the conserved token multiset -- known a priori."""
    count = spec["nprod"] * spec["per_prod"]
    total = 0
    for pid in range(spec["nprod"]):
        for seq in range(spec["per_prod"]):
            total += pid * 1000 + seq
    return count, total


# --------------------------------------------------------------------------- #
#  Program execution: build the goroutine graph from the spec and run it.      #
# --------------------------------------------------------------------------- #
def run_program(spec, timeout=20.0):
    """Build + run the program in-process, check every oracle.

    Returns (ok, reason).  ok=False reason is a short finding tag.  Raises
    TimeoutError (via the watchdog) on a hang."""
    import runloom_c
    from tools.watchdog import run_guarded

    mode = spec["mode"]
    nchan = spec["nchan"]
    caps = spec["caps"]
    nprod = spec["nprod"]
    ncons = spec["ncons"]
    per_prod = spec["per_prod"]
    nest = spec["nest"]
    timer_s = spec["timer_us"] / 1e6
    yield_mask = spec["yield_mask"]

    def spawn(fn, stack):
        # M:N and single-thread spawn, with an optional pinned stack size.
        # (stack_size must be omitted, not passed None, when unset.)
        gofn = runloom_c.mn_go if mode == "mn" else runloom_c.go
        if stack is None:
            return gofn(fn)
        try:
            return gofn(fn, stack_size=stack)
        except TypeError:
            return gofn(fn)

    def work_body(fn):
        return run_guarded(fn, seconds=timeout, label="lifefuzz seed={0}".format(spec["seed"]))

    def driver():
        # the whole M:N lifecycle (init/run/fini) must run on ONE thread -- here,
        # the watchdog's guarded worker thread (mirrors tools/mn_stress.py).
        if mode == "mn":
            runloom_c.mn_init(spec["nhubs"])
        chans = [runloom_c.Chan(caps[i]) for i in range(nchan)]
        prod_done = runloom_c.Chan(nprod)
        results = runloom_c.Chan(ncons)

        def producer(pid):
            def run():
                # optional nested children: pure stack/migration stress, no tokens
                for k in range(nest):
                    def child():
                        runloom_c.sched_yield()
                        return None
                    spawn(child, spec["prod_stacks"][pid])
                for seq in range(per_prod):
                    token = pid * 1000 + seq
                    ch = chans[(pid + seq) % nchan]
                    if timer_s and (seq % 4 == 0):
                        runloom_c.sched_sleep(timer_s)
                    ch.send(token)
                    if yield_mask and (seq & yield_mask) == 0:
                        runloom_c.sched_yield()
                prod_done.send(pid)
            return run

        def closer():
            for _ in range(nprod):
                prod_done.recv()
            for ch in chans:
                ch.close()

        def consumer_range(cid):
            def run():
                ch = chans[cid % nchan]
                count = 0
                total = 0
                for v in ch:
                    count += 1
                    total += v
                results.send((count, total))
            return run

        def consumer_select(cid):
            def run():
                count = 0
                total = 0
                closed = [False] * nchan
                while not all(closed):
                    cases = [("recv", chans[i]) for i in range(nchan) if not closed[i]]
                    if not cases:
                        break
                    idx, (val, ok) = runloom_c.select(cases)
                    live = [i for i in range(nchan) if not closed[i]]
                    ci = live[idx]
                    if ok:
                        count += 1
                        total += val
                    else:
                        closed[ci] = True
                results.send((count, total))
            return run

        def scratch_churn(sid):
            # Create a buffered channel, fill it with PyObjects, DROP it undrained
            # -> Chan dealloc must release the buffered refs (model #8).
            def run():
                sc = runloom_c.Chan(4)
                for j in range(3):
                    sc.try_send(("scratch", sid, j))
                # no drain, no close: let sc go out of scope -> dealloc path
                return None
            return run

        # spawn consumers, producers, scratch, closer
        for cid in range(ncons):
            if spec["cons_select"][cid] and nchan > 0:
                spawn(consumer_select(cid), spec["cons_stacks"][cid])
            else:
                spawn(consumer_range(cid), spec["cons_stacks"][cid])
        for sid in range(spec["scratch"]):
            spawn(scratch_churn(sid), None)
        for pid in range(nprod):
            spawn(producer(pid), spec["prod_stacks"][pid])
        spawn(closer, None)

        # run + capture the completion count
        if mode == "mn":
            completed = runloom_c.mn_run()
        else:
            runloom_c.run()
            completed = None

        # parked-leak snapshot BEFORE teardown (all gs done -> nothing parked)
        st = runloom_c.stats()

        recv_count = 0
        recv_sum = 0
        drained = 0
        while drained < ncons:
            got = results.try_recv()
            if got is None:
                break
            (c, s), ok = got
            if not ok:
                break
            recv_count += c
            recv_sum += s
            drained += 1
        if mode == "mn":
            runloom_c.mn_fini()
        return completed, st, recv_count, recv_sum, drained

    # --- run the whole program under the hang watchdog ---
    completed, st, recv_count, recv_sum, drained = work_body(driver)

    # --- oracles ---
    sc_count, sc_sum = sent_checksum(spec)
    if (recv_count, recv_sum) != (sc_count, sc_sum):
        return False, ("CONSERVATION sent=({0},{1}) recv=({2},{3}) drained={4}/{5}"
                       .format(sc_count, sc_sum, recv_count, recv_sum, drained, ncons))
    if completed is not None and completed != spawned_count(spec):
        return False, ("COMPLETION completed={0} spawned={1}"
                       .format(completed, spawned_count(spec)))
    parked = st.get("sleeping", 0) + st.get("netpoll_parked", 0) + st.get("running", 0)
    if parked != 0:
        return False, ("PARKED_LEAK sleeping={0} netpoll_parked={1} running={2}"
                       .format(st.get("sleeping"), st.get("netpoll_parked"), st.get("running")))
    v = runloom_c._self_check(0)
    if v != 0:
        runloom_c._self_check(1)
        return False, "SELF_CHECK violations={0}".format(v)
    return True, "ok"


# --------------------------------------------------------------------------- #
#  Subprocess worker + parent-side sweep / repro / shrink.                     #
# --------------------------------------------------------------------------- #
# The default-safe scheduler config knobs (from tools/combinatorial/covering.py).
# Folding them into the per-seed env makes every run a distinct point in
# workload x schedule x CONFIG space -- where interaction bugs hide -- and stays
# replayable because the choice is a pure function of the seed.
KNOB_FACTORS = (
    ("RUNLOOM_NETPOLL", ["epoll", "select", "io_uring"]),
    ("RUNLOOM_HANDOFF", ["0", "1"]),
    ("RUNLOOM_PREEMPT", ["0", "1"]),
    ("RUNLOOM_SYSMON",  ["0", "1"]),
)


def knobs_for_seed(seed):
    rng = random.Random((seed << 1) ^ 0x5EED)
    return {name: rng.choice(vals) for name, vals in KNOB_FACTORS}


def worker_env(seed, mn_seed, knobs=True, unsafe_migrate=False, extra=None):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src") + os.pathsep + env.get("PYTHONPATH", "")
    env["RUNLOOM_DEBUG"] = "ring,gstate"        # flight recorder for crash dumps
    env["RUNLOOM_DBG_GSTATE"] = "1"             # freed-state timer oracle
    if mn_seed is not None:
        env["RUNLOOM_MN_SEED"] = str(mn_seed)   # deterministic baton -> replay
    if knobs:
        env.update(knobs_for_seed(seed))
    if unsafe_migrate:
        # Teeth check: actually ENABLE the gated per-g-tstate migration so the
        # known mimalloc hazard manifests and the oracle (or a crash) is caught.
        env["RUNLOOM_PER_G_TSTATE"] = "1"
        env["RUNLOOM_ALLOW_UNSAFE_MIGRATION"] = "1"
        env["RUNLOOM_DBG_MIGRATE"] = "1"
    if extra:
        env.update(extra)
    return env


def run_worker_subprocess(seed, mn_seed, timeout, unsafe_migrate=False, spec_file=None):
    """Run one program as an isolated subprocess.  Returns a finding dict or None."""
    py = sys.executable
    argv = [py, os.path.abspath(__file__), "worker", str(seed),
            str(mn_seed if mn_seed is not None else -1), str(timeout)]
    if spec_file:
        argv += ["--spec-file", spec_file]
    env = worker_env(seed, mn_seed, unsafe_migrate=unsafe_migrate)
    try:
        p = subprocess.run(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           timeout=timeout + 10)
    except subprocess.TimeoutExpired as e:
        out = (e.output or b"").decode("utf-8", "replace")
        return {"seed": seed, "mn_seed": mn_seed, "signal": "HANG",
                "rc": None, "tail": out[-2000:]}
    out = p.stdout.decode("utf-8", "replace")
    bad = p.returncode != 0 or "LIFEFUZZ_OK" not in out
    if not bad:
        for pat in FINDING_PATTERNS:
            if pat in out:
                bad = True
                break
    if bad:
        sig = "CRASH" if p.returncode and p.returncode < 0 else "FAIL"
        return {"seed": seed, "mn_seed": mn_seed, "signal": sig,
                "rc": p.returncode, "tail": out[-2000:]}
    return None


def worker_main(seed, mn_seed, timeout, spec_file=None):
    """One-shot worker: build the spec, run it, print LIFEFUZZ_OK or fail loudly."""
    if spec_file:
        with open(spec_file) as f:
            spec = json.load(f)
    else:
        spec = build_spec(seed)
    ok, reason = run_program(spec, timeout=timeout)
    if ok:
        print("LIFEFUZZ_OK seed={0}".format(seed))
        return 0
    print("LIFEFUZZ_FAIL seed={0} reason={1}".format(seed, reason))
    print("MISMATCH" if reason.startswith("CONSERVATION") else reason)
    return 1


def sweep(n, workers, seed0, timeout, mn_seed, unsafe_migrate=False):
    import concurrent.futures
    corpus = os.path.join(HERE, "corpus")
    os.makedirs(corpus, exist_ok=True)
    print("lifefuzz sweep: seeds [{0},{1}) workers={2} timeout={3}s mn_seed={4} unsafe_migrate={5}"
          .format(seed0, seed0 + n, workers, timeout, mn_seed, unsafe_migrate))
    findings = []
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(run_worker_subprocess, seed0 + i,
                          (mn_seed + i) if mn_seed is not None else None,
                          timeout, unsafe_migrate): seed0 + i for i in range(n)}
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            f = fut.result()
            if f is not None:
                findings.append(f)
                path = os.path.join(corpus, "seed_{0}.json".format(f["seed"]))
                with open(path, "w") as out:
                    json.dump(f, out, indent=2)
                print("\n  !! FINDING seed={0} signal={1} rc={2}  -> {3}"
                      .format(f["seed"], f["signal"], f["rc"], path))
                print("     repro: tools/lifefuzz/lifefuzz.py repro {0}{1}"
                      .format(f["seed"], "" if mn_seed is None else
                              " --mn-seed {0}".format(f["mn_seed"])))
            if done % 25 == 0 or done == n:
                sys.stdout.write("\r  progress {0}/{1}  findings={2}   "
                                 .format(done, n, len(findings)))
                sys.stdout.flush()
    print("\nsweep done: {0} runs, {1} findings".format(n, len(findings)))
    return 1 if findings else 0


def shrink(seed, mn_seed, timeout):
    """Delta-debug the spec to a minimal still-failing program."""
    spec = build_spec(seed)
    spec_path = os.path.join(HERE, "shrink_{0}.json".format(seed))

    def fails(s):
        with open(spec_path, "w") as f:
            json.dump(s, f)
        res = run_worker_subprocess(seed, mn_seed, timeout, spec_file=spec_path)
        return res is not None

    if not fails(spec):
        print("shrink: seed {0} does NOT fail as-is -- nothing to shrink".format(seed))
        return 1
    # Coarse category/count reductions, each kept only if it still fails.
    reductions = [
        ("nest", 0), ("scratch", 0), ("timer_us", 0), ("yield_mask", 0),
        ("ncons", 1), ("nprod", 1), ("per_prod", 1), ("nchan", 1),
    ]
    cur = dict(spec)
    for key, lo in reductions:
        if key not in cur:
            continue
        old = cur[key]
        if key in ("cons_select", "prod_stacks", "cons_stacks", "caps"):
            continue
        if isinstance(old, int) and old > lo:
            trial = dict(cur)
            trial[key] = lo
            # keep dependent lists consistent
            trial = build_consistent(trial)
            if fails(trial):
                cur = trial
                print("  shrunk {0}: {1} -> {2}".format(key, old, lo))
    print("\nminimal failing spec:")
    print(json.dumps(cur, indent=2))
    return 0


def build_consistent(spec):
    """After mutating counts, resize the dependent per-goroutine lists."""
    nprod, ncons, nchan = spec["nprod"], spec["ncons"], spec["nchan"]
    spec["nchan"] = min(nchan, ncons)
    nchan = spec["nchan"]
    spec["caps"] = (spec["caps"] + CHAN_CAPS)[:nchan] if nchan else [0]
    spec["cons_select"] = (spec["cons_select"] + [False] * ncons)[:ncons]
    spec["prod_stacks"] = (spec["prod_stacks"] + [None] * nprod)[:nprod]
    spec["cons_stacks"] = (spec["cons_stacks"] + [None] * ncons)[:ncons]
    return spec


# --------------------------------------------------------------------------- #
def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd")

    g = sub.add_parser("gen"); g.add_argument("seed", type=int)
    r = sub.add_parser("run"); r.add_argument("seed", type=int)
    r.add_argument("--timeout", type=float, default=20.0)

    w = sub.add_parser("worker")
    w.add_argument("seed", type=int); w.add_argument("mn_seed", type=int)
    w.add_argument("timeout", type=float); w.add_argument("--spec-file", default=None)

    s = sub.add_parser("sweep")
    s.add_argument("n", type=int, nargs="?", default=500)
    s.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 2))
    s.add_argument("--seed0", type=int, default=1)
    s.add_argument("--timeout", type=float, default=20.0)
    s.add_argument("--mn-seed", type=int, default=1)
    s.add_argument("--unsafe-migrate", action="store_true")

    rp = sub.add_parser("repro"); rp.add_argument("seed", type=int)
    rp.add_argument("--mn-seed", type=int, default=None)
    rp.add_argument("--timeout", type=float, default=20.0)

    sh = sub.add_parser("shrink"); sh.add_argument("seed", type=int)
    sh.add_argument("--mn-seed", type=int, default=None)
    sh.add_argument("--timeout", type=float, default=20.0)

    args = p.parse_args(argv)

    if args.cmd == "gen":
        print(json.dumps(build_spec(args.seed), indent=2)); return 0
    if args.cmd == "run":
        spec = build_spec(args.seed)
        ok, reason = run_program(spec, timeout=args.timeout)
        print("seed={0} -> {1} ({2})".format(args.seed, "OK" if ok else "FAIL", reason))
        return 0 if ok else 1
    if args.cmd == "worker":
        ms = None if args.mn_seed < 0 else args.mn_seed
        return worker_main(args.seed, ms, args.timeout, spec_file=args.spec_file)
    if args.cmd == "sweep":
        return sweep(args.n, args.workers, args.seed0, args.timeout,
                     args.mn_seed, unsafe_migrate=args.unsafe_migrate)
    if args.cmd == "repro":
        f = run_worker_subprocess(args.seed, args.mn_seed, args.timeout)
        if f is None:
            print("seed {0} ran CLEAN (no finding reproduced)".format(args.seed)); return 0
        print("FINDING reproduced:\n" + json.dumps(f, indent=2)); return 1
    if args.cmd == "shrink":
        return shrink(args.seed, args.mn_seed, args.timeout)
    p.print_help(); return 2


if __name__ == "__main__":
    sys.exit(main())
