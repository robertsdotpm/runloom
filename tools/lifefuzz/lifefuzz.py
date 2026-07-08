"""lifefuzz -- a generative, seed-replayable LIFE-CYCLE fuzzer for runloom.

Where the existing fuzzers vary the SCHEDULE (tools/dst, tools/pct,
tools/mn_controlled) or the CONFIG (tools/combinatorial) over a mostly-fixed
workload, or hunt HANGS (tools/hang_hunter), this one mass-produces
structurally-DIVERSE programs that exercise the OBJECT-LIFE-CYCLE operations the
tools/verify/ models specify -- channel ref churn, varied-stack goroutines, nested
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

The design rationale + a map of which knob targets which tools/verify/ model lives in
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
    """Deterministically derive a program spec from an integer seed.

    Program KINDS:
      core    -- runloom_c goroutines + channels + select + timers (the default).
      aio     -- a small asyncio program under runloom.aio (queue + create_task +
                 cancel + call_later + run_in_executor) -- reaches the timer-leak,
                 task-cancel, and blockpool-job seams the core path can't.
      grammar -- a syzlang-style RESOURCE-TYPED op sequence (LIFEFUZZ_KIND=grammar);
                 see build_grammar_spec.
    A `scale` draw (~12%) inflates the core counts to stress the stack-depot /
    fiber-admission at scale (model #1 / #7)."""
    forced = os.environ.get("LIFEFUZZ_KIND")
    if forced == "grammar":
        return build_grammar_spec(seed)
    if forced == "sim":
        return {"seed": seed, "kind": "sim"}
    # ~20% grammar + ~10% sim-network, each via a SEPARATE rng stream so the other
    # seeds keep their EXACT original core/aio program (main stream undisturbed --
    # the fleet's known corpus stays valid).  So the existing `lifefuzz.py run
    # <seed>` fleet (rr chaos, millions of seeds) soaks grammar AND the
    # deterministic sim network too -- no separate forever-wrapper.  A sim run is
    # single-thread + logical-clock deterministic, so its lost-wake oracle is the
    # INSTANT count_deadlocked check, not a wall-clock timeout.
    if random.Random(seed ^ 0x9E3779B97F4A7C15).random() < 0.20:
        return build_grammar_spec(seed)
    if random.Random(seed ^ 0x2545F4914F6CDD1D).random() < 0.10:
        return {"seed": seed, "kind": "sim"}
    rng = random.Random(seed)
    kind = rng.choice(["core", "core", "core", "aio"])   # ~25% aio
    scale = (rng.random() < 0.12)
    mode = rng.choice(["mn", "mn", "st"])           # bias toward the M:N path
    if scale and kind == "core":
        nprod = rng.randint(20, 60)
        per_prod = rng.randint(80, 350)
        ncons = rng.randint(8, 24)
        nest = rng.randint(0, 5)
    else:
        nprod = rng.randint(1, 8)
        per_prod = rng.randint(3, 30)
        ncons = rng.randint(1, 6)
        nest = rng.randint(0, 3)
    # Every channel needs >=1 range-consumer covering it or its tokens are never
    # drained; cap nchan at ncons (mirrors mn_stress 'stable' coverage rule).
    nchan = min(rng.randint(1, 6 if scale else 5), ncons)
    spec = {
        "seed": seed,
        "kind": kind,
        "scale": scale,
        "mode": mode,
        "nhubs": rng.choice([2, 3, 4, 6, 8] if scale else [2, 3, 4]) if mode == "mn" else 1,
        "nchan": nchan,
        "caps": [rng.choice(CHAN_CAPS) for _ in range(nchan)],
        "nprod": nprod,
        "per_prod": per_prod,
        "ncons": ncons,
        # consumer styles: range (for v in ch) drains one channel; select drains
        # across all (exercises the select+close lifecycle / Finding A class).
        "cons_select": [rng.random() < 0.4 for _ in range(ncons)],
        # per-goroutine pinned stack size -> stack-depot push/pop/flush diversity.
        "prod_stacks": [rng.choice(STACK_CHOICES) for _ in range(nprod)],
        "cons_stacks": [rng.choice(STACK_CHOICES) for _ in range(ncons)],
        # nested child goroutines spawned from inside a producer (snap/migration
        # under M:N + more stack-depot traffic).
        "nest": nest,
        # timed parks between sends -> deadline heap + park/wake + the freed-state
        # timer oracle.  Kept tiny so the program still terminates promptly.
        "timer_us": rng.choice([0, 0, 50, 200, 800]),
        # scratch buffered channels: filled with PyObjects then DROPPED undrained
        # -> Chan dealloc must release the buffered refs (model #8 chan_refflow
        # FREE_NO_BUFFER_DRAIN).
        "scratch": rng.randint(0, 4),
        "yield_mask": rng.choice([0, 1, 3, 7]),     # sched_yield every (n & mask)==0
        # --- aio-bridge program fields (used when kind == "aio") ---
        "aio_prod": rng.randint(1, 6),
        "aio_per": rng.randint(2, 20) * (8 if scale else 1),
        "aio_decoys": rng.randint(0, 4),            # tasks cancelled mid-flight
        "aio_timers": rng.randint(0, 4),            # call_later, all cancelled (leak seam)
        "aio_executor": (rng.random() < 0.5),       # run_in_executor (blockpool job)
        "aio_sleep_us": rng.choice([0, 0, 50, 200]),
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
    if spec.get("kind") == "grammar":
        return spec["exp_count"], spec["exp_sum"]
    count = spec["nprod"] * spec["per_prod"]
    total = 0
    for pid in range(spec["nprod"]):
        for seq in range(spec["per_prod"]):
            total += pid * 1000 + seq
    return count, total


def build_grammar_spec(seed):
    """A syzlang-style RESOURCE-TYPED op sequence (QA-steal-V2 #17): each op
    references handles produced by EARLIER ops (a chan -> its producers, draining
    consumers, and close), so the program STRUCTURE itself varies with the seed --
    unlike build_spec, whose producer/consumer/select graph shape is fixed and
    only the counts vary.

    Well-formed BY CONSTRUCTION so the strong oracles keep their teeth: every
    channel gets >=1 draining consumer and is closed after its producers, so the
    program terminates, and the exact conserved token multiset is known -- tracked
    here (exp_count/exp_sum) as the op list is generated -- rather than relaxing
    the conservation oracle for a free grammar.  Resource types: Chan (channels)
    and G (goroutines: producers, consumers, nested children, scratch, closer);
    Fd/socketpair is the netpoll follow-up.  Pure function of seed -> findings
    replay via the existing repro/shrink path (with LIFEFUZZ_KIND=grammar set)."""
    rng = random.Random((seed ^ 0x6C696665677AABCD) & 0xFFFFFFFFFFFFFFFF)
    mode = rng.choice(["mn", "mn", "st"])
    nchan = rng.randint(1, 5)
    ops = [{"t": "chan", "id": c, "cap": rng.choice(CHAN_CAPS)} for c in range(nchan)]

    exp_count = exp_sum = exp_sumsq = nspawned = tok = 0
    # producers: each sends a run of UNIQUE tokens to one chosen chan, plus
    # optional nested children (stack stress).  The delivered multiset is
    # fingerprinted by (count, sum, sum-of-squares): count+sum alone cannot tell
    # {0,1,2,3} from a buffer index-bug delivering {0,0,3,3} (both count 4 sum 6),
    # but sumsq 14 vs 18 does -- teeth for the reorder/dup-drop class the grammar
    # actually reaches through the Chan buffer.
    for _ in range(rng.randint(1, 6)):
        cid = rng.randrange(nchan)
        n = rng.randint(1, 20)
        base = tok
        tok += n
        exp_count += n
        exp_sum += sum(range(base, base + n))
        exp_sumsq += sum(v * v for v in range(base, base + n))
        nest = rng.randint(0, 2)
        ops.append({"t": "producer", "chan": cid, "base": base, "n": n,
                    "stack": rng.choice(STACK_CHOICES), "nest": nest})
        nspawned += 1 + nest
    # consumers: range (drains one chan) or select (drains a subset).  Track
    # coverage so every chan is guaranteed a drainer (else its tokens strand).
    covered = set()
    for _ in range(rng.randint(1, 5)):
        if rng.random() < 0.4 and nchan >= 2:
            chans = sorted(rng.sample(range(nchan), rng.randint(2, nchan)))
            ops.append({"t": "select_cons", "chans": chans,
                        "stack": rng.choice(STACK_CHOICES)})
            covered.update(chans)
        else:
            cid = rng.randrange(nchan)
            ops.append({"t": "range_cons", "chan": cid,
                        "stack": rng.choice(STACK_CHOICES)})
            covered.add(cid)
        nspawned += 1
    for cid in range(nchan):
        if cid not in covered:
            ops.append({"t": "range_cons", "chan": cid, "stack": None})
            nspawned += 1
    # scratch: fill a buffered chan with PyObjects then DROP it undrained -> Chan
    # dealloc must release the buffered refs (model #8 chan_refflow).
    for _ in range(rng.randint(0, 3)):
        ops.append({"t": "scratch"})
        nspawned += 1
    nspawned += 1                                   # the closer
    nprod = sum(1 for o in ops if o["t"] == "producer")
    return {
        "seed": seed, "kind": "grammar", "mode": mode,
        "nhubs": rng.choice([2, 3, 4]) if mode == "mn" else 1,
        "nchan": nchan, "nprod": nprod, "ops": ops,
        "exp_count": exp_count, "exp_sum": exp_sum, "exp_sumsq": exp_sumsq,
        "exp_spawned": nspawned,
        "timer_us": rng.choice([0, 0, 50, 200]),
        "yield_mask": rng.choice([0, 1, 3, 7]),
    }


# --------------------------------------------------------------------------- #
#  Program execution: build the goroutine graph from the spec and run it.      #
# --------------------------------------------------------------------------- #
def run_program(spec, timeout=20.0):
    """Build + run the program in-process, check every oracle.

    Returns (ok, reason).  ok=False reason is a short finding tag.  Raises
    TimeoutError (via the watchdog) on a hang."""
    import runloom_c
    from tools.watchdog import run_guarded

    if spec.get("kind") == "aio":
        return run_aio_program(spec, timeout=timeout)
    if spec.get("kind") == "grammar":
        return run_grammar_program(spec, timeout=timeout)
    if spec.get("kind") == "sim":
        sys.path.insert(0, os.path.join(ROOT, "tools", "dst"))
        import simnet                             # sets RUNLOOM_LOGICAL_CLOCK on import
        return simnet.sim_program(spec["seed"], timeout=timeout)

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
        gofn = runloom_c.mn_fiber if mode == "mn" else runloom_c.fiber
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


def run_aio_program(spec, timeout=20.0):
    """Build + run a small asyncio program under runloom.aio, checked against the
    same life-cycle oracles.  Reaches the seams the core path can't: call_later +
    cancel (timer-leak), task cancel mid-flight (task lifecycle / cancel-of-wait_fd),
    and run_in_executor (the blockpool stack-job, model #3).  Always-terminating:
    producers put a known token multiset on an asyncio.Queue, a consumer drains
    exactly that many; decoy tasks + timers are cancelled and carry no tokens."""
    import asyncio
    import runloom.aio as paio
    import runloom_c
    from tools.watchdog import run_guarded

    P = spec["aio_prod"]
    N = spec["aio_per"]
    sleep_s = spec["aio_sleep_us"] / 1e6
    expected_count = P * N
    expected_sum = sum(p * 100000 + i for p in range(P) for i in range(N))

    async def main():
        q = asyncio.Queue()
        loop = asyncio.get_event_loop()

        async def producer(pid):
            for i in range(N):
                if sleep_s:
                    await asyncio.sleep(sleep_s)
                await q.put(pid * 100000 + i)

        async def decoy():
            # cancelled mid-flight: exercises task teardown + cancel-of-a-parked-wait
            await asyncio.sleep(1000)

        prods = [asyncio.create_task(producer(p)) for p in range(P)]
        decoys = [asyncio.create_task(decoy()) for _ in range(spec["aio_decoys"])]
        # call_later timers, all cancelled before firing -> the timer-leak seam
        # (a cancelled timer's goroutine must hold no ref to its callback graph).
        for _ in range(spec["aio_timers"]):
            loop.call_later(1000, lambda: None).cancel()

        got = []
        while len(got) < expected_count:
            got.append(await q.get())
        for t in prods:
            await t
        for d in decoys:
            d.cancel()
        for d in decoys:
            try:
                await d
            except asyncio.CancelledError:
                pass
        executor_ok = True
        if spec["aio_executor"]:
            r = await loop.run_in_executor(None, lambda: sum(range(2000)))
            executor_ok = (r == sum(range(2000)))
        return sum(got), len(got), executor_ok

    def work():
        res = paio.run(main())
        # snapshot oracles on THIS (worker) thread, where the loop's sched lives
        return res, dict(runloom_c.stats()), runloom_c._self_check(0)

    (got_sum, got_count, executor_ok), st, sc = run_guarded(
        work, seconds=timeout, label="lifefuzz-aio seed={0}".format(spec["seed"]))

    if (got_count, got_sum) != (expected_count, expected_sum):
        return False, ("AIO_CONSERVATION expected=({0},{1}) got=({2},{3})"
                       .format(expected_count, expected_sum, got_count, got_sum))
    if not executor_ok:
        return False, "AIO_EXECUTOR wrong result"
    parked = st.get("sleeping", 0) + st.get("netpoll_parked", 0)
    if parked != 0:
        return False, ("AIO_PARKED_LEAK sleeping={0} netpoll_parked={1}"
                       .format(st.get("sleeping"), st.get("netpoll_parked")))
    if sc != 0:
        return False, "AIO_SELF_CHECK violations={0}".format(sc)
    return True, "ok"


def run_grammar_program(spec, timeout=20.0):
    """Interpret a resource-typed op list (build_grammar_spec) into a real
    runloom goroutine graph and check the SAME life-cycle oracles as run_program:
    exact token conservation (against the generator-tracked exp_count/exp_sum),
    completion, parked-leak, self_check.  A closer waits for every producer then
    closes every channel, so all range/select consumers terminate."""
    import runloom_c
    from tools.watchdog import run_guarded

    mode = spec["mode"]
    nchan = spec["nchan"]
    ops = spec["ops"]
    nprod = spec["nprod"]
    timer_s = spec.get("timer_us", 0) / 1e6
    yield_mask = spec.get("yield_mask", 0)

    def spawn(fn, stack):
        gofn = runloom_c.mn_fiber if mode == "mn" else runloom_c.fiber
        if stack is None:
            return gofn(fn)
        try:
            return gofn(fn, stack_size=stack)
        except TypeError:
            return gofn(fn)

    def driver():
        if mode == "mn":
            runloom_c.mn_init(spec["nhubs"])
        chans = [None] * nchan
        for o in ops:
            if o["t"] == "chan":
                chans[o["id"]] = runloom_c.Chan(o["cap"])
        ncons = sum(1 for o in ops if o["t"] in ("range_cons", "select_cons"))
        prod_done = runloom_c.Chan(max(1, nprod))
        results = runloom_c.Chan(max(1, ncons))

        def make_producer(o):
            def run():
                for _ in range(o["nest"]):
                    def child():
                        runloom_c.sched_yield()
                        return None
                    spawn(child, o["stack"])
                ch = chans[o["chan"]]
                for i in range(o["n"]):
                    if timer_s and (i % 4 == 0):
                        runloom_c.sched_sleep(timer_s)
                    ch.send(o["base"] + i)
                    if yield_mask and (i & yield_mask) == 0:
                        runloom_c.sched_yield()
                prod_done.send(1)
            return run

        def make_range(o):
            def run():
                cnt = tot = tot2 = 0
                for v in chans[o["chan"]]:
                    cnt += 1
                    tot += v
                    tot2 += v * v
                results.send((cnt, tot, tot2))
            return run

        def make_select(o):
            cids = list(o["chans"])

            def run():
                cnt = tot = tot2 = 0
                closed = dict((c, False) for c in cids)
                while not all(closed.values()):
                    live = [c for c in cids if not closed[c]]
                    if not live:
                        break
                    idx, (val, ok) = runloom_c.select([("recv", chans[c]) for c in live])
                    c = live[idx]
                    if ok:
                        cnt += 1
                        tot += val
                        tot2 += val * val
                    else:
                        closed[c] = True
                results.send((cnt, tot, tot2))
            return run

        def make_scratch():
            # Fill a buffered Chan then DROP it undrained, driving the Chan-dealloc
            # buffered-ref release path (model #8 chan_refflow).  NOTE: the ref leak
            # itself is only caught under an ASan/LSan build; the default oracle net
            # (scheduler self_check) does not see PyObject refcounts -- a dedicated
            # refcount-delta oracle is a follow-up.
            def run():
                sc = runloom_c.Chan(4)
                for j in range(3):
                    sc.try_send(("scratch", j))
                return None
            return run

        def closer():
            for _ in range(nprod):
                prod_done.recv()
            for ch in chans:
                if ch is not None:
                    ch.close()

        for o in ops:                                   # consumers first
            if o["t"] == "range_cons":
                spawn(make_range(o), o["stack"])
            elif o["t"] == "select_cons":
                spawn(make_select(o), o["stack"])
        for o in ops:                                   # then producers
            if o["t"] == "producer":
                spawn(make_producer(o), o["stack"])
        for o in ops:                                   # scratch churn
            if o["t"] == "scratch":
                spawn(make_scratch(), None)
        spawn(closer, None)

        if mode == "mn":
            completed = runloom_c.mn_run()
        else:
            runloom_c.run()
            completed = None
        st = runloom_c.stats()
        recv_count = recv_sum = recv_sumsq = drained = 0
        while drained < ncons:
            got = results.try_recv()
            if got is None:
                break
            (c, s, s2), ok = got
            if not ok:
                break
            recv_count += c
            recv_sum += s
            recv_sumsq += s2
            drained += 1
        if mode == "mn":
            runloom_c.mn_fini()
        return completed, st, recv_count, recv_sum, recv_sumsq, drained, ncons

    completed, st, recv_count, recv_sum, recv_sumsq, drained, ncons = run_guarded(
        driver, seconds=timeout, label="lifefuzz-grammar seed={0}".format(spec["seed"]))

    # --- the same oracle net as run_program, plus a sum-of-squares check that
    # catches count+sum-preserving multiset corruption (reorder/dup-drop) ---
    if (recv_count, recv_sum, recv_sumsq) != (spec["exp_count"], spec["exp_sum"], spec["exp_sumsq"]):
        return False, ("CONSERVATION sent=({0},{1},{2}) recv=({3},{4},{5}) drained={6}/{7}"
                       .format(spec["exp_count"], spec["exp_sum"], spec["exp_sumsq"],
                               recv_count, recv_sum, recv_sumsq, drained, ncons))
    if completed is not None and completed != spec["exp_spawned"]:
        return False, ("COMPLETION completed={0} spawned={1}"
                       .format(completed, spec["exp_spawned"]))
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
    # Optional per-worker exec wrapper (e.g. LIFEFUZZ_WORKER_WRAP="setarch x86_64 -R"
    # to pin ADDR_NO_RANDOMIZE for each TSan worker -- the personality does not
    # survive the ThreadPoolExecutor -> subprocess hop, so wrap every worker).
    wrap = os.environ.get("LIFEFUZZ_WORKER_WRAP", "").split()
    argv = wrap + [py, os.path.abspath(__file__), "worker", str(seed),
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
