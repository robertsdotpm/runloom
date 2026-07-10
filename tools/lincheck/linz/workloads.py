"""Random concurrent op-sequence generators, one per primitive.

Each `build_*` returns (thunks, meta): `thunks` is a list of (gid, callable) to
spawn as M:N fibers; each callable drives one process's op sequence, recording
every op through rec.timed(...).  Sequences are constructed so the run always
COMPLETES (no permanent block / deadlock) -- every lock is released, every acquire
is balanced by a release, a channel is drained and closed -- so mn_run terminates
and the history has a return for every call (the checker needs a complete history).
A genuine primitive bug that DOES wedge the run surfaces as a subprocess timeout in
the battery driver, which is itself a finding.

All randomness derives from the integer seed (random.Random(seed*..+gid)); under
--seeded that plus the baton makes the whole recorded history a function of seed.
Values are globally unique small ints so a recv can be matched to its send.
"""
import random

from runloom import sync as rsync
import runloom_c


def rng_for(seed, gid):
    return random.Random(seed * 100003 + gid)


# ---- classifiers (fn result -> (res_tag, ret_values)) ----------------------

def cls_void(_r):
    return "ok", []


def cls_recv(r):
    v, ok = r
    return ("ok", [v]) if ok else ("closed", [])


def cls_bool_isset(r):
    return "ok", [1 if r else 0]


# ---------------------------------------------------------------- chan

def build_chan(seed, hubs, procs, ops, cap, rec):
    nprod = procs or 3
    nper = ops or 6
    cap = 2 if cap is None else cap
    ch = runloom_c.Chan(cap)
    done = runloom_c.Chan(nprod)          # buffered barrier; producers never block on it
    nconsumers = nprod
    ngor = nprod + nconsumers + 1
    for g in range(ngor):
        rec.register(g)

    def producer(gid, base):
        for i in range(nper):
            v = base * 1000 + i + 1       # globally unique, nonzero
            rec.timed(gid, "send", [v], lambda vv=v: ch.send(vv), cls_void)
        done.send(1)

    def consumer(gid):
        while True:
            box = []
            rec.timed(gid, "recv", [], lambda: ch.recv(), cls_recv)
            ev = rec.logs[gid][-1]
            if ev["res"] == "closed":
                break

    def closer(gid):
        for _ in range(nprod):
            done.recv()
        rec.timed(gid, "close", [], lambda: ch.close(), cls_void)

    thunks = []
    for p in range(nprod):
        thunks.append((p, (lambda gid, base: (lambda: producer(gid, base)))(p, p)))
    for c in range(nconsumers):
        gid = nprod + c
        thunks.append((gid, (lambda g: (lambda: consumer(g)))(gid)))
    thunks.append((nprod + nconsumers, (lambda g: (lambda: closer(g)))(nprod + nconsumers)))
    return thunks, {"nprod": nprod, "nper": nper, "cap": cap}


# ---------------------------------------------------------------- mutex

def build_mutex(seed, hubs, procs, ops, cap, rec):
    k = procs or 4
    m = ops or 6
    lock = rsync.Lock()
    for g in range(k):
        rec.register(g)

    def worker(gid):
        for _ in range(m):
            rec.timed(gid, "lock", [], lambda: lock.acquire(), cls_void)
            runloom_c.sched_yield_classic()   # hold across a safe point -> real contention
            rec.timed(gid, "unlock", [], lambda: lock.release(), cls_void)

    thunks = [(g, (lambda gg: (lambda: worker(gg)))(g)) for g in range(k)]
    return thunks, {"procs": k, "ops_per": m}


# ---------------------------------------------------------------- rwmutex

def build_rwmutex(seed, hubs, procs, ops, cap, rec):
    k = procs or 4
    m = ops or 5
    rw = rsync.RWMutex()
    for g in range(k):
        rec.register(g)

    def worker(gid):
        rng = rng_for(seed, gid)
        for _ in range(m):
            if rng.random() < 0.6:
                rec.timed(gid, "rlock", [], lambda: rw.rlock(), cls_void)
                runloom_c.sched_yield_classic()
                rec.timed(gid, "runlock", [], lambda: rw.runlock(), cls_void)
            else:
                rec.timed(gid, "wlock", [], lambda: rw.lock(), cls_void)
                runloom_c.sched_yield_classic()
                rec.timed(gid, "wunlock", [], lambda: rw.unlock(), cls_void)

    thunks = [(g, (lambda gg: (lambda: worker(gg)))(g)) for g in range(k)]
    return thunks, {"procs": k, "ops_per": m}


# ---------------------------------------------------------------- semaphore

def build_semaphore(seed, hubs, procs, ops, cap, rec):
    k = procs or 4
    m = ops or 5
    capacity = cap or 4
    sem = rsync.Semaphore(capacity)
    for g in range(k):
        rec.register(g)

    def worker(gid):
        rng = rng_for(seed, gid)
        for _ in range(m):
            n = rng.randint(1, max(1, capacity))
            rec.timed(gid, "acquire", [n], lambda nn=n: sem.acquire(nn), cls_void)
            runloom_c.sched_yield_classic()   # hold permits across a safe point -> contention
            rec.timed(gid, "release", [n], lambda nn=n: sem.release(nn), cls_void)

    thunks = [(g, (lambda gg: (lambda: worker(gg)))(g)) for g in range(k)]
    return thunks, {"procs": k, "ops_per": m, "capacity": capacity}


# ---------------------------------------------------------------- waitgroup

def build_waitgroup(seed, hubs, procs, ops, cap, rec):
    # One controller adds K, K workers each done(), W waiters each wait().
    kw = procs or 4
    nwait = 2
    wg = rsync.WaitGroup()
    ngor = 1 + kw + nwait
    for g in range(ngor):
        rec.register(g)
    gate = runloom_c.Chan(kw)             # workers wait for the add to be visible

    def controller(gid):
        rec.timed(gid, "add", [kw], lambda: wg.add(kw), cls_void)
        for _ in range(kw):
            gate.send(1)                  # release workers only after add

    def worker(gid):
        gate.recv()
        rec.timed(gid, "add", [-1], lambda: wg.done(), cls_void)

    def waiter(gid):
        rec.timed(gid, "wait", [], lambda: wg.wait(), cls_void)

    thunks = [(0, (lambda: controller(0)))]
    for w in range(kw):
        gid = 1 + w
        thunks.append((gid, (lambda g: (lambda: worker(g)))(gid)))
    for w in range(nwait):
        gid = 1 + kw + w
        thunks.append((gid, (lambda g: (lambda: waiter(g)))(gid)))
    return thunks, {"workers": kw, "waiters": nwait}


# ---------------------------------------------------------------- event

def build_event(seed, hubs, procs, ops, cap, rec):
    nwait = procs or 3
    npeek = 2
    ev = rsync.Event()
    ngor = nwait + npeek + 1
    for g in range(ngor):
        rec.register(g)

    def setter(gid):
        # yield a few times so waiters genuinely park first, then set.
        for _ in range(3):
            runloom_c.sched_yield_classic()
        rec.timed(gid, "set", [], lambda: ev.set(), cls_void)

    def waiter(gid):
        rec.timed(gid, "wait", [], lambda: ev.wait(), cls_void)

    def peeker(gid):
        rng = rng_for(seed, gid)
        for _ in range(rng.randint(1, 3)):
            rec.timed(gid, "is_set", [], lambda: ev.is_set(), cls_bool_isset)
            runloom_c.sched_yield_classic()

    thunks = [(0, (lambda: setter(0)))]
    for w in range(nwait):
        gid = 1 + w
        thunks.append((gid, (lambda g: (lambda: waiter(g)))(gid)))
    for p in range(npeek):
        gid = 1 + nwait + p
        thunks.append((gid, (lambda g: (lambda: peeker(g)))(gid)))
    return thunks, {"waiters": nwait, "peekers": npeek}


BUILDERS = {
    "chan": build_chan,
    "mutex": build_mutex,
    "rwmutex": build_rwmutex,
    "semaphore": build_semaphore,
    "waitgroup": build_waitgroup,
    "event": build_event,
}


def build(primitive, seed, hubs, procs, ops, cap, rec):
    if primitive not in BUILDERS:
        raise ValueError("unknown primitive %r (have: %s)" % (
            primitive, ", ".join(sorted(BUILDERS))))
    return BUILDERS[primitive](seed, hubs, procs, ops, cap, rec)
