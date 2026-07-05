"""runloom.stats() -- the R0 process-wide gauge surface.

`runloom_c.stats()` (the C half) reports every internal live population as a
lock-free counter: g structs, live/pending fibers, coro stacks, netpoll
parkers + fd arms, offload/io_uring inflight, etc.  This module MERGES that
dict with the PYTHON-side populations the C layer cannot see -- the aio task
registry, the monkey DNS cache and socket-timeout side table, the offload
backend backlog -- so one `runloom.stats()` call surfaces every place a leak
can accumulate.

Design (docs/dev/RELIABILITY_PROGRAM.md R0): a soak test samples this dict
every 30 s and fails on a metric whose least-squares slope is not ~0.  So the
values here are LIVE POPULATIONS (counts that should return to a baseline),
plus a couple of cumulative odometers (``mn_completed_total``,
``stale_arm_heals``) that only ever rise and are used to confirm the workload
is actually doing work / to spot app-level socket leaks.

Every Python read below is a plain ``len()`` / ``qsize()`` / int-attr, which
is atomic on the free-threaded build (PEP 703) and takes no scheduler lock --
there is no deadlock surface (unlike the C side, this never runs on a pump
thread's critical path).  Reads are defensive: a gauge that raises (an import
that is not present, a dict mutated mid-read) is simply omitted rather than
failing the whole call.

Python-side keys are prefixed ``py_`` so they never collide with a C key and a
reader can tell at a glance which layer a leak lives in.
"""

import runloom_c as _core


def _py_gauges():
    """Collect the Python-layer live-population gauges as a flat dict.

    Each gauge is wrapped so one unavailable/racy source never sinks the rest:
    a KeyError/AttributeError/ImportError/RuntimeError on a single read drops
    just that key.  (RuntimeError covers 'dict changed size during iteration'
    on 3.13t for any future values()-walking gauge.)"""
    g = {}

    def _add(key, fn):
        try:
            v = fn()
            if v is not None:
                g[key] = int(v)
        except Exception:
            pass  # a missing/racy source omits its key, never fails stats()

    # ---- aio task + loop registries (aio/handles.py) --------------------
    # WeakSets: count RunloomTasks / RunloomEventLoops not yet garbage
    # collected.  A genuinely leaked (still strongly-referenced) task/loop
    # stays counted -- the leak signal.  Compare readings AFTER a gc.collect()
    # in the harness to separate a real leak from GC lag.
    def _aio_handles():
        from runloom.aio import handles as h
        return h
    _add("py_aio_tasks_live", lambda: len(_aio_handles()._PG_ALL_TASKS))
    _add("py_aio_open_loops", lambda: len(_aio_handles()._PG_OPEN_LOOPS))

    # _CURRENT_TASKS: per-loop currently-running task map.  Only meaningful
    # when the bridge OWNS this dict (older Python); on 3.14+ the authoritative
    # store is a C-internal per-loop map and this dict is stale -- gate on it.
    def _aio_current_tasks():
        from runloom.aio import _base as b
        if getattr(b, "_SWAP_CURRENT_TASK", None) is not None:
            return None  # 3.14+: dict is not the source of truth -> omit
        return len(b._CURRENT_TASKS)
    _add("py_aio_current_tasks", _aio_current_tasks)

    # ---- monkey DNS cache (monkey/dns.py) ------------------------------
    # PRIMARY Python leak gauge: there is no active eviction (expiry is checked
    # on read, entries are overwritten on refresh but never deleted), so the
    # dict grows with the number of DISTINCT (host, qtype) pairs ever resolved.
    def _dns_entries():
        from runloom.monkey import dns as d
        return len(d._dns_result_cache)
    _add("py_dns_cache_entries", _dns_entries)

    # ---- monkey per-fd cooperative-timeout side table (monkey/_base.py) --
    # A socket closed via a path that bypasses the patched close, or whose
    # timeout was never cleared, leaves a stale entry -> rising count is a
    # socket/fd leak.  (Keyed by fileno(), so fd-number reuse overwrites --
    # it undercounts leaks that recycle fd numbers.)
    def _mk_base():
        from runloom.monkey import _base as b
        return b
    _add("py_sock_timeouts", lambda: len(_mk_base()._SOCK_TIMEOUTS))

    # ---- monkey blocking-offload backend (monkey/_base.py) -------------
    # offload backlog = queued-but-unstarted jobs across worker shards; a
    # monotonic rise = workers can't keep up / a job is stranded.  workers =
    # fixed pool size, emitted only as the backlog denominator.  parker_free =
    # capped (<=64) idle self-pipe pool -- diagnostic, not a leak gauge.
    def _offload_queued():
        b = _mk_base()._backend
        if b is None:
            return 0
        return sum(q.qsize() for q in b._qs)
    _add("py_offload_queued", _offload_queued)

    def _offload_workers():
        b = _mk_base()._backend
        return 0 if b is None else b.size
    _add("py_offload_workers", _offload_workers)

    def _parker_free():
        return len(_mk_base()._Parker._pool)
    _add("py_parker_free", _parker_free)

    return g


def stats():
    """Return the merged process-wide gauge dict: every C internal population
    (runloom_c.stats()) plus the Python-layer populations (``py_*`` keys).

    The one call a soak sampler / a status endpoint / a human debugging a
    "why is RSS climbing" question needs.  Cheap enough to poll at seconds
    cadence; takes no scheduler lock.  See the module docstring for the
    leak-signal semantics of each key group."""
    d = dict(_core.stats())
    d.update(_py_gauges())
    return d
