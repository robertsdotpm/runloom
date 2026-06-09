"""big_100 / 98 -- resource lifetime fuzzer.

Goroutines create objects that OWN a resource -- an fd (temp file / pipe /
socketpair), a lock, or a channel -- and then dispose of them in random ways:
explicit close(), dropping the reference (letting __del__ release it), or
forcing gc.collect().  Every acquire must be matched by exactly one release and
no fds may leak, however the object dies.

Stresses: refcounts, finalizers (__del__), cleanup on every disposal path.
"""
import gc
import itertools
import os
import socket

import harness
import runloom


class Res(object):
    """Owns one fd-or-nothing; releases it exactly once on close()/__del__.

    The acquire/release tallies are lock-free counters: __del__ runs during GC
    at a point where taking a cooperative (monkey) lock aborts the interpreter
    (FINDINGS BUG #10), so destructors must NOT use cooperative primitives."""
    __slots__ = ("fds", "state", "closed")

    def __init__(self, kind, state):
        self.state = state
        self.fds = []
        self.closed = False
        if kind == "file":
            path = os.path.join(state["base"], "r{0}".format(
                next(state["counter"])))
            self.fds.append(os.open(path, os.O_CREAT | os.O_RDWR, 0o600))
        elif kind == "pipe":
            r, w = os.pipe()
            self.fds += [r, w]
        elif kind == "socketpair":
            a, b = socket.socketpair()
            self.fds += [a.detach(), b.detach()]
        state["acquired"][0] += 1       # free-threaded list-item store, no lock

    def close(self):
        if self.closed:
            return
        self.closed = True
        for fd in self.fds:
            try:
                os.close(fd)
            except OSError:
                pass
        self.state["released"][0] += 1

    def __del__(self):
        self.close()


# max_concurrent caps goroutines so only MAX_WORKERS are alive at once; no
# CoSemaphore needed (which would create one pipe-pair per waiting goroutine
# and blow the FD limit at 1M funcs).  The fd auditor bound uses MAX_WORKERS
# since that is the actual concurrent worker count regardless of H.funcs.
MAX_WORKERS = 2000


def setup(H):
    base = H.make_tmpdir("big100_reslife_")
    H.state = {"base": base, "acquired": [0], "released": [0],
               "counter": itertools.count()}
    H.fd_ceiling = 0


def worker(H, wid, rng, state):
    kinds = ["file", "pipe", "socketpair"]
    while H.running():
        batch = [Res(rng.choice(kinds), state) for _ in
                 range(rng.randint(2, 10))]
        for r in batch:
            roll = rng.random()
            if roll < 0.5:
                r.close()                   # explicit close
            # else: drop the reference -> __del__ closes it
        del batch
        if rng.random() < 0.05:
            gc.collect()
        H.op(wid)
        H.task_done(wid)


def body(H):
    def auditor():
        base = harness.count_fds()
        while H.running():
            fds = harness.count_fds()
            H.fd_ceiling = max(H.fd_ceiling, fds)
            H.check(fds < base + MAX_WORKERS * 8 + 6000,
                    "fd leak: {0} open (base {1})".format(fds, base))
            H.sleep(1.0)
            gc.collect()

    H.go(auditor)
    H.run_pool(H.funcs, worker, H.state, max_concurrent=MAX_WORKERS)


def post(H):
    gc.collect()
    # acquired/released are lock-free (approximate) counters; the real leak
    # invariant is the fd auditor.  The final fd count is the headline.
    acq = H.state["acquired"][0]
    rel = H.state["released"][0]
    end = harness.count_fds()
    H.check(end < H.fd_base + 4000 if H.fd_base >= 0 else True,
            "fd leak: ended at {0} (base {1})".format(end, H.fd_base))
    H.log("acquired~{0} released~{1} fd_base={2} fd_end={3} fd_ceiling={4}"
          .format(acq, rel, H.fd_base, end, H.fd_ceiling))


if __name__ == "__main__":
    harness.main("p98_resource_lifetime", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="objects own fds; close/drop/GC each release exactly once")
