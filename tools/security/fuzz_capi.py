"""S8 -- adversarial fuzzing of the runloom_c PUBLIC C-API boundary.

Threat model: a buggy or hostile *Python caller*.  Every prior security check
targets the scheduler internals or the network transport; nothing systematically
hammers the argument-handling of the public C functions with hostile inputs.  A
C extension that mis-validates an argument (a huge/negative fd, a wrong type, a
re-entrant callback, a value that overflows a size_t) is the classic path from
"buggy Python" to an out-of-bounds / use-after-free in the C -- an exploit
primitive.

Contract under test: EVERY hostile argument must raise a clean Python exception
(TypeError / ValueError / OSError / OverflowError / RuntimeError / SystemError)
or be handled -- NEVER segfault, abort, or corrupt memory.  This script runs the
whole sweep in-process and is meant to be launched as a SUBPROCESS (see __main__
/ run_all.sh): if any call crashes the interpreter, the process dies on a signal
and the launcher reports the last-attempted (target, args) -- which is logged to
stderr and flushed before every call.  Run it under ASan to also catch a
non-crashing OOB read/write in the argument handling.

    PYTHON_GIL=0 PYTHONPATH=src python tools/security/fuzz_capi.py --iters 4000
"""
import argparse
import os
import random
import sys

sys.path.insert(0, "src")
import runloom_c  # noqa: E402


# ---- hostile value pool -----------------------------------------------------

def _reentrant_spawn():
    # a callback that re-enters the API from inside a goroutine
    try:
        runloom_c.go(lambda: None)
    except Exception:  # noqa: BLE001
        pass


def _raiser():
    raise ValueError("hostile callback")


class _WeirdInt:
    def __index__(self):
        return 1 << 70        # __index__ that overflows a C integer

    def __int__(self):
        return -(1 << 70)


class _Boom:
    def __index__(self):
        raise RuntimeError("hostile __index__")

    def __repr__(self):
        return "<Boom>"


HOSTILE = [
    None, True, False,
    0, -1, 1, 2, 3,
    2**31 - 1, 2**31, 2**32, 2**63 - 1, 2**63, 2**64, -(2**63), -(2**64),
    1 << 200, -(1 << 200),
    0.0, -1.5, float("inf"), float("nan"),
    "", "x", "1024", b"", b"\x00\xff",
    [], {}, (), set(),
    object(), _WeirdInt(), _Boom(),
    lambda: None, _raiser, _reentrant_spawn,
    -2, 1000000, 65535, 2048, 1023, 4096,
]

# C functions whose args are ints/fds/sizes -- the OOB-prone surface.  We sweep
# each callable with 0..3 hostile positional args.
SWEEP_NAMES = [
    "wait_fd", "cancel_wait_fd", "netpoll_unregister", "netpoll_release_if_idle",
    "set_max_goroutines", "get_max_goroutines", "goroutine_stack", "mn_hub_states",
    "mn_init", "mn_go", "go", "go_noyield", "run_ready", "dump_goroutines",
    "goroutines", "live_goroutines", "goroutine_count", "cancel_all_parked",
    "netpoll_backend", "prewarm",
]

# Exceptions that mean "rejected cleanly" (GOOD).  A crash never reaches here.
OK_EXC = (TypeError, ValueError, OSError, OverflowError, RuntimeError,
          SystemError, MemoryError, AttributeError, KeyError, IndexError,
          NotImplementedError, BufferError, ReferenceError, StopIteration)


def _log(target, args):
    # flushed BEFORE the call so a crash leaves the culprit on stderr
    sys.stderr.write("CAPI_TRY %s%r\n" % (target, args))
    sys.stderr.flush()


def sweep_once(rng):
    name = rng.choice(SWEEP_NAMES)
    fn = getattr(runloom_c, name, None)
    if fn is None or not callable(fn):
        return
    arity = rng.randint(0, 3)
    args = tuple(rng.choice(HOSTILE) for _ in range(arity))
    _log(name, args)
    try:
        fn(*args)
    except OK_EXC:
        pass
    except BaseException as e:  # noqa: BLE001
        # An unexpected exception type is not a crash, but note it.
        sys.stderr.write("CAPI_UNEXPECTED %s%r -> %r\n" % (name, args, e))


def targeted(rng):
    """Hand-built nasty sequences the random sweep is unlikely to hit."""
    # huge max-goroutines then a spawn (size handling)
    for v in (-1, 0, 2**63, _WeirdInt()):
        _log("set_max_goroutines", (v,))
        try:
            runloom_c.set_max_goroutines(v)
        except OK_EXC:
            pass
    try:
        runloom_c.set_max_goroutines(100000)
    except OK_EXC:
        pass

    # non-callables + misbehaving callables to go(), then drive run()
    for bad in (None, 5, "f", object(), _Boom()):
        _log("go", (bad,))
        try:
            runloom_c.go(bad)
        except OK_EXC:
            pass
    _log("go+run", ("raiser/reentrant",))
    try:
        runloom_c.go(_raiser)
        runloom_c.go(_reentrant_spawn)
        runloom_c.run()
    except OK_EXC:
        pass

    # wait_fd matrix from OUTSIDE a goroutine (must reject, not park/crash)
    for fd in (-1, 0, 3, 2047, 2048, 1 << 30, 1 << 62):
        for ev in (-1, 0, 1, 2, 3, 99, 1 << 40):
            _log("wait_fd", (fd, ev, 0))
            try:
                runloom_c.wait_fd(fd, ev, 0)
            except OK_EXC:
                pass

    # introspection with hostile indices
    for v in (-1, 0, 1 << 62, _WeirdInt(), None, "x"):
        for nm in ("goroutine_stack", "mn_hub_states"):
            fn = getattr(runloom_c, nm, None)
            if not callable(fn):
                continue
            _log(nm, (v,))
            try:
                fn(v)
            except OK_EXC:
                pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--iters", type=int, default=4000)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()
    seed = a.seed if a.seed is not None else int.from_bytes(os.urandom(4), "little")
    rng = random.Random(seed)
    sys.stderr.write("fuzz_capi seed=%d iters=%d\n" % (seed, a.iters))

    targeted(rng)
    for _ in range(a.iters):
        sweep_once(rng)

    print("CAPI_SURVIVED iters=%d seed=%d" % (a.iters, seed))


if __name__ == "__main__":
    main()
