"""Sequential reference specifications for runloom's concurrent objects.

Each spec is a Model for checker.py: init/step/key/partition plus a decode_event
that maps one recorded history event to (inp, out).  These are the ENTIRE
correctness definition -- coverage is unbounded from these ~15-line objects.

A recorded event is a dict:
    {"proc": int, "op": str, "args": [int...], "res": str, "rets": [int...],
     "call": int, "ret": int}
`args` are the operation inputs, `res` a short outcome tag, `rets` the output
values.  (ChanFIFO also accepts the legacy Go-checker event shape
{"op","value","result"} so the Python checker can run on the existing
record_history.py JSON and be diffed against tools/lincheck/porcupine.)

Which primitives are here and why: the objects whose correctness IS a
linearizability property -- a queue, a lock, a counter.  Condition variables are
deliberately absent: a condvar's contract is about wakeups/signal ordering, not a
data-structure state, so it is covered behaviourally by the Go-runtime ports
(TestCondSignal/Broadcast/Generations), not here.
"""


class Model(object):
    """Base: states must be hashable, the whole history is one partition."""

    def init(self):
        raise NotImplementedError

    def step(self, state, inp, out):
        raise NotImplementedError

    def key(self, state):
        return state

    def partition(self, ops):
        return [ops]

    def decode_event(self, ev):
        raise NotImplementedError


# ---------------------------------------------------------------- Chan (FIFO)

class ChanFIFO(Model):
    """A closable FIFO queue -- the linearizability spec of a Go/runloom channel.

    Sends enqueue; recvs dequeue the front (so values arrive in send order); a
    recv on an empty *closed* channel returns closed.  Capacity is NOT modelled:
    a blocking send's linearization point is when it is accepted, so a bounded
    channel still linearizes to an unbounded FIFO (this is exactly the Go
    porcupine spec, kept identical for cross-checking).  Non-blocking try_* ops
    are NOT part of this history (their would-block outcome depends on buffer
    occupancy, a bounded-queue spec covered by the stateful machine instead)."""

    def init(self):
        return ((), False)  # (queue tuple, closed)

    def step(self, state, inp, out):
        q, closed = state
        op = inp[0]
        if op == "send":
            if closed:
                return False, state
            if out[0] != "ok":
                return False, state
            return True, (q + (inp[1],), closed)
        if op == "close":
            return out[0] == "ok", (q, True)
        if op == "recv":
            if q:
                if out[0] == "ok" and out[1] == q[0]:
                    return True, (q[1:], closed)
                return False, state
            if out[0] == "closed" and closed:
                return True, state
            return False, state
        return False, state

    def decode_event(self, ev):
        op = ev["op"]
        if "args" in ev or "rets" in ev:            # general schema
            if op == "send":
                return ("send", ev["args"][0]), ("ok",)
            if op == "close":
                return ("close",), ("ok",)
            if op == "recv":
                if ev["res"] == "ok":
                    return ("recv",), ("ok", ev["rets"][0])
                return ("recv",), ("closed", None)
        else:                                        # legacy Go-checker schema
            if op == "send":
                return ("send", ev["value"]), ("ok",)
            if op == "close":
                return ("close",), ("ok",)
            if op == "recv":
                if ev["result"] == "ok":
                    return ("recv",), ("ok", ev["value"])
                return ("recv",), ("closed", None)
        raise ValueError("ChanFIFO: unknown op %r" % (op,))


# ---------------------------------------------------------------- Mutex

class Mutex(Model):
    """A non-reentrant lock: lock free->held, unlock held->free.  A lock is only
    valid from the free state, so two lock ops cannot linearize without an
    intervening unlock -- mutual exclusion."""

    def init(self):
        return False  # held?

    def step(self, held, inp, out):
        op = inp[0]
        if op == "lock":
            if held:
                return False, held
            return True, True
        if op == "unlock":
            if not held:
                return False, held
            return True, False
        return False, held

    def decode_event(self, ev):
        return (ev["op"],), ("ok",)


# ---------------------------------------------------------------- RWMutex

class RWMutex(Model):
    """Read/write lock.  State is ('free',) | ('read', n) | ('write',).  A wlock
    is valid only from free, so it never overlaps readers or another writer; any
    number of rlocks stack.  (Writer-preference is a fairness property, not a
    linearizability one, so it is not asserted here.)"""

    def init(self):
        return ("free",)

    def step(self, st, inp, out):
        op = inp[0]
        tag = st[0]
        if op == "rlock":
            if tag == "free":
                return True, ("read", 1)
            if tag == "read":
                return True, ("read", st[1] + 1)
            return False, st        # cannot read while a writer holds
        if op == "runlock":
            if tag == "read":
                return (True, ("free",) if st[1] == 1 else ("read", st[1] - 1))
            return False, st
        if op == "wlock":
            if tag == "free":
                return True, ("write",)
            return False, st
        if op == "wunlock":
            if tag == "write":
                return True, ("free",)
            return False, st
        return False, st

    def decode_event(self, ev):
        return (ev["op"],), ("ok",)


# ---------------------------------------------------------------- Semaphore

class Semaphore(Model):
    """A weighted counting semaphore of capacity N.  acquire(k) is valid only
    when at least k permits are free (its linearization point); release(k)
    returns them.  Checks that concurrently-held weight never exceeds N."""

    def __init__(self, capacity):
        self.capacity = capacity

    def init(self):
        return self.capacity  # free permits

    def step(self, free, inp, out):
        op = inp[0]
        n = inp[1]
        if op == "acquire":
            if free >= n:
                return True, free - n
            return False, free
        if op == "release":
            return True, free + n
        return False, free

    def decode_event(self, ev):
        n = ev["args"][0] if ev.get("args") else 1
        return (ev["op"], n), ("ok",)


# ---------------------------------------------------------------- WaitGroup

class WaitGroup(Model):
    """Counter barrier.  add(d) moves the counter (never below 0); wait() is
    valid only when the counter is 0 at its linearization point -- so a wait that
    RETURNED while the counter was still positive is caught as non-linearizable."""

    def init(self):
        return 0

    def step(self, count, inp, out):
        op = inp[0]
        if op == "add":
            nc = count + inp[1]
            if nc < 0:
                return False, count
            return True, nc
        if op == "wait":
            return count == 0, count
        return False, count

    def decode_event(self, ev):
        if ev["op"] == "add":
            return ("add", ev["args"][0]), ("ok",)
        return ("wait",), ("ok",)


# ---------------------------------------------------------------- Event

class Event(Model):
    """One-way (with clear) flag.  wait() is valid only when the flag is set at
    its linearization point; is_set() must report the flag's current value."""

    def init(self):
        return False

    def step(self, flag, inp, out):
        op = inp[0]
        if op == "set":
            return True, True
        if op == "clear":
            return True, False
        if op == "wait":
            return flag, flag
        if op == "is_set":
            return out[1] == (1 if flag else 0), flag
        return False, flag

    def decode_event(self, ev):
        if ev["op"] == "is_set":
            return ("is_set",), ("ok", ev["rets"][0])
        return (ev["op"],), ("ok",)


REGISTRY = {
    "chan": lambda: ChanFIFO(),
    "mutex": lambda: Mutex(),
    "rwmutex": lambda: RWMutex(),
    "semaphore": lambda cap=4: Semaphore(cap),
    "waitgroup": lambda: WaitGroup(),
    "event": lambda: Event(),
}
