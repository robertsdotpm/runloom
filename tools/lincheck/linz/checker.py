"""A pure-Python Wing-&-Gong linearizability checker.

Faithful port of the algorithm behind Porcupine (github.com/anishathalye/porcupine,
MIT) -- itself the mechanization of Wing & Gong's 1993 checker and Lowe's 2017
refinement.  We keep it in Python (not Go) so a linearizability spec is a tiny
Python object, the checker composes with the DST seed machinery, and the whole
battery is self-contained.  The existing Go binary (tools/lincheck/porcupine)
stays as an INDEPENDENT second checker so any Chan history can be cross-validated
(two unrelated implementations agreeing is far stronger than one).

The question the checker answers: given a concurrent HISTORY -- each operation an
interval [call, ret] on one process, carrying its input and its observed output --
does there exist a single sequential ordering of the operations, consistent with
the real-time precedence (if A returns before B is called, A must come first),
that satisfies the sequential SPECIFICATION?  If yes the object linearized on that
run; if no, the run is a genuine correctness bug in the object.

Algorithm (Lowe/Porcupine `checkSingle`): the history is a doubly-linked list of
entries sorted by time -- each operation contributes a CALL entry (holding a
pointer `match` to its RETURN entry) and a RETURN entry (`match is None`).  We try
to linearize the earliest still-present call: apply `model.step`, and if the spec
accepts, LIFT that call+return pair out of the list and recurse from the head.
When we reach a return entry whose call is not yet linearized we are stuck on this
branch -- backtrack (pop the last linearized op, UNLIFT it, restore state, advance
past it).  A (linearized-set, state) cache prunes revisits; a step budget bounds
the search so a pathological history reports UNKNOWN rather than hanging.

The Model protocol (see specs.py):
    init()               -> state          (state must be hashable, or override key)
    step(state, inp, out) -> (ok, newstate) (ok False rejects this linearization)
    key(state)           -> hashable        (cache key; default: state itself)
    partition(ops)       -> list[list[op]]  (independent sub-histories; default: one)
"""


LINEARIZABLE = "LINEARIZABLE"
NOT_LINEARIZABLE = "NOT_LINEARIZABLE"
UNKNOWN = "UNKNOWN"

DEFAULT_BUDGET = 2000000


class Op(object):
    """One completed operation: a [call, ret] interval on process `proc`,
    carrying the spec input and the observed output."""
    __slots__ = ("proc", "inp", "out", "call", "ret")

    def __init__(self, proc, inp, out, call, ret):
        self.proc = proc
        self.inp = inp
        self.out = out
        self.call = call
        self.ret = ret


class Entry(object):
    __slots__ = ("is_call", "value", "opid", "time", "match", "nxt", "prv")

    def __init__(self, is_call, value, opid, time):
        self.is_call = is_call
        self.value = value          # inp for a call, out for a return
        self.opid = opid
        self.time = time
        self.match = None           # call -> its return entry; return -> None
        self.nxt = None
        self.prv = None


class Result(object):
    __slots__ = ("verdict", "nops", "steps", "detail")

    def __init__(self, verdict, nops, steps, detail=""):
        self.verdict = verdict
        self.nops = nops
        self.steps = steps
        self.detail = detail

    @property
    def linearizable(self):
        return self.verdict == LINEARIZABLE

    def __repr__(self):
        return "Result({0}, nops={1}, steps={2}{3})".format(
            self.verdict, self.nops, self.steps,
            ", " + self.detail if self.detail else "")


def build_entry_list(ops):
    """Build the sentinel-bounded doubly-linked entry list, sorted by time.

    Two sentinels (head, tail) mean every real entry always has a non-None
    neighbour on each side, so lift/unlift never special-case the ends.  Each
    op gets a compact 0..n-1 id (`opid`) so the linearized set fits one integer
    bitset.  Ties in `time` sort a call before a return and lower opid first; a
    return always sorts after its own call because ret > call by construction.
    """
    entries = []
    for i, op in enumerate(ops):
        c = Entry(True, op.inp, i, op.call)
        r = Entry(False, op.out, i, op.ret)
        c.match = r
        entries.append(c)
        entries.append(r)
    entries.sort(key=lambda e: (e.time, 0 if e.is_call else 1, e.opid))

    head = Entry(True, None, -1, None)
    tail = Entry(False, None, -1, None)
    prev = head
    for e in entries:
        prev.nxt = e
        e.prv = prev
        prev = e
    prev.nxt = tail
    tail.prv = prev
    return head, tail


def lift(call):
    """Remove a call entry and its matching return from the list."""
    call.prv.nxt = call.nxt
    call.nxt.prv = call.prv
    ret = call.match
    ret.prv.nxt = ret.nxt
    ret.nxt.prv = ret.prv


def unlift(call):
    """Re-insert a previously lifted (call, return) pair (exact inverse of lift).

    Undo in the reverse order lift removed them: the return first, then the call.
    Each removed node still points at the neighbours it had when lifted, so the
    re-link restores the list even when call and return were adjacent."""
    ret = call.match
    ret.prv.nxt = ret
    ret.nxt.prv = ret
    call.prv.nxt = call
    call.nxt.prv = call


def check(model, ops, budget=DEFAULT_BUDGET):
    """Check `ops` against `model`.  Returns a Result.

    Requires a COMPLETE history: every op has both a call and a return (the
    recorder guarantees this by draining every goroutine before checking).  A
    pending op would need speculative-return handling we deliberately omit."""
    parts = model.partition(list(ops))
    total = sum(len(p) for p in parts)
    steps_used = 0
    for part in parts:
        res = check_single(model, part, budget - steps_used)
        steps_used += res.steps
        if res.verdict != LINEARIZABLE:
            return Result(res.verdict, total, steps_used, res.detail)
    return Result(LINEARIZABLE, total, steps_used)


def check_single(model, ops, budget):
    """WGL search over one independent partition."""
    n = len(ops)
    if n == 0:
        return Result(LINEARIZABLE, 0, 0)

    head, tail = build_entry_list(ops)

    linearized = 0
    cache = set()
    calls = []                       # stack of (call_entry, saved_state)
    state = model.init()
    steps = 0

    entry = head.nxt
    while head.nxt is not tail:
        if steps >= budget:
            return Result(UNKNOWN, n, steps, "step budget exhausted")
        if entry is tail:
            # Ran off the end without emptying the list: nothing more to try at
            # this depth -> backtrack (or fail if we cannot).
            if not calls:
                return Result(NOT_LINEARIZABLE, n, steps)
            entry, state = calls.pop()
            linearized &= ~(1 << entry.opid)
            unlift(entry)
            entry = entry.nxt
            continue
        if entry.is_call:
            ret = entry.match
            ok, newstate = model.step(state, entry.value, ret.value)
            steps += 1
            if ok:
                newlin = linearized | (1 << entry.opid)
                ckey = (newlin, model.key(newstate))
                if ckey not in cache:
                    cache.add(ckey)
                    calls.append((entry, state))
                    state = newstate
                    linearized = newlin
                    lift(entry)
                    entry = head.nxt
                    continue
            entry = entry.nxt
        else:
            # A return whose call is still present and not yet linearized: this
            # branch is stuck -- backtrack.
            if not calls:
                return Result(NOT_LINEARIZABLE, n, steps)
            entry, state = calls.pop()
            linearized &= ~(1 << entry.opid)
            unlift(entry)
            entry = entry.nxt
    return Result(LINEARIZABLE, n, steps)


# ------------------------------------------------------------------ history I/O

def ops_from_events(events, spec):
    """Convert a recorded history (list of event dicts) into Op objects, using
    `spec.decode_event` to map each event to (inp, out).  This is the bridge
    between the recorder's JSON and the checker."""
    ops = []
    for ev in events:
        inp, out = spec.decode_event(ev)
        ops.append(Op(ev["proc"], inp, out, ev["call"], ev["ret"]))
    return ops
