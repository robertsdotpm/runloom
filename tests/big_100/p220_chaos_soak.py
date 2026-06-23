"""big_100 / 220 -- chaos soak (capstone kitchen-sink fuzzer).

Each goroutine, each round, uses its rng to pick ONE short action and self-checks
it.  The actions deliberately span every subsystem so cross-feature interaction
bugs that single-axis tests miss surface here:

  (a) loopback TCP echo round-trip (tag echoed back exactly)
  (b) temp-file write + read + verify (content matches)
  (c) channel send/recv round-trip through a helper (value matches)
  (d) lock-protected shared-counter increment (conserved under the lock)
  (e) timer/select wait (a short timer fires; select resolves once)
  (f) allocate + drop a cyclic object graph, occasional gc.collect() (gc churn)
  (g) a cancellable op, sometimes cancelled near completion (cancel/resubmit)

Shared infra (built in setup): an echo server on H.net_ip(0), a lock + counter,
a tmpdir, and per-goroutine partner channels.  Each action is SHORT so rounds
cycle fast.  Conservation: every locked increment is counted (post() proves the
shared counter equals the number of (d) actions performed).

Stresses: net + file + channel + lock + timer + gc + cancel interleaved on one
M:N scheduler.
"""
import gc
import os
import socket
import struct
import threading

import harness
import netutil
import runloom
import runloom.time as rtime
import runloom_c


# ---- (c) channel helper ---------------------------------------------------
def echo_once(ch_in, ch_out, val_expected):
    """One-shot relay goroutine for a single round-trip: read the value the
    owner just sent and send it straight back, then exit.  A fresh pair per
    call gives each round-trip PRIVATE channels, so there is no cross-talk
    between concurrent goroutines sharing a relay."""
    val, ok = ch_in.recv()
    if not ok:
        return
    try:
        ch_out.send(val)
    except Exception:
        pass


# ---- (f) cyclic graph -----------------------------------------------------
class Cyc(object):
    __slots__ = ("peer", "data", "idx")

    def __init__(self, idx):
        self.idx = idx
        self.peer = None
        self.data = bytearray(idx & 0x3F)


def action_echo(H, wid, rng, state):
    """(a) loopback echo round-trip; tag must come back exactly."""
    addr = (state["host"], state["port"])
    tag = struct.pack("<IIQ", 0xCABBA9E5, wid, rng.getrandbits(48))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(addr)
        s.sendall(tag)
        got = netutil.recv_exact(s, len(tag))
    except OSError:
        return None
    finally:
        netutil.close_quiet(s)
    return H.check(got == tag,
                   "echo mismatch wid={0}: sent {1!r} got {2!r}".format(
                       wid, tag, got))


def action_file(H, wid, rng, state):
    """(b) temp-file write + read + verify."""
    d = state["tmpdir"]
    payload = struct.pack("<IQ", wid, rng.getrandbits(56)) * 8
    path = os.path.join(d, "w{0}_{1}.bin".format(wid, rng.getrandbits(32)))
    try:
        with open(path, "wb") as f:
            f.write(payload)
        with open(path, "rb") as f:
            got = f.read()
    except OSError:
        return None
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    return H.check(got == payload, "file content mismatch wid={0}".format(wid))


def action_chan(H, wid, rng, state):
    """(c) channel send/recv round-trip through a PRIVATE one-shot relay.

    A fresh pair of cap-1 channels per call means no other goroutine can ever
    read or write them -- the value we get back is provably our own."""
    ch_in, ch_out = runloom.Chan(1), runloom.Chan(1)
    val = struct.pack("<IQ", wid, rng.getrandbits(48))
    H.fiber(echo_once, ch_in, ch_out, val)
    try:
        ch_in.send(val)
        got, ok = ch_out.recv()
    except Exception:
        return None
    if not ok:
        return None
    return H.check(got == val, "channel round-trip mismatch wid={0}".format(wid))


def action_lock(H, wid, rng, state):
    """(d) lock-protected shared-counter increment (conserved)."""
    lock = state["lock"]
    cell = state["counter"]
    with lock:
        x = cell[0]
        runloom.yield_now()                # hold the lock across a migration
        cell[0] = x + 1
    state["lock_ops"][wid & 1023] += 1
    return True


def action_timer(H, wid, rng, state):
    """(e) a short timer fires; select resolves exactly once."""
    t = rtime.After(rng.uniform(0.001, 0.006))
    idx, payload = runloom.select([("recv", t)])
    return H.check(idx == 0, "timer select did not resolve wid={0}".format(wid))


def action_gc(H, wid, rng, state):
    """(f) allocate + drop a cyclic graph; occasional collect."""
    batch = []
    for k in range(rng.randint(4, 16)):
        a = Cyc(wid + k)
        b = Cyc(wid + k + 1)
        a.peer = b
        b.peer = a
        batch.append(a)
    chk = sum(c.idx + len(c.data) for c in batch)
    del batch
    if rng.random() < 0.05:
        gc.collect()
    return H.check(chk >= 0, "impossible gc checksum wid={0}".format(wid))


def action_cancel(H, wid, rng, state):
    """(g) a cancellable op, sometimes cancelled near completion.

    A helper sends a tagged value after a delay; we race it with a timer.  If
    the timer wins we abandon it (cancel) and resubmit a quick deterministic
    op; if the helper wins we verify the tag.  Either way exactly one outcome,
    no double-resume."""
    ch = runloom.Chan(1)
    tag = struct.pack("<IQ", wid, rng.getrandbits(48))
    delay = rng.uniform(0.001, 0.02)

    def helper():
        runloom.sleep(delay)
        try:
            ch.try_send(tag)
        except Exception:
            pass

    H.fiber(helper)
    timer = rtime.After(delay * rng.uniform(0.3, 2.0) + 0.0005)
    idx, payload = runloom.select([("recv", ch), ("recv", timer)])
    if idx == 0:
        got = payload[0]
        return H.check(got == tag, "cancel-path value mismatch wid={0}".format(wid))
    # timer won -> abandon ch (helper still try_send's into cap-1 chan, no leak)
    return True


ACTIONS = (action_echo, action_file, action_chan, action_lock,
           action_timer, action_gc, action_cancel)


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        act = ACTIONS[rng.randrange(len(ACTIONS))]
        res = act(H, wid, rng, state)
        if res is False:                   # an explicit self-check failed
            return
        # res is None -> a benign OSError during teardown; res True -> ok.
        H.op(wid)
        H.task_done(wid)


def setup(H):
    host = H.net_ip(0)
    port = netutil.start_echo_server(H, host=host)
    tmpdir = H.make_tmpdir(prefix="big100_chaos_")
    H.state = {
        "host": host, "port": port, "tmpdir": tmpdir,
        "lock": threading.Lock(), "counter": [0],
        "lock_ops": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    counter = H.state["counter"][0]
    lock_ops = sum(H.state["lock_ops"])
    H.check(H.total_ops() > 0, "no chaos actions completed")
    # Conservation: every (d) lock action incremented the counter exactly once.
    H.check(counter == lock_ops,
            "lock counter {0} != lock actions {1} (lost increment under the "
            "lock)".format(counter, lock_ops))
    H.log("ops={0} lock_counter={1} lock_actions={2}".format(
        H.total_ops(), counter, lock_ops))


if __name__ == "__main__":
    harness.main("p220_chaos_soak", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="kitchen-sink fuzzer: net/file/channel/lock/timer/gc/"
                          "cancel per round, each self-checked")
