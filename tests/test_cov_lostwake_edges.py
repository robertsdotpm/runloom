"""Per-edge lost-wakeup micro-tests (QA-steal-V2 #9, jcstress Termination mode).

For each scheduler FSM edge that a lost wake could break -- unbuffered channel
send<->recv rendezvous, buffered-full send<->recv slot handoff, Future set<->wait,
WaitGroup done<->wait -- hammer N INDEPENDENT (waiter, signaler) pairs in a single
M:N run.  Each pair has its own primitive; every waiter MUST make progress and see
the exact handed-off value.  A lost/missed wake leaves that pair's done-slot 0 (or
mis-delivers -> -1); the assertion fails.  Running N pairs concurrently across
hubs gives the rare park<->unpark race high statistical power per run (jcstress's
"one spinner, one signal, assert it terminates", scaled), and under the
sched-randomization-under-sanitizer build the interleavings are shuffled too.

done-slots are a per-pair bytearray (one slot each) -- race-free with the GIL off.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import runloom
import runloom_c

N = int(os.environ.get("RUNLOOM_LOSTWAKE_N", "3000"))
HUBS = int(os.environ.get("RUNLOOM_LOSTWAKE_HUBS", "4"))


class TestLostWakeEdges(unittest.TestCase):
    def _assert_all_delivered(self, done, label):
        bad0 = sum(1 for d in done if d == 0)      # never woken (lost wake)
        badm = sum(1 for d in done if d == 255)    # woken but wrong value
        self.assertEqual((bad0, badm), (0, 0),
                         "{0}: {1} lost-wake (never woken) + {2} mis-deliver "
                         "out of {3} pairs".format(label, bad0, badm, N))

    def test_chan_unbuffered_rendezvous(self):
        # recv parks; send delivers directly (no buffer) -> the send<->recv
        # rendezvous wake.  A lost wake strands the receiver.
        done = bytearray(N)
        chans = [runloom_c.Chan(0) for _ in range(N)]

        def waiter(i):
            v, ok = chans[i].recv()
            done[i] = 1 if (ok and v == (i & 0x7FFFFFFF)) else 255

        def signaler(i):
            chans[i].send(i & 0x7FFFFFFF)

        def root():
            for i in range(N):
                runloom.fiber(lambda i=i: waiter(i))
                runloom.fiber(lambda i=i: signaler(i))

        runloom.run(HUBS, main_fn=root)
        self._assert_all_delivered(done, "unbuffered chan rendezvous")

    def test_chan_buffered_full_slot_handoff(self):
        # cap-1 channel pre-filled: the second send parks on full; a recv frees
        # the slot and must wake the parked sender -> the freed-slot wake edge.
        done = bytearray(N)
        chans = [runloom_c.Chan(1) for _ in range(N)]
        for ch in chans:
            ch.send(-1)                # fill the single slot (buffered, no park)

        def sender(i):
            chans[i].send(i & 0x7FFFFFFF)   # parks: buffer full
            done[i] = 1

        def freer(i):
            first, _ = chans[i].recv()      # frees a slot -> wakes the sender
            second, ok = chans[i].recv()    # drains the sender's value
            if not (first == -1 and ok and second == (i & 0x7FFFFFFF)):
                done[i] = 255

        def root():
            for i in range(N):
                runloom.fiber(lambda i=i: sender(i))
                runloom.fiber(lambda i=i: freer(i))

        runloom.run(HUBS, main_fn=root)
        self._assert_all_delivered(done, "buffered-full slot handoff")

    def test_future_set_vs_wait(self):
        # a fiber parks in Future.result(); another set_result()s -> the
        # future-completion wake edge.
        done = bytearray(N)
        futs = [runloom.Future() for _ in range(N)]

        def waiter(i):
            v = futs[i].result()
            done[i] = 1 if v == (i & 0x7FFFFFFF) else 255

        def setter(i):
            futs[i].set_result(i & 0x7FFFFFFF)

        def root():
            for i in range(N):
                runloom.fiber(lambda i=i: waiter(i))
                runloom.fiber(lambda i=i: setter(i))

        runloom.run(HUBS, main_fn=root)
        self._assert_all_delivered(done, "Future set-vs-wait")

    def test_waitgroup_done_vs_wait(self):
        # a fiber parks in WaitGroup.wait(); another done()s the last count ->
        # the waitgroup-drain wake edge.
        done = bytearray(N)
        wgs = [runloom.WaitGroup() for _ in range(N)]
        for wg in wgs:
            wg.add(1)

        def waiter(i):
            wgs[i].wait()
            done[i] = 1

        def doer(i):
            wgs[i].done()

        def root():
            for i in range(N):
                runloom.fiber(lambda i=i: waiter(i))
                runloom.fiber(lambda i=i: doer(i))

        runloom.run(HUBS, main_fn=root)
        self._assert_all_delivered(done, "WaitGroup done-vs-wait")


if __name__ == "__main__":
    unittest.main()
