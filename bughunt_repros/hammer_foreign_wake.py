"""Hammer: 4 foreign OS threads spam try_send/try_recv on channels whose other
side is parked fibers on M:N hubs.  Hunts crashes in the foreign-thread wake
path (wake_waiter -> mn_wake_g / sched_wake from a non-hub thread)."""
import threading
import time
import runloom
import runloom_c as rc

ch_in = rc.Chan(0)     # foreign -> fibers (unbuffered: direct handoff + wake)
ch_out = rc.Chan(0)    # fibers -> foreign (fiber parks as sender; foreign try_recv pops+wakes)
N = 4000
stop = threading.Event()
recv_count = [0]
foreign_got = []
mu = threading.Lock()

def foreign_producer():
    i = 0
    while not stop.is_set():
        if ch_in.try_send(i):
            i += 1
        # no sleep: hammer

def foreign_consumer():
    while not stop.is_set():
        r = ch_out.try_recv()
        if r is not None:
            v, ok = r
            if ok:
                with mu:
                    foreign_got.append(v)

def main():
    done = rc.Chan(8)
    def fiber_receiver():
        n = 0
        while n < N // 4:
            v, ok = ch_in.recv()      # parks; woken by foreign try_send
            n += 1
        done.send(1)
    def fiber_sender(base):
        for j in range(N // 4):
            ch_out.send(base + j)     # parks; foreign try_recv pops + wakes
        done.send(1)
    for k in range(4):
        rc.mn_fiber(fiber_receiver)
        rc.mn_fiber(lambda b=k * (N // 4): fiber_sender(b))
    for _ in range(8):
        done.recv()

threads = [threading.Thread(target=foreign_producer, daemon=True) for _ in range(2)] + \
          [threading.Thread(target=foreign_consumer, daemon=True) for _ in range(2)]
for t in threads:
    t.start()
runloom.run(8, main)
stop.set()
for t in threads:
    t.join(timeout=5)
assert sorted(foreign_got) == list(range(N)), "foreign consumer lost/dup: %d unique of %d" % (
    len(set(foreign_got)), len(foreign_got))
print("OK: foreign-thread wake hammer, %d values each way, no crash/loss" % N)
