"""monkey.patch() then stdlib torture: threads+fibers mixed, socketpair ping-pong, subprocess."""
import sys
import runloom
runloom.monkey.patch()

import threading, socket, subprocess, queue, time

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
mode = sys.argv[2] if len(sys.argv) > 2 else "all"

if mode in ("all", "thread"):
    # threading.Thread + fibers sharing a queue
    q = queue.Queue()
    got = []
    def os_producer():
        for i in range(1000):
            q.put(i)
    def main():
        t = threading.Thread(target=os_producer)
        t.start()
        done = runloom.Chan(0)
        def fib_consumer():
            n = 0
            for _ in range(500):
                got.append(q.get())
                n += 1
            done.send(n)
        runloom.fiber(fib_consumer)
        runloom.fiber(fib_consumer)
        def w():
            tot = 0
            for _ in range(2):
                v, ok = done.recv()
                tot += v
            t.join()
            assert tot == 1000 and sorted(got) == list(range(1000)), (tot, len(got))
            print("thread+queue OK")
        runloom.fiber(w)
    runloom.run(HUBS, main)

if mode in ("all", "sock"):
    def main():
        a, b = socket.socketpair()
        done = runloom.Chan(0)
        ROUNDS = 2000
        def ping():
            for i in range(ROUNDS):
                a.sendall(b"%06d" % i)
                r = a.recv(6)
                assert r == b"%06d" % i, r
            a.close()
            done.send(1)
        def pong():
            n = 0
            while True:
                d = b.recv(6)
                if not d:
                    break
                while len(d) < 6:
                    d += b.recv(6 - len(d))
                b.sendall(d)
                n += 1
            b.close()
            done.send(n)
        runloom.fiber(ping)
        runloom.fiber(pong)
        def w():
            done.recv(); done.recv()
            print("socketpair ping-pong OK (%d rounds)" % ROUNDS)
        runloom.fiber(w)
    runloom.run(HUBS, main)

if mode in ("all", "subproc"):
    def main():
        done = runloom.Chan(0)
        def spawn_loop(k):
            for i in range(10):
                out = subprocess.run(["/bin/echo", "hi%d-%d" % (k, i)],
                                     capture_output=True, text=True, timeout=10)
                assert out.stdout.strip() == "hi%d-%d" % (k, i), out.stdout
            done.send(1)
        for k in range(8):
            runloom.fiber(spawn_loop, k)
        def w():
            for _ in range(8):
                done.recv()
            print("subprocess loop OK")
        runloom.fiber(w)
    runloom.run(HUBS, main)

if mode in ("all", "sleep"):
    def main():
        t0 = time.monotonic()
        done = runloom.Chan(0)
        def s():
            time.sleep(0.2)   # patched -> cooperative
            done.send(1)
        for _ in range(50):
            runloom.fiber(s)
        def w():
            for _ in range(50):
                done.recv()
            dt = time.monotonic() - t0
            assert dt < 2.0, "50 parallel sleeps took %.1fs (serialized?)" % dt
            print("patched sleep parallel OK (%.2fs)" % dt)
        runloom.fiber(w)
    runloom.run(HUBS, main)
