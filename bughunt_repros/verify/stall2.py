import socket, time
import runloom

N = 10

def main():
    stop = []
    gaps = []
    def ticker():
        last = time.monotonic()
        while not stop:
            runloom.sleep(0.001)
            now = time.monotonic()
            gaps.append(now - last)
            last = now
    runloom.fiber(ticker)
    runloom.sleep(0.05)
    baseline = max(gaps)

    def phase(fn, label):
        gaps.clear()
        t0 = time.monotonic()
        fn()
        dt = time.monotonic() - t0
        runloom.sleep(0.01)   # let ticker run and record the gap
        g = max(gaps) if gaps else float("nan")
        print("%-28s total %6.0f ms, max ticker gap %6.1f ms" % (label, dt*1000, g*1000), flush=True)

    def do_connect():
        for i in range(N):
            c = socket.socket()
            try: c.connect(("nx-%d-stall.example.com" % i, 80))
            except OSError: pass
            c.close()
    def do_gai():
        for i in range(N):
            try: socket.getaddrinfo("nx-%d-coop.example.com" % i, 80)
            except OSError: pass

    print("baseline max ticker gap: %.1f ms" % (baseline*1000), flush=True)
    phase(do_connect, "connect(hostname) x%d" % N)
    phase(do_gai,     "patched getaddrinfo x%d" % N)
    stop.append(1)

runloom.monkey.patch()
runloom.run(1, main)
