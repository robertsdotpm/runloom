import socket, time, itertools
import runloom

N_LOOKUPS = 20

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
    baseline = max(gaps); gaps.clear()

    # 1) connect() with hostname: resolution inline in C connect_ex on the hub
    t0 = time.monotonic()
    for i in range(N_LOOKUPS):
        c = socket.socket()
        try:
            c.connect(("nx-%d-stall.example.com" % i, 80))
        except OSError:
            pass
        c.close()
    dt_connect = time.monotonic() - t0
    gap_connect = max(gaps); gaps.clear()

    # 2) patched cooperative getaddrinfo for comparison
    t0 = time.monotonic()
    for i in range(N_LOOKUPS):
        try:
            socket.getaddrinfo("nx-%d-coop.example.com" % i, 80)
        except OSError:
            pass
    dt_gai = time.monotonic() - t0
    gap_gai = max(gaps); gaps.clear()

    stop.append(1)
    print("baseline max ticker gap        : %6.1f ms" % (baseline*1000))
    print("connect(hostname) total %5.2fs, max ticker gap: %6.1f ms" % (dt_connect, gap_connect*1000))
    print("patched getaddrinfo total %5.2fs, max ticker gap: %6.1f ms" % (dt_gai, gap_gai*1000))

runloom.monkey.patch()
runloom.run(1, main)
