"""M:N + netpoll smoke test.

Server goroutine accepts on a loopback socket; spawns one handler
goroutine per accepted connection.  Client goroutines (round-robined
across hubs) connect, send a payload, read the echo, close.

Verifies that:
  - Multiple hubs can call socket.recv / sendall concurrently via
    the monkey-patched netpoll layer.
  - I/O-woken goroutines route back to their originating hub's
    submission list (and from there to its local FIFO via the
    g->snap.valid dispatch in hub_main).
  - No segfault from the previously-unlocked shared parked list,
    and no permanent stuck gs from pygo_sched_wake routing to the
    global sched instead of the hub.

Run with the free-threaded interpreter:
  ~/.pyenv/versions/3.13.13t/bin/python3.13t examples/bench_mn_netpoll.py
"""
import socket
import sys
import threading
import time

sys.path.insert(0, "src")
import pygo, pygo.monkey
import pygo_core

pygo.monkey.patch()

N_CLIENTS = 200
N_HUBS = 4
PAYLOAD = b"ping-from-pygo-hub" * 8

stats = {"served": 0, "echoed": 0}
lock = threading.Lock()


def handler(conn):
    try:
        data = conn.recv(4096)
        if not data:
            return
        conn.sendall(data)
        with lock:
            stats["served"] += 1
    finally:
        conn.close()


def server(listen_sock, ready_event, target):
    ready_event.set()
    for _ in range(target):
        try:
            conn, _ = listen_sock.accept()
        except OSError:
            return
        pygo_core.mn_go(lambda c=conn: handler(c))


def client(port):
    s = socket.socket()
    s.connect(("127.0.0.1", port))
    s.sendall(PAYLOAD)
    got = b""
    while len(got) < len(PAYLOAD):
        chunk = s.recv(len(PAYLOAD) - len(got))
        if not chunk:
            break
        got += chunk
    s.close()
    if got == PAYLOAD:
        with lock:
            stats["echoed"] += 1


def main():
    print("M:N + netpoll smoke (3.13t)")
    print()

    listen_sock = socket.socket()
    listen_sock.bind(("127.0.0.1", 0))
    listen_sock.listen(128)
    port = listen_sock.getsockname()[1]

    pygo_core.mn_init(N_HUBS)

    ready_event = threading.Event()
    pygo_core.mn_go(lambda: server(listen_sock, ready_event, N_CLIENTS))
    while not ready_event.is_set():
        time.sleep(0.001)

    t0 = time.perf_counter()
    for _ in range(N_CLIENTS):
        pygo_core.mn_go(lambda p=port: client(p))
    pygo_core.mn_run()
    dt = time.perf_counter() - t0

    listen_sock.close()
    pygo_core.mn_fini()

    print(f"  {N_HUBS} hubs, {N_CLIENTS} clients")
    print(f"  wall:    {dt*1000:.1f} ms")
    print(f"  served:  {stats['served']} / {N_CLIENTS}")
    print(f"  echoed:  {stats['echoed']} / {N_CLIENTS}")
    status = "OK" if stats["echoed"] == N_CLIENTS else "FAIL"
    print(f"  status:  {status}")


if __name__ == "__main__":
    main()
