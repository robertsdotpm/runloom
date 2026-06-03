"""Concurrent port scanner — fan-out over the network.

Spawns one goroutine per candidate port and connects to them all at
once.  This is where goroutines shine over threads: a thousand
in-flight connect()s cost ~thousands of cheap goroutines, not a
thousand 8 MB OS threads.  Under runloom.monkey.patch() the ordinary
blocking socket.connect parks the goroutine on netpoll instead of the
OS thread, so they really do overlap.

To keep it self-contained it opens a few listeners first, then scans a
mix of those (open) and unused (refused) ports on localhost.

Run:
    python3 examples/port_scanner.py
"""
import socket

import runloom

runloom.monkey.patch()

def probe(host, port, results):
    s = socket.socket()
    try:
        s.connect((host, port))           # parks cooperatively; refused = closed
        results.send((port, True))
    except OSError:
        results.send((port, False))
    finally:
        s.close()

def main():
    host = "127.0.0.1"

    # Open three listeners so the scan finds something open.
    listeners = []
    open_ports = []
    for _ in range(3):
        ln = socket.socket()
        ln.bind((host, 0))                # 0 -> OS picks a free port
        ln.listen(8)
        listeners.append(ln)
        open_ports.append(ln.getsockname()[1])

    # Candidates: the open ports plus some that are almost certainly closed.
    candidates = sorted(set(open_ports + [40001, 40002, 40003, 40004, 40005]))

    results = runloom.Chan(len(candidates))
    for port in candidates:
        runloom.go(probe, host, port, results)

    found = []
    for _ in range(len(candidates)):
        port, is_open = results.recv()[0]
        if is_open:
            found.append(port)

    for ln in listeners:
        ln.close()

    print("scanned {0} ports; open: {1}".format(len(candidates), sorted(found)))

if __name__ == "__main__":
    runloom.run(main)
