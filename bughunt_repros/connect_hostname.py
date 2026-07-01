"""_patched_connect routes through connect_ex (C), which resolves hostnames
with BLOCKING libc getaddrinfo inline on the hub thread -- bypassing the
cooperative resolver and stalling every fiber on that hub for the DNS RTT."""
import socket, sys, time, threading
import runloom

def main():
    srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
    port = srv.getsockname()[1]
    # sabotage the (patched) getaddrinfo to prove connect() never uses it
    def boom(*a, **k):
        raise RuntimeError("patched getaddrinfo called")
    socket.getaddrinfo = boom
    def f():
        c = socket.socket()
        try:
            c.connect(("localhost", port))   # hostname, not IP
            print("connect succeeded WITHOUT patched getaddrinfo -> inline libc DNS on hub", flush=True)
        except Exception as e:
            print("connect ->", type(e).__name__, e, flush=True)
        c.close(); srv.close()
    runloom.fiber(f)

runloom.monkey.patch()
runloom.run(2, main)
