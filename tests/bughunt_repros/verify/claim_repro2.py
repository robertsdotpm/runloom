import socket
import runloom

def main():
    srv = socket.socket(); srv.bind(("localhost", 0)); srv.listen(1)
    port = srv.getsockname()[1]
    def boom(*a, **k): raise RuntimeError("patched getaddrinfo called")
    socket.getaddrinfo = boom
    def f():
        c = socket.socket()
        c.connect(("localhost", port))   # hostname
        print("connect succeeded WITHOUT patched getaddrinfo -> inline libc DNS on hub", flush=True)
    runloom.fiber(f)
runloom.monkey.patch(); runloom.run(2, main)
