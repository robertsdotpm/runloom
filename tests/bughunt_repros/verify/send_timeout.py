import socket, sys, time
import runloom

def scenario(tag):
    srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
    c = socket.socket()
    c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
    c.connect(srv.getsockname())
    peer, _ = srv.accept()          # never read
    c.settimeout(0.5)
    t0 = time.monotonic()
    try:
        c.sendall(b"x" * (4 << 20))
        print(tag, "sendall COMPLETED (unexpected)")
    except socket.timeout:
        print(tag, "socket.timeout after %.2fs (correct)" % (time.monotonic()-t0))

if sys.argv[1] == "stock":
    scenario("stock:")
else:
    def main(): runloom.fiber(lambda: scenario("patched:"))
    runloom.monkey.patch(); runloom.run(2, main)
