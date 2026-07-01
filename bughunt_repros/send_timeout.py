"""sendall()/send() on a socket with settimeout() must raise socket.timeout
when the peer never drains.  Stock CPython raises; patched runloom hangs."""
import socket, sys, time

def scenario(tag):
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0)); srv.listen(1)
    c = socket.socket()
    c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
    c.connect(srv.getsockname())
    peer, _ = srv.accept()          # never read from peer
    c.settimeout(0.5)
    big = b"x" * (4 << 20)          # 4 MB >> buffers
    t0 = time.monotonic()
    try:
        c.sendall(big)
        print(tag, "sendall COMPLETED (unexpected)")
    except socket.timeout:
        print(tag, "raised socket.timeout after %.2fs (correct)" % (time.monotonic()-t0))
    except Exception as e:
        print(tag, "raised", type(e).__name__, e)
    finally:
        c.close(); peer.close(); srv.close()

if sys.argv[1] == "stock":
    scenario("stock:")
else:
    import runloom
    def main():
        runloom.fiber(lambda: scenario("patched-fiber:"))
    runloom.monkey.patch()
    runloom.run(2, main)
