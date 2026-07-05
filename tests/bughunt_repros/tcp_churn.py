"""TCP echo churn under M:N: many concurrent connections, checksummed data."""
import sys, hashlib
import runloom
runloom.monkey.patch()
import socket

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
NCONN = 50
ROUNDS = 50

def main():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(128)
    port = srv.getsockname()[1]
    done = runloom.Chan(16)

    def handler(c):
        try:
            while True:
                d = c.recv(4096)
                if not d:
                    break
                c.sendall(d)
        finally:
            c.close()

    def acceptor():
        for _ in range(NCONN):
            c, _ = srv.accept()
            runloom.fiber(handler, c)
        srv.close()

    def client(i):
        s = socket.create_connection(("127.0.0.1", port))
        ok = True
        for r in range(ROUNDS):
            msg = (b"%d:%d:" % (i, r)) * 30
            s.sendall(msg)
            buf = b""
            while len(buf) < len(msg):
                d = s.recv(4096)
                if not d:
                    ok = False
                    break
                buf += d
            if buf != msg:
                ok = False
                break
        s.close()
        done.send(1 if ok else 0)

    runloom.fiber(acceptor)
    for i in range(NCONN):
        runloom.fiber(client, i)

    def collect():
        good = 0
        for _ in range(NCONN):
            v, _ = done.recv()
            good += v
        assert good == NCONN, "only %d/%d clients verified" % (good, NCONN)
        print("tcp churn hubs=%d conns=%d rounds=%d OK" % (HUBS, NCONN, ROUNDS))
    runloom.fiber(collect)

runloom.run(HUBS, main)

# fast spawn entries
def main2():
    ran = bytearray(1000)
    def w(i):
        ran[i] = 1
    for i in range(1000):
        runloom.fiber_fast(lambda i=i: w(i))
    def check():
        runloom.sleep(0.3)
        assert sum(ran) == 1000, sum(ran)
        print("fiber_fast OK")
    runloom.fiber(check)
runloom.run(HUBS, main2)
