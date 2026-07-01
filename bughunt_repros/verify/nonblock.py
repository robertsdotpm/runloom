import socket, sys
import runloom

def scenario(tag):
    a, b = socket.socketpair()
    b.setblocking(False)
    try:
        print(tag, "recv ->", b.recv(10), flush=True)
    except BlockingIOError:
        print(tag, "BlockingIOError (correct)", flush=True)

if sys.argv[1] == "stock": scenario("stock:")
else:
    def main(): runloom.fiber(lambda: scenario("patched:"))
    runloom.monkey.patch(); runloom.run(2, main)
