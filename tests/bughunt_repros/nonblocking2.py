import socket, sys, time

def nb_recv(tag):
    a, b = socket.socketpair()
    b.setblocking(False)
    try:
        data = b.recv(10)
        print(tag, "recv returned", data, "(unexpected)", flush=True)
    except BlockingIOError:
        print(tag, "raised BlockingIOError (correct)", flush=True)
    a.close(); b.close()

def timeout0(tag):
    a, b = socket.socketpair()
    b.settimeout(5.0)
    a.sendall(b"x"); b.recv(1)
    b.settimeout(0)
    t0 = time.monotonic()
    try:
        b.recv(10)
        print(tag, "recv returned (unexpected)", flush=True)
    except BlockingIOError:
        print(tag, "BlockingIOError after %.2fs (correct)" % (time.monotonic()-t0), flush=True)
    except socket.timeout:
        print(tag, "socket.timeout after %.2fs (WRONG: stale 5s timeout applied)" % (time.monotonic()-t0), flush=True)
    a.close(); b.close()

which = sys.argv[2]
if sys.argv[1] == "stock":
    (nb_recv if which == "nb" else timeout0)("stock %s:" % which)
else:
    import runloom
    def main():
        runloom.fiber(lambda: (nb_recv if which == "nb" else timeout0)("patched %s:" % which))
    runloom.monkey.patch()
    runloom.run(2, main)
