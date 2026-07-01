"""Cross-fiber close of an SSL socket blocked in recv(): the tls.py wait loops
call raw runloom_c.wait_fd and ignore the WAIT_FD_CANCELLED sentinel, so the
cancel wake from _patched_close can be consumed by a retry that re-parks just
before the fd is actually closed -> parked forever (plain sockets raise
OSError(ECANCELED) here)."""
import socket, ssl, os, sys, time
import runloom

D = os.path.dirname(os.path.abspath(__file__))
N = int(sys.argv[1]) if len(sys.argv) > 1 else 15

def main():
    def iteration(i):
        a, b = socket.socketpair()
        sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sctx.load_cert_chain(os.path.join(D, "cert.pem"), os.path.join(D, "key.pem"))
        cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        cctx.check_hostname = False; cctx.verify_mode = ssl.CERT_NONE
        result = []
        def server():
            try:
                stls = sctx.wrap_socket(a, server_side=True)
                result.append(("server-ok", stls))
            except Exception as e:
                result.append(("server-err", e))
        runloom.fiber(server)
        ctls = cctx.wrap_socket(b)
        # reader fiber blocks in TLS recv
        state = {"done": False}
        def reader():
            try:
                d = ctls.recv(100)
                state["out"] = ("recv", d)
            except Exception as e:
                state["out"] = ("exc", type(e).__name__, str(e))
            state["done"] = True
        runloom.fiber(reader)
        runloom.sleep(0.15)
        ctls.close()                     # cross-fiber close
        t0 = time.monotonic()
        while not state["done"] and time.monotonic() - t0 < 3.0:
            runloom.sleep(0.01)
        if state["done"]:
            print("iter %d: reader unwound: %s" % (i, state.get("out")), flush=True)
        else:
            print("iter %d: READER STILL PARKED 3s AFTER close() -> HANG" % i, flush=True)
        while not result:
            runloom.sleep(0.01)
        tag = result[0]
        if tag[0] == "server-ok":
            try: tag[1].close()
            except Exception: pass
        return state["done"]

    hangs = 0
    for i in range(N):
        if not iteration(i):
            hangs += 1
    print("hangs: %d/%d" % (hangs, N), flush=True)

runloom.monkey.patch()
runloom.run(4, main)
print("EXITED CLEANLY", flush=True)
