"""cancel_all_parked (teardown backstop) must unwind fibers parked in I/O.
A plain-socket recv waiter honours the CANCELLED sentinel (OSError ECANCELED)
and unwinds; a TLS recv waiter ignores it (raw runloom_c.wait_fd in tls.py)
and RE-PARKS on the still-open fd forever."""
import socket, ssl, os, sys, time
import runloom, runloom_c

D = os.path.dirname(os.path.abspath(__file__))
MODE = sys.argv[1]   # "plain" or "tls"

def main():
    a, b = socket.socketpair()
    state = {}
    if MODE == "plain":
        def reader():
            try:
                d = b.recv(100)
                state["out"] = ("recv", d)
            except Exception as e:
                state["out"] = ("exc", type(e).__name__, str(e))
        runloom.fiber(reader)
    else:
        sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sctx.load_cert_chain(os.path.join(D, "cert.pem"), os.path.join(D, "key.pem"))
        cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        cctx.check_hostname = False; cctx.verify_mode = ssl.CERT_NONE
        holder = {}
        def server():
            holder["s"] = sctx.wrap_socket(a, server_side=True)
        runloom.fiber(server)
        ctls = cctx.wrap_socket(b)
        def reader():
            try:
                d = ctls.recv(100)
                state["out"] = ("recv", d)
            except Exception as e:
                state["out"] = ("exc", type(e).__name__, str(e))
        runloom.fiber(reader)
    runloom.sleep(0.3)          # reader is parked now
    n = runloom_c.cancel_all_parked()
    print("cancelled %d parked" % n, flush=True)
    runloom.sleep(0.5)
    print("reader state after cancel_all_parked:", state.get("out", "STILL PARKED"), flush=True)

runloom.monkey.patch()
runloom.run(2, main)
print("run() returned", flush=True)
