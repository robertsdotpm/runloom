import socket, ssl, os, sys
import runloom, runloom_c
D = os.path.dirname(os.path.abspath(__file__))
MODE = sys.argv[1]  # plain | tls

def main():
    a, b = socket.socketpair(); state = {}
    if MODE == "plain":
        def reader():
            try: state["out"] = ("recv", b.recv(100))
            except Exception as e: state["out"] = ("exc", type(e).__name__, str(e))
        runloom.fiber(reader)
    else:
        sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        sctx.load_cert_chain(D+"/cert.pem", D+"/key.pem")
        cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        cctx.check_hostname = False; cctx.verify_mode = ssl.CERT_NONE
        def server():
            state["srv"] = sctx.wrap_socket(a, server_side=True)  # keep alive
        runloom.fiber(server)
        ctls = cctx.wrap_socket(b)
        def reader():
            try: state["out"] = ("recv", ctls.recv(100))
            except Exception as e: state["out"] = ("exc", type(e).__name__, str(e))
        runloom.fiber(reader)
    runloom.sleep(0.3)
    print("cancelled", runloom_c.cancel_all_parked(), flush=True)
    runloom.sleep(0.5)
    print("reader:", state.get("out", "STILL PARKED"), flush=True)

runloom.monkey.patch(); runloom.run(2, main)
print("run() returned", flush=True)
