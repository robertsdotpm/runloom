import socket, ssl, os, sys
import runloom
D = os.path.dirname(os.path.abspath(__file__))

def main():
    a, b = socket.socketpair(); state = {}
    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sctx.load_cert_chain(D+"/cert.pem", D+"/key.pem")
    cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    cctx.check_hostname = False; cctx.verify_mode = ssl.CERT_NONE
    def server():
        state["srv"] = sctx.wrap_socket(a, server_side=True)
    runloom.fiber(server)
    ctls = cctx.wrap_socket(b)
    def reader():
        try: state["out"] = ("recv", ctls.recv(100))
        except Exception as e: state["out"] = ("exc", type(e).__name__, str(e))
    runloom.fiber(reader)
    runloom.sleep(0.3)
    ctls.close()   # cross-fiber close while reader parked in SSL recv
    runloom.sleep(0.7)
    print("reader:", state.get("out", "STILL PARKED"), flush=True)

runloom.monkey.patch(); runloom.run(4, main)
print("run() returned", flush=True)
