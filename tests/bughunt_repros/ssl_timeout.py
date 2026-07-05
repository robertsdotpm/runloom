"""ssl socket with settimeout() must raise socket.timeout on a stalled peer.
Patched runloom's SSL recv loop never consults the timeout -> hangs forever."""
import socket, ssl, sys, threading, os

D = os.path.dirname(os.path.abspath(__file__))

def server(srv, hold):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(os.path.join(D, "cert.pem"), os.path.join(D, "key.pem"))
    conn, _ = srv.accept()
    tls = ctx.wrap_socket(conn, server_side=True)
    hold.wait()          # stall: never send application data
    tls.close()

def scenario(tag):
    srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
    hold = threading.Event()
    th = threading.Thread(target=server, args=(srv, hold), daemon=True)
    th.start()
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    c = socket.socket()
    c.connect(srv.getsockname())
    c.settimeout(1.0)
    tls = ctx.wrap_socket(c, server_hostname="localhost")
    import time
    t0 = time.monotonic()
    try:
        tls.recv(100)
        print(tag, "recv returned (unexpected)", flush=True)
    except socket.timeout:
        print(tag, "socket.timeout after %.2fs (correct)" % (time.monotonic()-t0), flush=True)
    except Exception as e:
        print(tag, "raised", type(e).__name__, e, flush=True)
    hold.set()
    tls.close(); srv.close()

if sys.argv[1] == "stock":
    scenario("stock:")
else:
    import runloom
    def main():
        runloom.fiber(lambda: scenario("patched-fiber:"))
    runloom.monkey.patch()
    runloom.run(2, main)
