# needs cert.pem/key.pem: openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 2 -nodes -subj /CN=localhost
import socket, ssl, sys, threading, os, time
import runloom
D = os.path.dirname(os.path.abspath(__file__))

def server(srv, hold):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(D+"/cert.pem", D+"/key.pem")
    conn, _ = srv.accept()
    tls = ctx.wrap_socket(conn, server_side=True)
    hold.wait(); tls.close()

def scenario(tag):
    srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
    hold = threading.Event()
    threading.Thread(target=server, args=(srv, hold), daemon=True).start()
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    c = socket.socket(); c.connect(srv.getsockname()); c.settimeout(1.0)
    tls = ctx.wrap_socket(c, server_hostname="localhost")
    t0 = time.monotonic()
    try:
        tls.recv(100)
    except socket.timeout:
        print(tag, "socket.timeout after %.2fs (correct)" % (time.monotonic()-t0), flush=True)
    hold.set()

if sys.argv[1] == "stock": scenario("stock:")
else:
    def main(): runloom.fiber(lambda: scenario("patched:"))
    runloom.monkey.patch(); runloom.run(2, main)
